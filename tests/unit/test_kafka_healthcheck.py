"""Unit tests of the JVM-free Kafka healthcheck probe (08-operations.md §3, QA F2).

A fake broker on a loopback socket answers the ApiVersions request; no Docker needed.
"""

from __future__ import annotations

import importlib.util
import socket
import struct
import threading
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[2] / "containers" / "kafka" / "healthcheck.py"
_spec = importlib.util.spec_from_file_location("kafka_healthcheck", SCRIPT)
hc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hc)


def _response(correlation_id: int, error_code: int, api_keys: list[int]) -> bytes:
    body = struct.pack(">ihi", correlation_id, error_code, len(api_keys))
    body += b"".join(struct.pack(">hhh", k, 0, 9) for k in api_keys)
    return struct.pack(">i", len(body)) + body


def _fake_broker(reply: bytes | None) -> tuple[int, list[bytes]]:
    """Serve one connection: record the request, send `reply` (None = close at once)."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    seen: list[bytes] = []

    def serve():
        conn, _ = srv.accept()
        with conn, srv:
            if reply is None:
                return
            (size,) = struct.unpack(">i", conn.recv(4))
            seen.append(conn.recv(size))
            conn.sendall(reply)

    threading.Thread(target=serve, daemon=True).start()
    return srv.getsockname()[1], seen


def test_request_is_api_versions_v0_with_header_v1():
    req = hc.build_request()
    (size,) = struct.unpack_from(">i", req, 0)
    assert size == len(req) - 4
    api_key, version, corr, client_len = struct.unpack_from(">hhih", req, 4)
    assert (api_key, version, corr) == (18, 0, hc.CORRELATION_ID)
    assert req[14:] == hc.CLIENT_ID and client_len == len(hc.CLIENT_ID)


def test_probe_accepts_a_broker_answer():
    port, seen = _fake_broker(_response(hc.CORRELATION_ID, 0, [0, 1, 3, 18]))
    assert hc.probe("127.0.0.1", port, 2) == [0, 1, 3, 18]
    assert seen == [hc.build_request()[4:]]


@pytest.mark.parametrize(
    "reply, error, message",
    [
        (_response(hc.CORRELATION_ID, 35, [18]), ValueError, "error_code 35"),
        (_response(1, 0, [0, 3, 18]), ValueError, "correlation id"),
        # closed before answering: EOF or reset, depending on the OS timing
        (None, OSError, None),
    ],
)
def test_probe_rejects_errors_and_closed_connections(reply, error, message):
    port, _ = _fake_broker(reply)
    with pytest.raises(error, match=message):
        hc.probe("127.0.0.1", port, 2)


def test_main_requires_produce_and_metadata(monkeypatch):
    """The KRaft controller listener answers ApiVersions too, but without Produce/Metadata."""
    monkeypatch.setattr(hc, "probe", lambda: [1, 17, 18, 55])
    assert hc.main() == 1
    monkeypatch.setattr(hc, "probe", lambda: [0, 1, 3, 18])
    assert hc.main() == 0


def test_main_fails_when_nothing_listens():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with pytest.raises(ConnectionRefusedError):
        hc.probe("127.0.0.1", port, 1)
