"""Kafka broker healthcheck without a JVM (08-operations.md §3).

Sends one Kafka protocol ApiVersions request (api_key 18, v0) to the INTERNAL
listener and requires a well-formed answer with error_code 0. This is the first
request every Kafka client sends, so a success means "the broker's request
handlers accept client requests", not merely "the port is open": the socket
server binds its acceptors before request processing is enabled, so a bare TCP
connect can succeed while the broker still cannot serve clients.

Replaces `kafka-topics --list`, which starts a full JVM (it inherits the broker's
KAFKA_HEAP_OPTS) and took 6 s idle and 17-28 s under Spark load, longer than the
healthcheck timeout (QA finding F2). This probe uses only the python3 (3.9)
standard library shipped in the cp-kafka image and answers in milliseconds.

Exit code 0 = healthy, 1 = unhealthy (reason on stderr).
"""

from __future__ import annotations

import os
import socket
import struct
import sys

HOST = os.environ.get("KAFKA_HC_HOST", "kafka")
PORT = int(os.environ.get("KAFKA_HC_PORT", "29092"))
TIMEOUT_S = float(os.environ.get("KAFKA_HC_TIMEOUT_S", "4"))

API_VERSIONS = 18
CORRELATION_ID = 0x5750_4442  # "WPDB"
CLIENT_ID = b"wpdb-healthcheck"


def build_request() -> bytes:
    # request header v1: api_key, api_version, correlation_id, client_id (nullable string)
    body = struct.pack(">hhih", API_VERSIONS, 0, CORRELATION_ID, len(CLIENT_ID)) + CLIENT_ID
    return struct.pack(">i", len(body)) + body


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"connection closed after {len(buf)}/{n} bytes")
        buf += chunk
    return buf


def parse_response(payload: bytes) -> list[int]:
    """Return the api keys advertised by the broker; raise on any protocol error."""
    correlation_id, error_code, n = struct.unpack_from(">ihi", payload, 0)
    if correlation_id != CORRELATION_ID:
        raise ValueError(f"correlation id {correlation_id} != {CORRELATION_ID}")
    if error_code != 0:
        raise ValueError(f"ApiVersions error_code {error_code}")
    if n <= 0:
        raise ValueError(f"broker advertised {n} APIs")
    return [struct.unpack_from(">h", payload, 10 + 6 * i)[0] for i in range(n)]


def probe(host: str = HOST, port: int = PORT, timeout: float = TIMEOUT_S) -> list[int]:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(build_request())
        (size,) = struct.unpack(">i", recv_exact(sock, 4))
        if not 10 <= size <= 1_000_000:
            raise ValueError(f"implausible response size {size}")
        return parse_response(recv_exact(sock, size))


def main() -> int:
    try:
        apis = probe()
    except Exception as exc:  # any failure = unhealthy
        print(f"kafka unhealthy: {exc!r}", file=sys.stderr)
        return 1
    # Metadata (3) and Produce (0) must be served by a broker that accepts clients
    if 0 not in apis or 3 not in apis:
        print(f"kafka unhealthy: Produce/Metadata missing in {sorted(apis)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
