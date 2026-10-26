"""07 §2 "Zdrowie potoku": container status from network probes, against local servers
(no Docker). A dead or unhealthy service is a red cell, never an exception."""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from dashboard import probes

pytestmark = pytest.mark.unit

CONNECTOR = {
    "running": {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]},
    "failed": {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "FAILED"}]},
    "no-task": {"connector": {"state": "RUNNING"}, "tasks": []},
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server API
        name = self.path.strip("/")
        if name == "slow":
            time.sleep(3)
        if name in CONNECTOR or name == "ok":
            body = json.dumps(CONNECTOR.get(name, {})).encode()
            self.send_response(200)
        else:
            body = b"{}"
            self.send_response(500)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def server():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{srv.server_port}"
    srv.shutdown()


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_healthy_services_are_ok(server):
    results = probes.check_all(
        [
            probes.Probe("kafka", "tcp", server),
            probes.Probe("minio", "http", f"http://{server}/ok"),
            probes.Probe("connect", "connector", f"http://{server}/running"),
        ]
    )
    assert [(r.service, r.ok) for r in results] == [
        ("kafka", True),
        ("minio", True),
        ("connect", True),
    ]
    assert results[2].detail == "konektor RUNNING, zadania RUNNING"


def test_down_or_unhealthy_services_are_not_ok(server):
    results = probes.check_all(
        [
            probes.Probe("kafka", "tcp", f"127.0.0.1:{_closed_port()}"),
            probes.Probe("schema-registry", "http", f"http://{server}/error"),
            probes.Probe("connect", "connector", f"http://{server}/failed"),
            probes.Probe("connect", "connector", f"http://{server}/no-task"),
            probes.Probe("spark", "http", "http://no-such-host.invalid:4040/"),
        ]
    )
    assert [r.ok for r in results] == [False] * 5
    assert results[0].detail.startswith("nie odpowiada")
    assert results[2].detail == "konektor RUNNING, zadania FAILED"


def test_probes_run_in_parallel_with_a_timeout(server):
    """A hanging service costs one timeout per refresh, not one per probe."""
    slow = [probes.Probe(f"s{i}", "http", f"http://{server}/slow") for i in range(4)]
    start = time.monotonic()
    results = probes.check_all(slow, timeout=0.5)
    assert time.monotonic() - start < 2.0
    assert not any(r.ok for r in results)


def test_default_probes_cover_the_services_with_an_endpoint():
    assert [p.service for p in probes.default_probes()] == [
        "kafka",
        "schema-registry",
        "connect",
        "minio",
        "spark",
    ]


def test_container_map_colours_each_box_by_its_status():
    """The map has one box per container of the architecture diagram, coloured by the
    status of that container (red for a failed probe, grey without a probe)."""
    from dashboard import charts

    status = {s: ("good", "OK", "") for s in charts.ARCH_NODES}
    status["kafka"] = ("critical", "problem", "nie odpowiada")
    status["simulator"] = ("info", "bez sondy", "")
    fig = charts.architecture(status)
    borders = [s.line.color for s in fig.layout.shapes if s.fillcolor == "rgba(0,0,0,0)"]
    assert len(borders) == len(charts.ARCH_NODES) == 9
    colour = dict(zip(charts.ARCH_NODES, borders, strict=True))
    assert colour["kafka"] == charts.STATUS["critical"]
    assert colour["simulator"] == charts.STATUS_NEUTRAL
    assert colour["spark"] == charts.STATUS["good"]
    arrows = [a for a in fig.layout.annotations if a.showarrow]
    assert len(arrows) == len(charts.ARCH_EDGES)
