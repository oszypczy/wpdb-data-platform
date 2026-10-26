"""Liveness of the other containers, probed over the compose network (07 §2 "Zdrowie
potoku").

Only health endpoints and TCP ports: no data is read from Kafka, Connect or Spark, and
Postgres and the simulator are never contacted (only the simulator and Debezium talk to
the database); the pipeline page infers their state from the lake instead. Standard
library only, all probes in parallel with a short timeout, so a dead service costs at
most ``TIMEOUT_S`` per refresh and never an exception.
"""

from __future__ import annotations

import json
import os
import socket
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import urlparse

TIMEOUT_S = float(os.environ.get("DASHBOARD_PROBE_TIMEOUT_S", "1.5"))


@dataclass(frozen=True)
class Probe:
    service: str
    kind: str  # "tcp" (host:port accepts), "http" (2xx), "connector" (Connect status)
    target: str


@dataclass(frozen=True)
class Result:
    service: str
    ok: bool
    detail: str


def default_probes() -> list[Probe]:
    """The long-running services with a network endpoint (service names = compose)."""
    connector = os.environ.get("CONNECTOR_NAME", "olist-postgres")
    return [
        Probe("kafka", "tcp", os.environ.get("PROBE_KAFKA", "kafka:29092")),
        Probe(
            "schema-registry",
            "http",
            os.environ.get("PROBE_SCHEMA_REGISTRY", "http://schema-registry:8081/subjects"),
        ),
        Probe(
            "connect",
            "connector",
            os.environ.get("PROBE_CONNECT", f"http://connect:8083/connectors/{connector}/status"),
        ),
        Probe(
            "minio", "http", os.environ.get("PROBE_MINIO", "http://minio:9000/minio/health/live")
        ),
        Probe(
            "spark", "http", os.environ.get("PROBE_SPARK", "http://spark:4040/api/v1/applications")
        ),
    ]


def _tcp(target: str, timeout: float) -> str:
    host, port = target.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=timeout):
        return f"port {port} otwarty"


def _http(url: str, timeout: float) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - fixed URLs
        return resp.read()


def _connector(url: str, timeout: float) -> tuple[bool, str]:
    status = json.loads(_http(url, timeout))
    states = [status["connector"]["state"]] + [t["state"] for t in status.get("tasks", [])]
    ok = len(states) > 1 and all(s == "RUNNING" for s in states)
    return ok, f"konektor {status['connector']['state']}, zadania " + ", ".join(states[1:])


def check(probe: Probe, timeout: float = TIMEOUT_S) -> Result:
    try:
        if probe.kind == "tcp":
            return Result(probe.service, True, _tcp(probe.target, timeout))
        if probe.kind == "connector":
            ok, detail = _connector(probe.target, timeout)
            return Result(probe.service, ok, detail)
        _http(probe.target, timeout)
        return Result(probe.service, True, f"{urlparse(probe.target).path} odpowiada")
    except Exception as exc:  # noqa: BLE001 - any failure means "not healthy"
        return Result(probe.service, False, f"nie odpowiada ({type(exc).__name__})")


def check_all(probes: list[Probe] | None = None, timeout: float = TIMEOUT_S) -> list[Result]:
    probes = default_probes() if probes is None else probes
    with ThreadPoolExecutor(max_workers=max(1, len(probes))) as pool:
        return list(pool.map(lambda p: check(p, timeout), probes))
