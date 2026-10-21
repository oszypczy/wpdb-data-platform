"""Static checks of docker-compose.yml and .env.example (08-operations.md §10, stage 0).

No Docker needed: the files are parsed as text / YAML.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"

# ${VAR}, ${VAR:-default}, ${VAR:?msg} ... but not $${...} (container-side)
COMPOSE_VAR = re.compile(r"(?<!\$)\$\{([A-Za-z_][A-Za-z0-9_]*)")
# a single $ not part of $$ (host-side interpolation in a container command)
SINGLE_DOLLAR = re.compile(r"(?<!\$)\$(?!\$)")

pytestmark = pytest.mark.unit


def compose_without_comments() -> str:
    """The compose text with YAML comments removed (comments may quote ${VAR} examples)."""
    out = []
    for line in COMPOSE.read_text().splitlines():
        if line.lstrip().startswith("#"):
            continue
        out.append(re.sub(r"\s+#.*$", "", line))
    return "\n".join(out)


def env_example_keys() -> set[str]:
    keys = set()
    for line in ENV_EXAMPLE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def test_env_example_covers_compose_variables():
    """08 §G: every ${VAR} used in docker-compose.yml is declared in .env.example."""
    used = set(COMPOSE_VAR.findall(compose_without_comments()))
    assert used, "compose file references no variables?"
    missing = sorted(used - env_example_keys())
    assert missing == [], f"variables used in compose but absent from .env.example: {missing}"


def _strings(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [s for s in value if isinstance(s, str)]


def test_commands_use_double_dollar():
    """08 §G: command:/entrypoint:/healthcheck use $$ for container-side variables.

    Compose resolves ${VAR} from the host .env at parse time, so a single $ in a
    container command is almost always a bug (the tutorial pitfall from CLAUDE.md).
    """
    doc = yaml.safe_load(COMPOSE.read_text())
    offenders = []
    for name, svc in doc["services"].items():
        for key in ("command", "entrypoint"):
            for s in _strings(svc.get(key)):
                if SINGLE_DOLLAR.search(s):
                    offenders.append(f"{name}.{key}: {s!r}")
        hc = svc.get("healthcheck") or {}
        for s in _strings(hc.get("test")):
            if SINGLE_DOLLAR.search(s):
                offenders.append(f"{name}.healthcheck.test: {s!r}")
    assert offenders == [], "single $ in container commands:\n" + "\n".join(offenders)


def test_every_long_running_service_has_healthcheck_and_restart_policy():
    """08 §3/§5: long-running services declare a healthcheck and restart: unless-stopped;
    init containers declare restart: "no" and depend on a healthy parent."""
    doc = yaml.safe_load(COMPOSE.read_text())
    init = {"kafka-init", "connect-init", "minio-init"}
    tools = {"consumer"}  # profile tools: run on demand, one-shot, no healthcheck
    placeholders = {"spark", "dashboard"}  # healthchecks arrive with their stages
    for name, svc in doc["services"].items():
        if name in tools:
            assert svc.get("profiles") == ["tools"], f"{name}: must live in profile tools"
            assert str(svc.get("restart")) == "no", f"{name}: tools must not restart"
            continue
        if name in init:
            assert str(svc.get("restart")) == "no", f"{name}: init containers must not restart"
            assert svc.get("depends_on"), f"{name}: init container without depends_on"
            continue
        assert svc.get("restart") == "unless-stopped", f"{name}: missing restart: unless-stopped"
        if name not in placeholders:
            assert "healthcheck" in svc, f"{name}: missing healthcheck"
    for name, svc in doc["services"].items():
        for dep, cond in (svc.get("depends_on") or {}).items():
            assert cond.get("condition") in ("service_healthy", "service_completed_successfully"), (
                f"{name} -> {dep}: depends_on needs a condition, not plain ordering"
            )


def test_no_latest_tags():
    """version-matrix skill: every image is pinned to an exact tag, never latest/untagged."""
    doc = yaml.safe_load(COMPOSE.read_text())
    for name, svc in doc["services"].items():
        image = svc.get("image", "")
        if "build" in svc or image.startswith("wpdb/"):
            continue  # built images are tagged wpdb/<name>; versions live in the Dockerfile
        assert ":" in image and not image.endswith(":latest"), f"{name}: unpinned image {image!r}"


CONTAINERS = ROOT / "containers"
DOCKERFILES = [
    CONTAINERS / "connect" / "Dockerfile",
    CONTAINERS / "consumer" / "Dockerfile",
    CONTAINERS / "minio" / "Dockerfile",
    CONTAINERS / "simulator" / "Dockerfile",
]


def test_dockerfiles_pin_base_images():
    """08 §1: every FROM in the built images resolves to an image with an exact tag.

    ARG defaults are substituted first; no latest, no untagged, no unresolved variable.
    """
    offenders = []
    for df in DOCKERFILES:
        args: dict[str, str] = {}
        for raw in df.read_text().splitlines():
            line = raw.strip()
            if m := re.match(r"ARG\s+([A-Za-z_]+)=(\S+)", line):
                args[m.group(1)] = m.group(2)
                continue
            if m := re.match(r"FROM\s+(\S+)", line):
                ref = m.group(1)
                for name, value in args.items():
                    ref = ref.replace("${" + name + "}", value)
                image = ref.split("@")[0]
                last = image.split("/")[-1]
                tag = last.split(":", 1)[1] if ":" in last else ""
                if "${" in ref or not tag or tag == "latest":
                    offenders.append(f"{df.name}: FROM {ref}")
    assert offenders == [], "unpinned base images:\n" + "\n".join(offenders)


def test_all_published_ports_bound_to_loopback():
    """08 §3: every ports: entry is published on 127.0.0.1 only (single-machine lab; Connect REST
    with config.providers=env must not be reachable from the LAN)."""
    doc = yaml.safe_load(COMPOSE.read_text())
    offenders = []
    for name, svc in doc["services"].items():
        for entry in svc.get("ports") or []:
            text = entry if isinstance(entry, str) else f"{entry.get('host_ip', '')}:{entry}"
            if not str(text).startswith("127.0.0.1:"):
                offenders.append(f"{name}: {entry!r}")
    assert offenders == [], "ports not bound to loopback:\n" + "\n".join(offenders)


def test_connect_init_waits_for_the_simulator_preload():
    """03 §2 / 08 §14: connect-init depends on simulator: service_healthy, so the initial
    snapshot covers the history before SIM_START and the stream continues without a gap."""
    doc = yaml.safe_load(COMPOSE.read_text())
    deps = doc["services"]["connect-init"]["depends_on"]
    assert deps["simulator"] == {"condition": "service_healthy"}
    assert deps["connect"] == {"condition": "service_healthy"}


def test_connect_worker_flushes_offsets_every_5_s():
    """03 §9 / 08 §5: the worker commits source offsets every 5 s (Debezium image env
    OFFSET_FLUSH_INTERVAL_MS → offset.flush.interval.ms), so a task restart re-delivers at
    most ~5 s of events instead of the image default 60 s."""
    env = yaml.safe_load(COMPOSE.read_text())["services"]["connect"]["environment"]
    assert int(env["OFFSET_FLUSH_INTERVAL_MS"]) == 5000


def test_connector_json_is_pure_json_with_env_secret_and_topic_groups():
    """03 §8, §10 / 04 §3, §7: the connector file parses as JSON (no // comments), the
    password is a ${env:…} placeholder, topics come from topic.creation groups and the
    producer overrides of 04 §7 are present."""
    import json

    cfg = json.loads((CONTAINERS / "connect" / "connectors" / "olist-postgres.json").read_text())
    assert cfg["database.password"] == "${env:DEBEZIUM_DB_PASSWORD}"
    assert not any("password" in k and v != "${env:DEBEZIUM_DB_PASSWORD}" for k, v in cfg.items())
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["topic.prefix"] == "olist" and cfg["schema.include.list"] == "shop"
    assert cfg["table.exclude.list"] == r"shop\._.*"  # `_…` tables are test probes
    assert cfg["snapshot.mode"] == "initial"
    assert cfg["decimal.handling.mode"] == "precise" and cfg["time.precision.mode"] == "connect"
    assert cfg["tombstones.on.delete"] == "true"
    assert cfg["provide.transaction.metadata"] == "true"
    assert (
        cfg["heartbeat.interval.ms"] == "10000" and "dbz.heartbeat" in cfg["heartbeat.action.query"]
    )
    groups = cfg["topic.creation.groups"].split(",")
    assert groups == ["large", "transaction", "heartbeat"]
    for g in groups:
        assert cfg[f"topic.creation.{g}.include"], g
    assert cfg["topic.creation.default.replication.factor"] == "1"
    assert cfg["producer.override.acks"] == "all"
    assert cfg["producer.override.enable.idempotence"] == "true"
    assert cfg["producer.override.compression.type"] == "zstd"
