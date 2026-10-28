"""Stack-level acceptance tests of stage 0 (docs/design/08-operations.md §10).

Needs Docker. `stack` starts the default compose services and waits for healthy.
"""

from __future__ import annotations

import json
import os

import pytest

from tests.helpers.compose import ComposeStack

pytestmark = pytest.mark.integration

DEFAULT_SERVICES = {
    "postgres",
    "kafka",
    "kafka-init",
    "schema-registry",
    "connect",
    "connect-init",
    "minio",
    "minio-init",
    "simulator",  # stage 1: runs by default, healthy once its preload is done
    "spark",  # stage 3: runs by default, healthy once its Kafka -> bronze query is active
    "dashboard",  # stage 4: runs by default, healthy also with an empty gold
}
INIT_SERVICES = ("kafka-init", "minio-init", "connect-init")
HEALTHY_BUDGET_S = 180


def test_compose_config_valid(compose: ComposeStack):
    """08 §G: `docker compose config` passes by default and with --profile tools."""
    assert compose.config_ok(), "docker compose config -q failed"
    assert compose.config_ok(profiles=["tools"]), "docker compose --profile tools config -q failed"


def test_all_services_healthy(stack: ComposeStack):
    """08 §G: every long-running service is healthy and every init container exited 0."""
    rows = {r.service: r for r in stack.ps()}
    assert set(rows) == DEFAULT_SERVICES, f"unexpected set of containers: {sorted(rows)}"
    for name in INIT_SERVICES:
        assert rows[name].state == "exited" and rows[name].exit_code == 0, rows[name]
    for name in DEFAULT_SERVICES - set(INIT_SERVICES):
        assert rows[name].state == "running" and rows[name].health == "healthy", rows[name]


@pytest.mark.slow
def test_fresh_stack_healthy_within_budget(stack: ComposeStack):
    """08 §2: from `down -v` to all healthy in <= 180 s (images already built)."""
    if os.environ.get("WPDB_FRESH") != "1":
        pytest.skip("needs WPDB_FRESH=1 (the stack fixture then starts from wiped volumes)")
    assert stack.startup_seconds is not None
    assert stack.startup_seconds <= HEALTHY_BUDGET_S, (
        f"fresh stack took {stack.startup_seconds:.0f}s (budget {HEALTHY_BUDGET_S}s)"
    )


def _snapshot(stack: ComposeStack, service: str) -> object:
    """Observable state each init container is responsible for."""
    if service == "kafka-init":
        out = stack.exec(
            "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"
        ).stdout
        return sorted(t for t in out.split() if t)
    if service == "minio-init":
        ls = stack.exec("minio", "mc", "ls", "local").stdout
        users = stack.exec("minio", "mc", "admin", "user", "list", "local").stdout
        policies = stack.exec("minio", "mc", "admin", "policy", "ls", "local").stdout
        attachments = {}
        for user in ("spark", "dashboard", "agent"):
            info = json.loads(
                stack.exec("minio", "mc", "admin", "user", "info", "--json", "local", user).stdout
            )
            attachments[user] = (info["userStatus"], sorted(info.get("policyName", "").split(",")))
        return {
            "buckets": sorted(line.split()[-1] for line in ls.splitlines() if line.strip()),
            "users": sorted(
                " ".join(line.split()[1:]) for line in users.splitlines() if line.strip()
            ),
            "policies": sorted(p for p in policies.split() if p),
            "attachments": attachments,
        }
    if service == "connect-init":
        import requests

        return requests.get("http://localhost:8083/connectors", timeout=15).json()
    raise ValueError(service)


@pytest.mark.parametrize("service", INIT_SERVICES)
def test_init_container_idempotent(stack: ComposeStack, service: str):
    """08 §4: re-running an init container exits 0 and changes nothing."""
    if service == "minio-init":
        # the minio container has no alias yet: create one for the snapshot helper
        stack.exec(
            "minio",
            "sh",
            "-c",
            'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" '
            ">/dev/null",
        )
    before = _snapshot(stack, service)
    code = stack.rerun_init(service)
    logs = stack.logs(service)
    assert code == 0, f"{service} second run exited {code}:\n{logs}"
    after = _snapshot(stack, service)
    assert after == before, f"{service} changed state on re-run: {before} -> {after}"
    if service == "minio-init":
        assert before["attachments"] == {
            "spark": ("enabled", ["spark"]),
            "dashboard": ("enabled", ["dashboard"]),
            "agent": ("enabled", ["agent-ro"]),
        }, before["attachments"]
        assert {"spark", "dashboard", "agent-ro"} <= set(before["policies"]), before["policies"]


@pytest.mark.slow
def test_crashed_long_running_container_is_restarted_by_docker(stack: ComposeStack):
    """08 §5: "Usługi długotrwałe mają `restart: unless-stopped` [...] polityka działa przy
    OOM, błędzie procesu i restarcie demona" (Docker, not compose, brings a crashed
    service back). Representative service: `spark`. Its driver (`python3 /app/main.py`,
    child of the spark-submit JVM, PID 1) is SIGKILLed from inside the container, as a
    crash would end it; nothing else is done (no `compose up`/`start`). The JVM exits
    non-zero (spark-submit returns the driver's status), Docker restarts the SAME
    container (same Id, RestartCount + 1, new StartedAt), and it becomes healthy again
    with its Kafka -> bronze query active. This is also the container half of 05 §12
    "> 10 failures in 10 min → exit 1 → Docker restarts the container" (the supervisor
    half: tests/unit/test_spark_supervisor.py)."""
    from tests.helpers.wait import wait_until

    before = stack.inspect("spark")
    assert before["HostConfig"]["RestartPolicy"]["Name"] == "unless-stopped"
    assert before["State"]["Health"]["Status"] == "healthy", before["State"]["Health"]
    pid = stack.exec("spark", "pgrep", "-f", "^python3 /app/main.py").stdout.split()
    assert len(pid) == 1, pid
    stack.exec("spark", "kill", "-9", pid[0], check=False)

    def restarted() -> dict | None:
        st = stack.inspect("spark")
        return st if st["RestartCount"] == before["RestartCount"] + 1 else None

    after = wait_until(restarted, 180, every=1, desc="Docker restarted spark by itself")
    assert after["Id"] == before["Id"]
    assert after["State"]["StartedAt"] != before["State"]["StartedAt"]
    wait_until(lambda: stack.health("spark") == "healthy", 300, every=2,
               desc="spark healthy again")  # fmt: skip
    health = json.loads(stack.exec("spark", "cat", "/tmp/spark/health.json").stdout)
    assert health["status"] == "ok" and health["queries"]["kafka_to_bronze"]["active"], health
    assert stack.inspect("spark")["RestartCount"] == before["RestartCount"] + 1
