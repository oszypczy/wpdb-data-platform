"""End-to-end helpers (tests/e2e, skill `fault-injection`).

- ``StackSpec`` / ``StackManager``: one fresh stack per spec (``down -v`` + ``up -d
  --build`` with env overrides and extra compose files); consecutive tests with the same
  spec share it. ``restore_default()`` leaves the default stack (``.env``) up and healthy.
- ``Pipeline``: the running stack seen from the host: simulator state (Postgres as
  ``reader``), pause/resume of the simulator, "stream flowing", slot lag, recovery of
  the services and exact reconciliation Postgres ↔ silver ↔ gold.
- ``inject(...)``: one fault (kill + up, restart, kill + rm + up) with its timings.
- ``report(...)``: a JSON line per fault/measurement in
  ``.pytest_cache/wpdb-e2e/report.jsonl`` (also printed), the evidence of a run.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from tests.helpers import cdc, reconcile, simfold
from tests.helpers.clients import CONNECT_URL, SCHEMA_REGISTRY_URL, Http, minio_client, pg_connect
from tests.helpers.compose import ComposeStack
from tests.helpers.lake import Lake
from tests.helpers.wait import wait_until

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / ".pytest_cache" / "wpdb-e2e" / "report.jsonl"


def now_utc() -> datetime:
    return datetime.now(UTC)


def report(event: str, **data: Any) -> None:
    line = {"at": now_utc().isoformat(timespec="seconds"), "event": event, **data}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("a") as f:
        f.write(json.dumps(line, default=str) + "\n")
    print(f"\n[e2e] {json.dumps(line, default=str)}")


# --------------------------------------------------------------------------- stacks
@dataclass(frozen=True)
class StackSpec:
    name: str
    env: dict[str, str] = field(default_factory=dict)
    files: tuple[str, ...] = ()


class StackManager:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.current: ComposeStack | None = None
        self.spec: StackSpec | None = None

    def start(self, spec: StackSpec, timeout: float = 420) -> ComposeStack:
        """A fresh stack for `spec` (volumes wiped), healthy; reused while the spec is
        the same. Images are rebuilt from the working tree (cached layers: seconds)."""
        if self.spec == spec and self.current is not None:
            return self.current
        self.teardown_current()
        files = list(spec.files) and ["docker-compose.yml", *spec.files]
        stack = ComposeStack(root=self.root, env=dict(spec.env), files=files)
        stack.down(volumes=True)
        t0 = time.monotonic()
        stack.up(build=True)
        stack.wait_healthy(timeout)
        stack.startup_seconds = time.monotonic() - t0
        report("stack_started", stack=spec.name, env=spec.env, files=spec.files,
               healthy_after_s=round(stack.startup_seconds))  # fmt: skip
        self.current, self.spec = stack, spec
        return stack

    def teardown_current(self) -> None:
        """Wipe whatever runs now (the default stack before the first e2e stack)."""
        (self.current or ComposeStack(root=self.root)).down(volumes=True)
        self.current, self.spec = None, None

    def restore_default(self, timeout: float = 420) -> None:
        """The default stack (`.env`, `docker-compose.yml` only) from wiped volumes."""
        self.teardown_current()
        stack = ComposeStack(root=self.root)
        stack.up()
        stack.wait_healthy(timeout)
        report("default_stack_restored")


# ------------------------------------------------------------------------- pipeline
SERVICE_TIMEOUT = 420  # Spark: start_period 120 s, first micro-batches of 13 queries


class Pipeline:
    def __init__(self, stack: ComposeStack, settings: dict[str, str]) -> None:
        self.stack, self.settings = stack, settings
        self.lake = Lake(settings, "agent")
        self.connect = Http(CONNECT_URL)
        self.registry = Http(SCHEMA_REGISTRY_URL)
        self._pg: psycopg.Connection | None = None

    # -- Postgres (reader), reconnecting after a restart of postgres ------------------
    @property
    def pg(self) -> psycopg.Connection:
        if self._pg is None or self._pg.closed or self._pg.broken:
            self._pg = pg_connect("reader", self.settings["READER_PASSWORD"])
            self._pg.autocommit = True
        return self._pg

    def close(self) -> None:
        if self._pg is not None:
            self._pg.close()

    def sim(self) -> dict:
        try:
            return simfold.read_state(self.pg)
        except psycopg.OperationalError:
            self._pg = None
            raise

    def current_lsn(self) -> int:
        return cdc.lsn_to_int(self.pg.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])

    # -- the simulator -----------------------------------------------------------------
    def resume_simulator(self, timeout: float = 240) -> dict:
        """`docker compose up -d simulator` (the stack's env), healthy again, past preload."""
        self.stack.run("up", "-d", "simulator")
        wait_until(lambda: self.stack.health("simulator") == "healthy", timeout, every=2,
                   desc="simulator healthy")  # fmt: skip
        return wait_until(lambda: self.sim()["phase"] in ("replay", "done") and self.sim(),
                          timeout, every=2, desc="simulator past preload")  # fmt: skip

    def pause_simulator(self) -> int:
        """`docker compose stop simulator` (SIGTERM: stops after the current transaction,
        01 §5). Returns the WAL position after the last business change."""
        self.stack.run("stop", "simulator")
        state = self.stack.inspect("simulator")["State"]
        assert state["Status"] == "exited" and state["ExitCode"] == 0, state
        return self.current_lsn()

    def ensure_replaying(self) -> dict:
        state = self.sim()
        if self.stack.health("simulator") != "healthy":
            state = self.resume_simulator()
        assert state["phase"] == "replay", (
            f"simulator in phase {state['phase']} (sim_clock {state['sim_clock']}): the "
            "e2e window ended before this fault test; lengthen SIM_END of the stack"
        )
        return state

    # -- the lake ----------------------------------------------------------------------
    def silver_lsn(self, table: str = "orders") -> int:
        return self.lake.scalar(
            f"SELECT coalesce(max(_lsn), 0) FROM delta_scan('s3://silver/{table}')"
        )

    def wait_flowing(self, timeout: float = 300) -> int:
        """New CDC changes reach silver: the highest LSN in silver/orders grows."""
        start = wait_until(lambda: self.silver_lsn() or None, timeout, every=3,
                           desc="silver/orders readable")  # fmt: skip
        return wait_until(lambda: (lsn := self.silver_lsn()) > start and lsn, timeout,
                          every=3, desc="new changes reaching silver/orders")  # fmt: skip

    # -- recovery ----------------------------------------------------------------------
    def wait_services_healthy(self, timeout: float = SERVICE_TIMEOUT) -> None:
        self.stack.wait_healthy(timeout)

    def wait_connector_running(self, timeout: float = 180) -> dict:
        return cdc.wait_running(self.connect, timeout)

    def wait_slot_caught_up(self, lsn: int, timeout: float = 180) -> dict:
        """03 §11: the slot's confirmed_flush_lsn passes the last business change."""

        def caught_up():
            st = cdc.slot_state(self.pg)
            ok = st and st["active"] and cdc.lsn_to_int(st["confirmed_flush_lsn"]) >= lsn
            return st if ok else None

        return wait_until(caught_up, timeout, every=3, desc=f"slot confirmed past {lsn}")

    # -- reconciliation ----------------------------------------------------------------
    def wait_reconciled(self, timeout: float = 600) -> dict:
        """Exact equality once Spark has caught up (the simulator must be paused or done):
        every truth view = silver, every table row by row = silver (no PK or bronze
        offset duplicate), gold sums = silver sums. Polls each layer until equal; on
        timeout fails with the remaining differences."""
        last: dict[str, Any] = {}
        deadline = time.monotonic() + timeout

        def left() -> float:
            return max(5.0, deadline - time.monotonic())

        def views_equal() -> bool:
            truth = reconcile.truth_aggregates(self.pg)
            tables = list(truth["v_truth_row_counts"])
            last["views"] = reconcile.aggregate_diff(
                truth, reconcile.silver_aggregates(self.lake, tables)
            )
            last["truth"] = truth
            return not last["views"]

        def mirror_equal() -> bool:
            diffs = reconcile.mirror_diffs(self.pg, self.lake)
            last["mirror"] = [d.summary() for d in diffs if not d.ok]
            last["tables"] = {d.table: d.pg_rows for d in diffs}
            return not last["mirror"]

        def gold_equal() -> bool:
            last["gold"] = reconcile.gold_vs_silver(self.lake)
            return not last["gold"]

        try:
            wait_until(views_equal, left(), every=10, desc="sim.v_truth_* = silver")
            wait_until(mirror_equal, left(), every=10, desc="shop.* = silver row by row")
            wait_until(gold_equal, left(), every=10, desc="gold sums = silver sums")
        except TimeoutError as exc:
            raise AssertionError(
                f"{exc}; views (truth, silver): {last.get('views')}; mirror: "
                f"{last.get('mirror')}; gold (gold, silver): {last.get('gold')}"
            ) from None
        totals = last["truth"]["v_truth_totals"]
        return {"rows": last["tables"], "orders": totals[0], "revenue": str(totals[5])}

    def logs_since(self, service: str, since: str) -> str:
        """Container log (stdout and stderr) since an RFC 3339 instant."""
        out = self.stack.docker("logs", "--since", since, self.stack.container_name(service))
        return out.stdout + out.stderr

    def checkpoint(self, timeout: float = 300) -> tuple[str, int]:
        """(txnAppId token, last committed micro-batch) of the kafka_to_bronze checkpoint
        in MinIO (06 §8, 05 §12): both continue after a restart from the checkpoint.
        On a young stack (a test run on its own) waits for the first committed batch."""
        s3 = minio_client(
            self.settings["MINIO_AGENT_ACCESS_KEY"], self.settings["MINIO_AGENT_SECRET_KEY"]
        )

        def read() -> tuple[str, int] | None:
            batches = []
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket="checkpoints", Prefix="kafka_to_bronze/commits/"
            ):
                for obj in page.get("Contents", []):
                    name = obj["Key"].rsplit("/", 1)[1]
                    if name.isdigit():
                        batches.append(int(name))
            if not batches:
                return None
            token = s3.get_object(Bucket="checkpoints", Key="kafka_to_bronze/_wpdb_txn_app_id")
            return token["Body"].read().decode().strip(), max(batches)

        return wait_until(read, timeout, every=3, desc="a committed kafka_to_bronze batch")

    def spark_queries(self) -> dict:
        out = self.stack.exec("spark", "cat", "/tmp/spark/health.json", check=False).stdout
        return json.loads(out) if out.strip() else {}


# --------------------------------------------------------------------------- faults
@dataclass
class FaultRun:
    service: str
    action: str
    events_before: int = 0
    events_at_restore: int = 0
    down_s: float = 0.0
    recovered_s: float = 0.0
    details: dict = field(default_factory=dict)

    @property
    def events_during_outage(self) -> int:
        return self.events_at_restore - self.events_before


def _exited(stack: ComposeStack, service: str) -> bool:
    return stack.inspect(service)["State"]["Status"] == "exited"


def inject(pipe: Pipeline, service: str, action: str, hold_events: int = 20) -> FaultRun:
    """`kill`: SIGKILL (`docker compose kill`), then `docker compose up -d <service>`
    (`restart: unless-stopped` does not bring back a killed container, 08 §5).
    `recreate`: kill, remove the container, `up -d` creates a new one. `restart`:
    `docker compose restart <service>`. While the service is down the simulator keeps
    writing: it is brought back once `hold_events` more events were applied (0 = at
    once). Returns the timings; recovery of the pipeline is checked by the caller."""
    stack = pipe.stack
    run = FaultRun(service, action, events_before=pipe.sim()["events_applied"])
    t0 = time.monotonic()
    if action == "restart":
        stack.run("restart", service)
    else:
        stack.kill(service)
        wait_until(lambda: _exited(stack, service), 60, every=1, desc=f"{service} exited")
        if hold_events:
            target = run.events_before + hold_events
            wait_until(lambda: pipe.sim()["events_applied"] >= target, 180, every=1,
                       desc=f"{hold_events} events applied while {service} is down")  # fmt: skip
        if action == "recreate":
            stack.run("rm", "-f", service)
        try:
            run.events_at_restore = pipe.sim()["events_applied"]
        except psycopg.OperationalError:  # postgres itself is down
            run.events_at_restore = run.events_before
        stack.run("up", "-d", service)
    run.down_s = round(time.monotonic() - t0, 1)
    if action == "restart":
        run.events_at_restore = run.events_before
    wait_until(lambda: stack.health(service) in ("healthy", "running"), SERVICE_TIMEOUT,
               every=2, desc=f"{service} healthy again")  # fmt: skip
    run.recovered_s = round(time.monotonic() - t0, 1)
    return run


def record(run: FaultRun, recon: dict, **extra: Any) -> None:
    report("fault", **asdict(run), events_during_outage=run.events_during_outage,
           reconciliation=recon, **extra)  # fmt: skip
