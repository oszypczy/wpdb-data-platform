"""E2E on one fresh full stack: freshness and latency during the simulation, a fault in
every service while the stream flows (each followed by exact reconciliation), and the
reconciliation and dashboard KPIs after the run (02 §10, 03 §11, 04 §8, 05 §10,
06 §11, 07 §7, 08 §5).

Stack: default rate (``SIM_SPEED=1440``, ``MAX_EVENTS_PER_SEC=20``) on a short window
(2017-01-05 → 2017-02-01). Each fault test: the simulator is writing and new changes
reach silver, the service is killed (SIGKILL) and brought back while the simulator keeps
writing, the pipeline recovers and processes new changes, then the simulator is paused
(SIGTERM, 01 §5) so Postgres stops changing, the replication slot catches up, and
Postgres ↔ silver ↔ gold must be exactly equal: every ``sim.v_truth_*`` view, every
table row by row and column by column, no PK duplicate in silver, no
``(topic, partition, offset)`` duplicate in bronze, gold sums = silver sums. The next
test resumes the simulator. The last two tests let the window finish (the remaining
days at a higher ``SIM_SPEED``, still capped at 20 rows/s) and reconcile the whole run.
"""

from __future__ import annotations

import math
import time

import pyarrow.compute as pc
import pytest

from dashboard import queries as q
from tests.helpers import cdc, kpis, simfold
from tests.helpers.e2e import Pipeline, StackSpec, inject, now_utc, record, report
from tests.helpers.wait import wait_until

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

MAIN = StackSpec(
    "main",
    env={
        "SIM_START": "2017-01-05",
        "SIM_END": "2017-02-01",
        "SIM_SPEED": "1440",
        "MAX_EVENTS_PER_SEC": "20",
        "SIM_SCHEMA_EVOLUTION_AT": "",
    },
)
FINISH_SPEED = "11520"  # 8 simulated days per minute; MAX_EVENTS_PER_SEC still caps the rate

# 05 §12 / 07 §7 thresholds (the same ones colour the "Zdrowie potoku" tiles)
CDC_FRESHNESS_S = 30
LATENCY_P95_S = {"silver": 60, "gold": 120}
OBSERVE_S = 240


@pytest.fixture(scope="module")
def pipe(stacks, settings):
    """The MAIN stack with the stream flowing: the snapshot is in bronze (Debezium streams
    only after it) and new changes reach silver, so every test also runs on its own
    (`-k minio` on a fresh stack): baselines such as the snapshot reads in bronze or the
    last checkpointed batch are taken from a pipeline that is past its start-up."""
    p = Pipeline(stacks.start(MAIN), settings)
    p.ensure_replaying()
    p.wait_flowing()
    yield p
    p.close()


def _bronze_snapshot_reads(pipe: Pipeline) -> int:
    """Snapshot records (op = r) of all business tables in bronze: a connector that
    resumes from its offset never snapshots again (03 §2)."""
    return sum(
        pipe.lake.scalar(f"SELECT count(*) FROM delta_scan('s3://bronze/{t}') WHERE op = 'r'")
        for t in ("orders", "order_items", "order_payments", "order_reviews", "customers")
    )


def _fault(pipe: Pipeline, service: str, action: str, hold_events: int = 20):
    """Kill/restart `service` mid-stream, recover, pause the simulator, reconcile."""
    pipe.ensure_replaying()
    pipe.wait_flowing()
    run = inject(pipe, service, action, hold_events)
    pipe.wait_services_healthy()
    status = pipe.wait_connector_running()
    resumed = pipe.sim()["events_applied"]
    wait_until(lambda: pipe.sim()["events_applied"] > resumed, 180, every=2,
               desc="simulator writing after the recovery")  # fmt: skip
    pipe.wait_flowing()
    lsn = pipe.pause_simulator()
    slot = pipe.wait_slot_caught_up(lsn)
    recon = pipe.wait_reconciled()
    record(run, recon, connector=cdc.states(status), slot_lag_bytes=slot["lag_bytes"])
    if hold_events:
        assert run.events_during_outage >= hold_events, run
    return run, recon


# ------------------------------------------------------------- freshness and latency
@pytest.mark.timeout(1500)
def test_cdc_freshness_and_end_to_end_latency_stay_below_the_thresholds(pipe, settings):
    """07 §7 "świeżość": podczas symulacji świeżość CDC < 30 s, p95 e2e silver < 60 s,
    gold < 120 s; 05 §10 "opóźnienie": opóźnienie end-to-end poniżej progu przy
    domyślnym tempie (progi 05 §12). Measured as the dashboard shows them (query layer
    on ops/cdc_heartbeat and ops/e2e_latency, account `dashboard`): CDC freshness sampled
    every 10 s for 4 min, and per layer the median of the micro-batch p95 latencies
    measured in those 4 min (the "Zdrowie potoku" tile), after the start-up backlog."""
    reader = kpis.dashboard_reader(settings)
    pipe.ensure_replaying()

    def gold_measured() -> int:
        lat = reader.get(kpis.LATENCY)
        return int(q.sql("SELECT count(*) AS n FROM e WHERE layer = 'gold'", e=lat).at[0, "n"])

    # warm-up: Spark caught up with the snapshot and the queued start of the replay
    wait_until(lambda: gold_measured() >= 3, 600, every=10, desc="gold latency measured")
    pipe.wait_flowing()
    t0, samples = now_utc(), []
    end = time.monotonic() + OBSERVE_S

    def sample() -> bool:
        fresh = q.cdc_freshness(reader.get(kpis.HEARTBEAT), now_utc()).iloc[0]
        samples.append(None if fresh["freshness_s"] is None else round(fresh["freshness_s"], 1))
        return time.monotonic() >= end

    wait_until(sample, OBSERVE_S + 120, every=10, desc="freshness samples")
    t1 = now_utc()
    lat = reader.get(kpis.LATENCY)
    aware = lat.schema.field("measured_at").type.tz is not None
    lo, hi = (t0, t1) if aware else (t0.replace(tzinfo=None), t1.replace(tzinfo=None))
    # the latency buffer is flushed every 30 s: wait for a measurement after the window
    wait_until(
        lambda: pc.max(reader.get(kpis.LATENCY)["measured_at"]).as_py() > hi,
        180,
        every=5,
        desc="latency flushed past the window",
    )
    lat = reader.get(kpis.LATENCY)
    window = lat.filter(
        pc.and_(pc.greater_equal(lat["measured_at"], lo), pc.less_equal(lat["measured_at"], hi))
    )
    summary = {r.layer: r for r in q.latency_summary(window, 60).itertuples()}
    state = pipe.sim()
    report("latency", window=[t0, t1], freshness_s=samples,
           summary={k: {"batches": v.batches, "p50_s": round(v.p50_s, 1),
                        "p95_s": round(v.p95_s, 1), "max_s": round(v.max_s, 1)}
                    for k, v in summary.items()},
           sim_phase=state["phase"], sim_clock=state["sim_clock"])  # fmt: skip
    assert state["phase"] == "replay", "the simulation must be running during the window"
    assert samples and all(s is not None and s < CDC_FRESHNESS_S for s in samples), samples
    for layer, limit in LATENCY_P95_S.items():
        row = summary.get(layer)
        assert row is not None and row.batches >= 3, (layer, summary)
        assert row.p95_s < limit, (layer, row)


# ------------------------------------------------------------------------ faults
@pytest.mark.timeout(1200)
def test_kill_connect_mid_stream_resumes_from_its_offset_without_loss_or_duplicates(pipe):
    """03 §11 (e2e, awaria): `kill` kontenera `connect` w trakcie strumienia, restart →
    wznowienie od offsetu, opóźnienie slotu wraca do zera, uzgadnianie Postgres ↔ silver
    zgodne. "From its offset": no new snapshot (the op=r records in bronze stay the
    preload's), the connector and its task RUNNING, slot confirmed past the last change."""
    reads = _bronze_snapshot_reads(pipe)
    assert reads > 0
    _fault(pipe, "connect", "kill")
    assert _bronze_snapshot_reads(pipe) == reads


@pytest.mark.timeout(1200)
def test_kill_kafka_mid_stream_debezium_and_spark_resume_without_loss_or_duplicates(pipe):
    """04 §8 (e2e, awaria): `kill` brokera w trakcie strumienia, restart → Debezium i Spark
    wznawiają pracę; uzgadnianie Postgres ↔ silver zgodne (brak strat i duplikatów)."""
    topics = set(pipe.stack.exec("kafka", "kafka-topics", "--bootstrap-server",
                                 "kafka:29092", "--list").stdout.split())  # fmt: skip
    reads = _bronze_snapshot_reads(pipe)
    _fault(pipe, "kafka", "kill")
    after = set(pipe.stack.exec("kafka", "kafka-topics", "--bootstrap-server",
                                "kafka:29092", "--list").stdout.split())  # fmt: skip
    assert after == topics  # the broker came back on its own log (fixed CLUSTER_ID)
    assert _bronze_snapshot_reads(pipe) == reads


@pytest.mark.timeout(1200)
def test_kill_schema_registry_mid_stream_clients_retry_without_loss_or_duplicates(pipe):
    """08 §5: `schema-registry` umiera → rejestr jest bezstanowy (schematy w `_schemas`),
    Docker/compose go podnosi, klienci (konwerter, Spark) ponawiają; uzgadnianie zgodne.
    Subjects and their versions are identical after the restart."""

    def subjects() -> dict:
        return {s: pipe.registry.json(f"/subjects/{s}/versions")
                for s in pipe.registry.json("/subjects")}  # fmt: skip

    before = subjects()
    _fault(pipe, "schema-registry", "kill")
    assert subjects() == before


@pytest.mark.timeout(1200)
def test_kill_postgres_mid_stream_simulator_and_debezium_reconnect_without_loss(pipe):
    """08 §5 / 01 §5: `postgres` umiera → WAL i slot są w `pg_data`; symulator łapie utratę
    połączenia, odnawia je i wznawia z kursora (bez restartu kontenera), Debezium łączy
    się ponownie i czyta od `confirmed_flush_lsn`; uzgadnianie zgodne, stan bazy = czysty
    model zdarzeń do kursora (żadne zdarzenie nie zginęło ani nie powtórzyło się)."""
    pipe.ensure_replaying()  # resumed before the StartedAt below is taken
    sim_started = pipe.stack.inspect("simulator")["State"]["StartedAt"]
    since = now_utc().isoformat()
    _fault(pipe, "postgres", "kill", hold_events=0)
    logs = pipe.logs_since("simulator", since)
    sim_state = pipe.stack.inspect("simulator")["State"]
    # paused by _fault (stop + exit 0): the container ran from before the kill until then
    assert sim_state["StartedAt"] == sim_started, "simulator was restarted, not reconnected"
    assert "database connection lost" in logs and "reconnecting" in logs, logs[-2000:]
    if simfold.data_present():
        simfold.assert_matches_uninterrupted_run(pipe.pg)


@pytest.mark.timeout(1200)
def test_kill_simulator_mid_stream_resumes_from_its_cursor_and_silver_reconciles(pipe):
    """08 §10 (02–06 awarie, `simulator`): `kill` symulatora i `up -d` → wznowienie od
    kursora (01 §5) bez duplikatów i strat, uzgadnianie Postgres ↔ silver zgodne."""
    pipe.ensure_replaying()
    since = now_utc().isoformat()
    _fault(pipe, "simulator", "kill", hold_events=0)
    logs = pipe.logs_since("simulator", since)
    assert "resuming: phase=replay" in logs and "initialized" not in logs, logs[-2000:]
    if simfold.data_present():
        simfold.assert_matches_uninterrupted_run(pipe.pg)


@pytest.mark.timeout(1200)
def test_kill_spark_mid_stream_resumes_from_checkpoints_and_reconciles(pipe):
    """05 §10 (e2e, awaria): `kill` Sparka w trakcie strumienia, restart → uzgadnianie
    zgodne. The kafka_to_bronze checkpoint in MinIO continues (same txnAppId token,
    micro-batch numbers go on), bronze has no (topic, partition, offset) duplicate."""
    token, last = pipe.checkpoint()
    _fault(pipe, "spark", "kill")
    token2, last2 = pipe.checkpoint()
    assert token2 == token and last2 > last, (token, last, token2, last2)


@pytest.mark.timeout(1200)
def test_removed_and_recreated_spark_container_resumes_from_minio_checkpoints(pipe):
    """06 §11: usunięcie i ponowne utworzenie kontenera Sparka → wznowienie z checkpointów
    w MinIO, brak duplikatów, uzgadnianie zgodne. The container is killed and removed
    (nothing local survives) and a new one is created (new container id)."""
    old_id = pipe.stack.inspect("spark")["Id"]
    token, last = pipe.checkpoint()
    _fault(pipe, "spark", "recreate")
    assert pipe.stack.inspect("spark")["Id"] != old_id
    token2, last2 = pipe.checkpoint()
    assert token2 == token and last2 > last, (token, last, token2, last2)


@pytest.mark.timeout(1200)
def test_restart_minio_mid_stream_spark_retries_and_reconciles(pipe):
    """06 §11 (e2e, awaria): restart MinIO w trakcie strumienia → Spark ponawia zapis;
    uzgadnianie zgodne (`docker compose restart minio` while Spark writes every 10 s).
    MinIO really restarted (new StartedAt); Spark recovered by itself: same container
    (StartedAt unchanged), checkpoint continued."""
    _minio_fault(pipe, "restart", hold_events=0)


@pytest.mark.timeout(1200)
def test_kill_minio_mid_stream_spark_retries_and_reconciles(pipe):
    """06 §11 / 08 §5 (e2e, awaria): `minio` umiera w trakcie strumienia → Spark nie może
    zapisać, ponawia (S3A retry); Docker/compose podnosi MinIO, dane w `minio_data`;
    uzgadnianie zgodne. SIGKILL and 20 more events written before `up -d` (an outage
    longer than a trigger, so Spark's writes do hit a dead MinIO); Spark recovers in the
    same container."""
    _minio_fault(pipe, "kill", hold_events=20)


def _minio_fault(pipe: Pipeline, action: str, hold_events: int) -> None:
    pipe.ensure_replaying()
    minio0 = pipe.stack.inspect("minio")["State"]["StartedAt"]
    spark0 = pipe.stack.inspect("spark")["State"]["StartedAt"]
    token, last = pipe.checkpoint()
    _fault(pipe, "minio", action, hold_events=hold_events)
    token2, last2 = pipe.checkpoint()
    assert pipe.stack.inspect("minio")["State"]["StartedAt"] != minio0
    assert pipe.stack.inspect("spark")["State"]["StartedAt"] == spark0, pipe.spark_queries()
    assert token2 == token and last2 > last, (token, last, token2, last2)
    assert pipe.stack.health("spark") == "healthy", pipe.spark_queries()


# ----------------------------------------------------------------- after the run
@pytest.fixture(scope="module")
def finished_run(pipe):
    """The window runs to its end (`done`), the remaining simulated days at a higher
    SIM_SPEED (MAX_EVENTS_PER_SEC still caps the rate), and the pipeline drained."""
    pipe.stack.env["SIM_SPEED"] = FINISH_SPEED
    pipe.resume_simulator()
    t0 = time.monotonic()
    state = wait_until(lambda: (s := pipe.sim())["phase"] == "done" and s, 1800, every=10,
                       desc="simulator phase done")  # fmt: skip
    slot = pipe.wait_slot_caught_up(pipe.current_lsn())
    report("run_finished", finish_s=round(time.monotonic() - t0), sim_clock=state["sim_clock"],
           events=state["events_applied"], slot_lag_bytes=slot["lag_bytes"])  # fmt: skip
    return state


@pytest.mark.timeout(2700)
def test_after_the_run_truth_views_equal_silver_and_gold_sums_equal_silver(pipe, finished_run):
    """02 §10 (e2e): widoki `sim.v_truth_*` zgodne z tabelami silver po zakończeniu
    przebiegu; 05 §10 (e2e): `sim.v_truth_*` = silver; sumy gold = sumy silver. After a
    run with a fault in every service: all truth views, every table row by row, no
    duplicates in silver or bronze, gold facts and aggregates = silver; the database
    equals the pure event model of the whole window."""
    recon = pipe.wait_reconciled()
    report("after_run_reconciliation", **recon)
    if simfold.data_present():
        state = simfold.assert_matches_uninterrupted_run(pipe.pg)
        assert state["phase"] == "done"
    assert recon["orders"] > 0


@pytest.mark.timeout(2700)
def test_dashboard_kpis_equal_the_truth_views_after_the_run(pipe, finished_run, settings):
    """07 §7 (e2e): KPI z warstwy zapytań = widoki uzgadniania w Postgresie
    (`sim.v_truth_*`) po zakończeniu przebiegu. The overview page (net revenue, gross
    sales + corrections, revenue events, freight, items, orders, average basket,
    delivered, late, % late, median delivery days, simulation clock), the map (orders
    and revenue per state) and the "Płatności i opinie" page, read through the dashboard's
    query layer with its account, equal Postgres."""
    pipe.wait_reconciled()  # gold caught up with the finished run
    reader = kpis.dashboard_reader(settings)
    state: dict = {}

    def equal() -> bool:
        state["dash"] = kpis.overview_of_the_query_layer(reader)
        state["truth"] = kpis.overview_truth(pipe.pg)
        state["dash_pr"] = kpis.payments_reviews_of_the_query_layer(reader)
        state["truth_pr"] = kpis.payments_reviews_truth(pipe.pg)
        dm = state["dash"].pop("median_delivery_days")
        tm = state["truth"].pop("median_delivery_days")
        state["median"] = (dm, tm)
        same_median = dm is not None and tm is not None and math.isclose(dm, tm, rel_tol=1e-12)
        return (
            same_median
            and state["dash"] == state["truth"]
            and (state["dash_pr"] == state["truth_pr"])
        )

    try:
        wait_until(equal, 300, every=10, desc="dashboard KPIs = truth views")
    except TimeoutError:
        diff = {k: (state["dash"].get(k), v) for k, v in state["truth"].items()
                if state["dash"].get(k) != v}  # fmt: skip
        diff_pr = {k: (state["dash_pr"].get(k), v) for k, v in state["truth_pr"].items()
                   if state["dash_pr"].get(k) != v}  # fmt: skip
        pytest.fail(f"dashboard != truth (dashboard, truth): {diff} {diff_pr} "
                    f"median {state.get('median')}")  # fmt: skip
    report("dashboard_kpis", revenue=state["truth"]["revenue"], orders=state["truth"]["orders"],
           late=state["truth"]["late"], median_days=state["median"])  # fmt: skip
    assert state["truth"]["orders"] > 0 and state["truth"]["late"] >= 0
    assert pipe.stack.health("dashboard") == "healthy"
