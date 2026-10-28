"""E2E on the simulator's own short stack (01 §8 e2e rows): live schema evolution with a
kill after the DDL, then the end of the window and its extension by a later SIM_END.

Stack: window 2017-01-05 → 2017-01-09 at the default rate, ``SIM_SCHEMA_EVOLUTION_AT``
= 2017-01-07 12:00 (inside the window). The full pipeline runs, so the new schema
version is followed through Debezium, the Schema Registry and Spark (silver, gold).
"""

from __future__ import annotations

import json
import time
from datetime import datetime

import pytest

from tests.helpers import cdc, simfold
from tests.helpers.e2e import Pipeline, StackSpec, now_utc, report
from tests.helpers.wait import wait_until

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

EVOLUTION_AT = datetime(2017, 1, 7, 12, 0, 0)
WINDOW = StackSpec(
    "simulator-window",
    env={
        "SIM_START": "2017-01-05",
        "SIM_END": "2017-01-09",
        "SIM_SPEED": "1440",
        "MAX_EVENTS_PER_SEC": "20",
        "SIM_SCHEMA_EVOLUTION_AT": EVOLUTION_AT.isoformat(sep=" "),
    },
)
END, LATER_END = datetime(2017, 1, 9, 23, 59, 59), "2017-01-11"
IDLE_S = 30
ORDERS_VALUE = "olist.shop.orders-value"


@pytest.fixture(scope="module")
def pipe(stacks, settings):
    p = Pipeline(stacks.start(WINDOW), settings)
    yield p
    p.close()


def _channels_in_pg(pipe: Pipeline) -> int:
    return pipe.pg.execute(
        "SELECT count(*) FROM shop.orders WHERE delivery_channel IS NOT NULL"
    ).fetchone()[0]


def _wait_done(pipe: Pipeline, timeout: float = 900) -> dict:
    return wait_until(lambda: (s := pipe.sim())["phase"] == "done" and s, timeout, every=5,
                      desc="simulator phase done")  # fmt: skip


def _kill_simulator(pipe: Pipeline) -> None:
    pipe.stack.kill("simulator")  # SIGKILL
    wait_until(lambda: pipe.stack.health("simulator") == "exited", 60, every=1,
               desc="simulator exited")  # fmt: skip


def _start_simulator(pipe: Pipeline) -> str:
    """`docker compose up -d simulator` with the stack's env (a changed env recreates the
    container); the logs of the new run once it logged its resume position and is
    healthy again."""
    since = now_utc().isoformat()
    pipe.stack.run("up", "-d", "simulator")
    wait_until(lambda: "resuming: phase=" in pipe.logs_since("simulator", since), 120, every=2,
               desc="simulator logged its resume position")  # fmt: skip
    wait_until(lambda: pipe.stack.health("simulator") == "healthy", 180, every=2,
               desc="simulator healthy")  # fmt: skip
    return pipe.logs_since("simulator", since)


def _drain_and_reconcile(pipe: Pipeline) -> dict:
    pipe.wait_slot_caught_up(pipe.current_lsn())
    return pipe.wait_reconciled()


@pytest.mark.timeout(2400)
def test_live_schema_evolution_adds_the_column_once_and_the_pipeline_takes_the_new_schema(pipe):
    """01 §8 (e2e) "ewolucja schematu na żywo": własny stos z `SIM_SCHEMA_EVOLUTION_AT` w
    oknie: kolumna dodana raz, `kill` po niej nie powtarza DDL, liczba zamówień per kanał
    = czysty model, Debezium/Spark przyjmują nową wersję schematu. Exactly: after a kill
    past the DDL and the end of the window, events_applied (which counts the schema event
    once) and the orders per channel equal the pure model; `delivery_channel` is one
    nullable text column with REPLICA IDENTITY FULL kept; the orders value subject has
    versions [1, 2] (v2 adds the nullable field); bronze/orders holds both schema ids;
    silver = Postgres row by row including the channel; the connector stays RUNNING and
    the Spark container is never restarted."""
    if not simfold.data_present():
        pytest.skip("data/olist missing: run `make data`")
    spark_started = pipe.stack.inspect("spark")["State"]["StartedAt"]
    wait_until(
        lambda: pipe.sim()["sim_clock"] > EVOLUTION_AT and _channels_in_pg(pipe) > 0,
        900,
        every=2,
        desc="DDL applied and a purchase with a channel",
    )
    _kill_simulator(pipe)
    at_kill = pipe.sim()  # read after the process died: nothing moves it any more
    assert at_kill["phase"] == "replay" and at_kill["schema_evolution_at"] == EVOLUTION_AT
    logs = _start_simulator(pipe)
    assert "initialized" not in logs, logs
    assert f"replay: clock at {at_kill['sim_clock']}," in logs, logs
    done = _wait_done(pipe)
    state = simfold.assert_matches_uninterrupted_run(pipe.pg)
    assert state["schema_evolution_at"] == EVOLUTION_AT and done["sim_end"] == END

    cols = pipe.pg.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'shop' AND table_name = 'orders' AND column_name = 'delivery_channel'"
    ).fetchall()
    assert cols == [("text", "YES")]
    ident = pipe.pg.execute("SELECT relreplident FROM pg_class WHERE oid = 'shop.orders'::regclass")
    assert ident.fetchone()[0] == "f"

    assert pipe.registry.json(f"/subjects/{ORDERS_VALUE}/versions") == [1, 2]
    fields = {}
    for v in (1, 2):
        schema = json.loads(pipe.registry.json(f"/subjects/{ORDERS_VALUE}/versions/{v}")["schema"])
        # the row record is defined in `before`; `after` refers to it by name
        env = {f["name"]: f["type"] for f in schema["fields"]}
        record = next(t for t in env["before"] if isinstance(t, dict))
        assert env["after"] == ["null", record["name"]], env["after"]
        fields[v] = {f["name"]: f for f in record["fields"]}
    assert "delivery_channel" not in fields[1]
    added = fields[2]["delivery_channel"]
    assert added["type"][0] == "null" and added.get("default", "missing") is None, added
    assert set(fields[2]) - set(fields[1]) == {"delivery_channel"}

    recon = _drain_and_reconcile(pipe)
    silver_channels = dict(
        pipe.lake.sql(
            "SELECT delivery_channel, count(*) FROM delta_scan('s3://silver/orders') "
            "WHERE NOT _is_deleted AND delivery_channel IS NOT NULL GROUP BY 1"
        )
    )
    assert silver_channels == simfold.delivery_channels(pipe.pg)
    ids = pipe.lake.scalar(
        "SELECT count(DISTINCT value_schema_id) FROM delta_scan('s3://bronze/orders')"
    )
    assert ids == 2
    cdc.wait_running(pipe.connect)
    assert pipe.stack.inspect("spark")["State"]["StartedAt"] == spark_started
    report("schema_evolution", events=state["events_applied"], channels=silver_channels,
           at_kill=at_kill["sim_clock"], reconciliation=recon)  # fmt: skip


@pytest.mark.timeout(1800)
def test_end_of_window_done_idle_healthy_and_a_later_sim_end_resumes_from_the_cursor(pipe):
    """01 §8 (e2e) "koniec okna": po `SIM_END` faza `done`, kontener bezczynny i zdrowy;
    późniejszy `SIM_END` w `.env` + restart wznawia od kursora (§4). Idle = for 30 s the
    state, every truth row count and the container health do not change; the restart
    with a later SIM_END logs the change, resumes (no initialisation) at the stored
    sim_clock, finishes the longer window and equals the pure model of it; the pipeline
    reconciles exactly afterwards."""
    if not simfold.data_present():
        pytest.skip("data/olist missing: run `make data`")
    done = _wait_done(pipe)
    assert done["sim_end"] == END
    finished = pipe.logs_since("simulator", "2000-01-01T00:00:00Z")
    assert f"window finished at {END}" in finished, finished[-2000:]

    counts = dict(pipe.pg.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
    changes: list = []
    end = time.monotonic() + IDLE_S

    def idle_for_a_while() -> bool:
        now_state = pipe.sim()
        now_counts = dict(pipe.pg.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
        health = pipe.stack.health("simulator")
        if now_state != done or now_counts != counts or health != "healthy":
            changes.append((now_state, now_counts, health))
            return True
        return time.monotonic() >= end

    wait_until(idle_for_a_while, IDLE_S + 60, every=5, desc="simulator idle")
    assert changes == [], changes

    pipe.stack.env["SIM_END"] = LATER_END
    logs = _start_simulator(pipe)
    assert f"SIM_END changed {END} → {LATER_END} 23:59:59" in logs, logs
    assert "resuming: phase=done" in logs and "initialized" not in logs, logs
    wait_until(lambda: "replay: clock at" in pipe.logs_since("simulator", "2000-01-01T00:00:00Z")
               .split("SIM_END changed")[-1], 120, every=2, desc="replay restarted")  # fmt: skip
    logs = pipe.logs_since("simulator", "2000-01-01T00:00:00Z").split("SIM_END changed")[-1]
    assert f"replay: clock at {done['sim_clock']}," in logs, logs
    later = wait_until(
        lambda: (
            (s := pipe.sim())["phase"] == "done"
            and s["events_applied"] > done["events_applied"]
            and s
        ),
        900,
        every=5,
        desc="the longer window finished",
    )
    state = simfold.assert_matches_uninterrupted_run(pipe.pg)
    assert later["sim_end"] == datetime(2017, 1, 11, 23, 59, 59)
    assert state["sim_start"] == done["sim_start"] and state["sim_clock"] > done["sim_clock"]
    recon = _drain_and_reconcile(pipe)
    report("end_of_window", first_end=done["sim_clock"], events_first=done["events_applied"],
           second_end=state["sim_clock"], events_second=state["events_applied"],
           reconciliation=recon)  # fmt: skip
