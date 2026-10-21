"""Business simulator against PostgreSQL (01 §8 integration rows; 02 §6, §7, §8).

The `simulator` service runs by default and is healthy only once its preload is done,
so the `stack` fixture guarantees that the tables exist and sim.sim_state has a row.
The tests never write to the simulator's tables with ids from the data set: the
interrupted-purchase tests use synthetic `test-half-*` ids that are never committed.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import errors

from simulator import events as ev
from simulator.config import Config
from simulator.csvio import load_dir
from simulator.db import Database
from simulator.engine import Loaded, Simulator
from simulator.render import Renderer
from simulator.schema import infer_schemas
from tests.helpers import clients, olist_mini
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "olist"
SIMULATOR_SRC = ROOT / "containers" / "simulator"

STATE_COLUMNS = (
    "phase",
    "sim_start",
    "sim_end",
    "sim_clock",
    "cursor_order_id",
    "cursor_step",
    "events_applied",
    "orders_inserted",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "schema_evolution_at",
)

# 01 §3 on the real data: the public interface that Debezium captures
EXPECTED_COLUMNS = {
    "customers": [
        ("customer_id", "text", False),
        ("customer_unique_id", "text", False),
        ("customer_zip_code_prefix", "text", False),
        ("customer_city", "text", False),
        ("customer_state", "text", False),
    ],
    "orders": [
        ("order_id", "text", False),
        ("customer_id", "text", False),
        ("order_status", "text", False),
        ("order_purchase_timestamp", "timestamp without time zone", False),
        ("order_approved_at", "timestamp without time zone", True),
        ("order_delivered_carrier_date", "timestamp without time zone", True),
        ("order_delivered_customer_date", "timestamp without time zone", True),
        ("order_estimated_delivery_date", "timestamp without time zone", False),
    ],
    "order_items": [
        ("order_id", "text", False),
        ("order_item_id", "integer", False),
        ("product_id", "text", False),
        ("seller_id", "text", False),
        ("shipping_limit_date", "timestamp without time zone", False),
        ("price", "numeric(10,2)", False),
        ("freight_value", "numeric(10,2)", False),
    ],
    "order_payments": [
        ("order_id", "text", False),
        ("payment_sequential", "integer", False),
        ("payment_type", "text", False),
        ("payment_installments", "integer", False),
        ("payment_value", "numeric(10,2)", False),
    ],
    "order_reviews": [
        ("review_id", "text", False),
        ("order_id", "text", False),
        ("review_score", "integer", False),
        ("review_comment_title", "text", True),
        ("review_comment_message", "text", True),
        ("review_creation_date", "timestamp without time zone", False),
        ("review_answer_timestamp", "timestamp without time zone", False),
    ],
    "products": [
        ("product_id", "text", False),
        ("product_category_name", "text", True),
        ("product_name_lenght", "integer", True),
        ("product_description_lenght", "integer", True),
        ("product_photos_qty", "integer", True),
        ("product_weight_g", "integer", True),
        ("product_length_cm", "integer", True),
        ("product_height_cm", "integer", True),
        ("product_width_cm", "integer", True),
    ],
    "sellers": [
        ("seller_id", "text", False),
        ("seller_zip_code_prefix", "text", False),
        ("seller_city", "text", False),
        ("seller_state", "text", False),
    ],
    "product_category_name_translation": [
        ("product_category_name", "text", False),
        ("product_category_name_english", "text", False),
    ],
}
EXPECTED_PKS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}
EXPECTED_FKS = {
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
}
# 02 §7: exactly these indexes besides the primary keys
EXPECTED_EXTRA_INDEXES = {
    "orders": {"orders_customer_id_idx": ["customer_id"]},
    "order_items": {
        "order_items_product_id_idx": ["product_id"],
        "order_items_seller_id_idx": ["seller_id"],
    },
    "order_reviews": {"order_reviews_order_id_idx": ["order_id"]},
}
CATALOG_COUNTS = {"products": 32951, "sellers": 3095, "product_category_name_translation": 71}
TRUTH_VIEWS = {
    "v_truth_row_counts",
    "v_truth_orders",
    "v_truth_totals",
    "v_truth_revenue_by_state",
    "v_truth_payments",
    "v_truth_order_lifecycle",
}


def read_state(conn: psycopg.Connection) -> dict:
    row = conn.execute(
        f"SELECT {', '.join(STATE_COLUMNS)} FROM sim.sim_state WHERE id = 1"
    ).fetchone()
    assert row is not None, "sim.sim_state has no row"
    return dict(zip(STATE_COLUMNS, row, strict=True))


def state_ready(conn: psycopg.Connection) -> dict | None:
    try:
        state = read_state(conn)
    except errors.UndefinedTable:
        conn.rollback()
        return None
    return state if state["phase"] in ("replay", "done") else None


@pytest.fixture(scope="module")
def simulator_running(stack, settings):
    """The simulator has finished its preload (healthy) and the tables exist."""
    conn = clients.pg_connect("reader", settings["READER_PASSWORD"])
    conn.autocommit = True
    try:
        return wait_until(
            lambda: state_ready(conn), timeout=300, every=2, desc="simulator past preload"
        )
    finally:
        conn.close()


# --- 01 §8: tables with detected types, PK, FK and indexes ---------------------------
def test_tables_created_with_detected_types_pks_fks_and_indexes(simulator_running, pg_reader):
    """01 §8 (integration): tables created dynamically with the detected types, PKs, FKs and
    the FK indexes of 02 §7; REPLICA IDENTITY FULL; catalogs loaded; truth views applied."""
    columns: dict[str, list[tuple[str, str, bool]]] = {}
    for table, name, typ, notnull in pg_reader.execute(
        "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull "
        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'shop' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY c.relname, a.attnum"
    ).fetchall():
        columns.setdefault(table, []).append((name, typ, not notnull))
    assert columns == EXPECTED_COLUMNS

    pks: dict[str, list[str]] = {}
    fks: set[tuple[str, str, str, str]] = set()
    for table, contype, cols, ref_table, ref_cols in pg_reader.execute(
        "SELECT c.conrelid::regclass::text, c.contype, "
        "       (SELECT array_agg(a.attname ORDER BY k.ord) "
        "        FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord) "
        "        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum), "
        "       c.confrelid::regclass::text, "
        "       (SELECT array_agg(a.attname ORDER BY k.ord) "
        "        FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord) "
        "        JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum) "
        "FROM pg_constraint c "
        "WHERE c.connamespace = 'shop'::regnamespace AND c.contype IN ('p', 'f')"
    ).fetchall():
        table = table.removeprefix("shop.")
        if contype == "p":
            pks[table] = list(cols)
        else:
            fks.add((table, cols[0], ref_table.removeprefix("shop."), ref_cols[0]))
    assert pks == EXPECTED_PKS
    assert fks == EXPECTED_FKS

    indexes: dict[str, dict[str, list[str]]] = {t: {} for t in EXPECTED_COLUMNS}
    for table, index, cols, is_pk in pg_reader.execute(
        "SELECT t.relname, i.relname, "
        "       (SELECT array_agg(a.attname ORDER BY k.ord) "
        "        FROM unnest(x.indkey::int2[]) WITH ORDINALITY k(attnum, ord) "
        "        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum), "
        "       x.indisprimary "
        "FROM pg_index x "
        "JOIN pg_class t ON t.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid "
        "WHERE t.relnamespace = 'shop'::regnamespace"
    ).fetchall():
        if not is_pk:
            indexes[table][index] = list(cols)
    assert {t: ix for t, ix in indexes.items() if ix} == EXPECTED_EXTRA_INDEXES

    identities = dict(
        pg_reader.execute(
            "SELECT relname, relreplident FROM pg_class "
            "WHERE relnamespace = 'shop'::regnamespace AND relkind = 'r'"
        ).fetchall()
    )
    assert identities == {t: "f" for t in EXPECTED_COLUMNS}, identities

    counts = dict(pg_reader.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
    assert {t: counts[t] for t in CATALOG_COUNTS} == CATALOG_COUNTS
    views = {
        r[0] for r in pg_reader.execute("SELECT viewname FROM pg_views WHERE schemaname = 'sim'")
    }
    assert views == TRUTH_VIEWS, views  # exactly the views of 02 §8, nothing else in sim

    state = read_state(pg_reader)
    assert state["phase"] in ("replay", "done")
    assert state["sim_start"] < state["sim_end"]
    assert state["sim_clock"] is not None and state["events_applied"] > 0


def test_healthcheck_reports_the_phase(simulator_running, stack, pg_reader):
    """01 §5: the container is healthy, its heartbeat file carries the phase of sim.sim_state."""
    assert (
        stack.docker("inspect", "-f", "{{.State.Health.Status}}", "simulator").stdout.strip()
        == "healthy"
    )
    assert stack.exec("simulator", "python", "/app/healthcheck.py", check=False).returncode == 0
    phase_file = stack.exec("simulator", "cat", "/tmp/simulator/heartbeat").stdout.strip()
    assert phase_file == read_state(pg_reader)["phase"]


# --- 01 §8: an interrupted purchase leaves no half order -------------------------------
def _catalog_ids(pg_reader) -> tuple[str, str]:
    """A real product and seller for the item of a synthetic purchase (FKs)."""
    product_id = pg_reader.execute(
        "SELECT product_id FROM shop.products ORDER BY 1 LIMIT 1"
    ).fetchone()[0]
    seller_id = pg_reader.execute(
        "SELECT seller_id FROM shop.sellers ORDER BY 1 LIMIT 1"
    ).fetchone()[0]
    return product_id, seller_id


def _synthetic_purchase(pg_reader, tag: str, *, duplicate_payment: bool = False):
    product_id, seller_id = _catalog_ids(pg_reader)
    return olist_mini.synthetic_purchase(
        product_id, seller_id, tag, duplicate_payment=duplicate_payment
    )


def _remove_test_rows(pg_app) -> None:
    """Belt and braces: no `test-half-*` row may ever stay in the live tables."""
    for table, column in (
        ("order_payments", "order_id"),
        ("order_items", "order_id"),
        ("order_reviews", "order_id"),
        ("orders", "order_id"),
        ("customers", "customer_id"),
    ):
        pg_app.execute(f"DELETE FROM shop.{table} WHERE {column} LIKE 'test-half-%'")


def _traces(pg_reader, oid: str, cid: str) -> dict[str, int]:
    return {
        "customers": pg_reader.execute(
            "SELECT count(*) FROM shop.customers WHERE customer_id = %s", (cid,)
        ).fetchone()[0],
        "orders": pg_reader.execute(
            "SELECT count(*) FROM shop.orders WHERE order_id = %s", (oid,)
        ).fetchone()[0],
        "order_items": pg_reader.execute(
            "SELECT count(*) FROM shop.order_items WHERE order_id = %s", (oid,)
        ).fetchone()[0],
        "order_payments": pg_reader.execute(
            "SELECT count(*) FROM shop.order_payments WHERE order_id = %s", (oid,)
        ).fetchone()[0],
    }


def _shop_app_conninfo(settings, application_name: str) -> dict:
    return {
        "host": clients.PG_HOST,
        "port": clients.PG_PORT,
        "dbname": clients.PG_DB,
        "user": "shop_app",
        "password": settings["SHOP_APP_PASSWORD"],
        "application_name": application_name,
    }


def test_writer_killed_before_its_last_statement_leaves_no_half_order(
    simulator_running, pg_reader, pg_app, settings
):
    """01 §8 (integration): the simulator's own writer (`Database.apply`, 02 §6: one
    transaction per purchase) is SIGKILLed after it executed the customer, order, item and
    payment INSERTs, while blocked on its last statement, the cursor UPDATE of sim.sim_state
    (the test holds that row's lock). Nothing of the purchase stays and the cursor is intact.

    The lock also holds the live simulator for the few seconds of the test; it is released
    in `finally`.
    """
    _remove_test_rows(pg_app)
    product_id, seller_id = _catalog_ids(pg_reader)
    oid, cid = "test-half-writer-order", "test-half-writer-customer"
    conninfo = _shop_app_conninfo(settings, "test-half-writer")
    # pg_stat_activity shows state/wait_event/query only for sessions of the same role (or
    # superusers), hence pg_app for the observations below
    blocked_query = (
        "SELECT query FROM pg_stat_activity WHERE application_name = 'test-half-writer' "
        "AND state = 'active' AND wait_event_type = 'Lock'"
    )
    alive = "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'test-half-writer'"
    lock = clients.pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    lock.execute("SELECT id FROM sim.sim_state WHERE id = 1 FOR UPDATE")  # held until rollback
    proc = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "tests" / "helpers" / "writer_child.py"),
            json.dumps(conninfo),
            product_id,
            seller_id,
            "writer",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": f"{ROOT}{os.pathsep}{SIMULATOR_SRC}"},
    )
    try:
        assert proc.stdout is not None and proc.stderr is not None
        if proc.stdout.readline().strip() != "connected":
            proc.wait(timeout=30)
            pytest.fail(f"writer process did not start: {proc.stderr.read()}")
        blocked = wait_until(
            lambda: pg_app.execute(blocked_query).fetchone(),
            timeout=30,
            every=0.2,
            desc="writer blocked on its last statement (sim_state cursor UPDATE)",
        )
        assert blocked[0].startswith('UPDATE "sim"."sim_state" SET sim_clock'), blocked
        assert proc.poll() is None, "writer process ended before being killed"
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)
        lock.rollback()  # releases the writer's backend and the live simulator
        lock.close()
    assert proc.returncode == -signal.SIGKILL, (proc.returncode, proc.stderr.read())
    wait_until(
        lambda: pg_app.execute(alive).fetchone()[0] == 0,
        timeout=60,
        desc="killed writer's backend gone",
    )
    _remove_test_rows(pg_app)  # nothing to remove when the claim holds
    assert _traces(pg_reader, oid, cid) == {
        "customers": 0,
        "orders": 0,
        "order_items": 0,
        "order_payments": 0,
    }
    assert read_state(pg_reader)["cursor_order_id"] != oid


def test_purchase_failing_on_its_last_statement_is_rolled_back_entirely(
    simulator_running, pg_reader, pg_app, settings
):
    """02 §6: the writer applies a purchase as one transaction; a failure on the last INSERT
    (duplicate payment PK) undoes the customer, order and items as well, and the cursor."""
    _remove_test_rows(pg_app)
    purchase, renderer, oid, cid = _synthetic_purchase(pg_reader, "fail", duplicate_payment=True)
    db = Database(_shop_app_conninfo(settings, "test-half-fail"))
    db.connect()
    try:
        before = read_state(pg_reader)
        with pytest.raises(errors.UniqueViolation):
            db.apply([purchase], renderer)
        assert db.conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    finally:
        db.close()
        _remove_test_rows(pg_app)
    assert _traces(pg_reader, oid, cid) == {
        "customers": 0,
        "orders": 0,
        "order_items": 0,
        "order_payments": 0,
    }
    after = read_state(pg_reader)
    assert after["cursor_order_id"] != oid
    assert after["events_applied"] >= before["events_applied"]


# --- 01 §8: kill + restart resumes from sim_state without duplicates ---------------------
def _expected_after(cursor_state: dict) -> ev.Summary:
    """What an uninterrupted run leaves in the database at this cursor: the preload plus
    every replay event up to and including the cursor, folded by the pure event model."""
    tables = load_dir(DATA)
    ds = ev.Dataset(tables)
    events = ev.build_events(ds.build_orders(), ds, until=cursor_state["sim_end"])
    events = ev.with_schema_evolution(events, cursor_state["schema_evolution_at"])
    preload, replay = ev.split_window(events, cursor_state["sim_start"], cursor_state["sim_end"])
    cursor = (
        cursor_state["sim_clock"],
        cursor_state["cursor_order_id"],
        cursor_state["cursor_step"],
    )
    if cursor_state["phase"] == "preload":
        applied = [e for e in preload if e.key <= cursor]
    else:
        applied = preload + [e for e in replay if e.key <= cursor]
    return ev.summarize(applied, ds)


def _snapshot(pg_reader) -> tuple[dict, dict[str, int], dict[str, tuple]]:
    """State, row counts and per-status totals read in one REPEATABLE READ snapshot."""
    with pg_reader.transaction():
        pg_reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        state = read_state(pg_reader)
        counts = dict(pg_reader.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
        by_status = {
            r[0]: (r[1], r[2], r[3])
            for r in pg_reader.execute(
                "SELECT order_status, orders, items, revenue FROM sim.v_truth_orders"
            )
        }
    return state, counts, by_status


def assert_matches_uninterrupted_run(pg_reader) -> dict:
    state, counts, by_status = _snapshot(pg_reader)
    expected = _expected_after(state)
    assert expected.by_status is not None
    assert state["events_applied"] == expected.events
    assert (state["rows_inserted"], state["rows_updated"], state["rows_deleted"]) == (
        expected.inserts,
        expected.updates,
        expected.deletes,
    )
    assert state["orders_inserted"] == expected.orders
    assert {t: counts[t] for t in ev.LIFECYCLE_TABLES} == {
        "customers": expected.customers,
        "orders": expected.orders,
        "order_items": expected.order_items,
        "order_payments": expected.order_payments,
        "order_reviews": expected.order_reviews,
    }
    assert {t: counts[t] for t in CATALOG_COUNTS} == CATALOG_COUNTS
    if expected.schema_evolved:  # 01 §7: only when SIM_SCHEMA_EVOLUTION_AT is set and passed
        channels = dict(
            pg_reader.execute(
                "SELECT delivery_channel, count(*) FROM shop.orders "
                "WHERE delivery_channel IS NOT NULL GROUP BY 1"
            ).fetchall()
        )
        assert channels == expected.delivery_channels
    assert by_status == {s: (t.orders, t.items, t.revenue) for s, t in expected.by_status.items()}
    return state


def test_database_state_equals_the_pure_fold_of_events_up_to_the_cursor(
    simulator_running, pg_reader
):
    """01 §5: at any moment the tables are exactly the preload plus the replay events up to
    the cursor in sim.sim_state (counts, rows c/u/d, orders per status with revenue)."""
    if not (DATA / "olist_orders_dataset.csv").exists():
        pytest.skip("data/olist missing: run `make data`")
    assert_matches_uninterrupted_run(pg_reader)


@pytest.mark.slow
def test_kill_and_restart_resumes_from_sim_state_without_duplicates(
    simulator_running, stack, pg_reader
):
    """01 §8 (integration): `kill` the simulator and restart it → the clock continues from
    sim_state, no event is applied twice or lost: the database equals what an uninterrupted
    run produces at the new cursor (the pure fold), and the counters continue."""
    if not (DATA / "olist_orders_dataset.csv").exists():
        pytest.skip("data/olist missing: run `make data`")
    before = read_state(pg_reader)
    live = before["phase"] == "replay"
    if live:
        wait_until(
            lambda: read_state(pg_reader)["events_applied"] > before["events_applied"],
            timeout=180,
            every=1,
            desc="simulator applying events before the kill",
        )
    stack.kill("simulator")  # SIGKILL; with restart: unless-stopped Docker does not bring it back
    wait_until(
        lambda: stack.container_name("simulator") and _state_of(stack, "simulator") == "exited",
        timeout=30,
        every=1,
        desc="simulator container exited",
    )
    at_kill = read_state(pg_reader)
    logs = _start_and_wait_resumed(stack)
    assert "initialized" not in logs, logs
    if live:
        # 01 §5: the simulated clock restarts from the stored sim_clock, not from SIM_START
        # (the `>=` checks below would also hold for a clock restarted at SIM_START)
        assert f"replay: clock at {at_kill['sim_clock']}," in logs, logs
    if live:
        wait_until(
            lambda: read_state(pg_reader)["events_applied"] > at_kill["events_applied"],
            timeout=180,
            every=1,
            desc="simulator applying events after the restart",
        )
    after = assert_matches_uninterrupted_run(pg_reader)
    assert after["events_applied"] >= at_kill["events_applied"]
    assert after["sim_clock"] >= at_kill["sim_clock"]
    assert after["sim_start"] == before["sim_start"] and after["sim_end"] == before["sim_end"]
    if live:
        assert (
            after["sim_clock"] > at_kill["sim_clock"]
            or after["cursor_order_id"] != at_kill["cursor_order_id"]
        )


def _logs_since(stack, since: str) -> str:
    """Log lines emitted after `since` (RFC 3339): only the run started after that moment,
    so an earlier run that also resumed can never be mistaken for the current one."""
    return stack.docker("logs", "--since", since, "simulator").stdout


def _logs_since_restart(stack, since: str) -> str | None:
    logs = _logs_since(stack, since)
    return logs if "resuming: phase=" in logs else None


def _start_and_wait_resumed(stack, env: dict[str, str] | None = None) -> str:
    """`docker compose up -d simulator` (with extra env for compose substitution) and wait
    until the new run logged its resume position and the container is healthy again."""
    since = datetime.now(UTC).isoformat()
    subprocess.run(
        ["docker", "compose", "up", "-d", "simulator"],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        check=True,
        capture_output=True,
        text=True,
    )
    wait_until(
        lambda: _state_of(stack, "simulator") == "running", timeout=60, every=1, desc="running"
    )
    logs = wait_until(
        lambda: _logs_since_restart(stack, since),
        timeout=120,
        every=2,
        desc="restarted simulator logged its resume position",
    )
    wait_until(
        lambda: _health(stack) == "healthy", timeout=180, every=2, desc="simulator healthy again"
    )
    return logs


def _health(stack) -> str:
    return stack.docker("inspect", "-f", "{{.State.Health.Status}}", "simulator").stdout.strip()


def _wait_live_progress(pg_reader, state: dict) -> None:
    wait_until(
        lambda: read_state(pg_reader)["events_applied"] > state["events_applied"],
        timeout=180,
        every=1,
        desc="simulator applying events",
    )


def _state_of(stack, service: str) -> str:
    for row in stack.ps():
        if row.service == service:
            return row.state
    return "missing"


def test_sim_clock_is_a_naive_timestamp_in_the_window(simulator_running, pg_reader):
    """sim_state.sim_clock is the (naive, Brazil local) time of the last applied event and lies
    before SIM_END; the cursor columns identify one event."""
    state = read_state(pg_reader)
    assert isinstance(state["sim_clock"], datetime) and state["sim_clock"].tzinfo is None
    assert state["sim_clock"] <= state["sim_end"]
    assert state["cursor_order_id"] and state["cursor_step"] is not None


# --- 01 §2: refusal to start on a half-initialized database -----------------------------
def test_refuses_to_start_when_tables_exist_without_a_state_row(
    simulator_running, pg_reader, settings
):
    """01 §2: tables `shop.*` exist but sim.sim_state has no row → the simulator refuses to
    start (RuntimeError naming `make reset`) before touching anything.

    Exercised through the engine's own phase runner against the live database: the state
    table is looked up in a schema where it does not exist (`shop.sim_state`), which is the
    same `load_state() is None` path as a missing row; `shop.*` tables do exist.
    """
    cfg = Config.from_env(
        {
            "PG_HOST": clients.PG_HOST,
            "PG_PORT": str(clients.PG_PORT),
            "PG_DB": clients.PG_DB,
            "PG_USER": "shop_app",
            "PG_PASSWORD": settings["SHOP_APP_PASSWORD"],
            "SIM_HEARTBEAT_FILE": "-",
        }
    )
    db = Database(cfg.conninfo, state_schema="shop")
    tables = olist_mini.tables()
    schemas, _ = infer_schemas(tables, force_nullable={"orders": ev.FUTURE_COLUMNS})
    ds = ev.Dataset(tables)
    loaded = Loaded(schemas, ds, [], Renderer(schemas, ds))
    before = read_state(pg_reader)
    try:
        with pytest.raises(RuntimeError, match=r"exist in shop but sim\.sim_state has no row"):
            Simulator(cfg, db)._run_phases(loaded)
        assert db.existing_tables(["sim_state"]) == set(), "no state table may be created"
    finally:
        db.close()
    assert read_state(pg_reader)["sim_start"] == before["sim_start"]


# --- 01 §5: SIGTERM stops after the current transaction; restart resumes --------------------
@pytest.mark.slow
def test_sigterm_stops_after_the_current_transaction_and_restart_resumes(
    simulator_running, stack, pg_reader
):
    """01 §5: `docker compose stop` (SIGTERM, 10 s grace) ends the run with exit code 0 after
    the current transaction (cursor consistent with the pure fold); `up -d` resumes it with
    no duplicates and no loss."""
    if not (DATA / "olist_orders_dataset.csv").exists():
        pytest.skip("data/olist missing: run `make data`")
    before = read_state(pg_reader)
    live = before["phase"] == "replay"
    if live:
        _wait_live_progress(pg_reader, before)
    since_stop = datetime.now(UTC).isoformat()
    stack.run("stop", "simulator")
    wait_until(
        lambda: _state_of(stack, "simulator") == "exited", timeout=30, every=1, desc="exited"
    )
    exit_code = stack.docker("inspect", "-f", "{{.State.ExitCode}}", "simulator").stdout.strip()
    assert exit_code == "0", f"SIGTERM must end the simulator cleanly, exit code {exit_code}"
    stopped_run = _logs_since(stack, since_stop)
    assert "SIGTERM received: stopping after the current transaction" in stopped_run, stopped_run
    at_stop = assert_matches_uninterrupted_run(pg_reader)
    logs = _start_and_wait_resumed(stack)
    assert "initialized" not in logs, logs
    if live:
        assert f"replay: clock at {at_stop['sim_clock']}," in logs, logs
        _wait_live_progress(pg_reader, at_stop)
    after = assert_matches_uninterrupted_run(pg_reader)
    assert after["events_applied"] >= at_stop["events_applied"]
    assert after["sim_start"] == before["sim_start"] and after["sim_end"] == before["sim_end"]


# --- 01 §5: a stale heartbeat makes the container unhealthy --------------------------------
def _failing_streak(stack) -> int:
    return int(stack.docker("inspect", "-f", "{{.State.Health.FailingStreak}}", "simulator").stdout)


@pytest.mark.slow  # slow: >= 30 s by design (staleness threshold + healthcheck interval)
def test_stale_heartbeat_fails_the_healthcheck(simulator_running, stack):
    """01 §5: when the process stops beating (frozen with SIGSTOP), `healthcheck.py` fails once
    the heartbeat is older than 30 s and Docker records failing checks; after SIGCONT the
    heartbeat resumes and the container is healthy again."""
    assert _health(stack) == "healthy" and _failing_streak(stack) == 0
    stack.docker("kill", "--signal=SIGSTOP", "simulator")
    try:
        wait_until(
            lambda: _failing_streak(stack) >= 1,
            timeout=90,
            every=2,
            desc="Docker healthcheck failing on the stale heartbeat",
        )
        assert stack.exec("simulator", "python", "/app/healthcheck.py", check=False).returncode == 1
    finally:
        stack.docker("kill", "--signal=SIGCONT", "simulator")
    wait_until(
        lambda: _failing_streak(stack) == 0 and _health(stack) == "healthy",
        timeout=60,
        every=2,
        desc="healthcheck passing again after SIGCONT",
    )
    assert stack.exec("simulator", "python", "/app/healthcheck.py", check=False).returncode == 0


# --- 01 §4/§9: a changed SIM_START after the first start is ignored ---------------------------
@pytest.mark.slow
def test_changed_sim_start_on_restart_is_ignored_with_a_warning(
    simulator_running, stack, pg_reader
):
    """01 §4/§9: restarting with another SIM_START logs a warning and keeps the stored window
    (the preload already happened with it); the state and the pure-fold equality are intact.
    The container is recreated with the original environment at the end."""
    if not (DATA / "olist_orders_dataset.csv").exists():
        pytest.skip("data/olist missing: run `make data`")
    before = read_state(pg_reader)
    other = (before["sim_start"] + timedelta(days=27)).strftime("%Y-%m-%d")
    assert other != before["sim_start"].strftime("%Y-%m-%d")
    try:
        logs = _start_and_wait_resumed(stack, env={"SIM_START": other})
        assert (
            f"SIM_START={other} 00:00:00 differs from the stored {before['sim_start']}: "
            "keeping the stored one"
        ) in logs, logs
        assert "initialized" not in logs and "SIM_END changed" not in logs, logs
        after = assert_matches_uninterrupted_run(pg_reader)
        assert after["sim_start"] == before["sim_start"] and after["sim_end"] == before["sim_end"]
    finally:
        logs = _start_and_wait_resumed(stack)  # back to the compose/.env environment
    assert "differs from the stored" not in logs, logs
    assert read_state(pg_reader)["sim_start"] == before["sim_start"]


# --- 01 §7: synthetic schema evolution through the real writer, rolled back ------------------
def test_schema_evolution_event_adds_a_nullable_column_and_fills_it_rolled_back(
    simulator_running, pg_reader, pg_app, settings
):
    """01 §7 (integration): the writer applies the schema event and a later purchase in one
    outer transaction that is rolled back at the end, so the shared stack (and Debezium,
    which never sees rolled-back changes) is unaffected: `delivery_channel` is added as a
    nullable TEXT column, keeps REPLICA IDENTITY FULL, the purchase fills it with the
    deterministic channel, and the cursor moves with it; after the rollback nothing remains.
    The live run keeps SIM_SCHEMA_EVOLUTION_AT off (default)."""
    column_sql = (
        "SELECT format_type(atttypid, atttypmod), attnotnull FROM pg_attribute "
        "WHERE attrelid = 'shop.orders'::regclass AND attname = 'delivery_channel' "
        "AND NOT attisdropped"
    )
    if pg_reader.execute(column_sql).fetchone() is not None:
        pytest.skip("shop.orders.delivery_channel exists: the Debezium evolution test is running")
    assert read_state(pg_reader)["schema_evolution_at"] is None  # default run: off
    _remove_test_rows(pg_app)
    purchase, renderer, oid, _cid = _synthetic_purchase(pg_reader, "evolution")
    events = ev.with_schema_evolution([purchase], purchase.ts - timedelta(seconds=1))
    assert [e.kind for e in events] == ["schema", "purchase"] and events[1].channel
    db = Database(_shop_app_conninfo(settings, "test-half-evolution"))
    db.connect()
    try:
        conn = db.conn
        with conn.transaction(force_rollback=True):
            # short lock waits: never hang the shared stack behind another session
            conn.execute("SET LOCAL lock_timeout = '5s'")
            db.apply(events, renderer)
            assert conn.execute(column_sql).fetchone() == ("text", False)
            assert conn.execute(
                "SELECT relreplident FROM pg_class WHERE oid = 'shop.orders'::regclass"
            ).fetchone() == ("f",)
            row = conn.execute(
                "SELECT order_status, delivery_channel FROM shop.orders WHERE order_id = %s",
                (oid,),
            ).fetchone()
            assert row == ("created", ev.delivery_channel(oid))
            cursor = conn.execute(
                "SELECT sim_clock, cursor_order_id, cursor_step FROM sim.sim_state"
            ).fetchone()
            assert cursor == events[-1].key
    finally:
        db.close()
        _remove_test_rows(pg_app)
    assert pg_reader.execute(column_sql).fetchone() is None
    assert _traces(pg_reader, oid, _cid)["orders"] == 0
    assert read_state(pg_reader)["cursor_order_id"] != oid
