"""The simulator's pure fold as the expected database state (01 §5, §8).

The event stream is deterministic, so the state after an uninterrupted run up to the
cursor in ``sim.sim_state`` is by definition the preload plus every replay event up to
that cursor folded by ``simulator.events.summarize``. Shared by the integration test of
the simulator and the e2e tests (end of window, live schema evolution).
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from simulator import events as ev
from simulator.csvio import load_dir

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "olist"

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
CATALOG_COUNTS = {"products": 32951, "sellers": 3095, "product_category_name_translation": 71}

_DATASET: ev.Dataset | None = None


def data_present() -> bool:
    return (DATA / "olist_orders_dataset.csv").exists()


def read_state(conn: psycopg.Connection) -> dict:
    row = conn.execute(
        f"SELECT {', '.join(STATE_COLUMNS)} FROM sim.sim_state WHERE id = 1"
    ).fetchone()
    assert row is not None, "sim.sim_state has no row"
    return dict(zip(STATE_COLUMNS, row, strict=True))


def _dataset() -> ev.Dataset:
    global _DATASET
    if _DATASET is None:
        _DATASET = ev.Dataset(load_dir(DATA))
    return _DATASET


def expected_after(state: dict) -> ev.Summary:
    """What an uninterrupted run leaves in the database at this cursor: the preload plus
    every replay event up to and including the cursor, folded by the pure event model."""
    ds = _dataset()
    events = ev.build_events(ds.build_orders(), ds, until=state["sim_end"])
    events = ev.with_schema_evolution(events, state["schema_evolution_at"])
    preload, replay = ev.split_window(events, state["sim_start"], state["sim_end"])
    cursor = (state["sim_clock"], state["cursor_order_id"], state["cursor_step"])
    if state["phase"] == "preload":
        applied = [e for e in preload if e.key <= cursor]
    else:
        applied = preload + [e for e in replay if e.key <= cursor]
    return ev.summarize(applied, ds)


def snapshot(pg: psycopg.Connection) -> tuple[dict, dict[str, int], dict[str, tuple]]:
    """State, row counts and per-status totals read in one REPEATABLE READ snapshot."""
    with pg.transaction():
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        state = read_state(pg)
        counts = dict(pg.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
        by_status = {
            r[0]: (r[1], r[2], r[3])
            for r in pg.execute(
                "SELECT order_status, orders, items, revenue FROM sim.v_truth_orders"
            )
        }
    return state, counts, by_status


def delivery_channels(pg: psycopg.Connection) -> dict[str, int]:
    return dict(
        pg.execute(
            "SELECT delivery_channel, count(*) FROM shop.orders "
            "WHERE delivery_channel IS NOT NULL GROUP BY 1"
        ).fetchall()
    )


def assert_matches_uninterrupted_run(pg: psycopg.Connection) -> dict:
    """Counters, row counts, orders per status with revenue and (once the evolution moment
    passed) orders per delivery channel equal the pure fold at the stored cursor."""
    state, counts, by_status = snapshot(pg)
    expected = expected_after(state)
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
        assert delivery_channels(pg) == expected.delivery_channels
    assert by_status == {s: (t.orders, t.items, t.revenue) for s, t in expected.by_status.items()}
    return state
