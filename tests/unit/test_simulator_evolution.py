"""Business simulator, synthetic schema evolution (01 §7, §8 unit row): the ALTER TABLE
event in the deterministic stream, delivery_channel on later purchases, the SQL rendered
for both, the expected-state fold and the configuration. No Docker."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from simulator import events as ev
from simulator.config import Config, ConfigError
from simulator.render import Renderer
from simulator.schema import infer_schemas
from tests.helpers import olist_mini

pytestmark = pytest.mark.unit

T = datetime.fromisoformat
AT = T("2017-10-04 00:00:00")  # after o1/o2's purchases, before o3's (10-04 12:00) and o4's


def build():
    tables = olist_mini.tables()
    ds = ev.Dataset(tables)
    schemas, _ = infer_schemas(tables, force_nullable={"orders": ev.FUTURE_COLUMNS})
    return ds, Renderer(schemas, ds), ev.build_events(ds.build_orders(), ds)


def test_off_leaves_the_stream_unchanged():
    """01 §7: SIM_SCHEMA_EVOLUTION_AT empty (default) → identical stream, no channel."""
    _, _, events = build()
    assert ev.with_schema_evolution(events, None) is events
    assert all(e.channel is None and e.kind != "schema" for e in events)
    assert Config.from_env({}).schema_evolution_at is None


def test_schema_event_sits_at_its_moment_and_only_later_purchases_get_a_channel():
    """01 §7: one ALTER TABLE event at the configured moment, sorted before every order event
    of that second; purchases after it carry a deterministic delivery_channel, earlier ones not."""
    _, _, events = build()
    evolved = ev.with_schema_evolution(events, AT)
    assert len(evolved) == len(events) + 1
    keys = [e.key for e in evolved]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)
    schema = [e for e in evolved if e.kind == "schema"]
    assert len(schema) == 1 and schema[0].key == (AT, "", 0) and schema[0].rows == 0
    pos = evolved.index(schema[0])
    purchases = [(i, e) for i, e in enumerate(evolved) if e.kind == "purchase"]
    assert {e.order_id for i, e in purchases if i < pos} == {"o1", "o2"}
    assert all(e.channel is None for i, e in purchases if i < pos)
    later = [e for i, e in purchases if i > pos]
    assert [e.order_id for e in later] == ["o3", "o4"]
    assert [e.channel for e in later] == [ev.delivery_channel("o3"), ev.delivery_channel("o4")]
    assert {e.channel for e in later} <= set(ev.DELIVERY_CHANNELS)
    # apart from the channel, the original events are kept in order
    stripped = [e if e.channel is None else replace(e, channel=None) for e in evolved]
    assert [e for e in stripped if e.kind != "schema"] == events
    assert all(e.channel is None for e in evolved if e.kind != "purchase")


def test_same_second_purchase_comes_after_the_schema_event():
    _, _, events = build()
    o1 = next(e for e in events if e.kind == "purchase" and e.order_id == "o1")
    evolved = ev.with_schema_evolution(events, o1.ts)  # evolution in the same second as o1
    i_schema = next(i for i, e in enumerate(evolved) if e.kind == "schema")
    i_o1 = next(i for i, e in enumerate(evolved) if e.kind == "purchase" and e.order_id == "o1")
    assert i_schema < i_o1 and evolved[i_o1].channel == ev.delivery_channel("o1")


def test_delivery_channel_is_deterministic_and_covers_all_values():
    assert ev.delivery_channel("abc") == ev.delivery_channel("abc")
    seen = {ev.delivery_channel(f"order-{i}") for i in range(200)}
    assert seen == set(ev.DELIVERY_CHANNELS)


def test_rendered_sql_adds_a_nullable_column_and_fills_it():
    """01 §7 / 03 §5: `ADD COLUMN IF NOT EXISTS delivery_channel TEXT NULL` (nullable, so the
    AVRO schema change is BACKWARD-compatible); later purchases INSERT the channel."""
    _, renderer, events = build()
    evolved = ev.with_schema_evolution(events, AT)
    schema = next(e for e in evolved if e.kind == "schema")
    assert renderer.statements(schema) == [
        ('ALTER TABLE "shop"."orders" ADD COLUMN IF NOT EXISTS "delivery_channel" TEXT NULL', ())
    ]
    o4 = next(e for e in evolved if e.kind == "purchase" and e.order_id == "o4")
    sql, params = renderer.statements(o4)[1]
    assert sql.startswith('INSERT INTO "shop"."orders" (')
    assert sql.split(") VALUES")[0].endswith('"order_estimated_delivery_date", "delivery_channel"')
    assert sql.count("%s") == len(params) == 9
    assert params[2] == "created" and params[4:7] == (None, None, None)
    assert params[-1] == ev.delivery_channel("o4")
    o1 = next(e for e in evolved if e.kind == "purchase" and e.order_id == "o1")
    assert "delivery_channel" not in renderer.statements(o1)[1][0]


def test_fold_counts_the_evolution_and_the_channels_exactly():
    """The pure fold stays exact: one more event, no row changes, channel per later order."""
    ds, _, events = build()
    plain, evolved = (
        ev.summarize(events, ds),
        ev.summarize(ev.with_schema_evolution(events, AT), ds),
    )
    assert evolved.events == plain.events + 1
    assert (evolved.inserts, evolved.updates, evolved.deletes) == (
        plain.inserts,
        plain.updates,
        plain.deletes,
    )
    assert not plain.schema_evolved and plain.delivery_channels == {}
    assert evolved.schema_evolved
    expected: dict[str, int] = {}
    for oid in ("o3", "o4"):
        expected[ev.delivery_channel(oid)] = expected.get(ev.delivery_channel(oid), 0) + 1
    assert evolved.delivery_channels == expected
    assert evolved.by_status is not None and plain.by_status is not None
    assert {k: (v.orders, v.items, v.revenue) for k, v in evolved.by_status.items()} == {
        k: (v.orders, v.items, v.revenue) for k, v in plain.by_status.items()
    }
    # resuming after the schema event never replays it
    marker = next(e for e in ev.with_schema_evolution(events, AT) if e.kind == "schema")
    rest = list(ev.after_cursor(ev.with_schema_evolution(events, AT), marker.key))
    assert all(e.kind != "schema" for e in rest) and rest[0].ts >= AT


def test_config_parses_the_evolution_moment():
    assert Config.from_env({"SIM_SCHEMA_EVOLUTION_AT": "2017-02-01"}).schema_evolution_at == T(
        "2017-02-01 00:00:00"
    )
    assert Config.from_env(
        {"SIM_SCHEMA_EVOLUTION_AT": "2017-02-01 12:30:00"}
    ).schema_evolution_at == T("2017-02-01 12:30:00")
    assert Config.from_env({"SIM_SCHEMA_EVOLUTION_AT": "  "}).schema_evolution_at is None
    with pytest.raises(ConfigError, match="not a date"):
        Config.from_env({"SIM_SCHEMA_EVOLUTION_AT": "soon"})


# --- resume rules for a changed setting (engine._reconcile_schema_evolution) -------------
class FakeDb:
    def __init__(self, state):
        self.state = state
        self.writes: list[datetime | None] = []

    def set_schema_evolution(self, at):
        self.writes.append(at)
        self.state = replace(self.state, schema_evolution_at=at)

    def load_state(self):
        return self.state


def _state(cursor_ts: str | None, stored: str | None):
    from simulator.db import SimState

    return SimState(
        phase="replay",
        sim_start=T("2017-01-05 00:00:00"),
        sim_end=T("2017-03-31 23:59:59"),
        sim_speed=1440,
        sim_clock=T(cursor_ts) if cursor_ts else None,
        cursor_order_id="x" if cursor_ts else None,
        cursor_step=0 if cursor_ts else None,
        events_applied=0,
        orders_inserted=0,
        rows_inserted=0,
        rows_updated=0,
        rows_deleted=0,
        schema_evolution_at=T(stored) if stored else None,
    )


@pytest.mark.parametrize(
    ("cursor", "stored", "wanted", "result", "written"),
    [
        # not applied yet: the setting may be switched on, moved or switched off
        ("2017-01-10 00:00:00", None, "2017-02-01", "2017-02-01 00:00:00", True),
        ("2017-01-10 00:00:00", "2017-02-01 00:00:00", "2017-03-01", "2017-03-01 00:00:00", True),
        ("2017-01-10 00:00:00", "2017-02-01 00:00:00", "", None, True),
        # already applied: fixed, whatever the setting says now
        ("2017-02-10 00:00:00", "2017-02-01 00:00:00", "", "2017-02-01 00:00:00", False),
        ("2017-02-10 00:00:00", "2017-02-01 00:00:00", "2017-03-01", "2017-02-01 00:00:00", False),
        # a moment behind the cursor cannot be inserted into the past
        ("2017-02-10 00:00:00", None, "2017-02-01", None, False),
        # unchanged
        ("2017-02-10 00:00:00", "2017-02-01 00:00:00", "2017-02-01", "2017-02-01 00:00:00", False),
    ],
)
def test_changed_setting_on_restart_follows_the_resume_rules(
    cursor, stored, wanted, result, written
):
    """01 §7/§9: before the cursor passed the stored moment it can change; afterwards it is
    fixed; a new moment behind the cursor is ignored. The stream stays a pure function of
    the stored value, so the resume cursor and the fold stay exact."""
    from simulator.engine import Simulator

    db = FakeDb(_state(cursor, stored))
    sim = Simulator(Config.from_env({"SIM_SCHEMA_EVOLUTION_AT": wanted}), db)  # type: ignore[arg-type]
    state = sim._reconcile_schema_evolution(db.state)
    assert state.schema_evolution_at == (T(result) if result else None)
    assert bool(db.writes) == written
