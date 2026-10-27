"""Validation rules of the AVRO consumer on hand-made events (04-kafka.md §6, §8).

04 §8 row "jednostkowy: reguły walidacji consumera na spreparowanych zdarzeniach →
każda reguła wykrywa swoje naruszenie, poprawne zdarzenia przechodzą". No Docker:
the package `consumer` (containers/consumer) is importable through pyproject's
pythonpath; the rules are pure functions over decoded envelopes.
"""

from __future__ import annotations

import struct
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from consumer import rules
from consumer.model import Message, OrderTracker, Stats, describe

pytestmark = pytest.mark.unit

T0 = datetime(2017, 1, 5, 15, 32, 59)


def ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=UTC).timestamp() * 1000)


def order_row(status: str = "created", **extra) -> dict:
    row = {
        "order_id": "e1fe072ef14b519af1f0a8ed997c1301",
        "customer_id": "758b633d88b82063db189810084f4ea9",
        "order_status": status,
        "order_purchase_timestamp": ms(T0),
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": ms(T0 + timedelta(days=40)),
    }
    row.update(extra)
    return row


def envelope(
    op: str, before: dict | None, after: dict | None, table: str = "orders", lsn: int = 33463976
) -> dict:
    return {
        "before": before,
        "after": after,
        "source": {"schema": "shop", "table": table, "lsn": lsn, "txId": 805},
        "transaction": {"id": "805:33463976", "total_order": 1, "data_collection_order": 1},
        "op": op,
        "ts_ms": 1792524523668,
    }


# --- rule 1: full `before` on UPDATE/DELETE (REPLICA IDENTITY FULL) ---------------------
def test_update_and_delete_need_before():
    ok = envelope("u", order_row("created"), order_row("approved"))
    assert rules.check_before_present("orders", ok) is None
    for op in ("u", "d"):
        bad = envelope(op, None, order_row("approved") if op == "u" else None)
        v = rules.check_before_present("orders", bad)
        assert v is not None and v.rule == "before_present" and f"op={op}" in v.message
    # inserts and snapshot reads legitimately have no before
    assert rules.check_before_present("orders", envelope("c", None, order_row())) is None
    assert rules.check_before_present("orders", envelope("r", None, order_row())) is None


# --- rule 2: status transitions ----------------------------------------------------------
@pytest.mark.parametrize(
    "old,new",
    [
        ("created", "approved"),
        ("approved", "shipped"),
        ("shipped", "delivered"),
        ("created", "shipped"),  # approval date missing in the data: step skipped (01 §4)
        ("approved", "invoiced"),
        ("approved", "processing"),
        ("approved", "unavailable"),
        ("approved", "delivered"),
        ("delivered", "canceled"),  # 6 canceled orders were delivered first (01 §4)
        ("created", "canceled"),
        ("shipped", "shipped"),  # no-op UPDATE (same values) is not a transition
    ],
)
def test_allowed_status_transitions(old, new):
    ev = envelope("u", order_row(old), order_row(new))
    assert rules.check_status_transition("orders", ev) is None


@pytest.mark.parametrize(
    "old,new",
    [
        ("delivered", "created"),  # the design's own example
        ("delivered", "shipped"),
        ("shipped", "approved"),
        ("shipped", "processing"),  # same rank, different branch
        ("canceled", "delivered"),  # canceled is terminal and has no rank
        ("approved", "bogus"),
    ],
)
def test_forbidden_status_transitions(old, new):
    ev = envelope("u", order_row(old), order_row(new))
    v = rules.check_status_transition("orders", ev)
    assert v is not None and v.rule == "status_transition", (old, new)


def test_status_rule_only_for_orders_updates():
    ev = envelope("u", order_row("delivered"), order_row("created"), table="order_items")
    assert rules.check_status_transition("order_items", ev) is None
    assert rules.check_status_transition("orders", envelope("c", None, order_row())) is None


# --- rule 3: amounts >= 0 ----------------------------------------------------------------
def item(price, freight) -> dict:
    return {
        "order_id": "o1",
        "order_item_id": 1,
        "product_id": "p",
        "seller_id": "s",
        "shipping_limit_date": ms(T0),
        "price": price,
        "freight_value": freight,
    }


def test_amounts_non_negative():
    ok = envelope("c", None, item(Decimal("58.90"), Decimal("13.29")), table="order_items")
    assert rules.check_amounts_non_negative("order_items", ok) is None
    zero = envelope("c", None, item(Decimal("0.00"), 0), table="order_items")
    assert rules.check_amounts_non_negative("order_items", zero) is None  # 9 payments are 0
    bad = envelope("c", None, item(Decimal("-1.00"), Decimal("13.29")), table="order_items")
    v = rules.check_amounts_non_negative("order_items", bad)
    assert v is not None and v.rule == "amount_non_negative" and "price" in v.message
    pay = envelope("c", None, {"order_id": "o1", "payment_value": "-0.01"}, "order_payments")
    assert rules.check_amounts_non_negative("order_payments", pay) is not None
    # a DELETE carries only `before`: history is not re-validated
    gone = envelope("d", item(Decimal("-5"), 0), None, table="order_items")
    assert rules.check_amounts_non_negative("order_items", gone) is None


# --- rule 4: dates not after the simulated clock ---------------------------------------
def test_dates_not_in_the_future_against_the_clock():
    clock = T0 + timedelta(hours=2)
    ok = envelope(
        "u", order_row(), order_row("approved", order_approved_at=ms(T0 + timedelta(minutes=42)))
    )
    assert rules.check_dates_not_after_clock("orders", ok, clock) is None
    # the estimate is a future date by nature and is not an event date
    assert ok["after"]["order_estimated_delivery_date"] > ms(clock)
    bad = envelope(
        "u",
        order_row(),
        order_row("shipped", order_delivered_carrier_date=ms(T0 + timedelta(days=40))),
    )
    v = rules.check_dates_not_after_clock("orders", bad, clock)
    assert v is not None and v.rule == "date_not_in_future"
    assert "order_delivered_carrier_date" in v.message
    # within an explicit tolerance (the estimate lags the real clock; default 30 days)
    assert rules.check_dates_not_after_clock("orders", bad, clock, timedelta(days=40)) is None
    # the two-week gap between the preload (2016) and SIM_START (01 §4) is tolerated
    gap = envelope(
        "u",
        order_row(),
        order_row("delivered", order_delivered_customer_date=ms(T0 + timedelta(days=14))),
    )
    assert rules.check_dates_not_after_clock("orders", gap, clock) is None
    # snapshot rows arrive in table order: not checked; no clock yet: not checked
    assert (
        rules.check_dates_not_after_clock("orders", envelope("r", None, bad["after"]), clock)
        is None
    )
    assert rules.check_dates_not_after_clock("orders", bad, None) is None
    # tz-aware datetimes (what AvroDeserializer yields for timestamp-millis) work too
    aware = envelope(
        "c",
        None,
        {"order_id": "o9", "review_creation_date": datetime(2017, 4, 1, tzinfo=UTC)},
        "order_reviews",
    )
    assert rules.check_dates_not_after_clock("order_reviews", aware, clock) is not None


def test_clock_follows_the_latest_event_date():
    clock = rules.advance_clock("orders", envelope("r", None, order_row()), None)
    assert clock == T0
    later = order_row("delivered", order_delivered_customer_date=ms(T0 + timedelta(days=3)))
    clock = rules.advance_clock("orders", envelope("u", order_row(), later), clock)
    assert clock == T0 + timedelta(days=3)
    # an older row (snapshot order) never moves the clock back; estimates and items never move it
    assert rules.advance_clock("orders", envelope("c", None, order_row()), clock) == clock
    assert rules.advance_clock("order_items", envelope("c", None, item(1, 1)), clock) == clock
    review = {"order_id": "o", "review_creation_date": ms(T0 + timedelta(days=9))}
    assert rules.advance_clock(
        "order_reviews", envelope("c", None, review, "order_reviews"), clock
    ) == T0 + timedelta(days=9)


# --- rule 5: schema id resolvable in the registry ----------------------------------------
def wire(schema_id: int, magic: int = 0) -> bytes:
    return bytes([magic]) + struct.pack(">I", schema_id) + b"\x02payload"


def test_schema_id_must_exist_in_registry():
    known = {3, 4, 10}
    assert rules.check_schema_id_known(wire(10), known) is None
    assert rules.check_schema_id_known(wire(10), lambda i: i in known) is None
    v = rules.check_schema_id_known(wire(99), known)
    assert v is not None and v.rule == "schema_id_known" and "99" in v.message
    v = rules.check_schema_id_known(wire(10, magic=1), known)
    assert v is not None and "magic byte" in v.message
    v = rules.check_schema_id_known(b"\x00\x01", known, field="key")
    assert v is not None and v.message.startswith("key:")
    assert rules.check_schema_id_known(None, known) is None  # tombstone
    assert rules.parse_wire_header(wire(10)) == (0, 10)


# --- everything together -----------------------------------------------------------------
def test_validate_event_runs_every_rule_and_clean_events_pass():
    clock = T0 + timedelta(hours=1)
    clean = envelope("u", order_row("created"), order_row("approved", order_approved_at=ms(T0)))
    assert rules.validate_event("orders", clean, clock) == []
    assert rules.validate_event("orders", None, clock) == []  # tombstone
    dirty = envelope(
        "u", None, order_row("created", order_delivered_customer_date=ms(T0 + timedelta(days=60)))
    )
    dirty["before"] = order_row("delivered")
    found = {v.rule for v in rules.validate_event("orders", dirty, clock)}
    assert found == {"status_transition", "date_not_in_future"}
    dirty["before"] = None
    found = {v.rule for v in rules.validate_event("orders", dirty, clock)}
    assert found == {"before_present", "date_not_in_future"}


# --- describe / stats / tracker ---------------------------------------------------------
def msg(
    topic: str, key: dict | None, value: dict | None, offset: int = 0, sid: int | None = 10
) -> Message:
    return Message(
        topic=topic, partition=0, offset=offset, key=key, value=value, value_schema_id=sid
    )


def test_describe_lines_and_tombstone():
    ev = envelope("u", order_row("approved"), order_row("shipped"))
    line = describe(msg("olist.shop.orders", {"order_id": ev["after"]["order_id"]}, ev), 1)
    assert line == "[orders] u e1fe072e approved → shipped lsn=33463976 tx=805:33463976 schema v1"
    tomb = describe(msg("olist.shop.orders", {"order_id": "e1fe072e"}, None, sid=None))
    assert tomb == "[orders] t e1fe072e tombstone"
    items = describe(
        msg(
            "olist.shop.order_items",
            {"order_id": "o1", "order_item_id": 1},
            envelope("c", None, item("9.90", "14.52"), "order_items"),
        )
    )
    assert items.startswith("[order_items] c o1/1 price=9.90 freight=14.52 lsn=")


def test_stats_count_ops_schemas_and_rates():
    s = Stats()
    s.add(msg("olist.shop.orders", {"order_id": "a"}, envelope("c", None, order_row())))
    s.add(
        msg(
            "olist.shop.orders",
            {"order_id": "a"},
            envelope("u", order_row(), order_row("approved")),
            sid=11,
        )
    )
    s.add(msg("olist.shop.orders", {"order_id": "a"}, None, sid=None), violations=1)
    s.set_lag("olist.shop.orders", 7)
    (line,) = s.lines(window_seconds=2.0)
    assert line == "olist.shop.orders 1.5/s c=1 u=1 d=0 r=0 t=1 lag=7 schemas=10,11 violations=1"
    assert s.summary()["topics"]["olist.shop.orders"]["ops"] == {"c": 1, "u": 1, "t": 1}
    assert s.summary()["violations"] == 1
    (line2,) = s.lines(window_seconds=2.0)  # the window counter was reset
    assert line2.startswith("olist.shop.orders 0.0/s")


def test_order_tracker_follows_the_order_and_its_customer():
    oid, cid = order_row()["order_id"], order_row()["customer_id"]
    t = OrderTracker(oid)
    # customers event before the order is known: kept aside, surfaces once the orders
    # event reveals the customer (topics are read independently)
    cust = msg(
        "olist.shop.customers",
        {"customer_id": cid},
        envelope("c", None, {"customer_id": cid}, "customers", lsn=10),
        offset=0,
    )
    other_cust = msg(
        "olist.shop.customers",
        {"customer_id": "someone-else"},
        envelope("c", None, {"customer_id": "someone-else"}, "customers", lsn=9),
        offset=0,
    )
    assert t.record(cust) is False and t.record(other_cust) is False
    assert t.record(
        msg("olist.shop.orders", {"order_id": oid}, envelope("c", None, order_row(), lsn=11), 1)
    )
    assert [m.table for m in t.seen] == ["customers", "orders"]
    assert t.record(other_cust) is False
    items_key = {"order_id": oid, "order_item_id": 1}
    assert t.record(
        msg(
            "olist.shop.order_items",
            items_key,
            envelope("c", None, item(1, 1), "order_items", lsn=12),
            2,
        )
    )
    other = {"order_id": "other", "order_item_id": 1}
    assert (
        t.record(
            msg(
                "olist.shop.order_items",
                other,
                envelope("c", None, item(1, 1), "order_items", lsn=13),
                3,
            )
        )
        is False
    )
    assert t.record(
        msg(
            "olist.shop.orders",
            {"order_id": oid},
            envelope("u", order_row(), order_row("approved"), lsn=14),
            4,
        )
    )
    assert t.record(
        msg(
            "olist.shop.orders",
            {"order_id": oid},
            envelope("d", order_row("approved"), None, lsn=15),
            5,
        )
    )
    assert t.record(msg("olist.shop.orders", {"order_id": oid}, None, 6, sid=None))
    heads = [line.split(" lsn=")[0] for line in t.timeline()]
    assert heads == [
        "[customers] c 758b633d",
        "[orders] c e1fe072e created 1483630379000",
        "[order_items] c e1fe072e/1 price=1 freight=1",
        "[orders] u e1fe072e created → approved",
        "[orders] d e1fe072e was approved",
        "[orders] t e1fe072e tombstone",
    ]


# --- probe tables of the integration tests play the role of the table they mirror ----------
PROBE = "zz_probe_1a2b3c4d"


def test_table_role_maps_probe_tables_to_the_business_table():
    assert rules.table_role(f"{PROBE}_orders") == "orders"
    assert rules.table_role(f"{PROBE}_order_items") == "order_items"
    for own in ("orders", "zz_stage2_probe_1a2b3c4d", "zz_probe_orders", "_stage0_probe"):
        assert rules.table_role(own) == own  # no hex tag: not a probe mirror
    assert msg(f"olist.shop.{PROBE}_customers", None, None).role == "customers"


def test_rules_apply_to_a_probe_table_by_its_role():
    """The forbidden transition of the integration test's bad probe is caught in the
    probe orders table exactly as in shop.orders; amounts and dates likewise."""
    bad = envelope("u", order_row("delivered"), order_row("created"), table=f"{PROBE}_orders")
    assert [str(v) for v in rules.validate_event(f"{PROBE}_orders", bad, None)] == [
        "status_transition: orders: delivered → created is not allowed"
    ]
    neg = envelope("c", None, item(-1, 1), table=f"{PROBE}_order_items")
    assert [v.rule for v in rules.validate_event(f"{PROBE}_order_items", neg, None)] == [
        "amount_non_negative"
    ]
    future = order_row("approved", order_approved_at=ms(T0 + timedelta(days=400)))
    ev = envelope("u", order_row(), future, table=f"{PROBE}_orders")
    assert [v.rule for v in rules.validate_event(f"{PROBE}_orders", ev, T0)] == [
        "date_not_in_future"
    ]
    assert rules.advance_clock(f"{PROBE}_orders", ev, None) == T0 + timedelta(days=400)


def test_order_tracker_and_describe_follow_probe_tables():
    oid, cid = order_row()["order_id"], order_row()["customer_id"]
    t = OrderTracker(oid)
    o, c = f"olist.shop.{PROBE}_orders", f"olist.shop.{PROBE}_customers"
    cust = msg(c, {"customer_id": cid}, envelope("c", None, {"customer_id": cid}, lsn=10))
    assert t.record(cust) is False  # pending until the order reveals its customer
    assert t.record(msg(o, {"order_id": oid}, envelope("c", None, order_row(), lsn=11), 1))
    assert t.record(
        msg(o, {"order_id": oid}, envelope("u", order_row(), order_row("approved"), lsn=12), 2)
    )
    heads = [line.split(" lsn=")[0] for line in t.timeline()]
    assert heads == [
        f"[{PROBE}_customers] c 758b633d",
        f"[{PROBE}_orders] c e1fe072e created 1483630379000",
        f"[{PROBE}_orders] u e1fe072e created → approved",
    ]
