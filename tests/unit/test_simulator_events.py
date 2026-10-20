"""Business simulator, order → events (01 §4, §8 unit rows): event sequence, time
normalization, final statuses, resume cursor and the expected-state fold. No Docker."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from simulator import events as ev
from simulator.render import Renderer
from simulator.schema import infer_schemas
from tests.helpers import olist_mini

pytestmark = pytest.mark.unit

T = datetime.fromisoformat


def build(rows: dict[str, list[tuple[str, ...]]] | None = None):
    """Dataset + renderer over the mini data set, optionally replacing some tables' rows."""
    tables = olist_mini.tables()
    for name, new_rows in (rows or {}).items():
        tables[name].rows = new_rows
    ds = ev.Dataset(tables)
    schemas, _ = infer_schemas(tables, force_nullable={"orders": ev.FUTURE_COLUMNS})
    return ds, Renderer(schemas, ds), {od.order_id: od for od in ds.build_orders()}


def kinds(events: list[ev.Event]) -> list[tuple[str, str | None]]:
    return [(e.kind, e.status) for e in events]


def test_delivered_order_gives_create_update_update_update_review():
    """01 §8: delivered order → c → u(approved) → u(shipped) → u(delivered) → c(review);
    future dates are NULL in the INSERT."""
    ds, renderer, orders = build()
    events = ev.order_events(orders["o1"], ds)
    assert kinds(events) == [
        ("purchase", None),
        ("status", "approved"),
        ("status", "shipped"),
        ("status", "delivered"),
        ("review", None),
        ("review", None),
    ]
    assert [e.step for e in events] == [0, 1, 2, 3, 10, 11]
    purchase = events[0]
    assert purchase.ts == T("2017-10-02 10:56:33")
    assert (purchase.inserts, purchase.updates, purchase.deletes) == (1 + 1 + 2 + 2, 0, 0)
    statements = renderer.statements(purchase)
    assert [s[0].split(" (")[0] for s in statements] == [
        'INSERT INTO "shop"."customers"',
        'INSERT INTO "shop"."orders"',
        'INSERT INTO "shop"."order_items"',
        'INSERT INTO "shop"."order_items"',
        'INSERT INTO "shop"."order_payments"',
        'INSERT INTO "shop"."order_payments"',
    ]
    assert statements[0][0].endswith("ON CONFLICT DO NOTHING")
    order_params = statements[1][1]
    assert order_params == (
        "o1", "c1", "created", "2017-10-02 10:56:33", None, None, None, "2017-10-18 00:00:00"
    )  # fmt: skip
    assert statements[2][1] == ("o1", "1", "p1", "s1", "2017-10-06 11:07:15", "58.90", "13.29")
    delivered = events[3]
    assert renderer.statements(delivered) == [
        (
            'UPDATE "shop"."orders" SET "order_status" = %s, "order_delivered_customer_date" = %s '
            'WHERE "order_id" = %s',
            ("delivered", "2017-10-10 21:25:13", "o1"),
        )
    ]
    assert renderer.statements(events[4])[0][1][:2] == ("r1", "o1")
    assert renderer.statements(events[5])[0][1][:2] == ("r3", "o1")
    assert renderer.statements(events[5])[0][1][:2] == ("r3", "o1")


def test_shipment_before_approval_is_moved_to_the_approval_time():
    """01 §8: shipped before approved → shipped at the approval time; the column keeps the
    original value, only the moment of the event moves."""
    ds, renderer, orders = build()
    events = ev.order_events(orders["o1"], ds)
    approved, shipped = events[1], events[2]
    assert approved.ts == T("2017-10-02 11:07:15")
    assert shipped.ts == approved.ts  # CSV says 2017-10-02 09:00:00
    assert shipped.value == "2017-10-02 09:00:00"
    assert renderer.statements(shipped)[0][1] == ("shipped", "2017-10-02 09:00:00", "o1")


def test_missing_step_date_skips_the_step():
    """01 §8: no date → step skipped (14 delivered orders have no order_approved_at)."""
    row = ("o9", "c1", "delivered", "2017-10-02 10:56:33", "", "2017-10-04 19:55:00",
           "2017-10-10 21:25:13", "2017-10-18 00:00:00")  # fmt: skip
    ds, _, orders = build({"orders": [row]})
    assert kinds(ev.order_events(orders["o9"], ds)) == [
        ("purchase", None),
        ("status", "shipped"),
        ("status", "delivered"),
    ]


def test_review_before_purchase_is_moved_to_the_purchase_time():
    """01 §8: review before purchase → review at the purchase time (74 such reviews)."""
    ds, _, orders = build()
    review = [e for e in ev.order_events(orders["o3"], ds) if e.kind == "review"][0]
    assert review.ts == T("2017-10-04 12:00:00")  # CSV review_creation_date 2017-10-01
    assert review.row is not None and review.row[ds.r_date] == "2017-10-01 00:00:00"


def test_review_after_purchase_keeps_its_time():
    ds, _, orders = build()
    review = [e for e in ev.order_events(orders["o1"], ds) if e.kind == "review"][0]
    assert review.ts == T("2017-10-11 00:00:00")


def test_canceled_order_updates_status_and_deletes_its_items():
    """01 §8: canceled → UPDATE status after the last known step + delay, then DELETE items."""
    ds, renderer, orders = build()
    events = ev.order_events(orders["o3"], ds)
    assert kinds(events) == [
        ("purchase", None),
        ("status", "approved"),
        ("final", "canceled"),
        ("review", None),
    ]
    final = events[2]
    assert final.step == ev.STEP_FINAL
    delay = final.ts - events[1].ts
    assert timedelta(days=1) <= delay < timedelta(days=3)
    assert delay == ev.final_status_delay("o3")
    assert (final.inserts, final.updates, final.deletes) == (0, 1, 1)
    assert renderer.statements(final) == [
        (
            'UPDATE "shop"."orders" SET "order_status" = %s WHERE "order_id" = %s',
            ("canceled", "o3"),
        ),
        ('DELETE FROM "shop"."order_items" WHERE "order_id" = %s', ("o3",)),
    ]


def test_invoiced_order_stays_invoiced_without_delete():
    """01 §8: invoiced → after approval + delay the status is set and nothing else happens."""
    row = ("o9", "c1", "invoiced", "2017-10-02 10:56:33", "2017-10-02 11:07:15", "", "",
           "2017-10-18 00:00:00")  # fmt: skip
    ds, renderer, orders = build({"orders": [row]})
    events = ev.order_events(orders["o9"], ds)
    assert kinds(events) == [("purchase", None), ("status", "approved"), ("final", "invoiced")]
    assert events[-1].deletes == 0
    assert len(renderer.statements(events[-1])) == 1
    assert events[-1].ts == events[1].ts + ev.final_status_delay("o9")


def test_canceled_after_delivery_and_delivered_without_delivery_date():
    """01 §4: 6 canceled orders have a delivery date (canceled after the full chain);
    8 delivered orders have no delivery date (final UPDATE reaches `delivered`)."""
    canceled = ("o8", "c1", "canceled", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
                "2017-10-04 19:55:00", "2017-10-10 21:25:13", "2017-10-18 00:00:00")  # fmt: skip
    undated = ("o9", "c2", "delivered", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
               "2017-10-04 19:55:00", "", "2017-10-18 00:00:00")  # fmt: skip
    ds, _, orders = build({"orders": [canceled, undated]})
    assert kinds(ev.order_events(orders["o8"], ds)) == [
        ("purchase", None),
        ("status", "approved"),
        ("status", "shipped"),
        ("status", "delivered"),
        ("final", "canceled"),
    ]
    assert kinds(ev.order_events(orders["o9"], ds)) == [
        ("purchase", None),
        ("status", "approved"),
        ("status", "shipped"),
        ("final", "delivered"),
    ]


def test_shipped_and_created_orders_end_at_their_last_step():
    """01 §4: shipped ends at the shipment (no delivery date); created/approved stay."""
    ds, _, orders = build()
    assert kinds(ev.order_events(orders["o2"], ds)) == [
        ("purchase", None),
        ("status", "approved"),
        ("status", "shipped"),
        ("review", None),
    ]
    assert kinds(ev.order_events(orders["o4"], ds)) == [("purchase", None)]
    approved = ("o9", "c1", "approved", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
                "2017-10-04 19:55:00", "", "2017-10-18 00:00:00")  # fmt: skip
    ds, _, orders = build({"orders": [approved]})
    # a carrier date on an order whose final status is `approved` is never applied
    assert kinds(ev.order_events(orders["o9"], ds)) == [("purchase", None), ("status", "approved")]


def test_final_status_delay_is_deterministic_and_bounded():
    assert ev.final_status_delay("abc") == ev.final_status_delay("abc")
    assert ev.final_status_delay("abc") != ev.final_status_delay("abd")
    for oid in ("a", "b", "e481f51cbdc54678b7cc49136f2d6af7"):
        assert timedelta(days=1) <= ev.final_status_delay(oid) < timedelta(days=3)


def test_event_stream_is_sorted_and_resumes_strictly_after_the_cursor():
    """01 §5: events are totally ordered by (ts, order_id, step); the cursor is the key of the
    last committed event and resuming yields exactly the events after it."""
    ds, _, orders = build()
    events = ev.build_events(orders.values(), ds)
    keys = [e.key for e in events]
    assert keys == sorted(keys) and len(set(keys)) == len(keys)
    assert events[0].kind == "purchase" and events[0].order_id == "o1"
    assert list(ev.after_cursor(events, None)) == events
    cursor = events[4].key
    assert list(ev.after_cursor(events, cursor)) == events[5:]
    assert list(ev.after_cursor(events, events[-1].key)) == []
    # same-second events of one order keep the step order (purchase before its reviews)
    o3 = [e for e in events if e.order_id == "o3"]
    assert [e.kind for e in o3] == ["purchase", "review", "status", "final"]
    until = ev.build_events(orders.values(), ds, until=T("2017-10-03 12:00:00"))
    assert all(e.ts <= T("2017-10-03 12:00:00") for e in until) and len(until) < len(events)


def test_split_window_preload_before_start_and_replay_inside():
    ds, _, orders = build()
    events = ev.build_events(orders.values(), ds)
    start, end = T("2017-10-03 00:00:00"), T("2017-10-05 23:59:59")
    preload, replay = ev.split_window(events, start, end)
    assert preload and all(e.ts < start for e in preload)
    assert replay and all(start <= e.ts <= end for e in replay)
    assert len(preload) + len(replay) + sum(e.ts > end for e in events) == len(events)


def test_summarize_is_the_state_an_uninterrupted_run_produces():
    """The fold used by the integration tests: counts per table and per status after
    applying a prefix of the stream."""
    ds, _, orders = build()
    events = ev.build_events(orders.values(), ds)
    s = ev.summarize(events, ds)
    assert (s.customers, s.orders, s.order_items, s.order_payments, s.order_reviews) == (
        4,
        4,
        4,
        5,
        4,
    )
    assert s.events == len(events)
    assert (s.inserts, s.updates, s.deletes) == (4 + 4 + 5 + 5 + 4, 3 + 2 + 1 + 1, 1)
    assert s.by_status is not None
    assert {k: (v.orders, v.items, v.revenue) for k, v in s.by_status.items()} == {
        "delivered": (1, 2, Decimal("80.00")),
        "shipped": (1, 1, Decimal("100.00")),
        "canceled": (1, 0, Decimal("0.00")),
        "created": (1, 1, Decimal("58.90")),
    }
    # before the cancellation the items of o3 are still there
    before_cancel = [e for e in events if not (e.kind == "final" and e.order_id == "o3")]
    assert ev.summarize(before_cancel, ds).by_status["approved"].revenue == Decimal("6735.00")
