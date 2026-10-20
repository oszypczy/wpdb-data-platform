"""Order lifecycle → events (01 §4), pure: no database.

One order yields: a purchase (customer + order + items + payments in one transaction),
one UPDATE per lifecycle step that has a date (approved → shipped → delivered), a final
status UPDATE when the dates alone do not reach the final status (terminal statuses,
plus the DELETE of the items for `canceled`), and one INSERT per review.

Time normalization (01 §4): a step never happens before the previous one
(ts = max(own date, previous step)); a review never before its purchase. The column
values written to the database stay the original CSV values; only the moment of the
event moves. Steps without a date are skipped. Events are totally ordered by
(ts, order_id, step), which is also the resume cursor stored in sim.sim_state.
"""

from __future__ import annotations

import zlib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import dropwhile

from simulator.csvio import CsvTable

# step numbers: the tie-breaker of the event order and the cursor position
STEP_PURCHASE, STEP_APPROVED, STEP_SHIPPED, STEP_DELIVERED, STEP_FINAL = 0, 1, 2, 3, 5
STEP_REVIEW_BASE = 10  # i-th review of an order (sorted by review_id) = 10 + i

LIFECYCLE = (
    (STEP_APPROVED, "approved", "order_approved_at"),
    (STEP_SHIPPED, "shipped", "order_delivered_carrier_date"),
    (STEP_DELIVERED, "delivered", "order_delivered_customer_date"),
)
FUTURE_COLUMNS = tuple(col for _, _, col in LIFECYCLE)  # NULL at purchase time
CHAIN = {"created": 0, "approved": 1, "shipped": 2, "delivered": 3}
TERMINAL = frozenset({"canceled", "unavailable", "invoiced", "processing"})
INITIAL_STATUS = "created"
CANCELED = "canceled"

# Olist column names the lifecycle depends on (01 §1); everything else is data
ORDER_ID = "order_id"
CUSTOMER_ID = "customer_id"
STATUS_COLUMN = "order_status"
PURCHASE_COLUMN = "order_purchase_timestamp"
REVIEW_ID = "review_id"
REVIEW_DATE_COLUMN = "review_creation_date"
PRICE_COLUMN = "price"

CATALOG_TABLES = ("product_category_name_translation", "products", "sellers")
LIFECYCLE_TABLES = ("customers", "orders", "order_items", "order_payments", "order_reviews")

# terminal status: last known step + 1 day + up to 2 days, fixed per order (crc32, not hash())
FINAL_DELAY_MIN = timedelta(days=1)
FINAL_DELAY_SPAN_SECONDS = 2 * 24 * 3600


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def final_status_delay(order_id: str) -> timedelta:
    return FINAL_DELAY_MIN + timedelta(
        seconds=zlib.crc32(order_id.encode()) % FINAL_DELAY_SPAN_SECONDS
    )


@dataclass(slots=True)
class OrderData:
    """Everything the CSVs know about one order; `customer` is None when missing."""

    order_id: str
    order: tuple[str, ...]
    customer: tuple[str, ...] | None
    items: list[tuple[str, ...]]
    payments: list[tuple[str, ...]]
    reviews: list[tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class Event:
    ts: datetime
    order_id: str
    step: int
    kind: str  # purchase | status | final | review
    inserts: int
    updates: int
    deletes: int
    status: str | None = None  # status / final
    column: str | None = None  # status: the date column set together with the status
    value: str | None = None  # status: original CSV value of that column
    row: tuple[str, ...] | None = None  # review
    order: OrderData | None = None  # purchase

    @property
    def rows(self) -> int:
        return self.inserts + self.updates + self.deletes

    @property
    def key(self) -> tuple[datetime, str, int]:
        return (self.ts, self.order_id, self.step)


def event_key(event: Event) -> tuple[datetime, str, int]:
    return event.key


class Dataset:
    """Column positions of the lifecycle tables, resolved once from the CSV headers."""

    def __init__(self, tables: Mapping[str, CsvTable]):
        missing = [t for t in (*CATALOG_TABLES, *LIFECYCLE_TABLES) if t not in tables]
        if missing:
            raise KeyError(f"missing tables for the order lifecycle: {missing}")
        self.tables = tables
        orders = tables["orders"]
        self.o_id = orders.index(ORDER_ID)
        self.o_customer = orders.index(CUSTOMER_ID)
        self.o_status = orders.index(STATUS_COLUMN)
        self.o_purchase = orders.index(PURCHASE_COLUMN)
        self.o_lifecycle = {col: orders.index(col) for _, _, col in LIFECYCLE}
        self.c_id = tables["customers"].index(CUSTOMER_ID)
        self.i_order = tables["order_items"].index(ORDER_ID)
        self.i_price = tables["order_items"].index(PRICE_COLUMN)
        self.p_order = tables["order_payments"].index(ORDER_ID)
        self.r_order = tables["order_reviews"].index(ORDER_ID)
        self.r_id = tables["order_reviews"].index(REVIEW_ID)
        self.r_date = tables["order_reviews"].index(REVIEW_DATE_COLUMN)

    def build_orders(self) -> list[OrderData]:
        customers = {r[self.c_id]: r for r in self.tables["customers"].rows}
        items = _group(self.tables["order_items"].rows, self.i_order)
        payments = _group(self.tables["order_payments"].rows, self.p_order)
        reviews = _group(self.tables["order_reviews"].rows, self.r_order)
        orders: list[OrderData] = []
        for row in self.tables["orders"].rows:
            oid = row[self.o_id]
            orders.append(
                OrderData(
                    order_id=oid,
                    order=row,
                    customer=customers.get(row[self.o_customer]),
                    items=items.get(oid, []),
                    payments=payments.get(oid, []),
                    reviews=sorted(reviews.get(oid, []), key=lambda r: r[self.r_id]),
                )
            )
        return orders


def _group(rows: Iterable[tuple[str, ...]], key: int) -> dict[str, list[tuple[str, ...]]]:
    out: dict[str, list[tuple[str, ...]]] = {}
    for r in rows:
        out.setdefault(r[key], []).append(r)
    return out


def order_events(
    od: OrderData, ds: Dataset, delay: Callable[[str], timedelta] = final_status_delay
) -> list[Event]:
    """The events of one order, in the order they happen (01 §4)."""
    oid = od.order_id
    purchase_ts = parse_ts(od.order[ds.o_purchase])
    final = od.order[ds.o_status]
    events = [
        Event(
            purchase_ts,
            oid,
            STEP_PURCHASE,
            "purchase",
            inserts=(1 if od.customer is not None else 0) + 1 + len(od.items) + len(od.payments),
            updates=0,
            deletes=0,
            order=od,
        )
    ]
    prev_ts = purchase_ts
    current = INITIAL_STATUS
    final_rank = CHAIN.get(final)  # None for terminal (and unknown) statuses
    for step, status, column in LIFECYCLE:
        raw = od.order[ds.o_lifecycle[column]]
        if not raw or (final_rank is not None and step > final_rank):
            continue
        ts = max(parse_ts(raw), prev_ts)
        events.append(
            Event(ts, oid, step, "status", 0, 1, 0, status=status, column=column, value=raw)
        )
        prev_ts = ts
        current = status
    if current != final:
        deletes = len(od.items) if final == CANCELED else 0
        events.append(
            Event(prev_ts + delay(oid), oid, STEP_FINAL, "final", 0, 1, deletes, status=final)
        )
    for i, review in enumerate(od.reviews):
        ts = max(parse_ts(review[ds.r_date]), purchase_ts)
        events.append(Event(ts, oid, STEP_REVIEW_BASE + i, "review", 1, 0, 0, row=review))
    return events


def build_events(
    orders: Iterable[OrderData], ds: Dataset, *, until: datetime | None = None
) -> list[Event]:
    """All events of all orders, sorted by (ts, order_id, step); `until` drops later ones."""
    events = [e for od in orders for e in order_events(od, ds) if until is None or e.ts <= until]
    events.sort(key=event_key)
    return events


Cursor = tuple[datetime, str, int]


def after_cursor(events: Iterable[Event], cursor: Cursor | None) -> Iterator[Event]:
    """Events strictly after the resume position (events are sorted by key)."""
    if cursor is None:
        return iter(events)
    return dropwhile(lambda e: e.key <= cursor, events)


def split_window(
    events: list[Event], start: datetime, end: datetime
) -> tuple[list[Event], list[Event]]:
    """(preload, replay): before `start` is bulk-loaded, [start, end] is replayed live."""
    preload = [e for e in events if e.ts < start]
    replay = [e for e in events if start <= e.ts <= end]
    return preload, replay


# --- expected state (for tests and the design numbers) ----------------------------
@dataclass(slots=True)
class StatusTotals:
    orders: int = 0
    items: int = 0
    revenue: Decimal = Decimal("0.00")


@dataclass(slots=True)
class Summary:
    customers: int = 0
    orders: int = 0
    order_items: int = 0
    order_payments: int = 0
    order_reviews: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    events: int = 0
    by_status: dict[str, StatusTotals] | None = None

    @property
    def rows(self) -> int:
        return self.inserts + self.updates + self.deletes


def summarize(events: Iterable[Event], ds: Dataset) -> Summary:
    """The database content that applying `events` (in order) produces: what an
    uninterrupted run yields, so a resumed run must match it exactly."""
    s = Summary(by_status={})
    customers: set[str] = set()
    status: dict[str, str] = {}
    live_items: dict[str, list[tuple[str, ...]]] = {}
    for e in events:
        s.events += 1
        s.inserts += e.inserts
        s.updates += e.updates
        s.deletes += e.deletes
        if e.kind == "purchase":
            od = e.order
            assert od is not None
            if od.customer is not None:
                customers.add(od.customer[ds.c_id])
            status[e.order_id] = INITIAL_STATUS
            live_items[e.order_id] = list(od.items)
            s.order_payments += len(od.payments)
        elif e.kind in ("status", "final"):
            assert e.status is not None
            status[e.order_id] = e.status
            if e.kind == "final" and e.status == CANCELED:
                live_items[e.order_id] = []
        elif e.kind == "review":
            s.order_reviews += 1
    s.customers = len(customers)
    s.orders = len(status)
    assert s.by_status is not None
    for oid, st in status.items():
        totals = s.by_status.setdefault(st, StatusTotals())
        totals.orders += 1
        items = live_items.get(oid, [])
        totals.items += len(items)
        totals.revenue += sum((Decimal(i[ds.i_price]) for i in items), Decimal("0.00"))
        s.order_items += len(items)
    return s
