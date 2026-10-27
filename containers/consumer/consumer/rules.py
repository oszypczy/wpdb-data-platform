"""Validation rules for Debezium CDC events (docs/design/04-kafka.md §6).

Pure functions over already-decoded events: no Kafka, no Schema Registry, so every
rule is unit-testable on hand-made dictionaries (tests/unit/test_consumer_rules.py).

An event is the Debezium envelope as the AVRO deserializer returns it:
``{"before": {...}|None, "after": {...}|None, "op": "c"|"u"|"d"|"r", "source": {...},
"ts_ms": int, "transaction": {...}|None}``. Timestamps may be ``datetime`` (what
confluent-kafka's AvroDeserializer yields for ``timestamp-millis``) or raw epoch
milliseconds; amounts may be ``Decimal`` (logical ``decimal``), ``int``/``float``
or a numeric string. The rules normalise both.
"""

from __future__ import annotations

import re
import struct
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

MAGIC_BYTE = 0
HEADER_LEN = 5  # magic byte + 4-byte big-endian schema id (Confluent wire format)

# Order lifecycle ranks (01-business-system.md §4): a status may only move forward
# (a skipped milestone is allowed: created → shipped when the approval date is
# missing), stay the same (no-op UPDATE), or become `canceled` from anywhere.
STATUS_RANK: Mapping[str, int] = {
    "created": 0,
    "approved": 1,
    "invoiced": 2,
    "processing": 2,
    "unavailable": 2,
    "shipped": 2,
    "delivered": 3,
}
CANCELED = "canceled"

# amounts that must never be negative, per table
AMOUNT_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "order_items": ("price", "freight_value"),
    "order_payments": ("payment_value",),
}

# dates set at the moment the business event happens: they can never be later than
# the simulated clock (estimates such as order_estimated_delivery_date are excluded)
EVENT_DATE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "orders": (
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
    ),
    "order_reviews": ("review_creation_date",),
}

# The consumer's estimate of the simulation clock is the latest event date it has
# seen so far (any table, any op, updated *after* the event was checked). The
# tolerance absorbs the gap between the preload and SIM_START (two weeks in the
# default window: 2016-12-28 → 2017-01-05) and the normal distance between events.
DEFAULT_CLOCK_TOLERANCE = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class Violation:
    rule: str
    message: str

    def __str__(self) -> str:
        return f"{self.rule}: {self.message}"


# --- normalisation helpers --------------------------------------------------------------
def as_datetime(value: Any) -> datetime | None:
    """Epoch millis (Debezium `connect` mode) or datetime → naive UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value / 1000, tz=UTC).replace(tzinfo=None)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    raise TypeError(f"not a timestamp: {value!r}")


def as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise TypeError(f"not an amount: {value!r}") from exc


def table_of(topic: str) -> str:
    """`olist.shop.orders` → `orders`."""
    return topic.rsplit(".", 1)[-1]


# Probe tables of the integration tests mirror a business table under the name
# `zz_probe_<hex>_<table>` (tests/helpers/cdc.py::ProbeShop: the tests never write into
# the business tables of a live stack). They play the role of the table they mirror, so
# every rule keyed on a table name applies to them unchanged.
PROBE_TABLE = re.compile(r"^zz_probe_[0-9a-f]+_(?P<role>[a-z_]+)$")


def table_role(table: str) -> str:
    """The business table whose rules apply: `zz_probe_1a2b3c4d_orders` → `orders`;
    every other name is its own role."""
    m = PROBE_TABLE.match(table)
    return m["role"] if m else table


def parse_wire_header(data: bytes | None) -> tuple[int, int] | None:
    """(magic byte, schema id) of a Confluent-framed payload; None for tombstones."""
    if data is None:
        return None
    if len(data) < HEADER_LEN:
        raise ValueError(f"payload shorter than the {HEADER_LEN}-byte wire header")
    return data[0], struct.unpack(">I", data[1:HEADER_LEN])[0]


# --- rules ------------------------------------------------------------------------------
def check_before_present(table: str, event: Mapping[str, Any]) -> Violation | None:
    """`u` and `d` carry the full old row (REPLICA IDENTITY FULL, 02 §2)."""
    op = event.get("op")
    if op in ("u", "d") and not event.get("before"):
        return Violation("before_present", f"{table}: op={op} without `before`")
    return None


def check_status_transition(table: str, event: Mapping[str, Any]) -> Violation | None:
    """orders: before.order_status → after.order_status must be a legal lifecycle move."""
    if table != "orders" or event.get("op") != "u":
        return None
    before, after = event.get("before") or {}, event.get("after") or {}
    old, new = before.get("order_status"), after.get("order_status")
    if old is None or new is None:
        return None  # covered by check_before_present / schema
    if old == new or new == CANCELED:
        return None
    if old not in STATUS_RANK or new not in STATUS_RANK:
        return Violation("status_transition", f"orders: unknown status {old!r} → {new!r}")
    if STATUS_RANK[new] > STATUS_RANK[old]:
        return None
    return Violation("status_transition", f"orders: {old} → {new} is not allowed")


def check_amounts_non_negative(table: str, event: Mapping[str, Any]) -> Violation | None:
    """Prices, freight and payments are never negative (in `after`; `before` is history)."""
    columns = AMOUNT_COLUMNS.get(table)
    after = event.get("after")
    if not columns or not after:
        return None
    for col in columns:
        amount = as_decimal(after.get(col))
        if amount is not None and amount < 0:
            return Violation("amount_non_negative", f"{table}.{col} = {amount} < 0")
    return None


def check_dates_not_after_clock(
    table: str,
    event: Mapping[str, Any],
    clock: datetime | None,
    tolerance: timedelta = DEFAULT_CLOCK_TOLERANCE,
) -> Violation | None:
    """Business dates in `after` may not lie in the simulated future.

    `clock` is the consumer's estimate of the simulation clock (latest event date seen
    before this event, see `advance_clock`). Snapshot rows (`op=r`) are history read
    in table order, so the rule applies to the live stream only. The tolerance covers
    the distance between the estimate and the real simulated clock.
    """
    columns = EVENT_DATE_COLUMNS.get(table)
    after = event.get("after")
    if not columns or not after or clock is None or event.get("op") not in ("c", "u"):
        return None
    limit = clock + tolerance
    for col in columns:
        ts = as_datetime(after.get(col))
        if ts is not None and ts > limit:
            return Violation(
                "date_not_in_future",
                f"{table}.{col} = {ts} is after the simulated clock {clock} (+{tolerance})",
            )
    return None


def check_schema_id_known(
    data: bytes | None, is_known: Callable[[int], bool] | Collection[int], field: str = "value"
) -> Violation | None:
    """The 5-byte header is well-formed and its schema id exists in the registry."""
    try:
        header = parse_wire_header(data)
    except ValueError as exc:
        return Violation("schema_id_known", f"{field}: {exc}")
    if header is None:
        return None  # tombstone
    magic, schema_id = header
    if magic != MAGIC_BYTE:
        return Violation("schema_id_known", f"{field}: magic byte {magic} != {MAGIC_BYTE}")
    known = is_known(schema_id) if callable(is_known) else schema_id in is_known
    if not known:
        return Violation("schema_id_known", f"{field}: schema id {schema_id} not in registry")
    return None


def advance_clock(table: str, event: Mapping[str, Any], clock: datetime | None) -> datetime | None:
    """New clock estimate after this event: the latest event date seen so far (any op)."""
    columns = EVENT_DATE_COLUMNS.get(table_role(table))
    after = event.get("after") or {}
    if not columns or not after:
        return clock
    for col in columns:
        ts = as_datetime(after.get(col))
        if ts is not None and (clock is None or ts > clock):
            clock = ts
    return clock


def validate_event(
    table: str,
    event: Mapping[str, Any] | None,
    clock: datetime | None,
    *,
    tolerance: timedelta = DEFAULT_CLOCK_TOLERANCE,
) -> list[Violation]:
    """All business rules for one decoded event; a tombstone (None) has nothing to check.
    `table` is the physical table; the rules of its role apply (`table_role`)."""
    if event is None:
        return []
    table = table_role(table)
    checks = (
        check_before_present(table, event),
        check_status_transition(table, event),
        check_amounts_non_negative(table, event),
        check_dates_not_after_clock(table, event, clock, tolerance),
    )
    return [v for v in checks if v is not None]
