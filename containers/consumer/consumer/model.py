"""Pure helpers around a decoded CDC message: one-line descriptions, statistics and
order tracking (04-kafka.md §6). No I/O here; `cli.py` feeds these from Kafka."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from consumer.rules import table_of

TOMBSTONE = "t"  # op label for a null value after DELETE (04 §2)
ORDER_TABLES = ("orders", "order_items", "order_payments", "order_reviews")


@dataclass(slots=True)
class Message:
    """A decoded Kafka message (keys/values already AVRO-deserialised)."""

    topic: str
    partition: int
    offset: int
    key: dict[str, Any] | None
    value: dict[str, Any] | None
    key_schema_id: int | None = None
    value_schema_id: int | None = None
    raw_bytes: int = 0  # len(key) + len(value) on the wire (after decompression)

    @property
    def table(self) -> str:
        return table_of(self.topic)

    @property
    def op(self) -> str:
        if self.value is None:
            return TOMBSTONE
        return str(self.value.get("op", "?"))


def short(value: Any, n: int = 8) -> str:
    return str(value)[:n] if value is not None else "-"


def key_text(key: dict[str, Any] | None) -> str:
    if not key:
        return "-"
    return "/".join(short(v) for v in key.values())


def describe(msg: Message, schema_version: int | None = None) -> str:
    """`[orders] u e481f5 approved → shipped lsn=24023128 tx=771 schema v1`."""
    head = f"[{msg.table}] {msg.op} {key_text(msg.key)}"
    if msg.value is None:
        return f"{head} tombstone"
    v = msg.value
    detail = ""
    if msg.table == "orders":
        before, after = v.get("before") or {}, v.get("after") or {}
        if msg.op == "u":
            detail = f"{before.get('order_status')} → {after.get('order_status')}"
        elif msg.op in ("c", "r"):
            detail = f"{after.get('order_status')} {after.get('order_purchase_timestamp')}"
        elif msg.op == "d":
            detail = f"was {before.get('order_status')}"
    elif msg.table == "order_items":
        row = v.get("after") or v.get("before") or {}
        detail = f"price={row.get('price')} freight={row.get('freight_value')}"
    elif msg.table == "order_payments":
        row = v.get("after") or v.get("before") or {}
        detail = f"{row.get('payment_type')} {row.get('payment_value')}"
    elif msg.table == "order_reviews":
        row = v.get("after") or v.get("before") or {}
        detail = f"score={row.get('review_score')}"
    source = v.get("source") or {}
    tx = (v.get("transaction") or {}).get("id") or source.get("txId")
    tail = f"lsn={source.get('lsn')} tx={tx}"
    if schema_version is not None:
        tail += f" schema v{schema_version}"
    elif msg.value_schema_id is not None:
        tail += f" schema id={msg.value_schema_id}"
    return f"{head} {detail} {tail}".replace("  ", " ")


@dataclass(slots=True)
class TopicStats:
    events: int = 0
    ops: Counter = field(default_factory=Counter)
    schema_ids: set[int] = field(default_factory=set)
    bytes: int = 0
    violations: int = 0
    lag: int = 0  # high watermark − position, summed over partitions (set by the CLI)
    window_events: int = 0


class Stats:
    """Per-topic counters and the periodic `--stats` line (04 §6)."""

    def __init__(self) -> None:
        self.topics: dict[str, TopicStats] = defaultdict(TopicStats)
        self.total = 0

    def add(self, msg: Message, violations: int = 0) -> None:
        t = self.topics[msg.topic]
        t.events += 1
        t.window_events += 1
        t.ops[msg.op] += 1
        if msg.value_schema_id is not None:
            t.schema_ids.add(msg.value_schema_id)
        t.bytes += msg.raw_bytes
        t.violations += violations
        self.total += 1

    def set_lag(self, topic: str, lag: int) -> None:
        self.topics[topic].lag = lag

    def lines(self, window_seconds: float) -> list[str]:
        """One line per topic; resets the per-window rate counters."""
        out = []
        for topic in sorted(self.topics):
            t = self.topics[topic]
            rate = t.window_events / window_seconds if window_seconds > 0 else 0.0
            ops = " ".join(f"{op}={t.ops.get(op, 0)}" for op in ("c", "u", "d", "r", TOMBSTONE))
            ids = ",".join(str(i) for i in sorted(t.schema_ids)) or "-"
            out.append(
                f"{topic} {rate:.1f}/s {ops} lag={t.lag} schemas={ids} violations={t.violations}"
            )
            t.window_events = 0
        return out

    def summary(self) -> dict[str, Any]:
        return {
            "events": self.total,
            "violations": sum(t.violations for t in self.topics.values()),
            "topics": {
                topic: {
                    "events": t.events,
                    "ops": dict(t.ops),
                    "schema_ids": sorted(t.schema_ids),
                    "bytes": t.bytes,
                    "violations": t.violations,
                }
                for topic, t in sorted(self.topics.items())
            },
        }


class OrderTracker:
    """`--follow-order <order_id>`: the lifecycle of one order across all topics.

    Matches every event whose key carries the order id (orders, order_items,
    order_payments, order_reviews) and the customers events of the order's customer.
    Topics are read independently, so a customers event may arrive before the orders
    event that reveals the customer id: customers events are kept per customer until
    then (a few hundred bytes each; only the --follow-order mode pays for it).
    """

    def __init__(self, order_id: str) -> None:
        self.order_id = order_id
        self.customer_id: str | None = None
        self.seen: list[Message] = []
        self._pending: dict[str, list[Message]] = {}

    def record(self, msg: Message) -> bool:
        """Remember `msg` if it belongs to the order; True when it does."""
        key = msg.key or {}
        if msg.table in ORDER_TABLES and key.get("order_id") == self.order_id:
            if msg.table == "orders" and msg.value is not None and self.customer_id is None:
                row = msg.value.get("after") or msg.value.get("before") or {}
                self.customer_id = row.get("customer_id")
                if self.customer_id is not None:
                    self.seen.extend(self._pending.pop(self.customer_id, []))
                    self._pending.clear()
            self.seen.append(msg)
            return True
        if msg.table == "customers":
            cid = key.get("customer_id")
            if self.customer_id is None:
                self._pending.setdefault(cid, []).append(msg)
                return False
            if cid == self.customer_id:
                self.seen.append(msg)
                return True
        return False

    def matches(self, msg: Message) -> bool:
        return self.record(msg)

    def timeline(self, version_of: Callable[[Message], int | None] | None = None) -> list[str]:
        """Events in LSN order; a tombstone (no value, no lsn) follows the DELETE of its key.

        `version_of` resolves a message's value schema id to its subject version for the
        `schema vN` suffix (the CLI passes the registry lookup)."""
        last_lsn: dict[tuple, int] = {}
        keyed: list[tuple[int, int, Message]] = []
        for i, m in enumerate(self.seen):
            k = (m.topic, tuple(sorted((m.key or {}).items())))
            if m.value is not None:
                last_lsn[k] = int((m.value.get("source") or {}).get("lsn") or 0)
            keyed.append((last_lsn.get(k, 0), i, m))
        ordered = [m for _, _, m in sorted(keyed, key=lambda x: (x[0], x[1]))]
        return [describe(m, version_of(m) if version_of else None) for m in ordered]
