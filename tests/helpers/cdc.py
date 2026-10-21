"""Debezium/CDC helpers shared by the stage-2 integration tests.

- connector status / wait helpers over the Connect REST API (`debezium-connector` skill),
- replication slot and simulator state lookups through the read-only `reader` role,
- the probe-order lifecycle: synthetic rows written as `shop_app` into the real
  `shop.*` tables (03 §11 "INSERT, UPDATE i DELETE w shop.orders") and removed again,
  so the truth views and the simulator's counters are unchanged afterwards. The probe
  keys start with `test-stage2-` and are never produced by the simulator.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from tests.helpers.clients import Http
from tests.helpers.kafka import Record, TopicReader, Watermarks
from tests.helpers.wait import wait_until

CONNECTOR = "olist-postgres"
ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_JSON = ROOT / "containers" / "connect" / "connectors" / f"{CONNECTOR}.json"
TOPIC_PREFIX = "olist"
SHOP_TABLES = (
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
)
LARGE_TABLES = ("orders", "order_items", "order_payments", "order_reviews", "customers")
TRANSACTION_TOPIC = f"{TOPIC_PREFIX}.transaction"
HEARTBEAT_TOPIC = f"__debezium-heartbeat.{TOPIC_PREFIX}"
SNAPSHOT_FLAGS = {"true", "first", "first_in_data_collection", "last_in_data_collection", "last"}
# tables whose name starts with `_` are not captured (table.exclude.list): the stage-0/1
# tests create and drop such probes within milliseconds, which would otherwise leave
# Debezium with events of a table it can no longer describe
PROBE_PREFIX = "zz_stage2_"


def topic(table: str) -> str:
    return f"{TOPIC_PREFIX}.shop.{table}"


def connector_config() -> dict[str, str]:
    return json.loads(CONNECTOR_JSON.read_text())


# --- Connect REST ------------------------------------------------------------------------
def connector_status(connect: Http) -> dict[str, Any]:
    return connect.json(f"/connectors/{CONNECTOR}/status")


def states(status: dict[str, Any]) -> tuple[str, list[str]]:
    return status["connector"]["state"], [t["state"] for t in status["tasks"]]


def wait_connector(connect: Http, connector: str, tasks: list[str], timeout: float = 120) -> dict:
    """Block until the connector state and the list of task states match exactly."""

    def ready():
        st = connector_status(connect)
        return st if states(st) == (connector, tasks) else None

    return wait_until(ready, timeout=timeout, every=2, desc=f"connector {connector}/{tasks}")


def wait_running(connect: Http, timeout: float = 120) -> dict:
    return wait_connector(connect, "RUNNING", ["RUNNING"], timeout)


def restart_failed_tasks(connect: Http) -> None:
    r = connect.post(
        f"/connectors/{CONNECTOR}/restart", params={"includeTasks": "true", "onlyFailed": "true"}
    )
    assert r.status_code in (200, 202, 204), (r.status_code, r.text)


def committed_lsn(bootstrap: str | None = None) -> int | None:
    """`lsn` of the connector's last committed offset (last record of connect-offsets).

    Debezium commits offsets every offset.flush.interval.ms (60 s on this worker); after a
    task restart it re-reads the WAL from here, so a captured table may only be DROPPED
    once this value passed its last change (the PK of a dropped table cannot be resolved
    any more: the re-delivered event would have no key and the compacted topic rejects it).
    """
    from confluent_kafka import Consumer, TopicPartition

    from tests.helpers.clients import KAFKA_BOOTSTRAP_HOST

    c = Consumer(
        {
            "bootstrap.servers": bootstrap or KAFKA_BOOTSTRAP_HOST,
            "group.id": f"wpdb-tests-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "broker.address.family": "v4",
        }
    )
    try:
        tp = TopicPartition("connect-offsets", 0)
        low, high = c.get_watermark_offsets(tp, timeout=10)
        if high <= low:
            return None
        c.assign([TopicPartition("connect-offsets", 0, high - 1)])
        msg = c.poll(10)
        if msg is None or msg.error() or msg.value() is None:
            return None
        key = json.loads(msg.key())
        if key[0] != CONNECTOR:
            return None
        return int(json.loads(msg.value())["lsn"])
    finally:
        c.close()


def wait_committed_past(lsn: int, timeout: float = 150) -> int:
    def passed() -> int | None:
        committed = committed_lsn()
        return committed if committed is not None and committed >= lsn else None

    return wait_until(
        passed, timeout=timeout, every=5, desc=f"connector offset committed past lsn {lsn}"
    )


# --- PostgreSQL side ---------------------------------------------------------------------
def slot_state(pg: psycopg.Connection, slot: str = "debezium_slot") -> dict[str, Any] | None:
    row = pg.execute(
        "SELECT slot_name, active, confirmed_flush_lsn::text, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, "
        "pg_current_wal_lsn()::text, wal_status "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (slot,),
    ).fetchone()
    if row is None:
        return None
    return {
        "slot": row[0],
        "active": row[1],
        "confirmed_flush_lsn": row[2],
        "lag_bytes": row[3],
        "current_lsn": row[4],
        "wal_status": row[5],
    }


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def sim_state(pg: psycopg.Connection) -> dict[str, Any]:
    row = pg.execute(
        "SELECT phase, sim_start, sim_end, sim_clock, preload_finished_at FROM sim.sim_state "
        "WHERE id = 1"
    ).fetchone()
    assert row is not None, "sim.sim_state has no row: simulator not initialised"
    names = ("phase", "sim_start", "sim_end", "sim_clock", "preload_finished_at")
    return dict(zip(names, row, strict=True))


def primary_key(pg: psycopg.Connection, table: str, schema: str = "shop") -> list[str]:
    rows = pg.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary "
        "ORDER BY array_position(i.indkey, a.attnum)",
        (f"{schema}.{table}",),
    ).fetchall()
    return [r[0] for r in rows]


# --- probe order lifecycle ---------------------------------------------------------------
@dataclass(frozen=True)
class ProbeOrder:
    order_id: str
    customer_id: str
    purchase_ts: datetime
    price: Decimal
    freight: Decimal
    payment: Decimal

    @property
    def key(self) -> tuple:
        return (self.order_id,)


def probe_ids(tag: str) -> tuple[str, str]:
    h = uuid.uuid4().hex[:12]
    return f"test-stage2-{tag}-o-{h}", f"test-stage2-{tag}-c-{h}"


def probe_purchase(
    app: psycopg.Connection, tag: str, status: str, purchase_ts: datetime
) -> ProbeOrder:
    """customer + order + item + payment in ONE transaction (02 §6), as the simulator does."""
    oid, cid = probe_ids(tag)
    product = app.execute("SELECT product_id FROM shop.products LIMIT 1").fetchone()[0]
    seller = app.execute("SELECT seller_id FROM shop.sellers LIMIT 1").fetchone()[0]
    price, freight, payment = Decimal("58.90"), Decimal("13.29"), Decimal("72.19")
    with app.transaction():
        app.execute(
            "INSERT INTO shop.customers (customer_id, customer_unique_id, "
            "customer_zip_code_prefix, customer_city, customer_state) "
            "VALUES (%s, %s, '01037', 'sao paulo', 'SP')",
            (cid, cid),
        )
        app.execute(
            "INSERT INTO shop.orders (order_id, customer_id, order_status, "
            "order_purchase_timestamp, order_estimated_delivery_date) "
            "VALUES (%s, %s, %s, %s, %s + interval '30 days')",
            (oid, cid, status, purchase_ts, purchase_ts),
        )
        app.execute(
            "INSERT INTO shop.order_items (order_id, order_item_id, product_id, seller_id, "
            "shipping_limit_date, price, freight_value) VALUES (%s, 1, %s, %s, %s, %s, %s)",
            (oid, product, seller, purchase_ts, price, freight),
        )
        app.execute(
            "INSERT INTO shop.order_payments (order_id, payment_sequential, payment_type, "
            "payment_installments, payment_value) VALUES (%s, 1, 'credit_card', 1, %s)",
            (oid, payment),
        )
    return ProbeOrder(oid, cid, purchase_ts, price, freight, payment)


def probe_set_status(app: psycopg.Connection, order: ProbeOrder, status: str, **cols: Any) -> None:
    sets = ", ".join(["order_status = %s"] + [f"{c} = %s" for c in cols])
    app.execute(
        f"UPDATE shop.orders SET {sets} WHERE order_id = %s",
        (status, *cols.values(), order.order_id),
    )


def probe_delete(app: psycopg.Connection, order: ProbeOrder) -> None:
    """Remove the probe rows in one transaction (children first)."""
    with app.transaction():
        app.execute("DELETE FROM shop.order_payments WHERE order_id = %s", (order.order_id,))
        app.execute("DELETE FROM shop.order_items WHERE order_id = %s", (order.order_id,))
        app.execute("DELETE FROM shop.orders WHERE order_id = %s", (order.order_id,))
        app.execute("DELETE FROM shop.customers WHERE customer_id = %s", (order.customer_id,))


def cleanup_probe_rows(app: psycopg.Connection) -> None:
    """Safety net for interrupted runs: drop every test-stage2-* row that is left."""
    with app.transaction():
        for t in ("order_payments", "order_items", "order_reviews"):
            app.execute(f"DELETE FROM shop.{t} WHERE order_id LIKE 'test-stage2-%%'")
        app.execute("DELETE FROM shop.orders WHERE order_id LIKE 'test-stage2-%%'")
        app.execute("DELETE FROM shop.customers WHERE customer_id LIKE 'test-stage2-%%'")


@dataclass
class Lifecycle:
    """What one run of the probe lifecycle produced (see conftest `probe_lifecycle`)."""

    clean: ProbeOrder  # created → approved → deleted
    bad: ProbeOrder  # delivered → created (forbidden transition, 04 §6) → deleted
    start: dict[str, Watermarks]
    records: dict[str, list[Record]]  # new records per topic since `start`

    def of(self, table: str, order: ProbeOrder) -> list[Record]:
        return [
            r
            for r in self.records[topic(table)]
            if (r.key or {}).get("order_id") == order.order_id
            or (table == "customers" and (r.key or {}).get("customer_id") == order.customer_id)
        ]


def run_probe_lifecycle(app: psycopg.Connection, reader: TopicReader) -> Lifecycle:
    tables = ("customers", "orders", "order_items", "order_payments")
    topics = [topic(t) for t in tables] + [TRANSACTION_TOPIC]
    start = {t: reader.watermarks(t) for t in topics}
    clock = sim_state(app)["sim_clock"] or datetime(2017, 1, 5)
    clean = probe_purchase(app, "clean", "created", clock)
    probe_set_status(app, clean, "approved", order_approved_at=clock)
    probe_delete(app, clean)
    bad = probe_purchase(app, "bad", "delivered", clock)
    probe_set_status(app, bad, "created")
    probe_delete(app, bad)

    def complete(recs: list[Record]) -> bool:
        ops = {}
        for r in recs:
            oid = (r.key or {}).get("order_id")
            ops.setdefault(oid, []).append(r.op)
        return all(ops.get(o.order_id, []).count("t") >= 1 for o in (clean, bad))

    records = {}
    for t in topics:
        if t == TRANSACTION_TOPIC:
            continue
        done = complete if t == topic("orders") else _has_tombstones(clean, bad, t)
        records[t] = reader.read_new(start[t], done, timeout=90)
    # the END record of every probe transaction carries the same id as its data events
    tx_ids = {
        tx_prefix(r.value["transaction"]["id"])
        for r in records[topic("orders")]
        if r.value
        and r.value.get("transaction")
        and (r.key or {}).get("order_id") in (clean.order_id, bad.order_id)
    }
    records[TRANSACTION_TOPIC] = reader.read_new(
        start[TRANSACTION_TOPIC], lambda recs: _transactions_done(recs, tx_ids), timeout=90
    )
    return Lifecycle(clean=clean, bad=bad, start=start, records=records)


def _has_tombstones(clean: ProbeOrder, bad: ProbeOrder, t: str):
    def done(recs: list[Record]) -> bool:
        tomb = {
            (r.key or {}).get("order_id") or (r.key or {}).get("customer_id")
            for r in recs
            if r.value is None
        }
        if t.endswith("customers"):
            wanted = {clean.customer_id, bad.customer_id}
        else:
            wanted = {clean.order_id, bad.order_id}
        return wanted <= tomb

    return done


def tx_prefix(transaction_id: str) -> str:
    """`803:33460600` → `803` (the PostgreSQL txId)."""
    return transaction_id.split(":", 1)[0]


def _transactions_done(recs: list[Record], tx_ids: set[str]) -> bool:
    ends = {tx_prefix(r.value["id"]) for r in recs if r.value and r.value.get("status") == "END"}
    return tx_ids <= ends


# --- captured probe table (session fixture `captured_probe_table`) ------------------------
@dataclass
class CapturedProbe:
    table: str
    topic: str
    first: Record  # the `c` event of row id=1
    last_lsn: int = 0  # highest lsn of a change the tests made on this table

    @property
    def subject(self) -> str:
        return f"{self.topic}-value"

    @classmethod
    def create(cls, app: psycopg.Connection, reader: TopicReader, admin) -> CapturedProbe:
        table = f"{PROBE_PREFIX}probe_{uuid.uuid4().hex[:8]}"
        t = topic(table)
        app.execute(f"CREATE TABLE shop.{table} (id int PRIMARY KEY, v text)")
        app.execute(f"INSERT INTO shop.{table} VALUES (1, '1')")
        wait_until(lambda: t in admin.list_topics(timeout=10).topics, 60, desc=f"topic {t}")
        start = Watermarks(t, dict.fromkeys(reader.partitions(t), 0))
        first = reader.read_new(start, lambda rs: len(rs) >= 1, timeout=60)[0]
        return cls(table=table, topic=t, first=first, last_lsn=int(first.source["lsn"]))

    def note(self, record: Record) -> None:
        self.last_lsn = max(self.last_lsn, int(record.source["lsn"]))

    def teardown(self, app, connect: Http, registry: Http, admin, reader: TopicReader) -> None:
        # never drop a captured table before its changes are behind the committed offset
        try:
            wait_committed_past(self.last_lsn)
        finally:
            app.execute(f"DROP TABLE IF EXISTS shop.{self.table}")
        wait_running(connect, timeout=120)
        registry.delete(f"/config/{self.subject}")
        for s in (f"{self.topic}-key", self.subject):
            if registry.delete(f"/subjects/{s}").status_code == 200:
                registry.delete(f"/subjects/{s}", params={"permanent": "true"})
        if self.topic in admin.list_topics(timeout=10).topics:
            admin.delete_topics([self.topic])[self.topic].result(timeout=30)
