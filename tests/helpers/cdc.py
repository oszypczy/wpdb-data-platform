"""Debezium/CDC helpers shared by the stage-2 integration tests.

- connector status / wait helpers over the Connect REST API (`debezium-connector` skill),
- replication slot and simulator state lookups through the read-only `reader` role,
- the probe-order lifecycle: synthetic rows written as `shop_app` into per-session
  mirrors of the business tables (`shop.zz_probe_<hex>_<table>`, `ProbeShop`) and
  removed again; the business tables of the live stack are never written (no residue in
  Spark's gold history). The probe keys start with `test-stage2-`.
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


def stop_connector(connect: Http, timeout: float = 60) -> None:
    """`PUT /connectors/<name>/stop` (KIP-875): the task shuts down, commits its last offset
    and closes the replication connection; the connector stays registered as STOPPED."""
    r = connect.put(f"/connectors/{CONNECTOR}/stop")
    assert r.status_code in (200, 202, 204), (r.status_code, r.text)
    wait_connector(connect, "STOPPED", [], timeout=timeout)


def resume_connector(connect: Http) -> None:
    r = connect.put(f"/connectors/{CONNECTOR}/resume")
    assert r.status_code in (200, 202, 204), (r.status_code, r.text)


def restart_failed_tasks(connect: Http) -> None:
    r = connect.post(
        f"/connectors/{CONNECTOR}/restart", params={"includeTasks": "true", "onlyFailed": "true"}
    )
    assert r.status_code in (200, 202, 204), (r.status_code, r.text)


def committed_lsn(bootstrap: str | None = None) -> int | None:
    """`lsn` of the connector's last committed offset (last record of connect-offsets).

    Debezium commits offsets every offset.flush.interval.ms (5 s on this worker); after a
    task restart it re-reads the WAL from here, so a captured table may only be DROPPED
    once this value passed its last change (the PK of a dropped table cannot be resolved
    any more: the re-delivered event would have no key and the compacted topic rejects it).
    """
    offset = stored_offset(bootstrap)
    return int(offset["lsn"]) if offset is not None else None


def stored_offset(bootstrap: str | None = None) -> dict[str, Any] | None:
    """The connector's last committed source offset (last record of connect-offsets):
    `lsn`, `lsn_proc` (last completely processed, what the start-up check compares with
    the slot's restart_lsn), `lsn_commit`, `txId`, `ts_usec`, `messageType`."""
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
        return json.loads(msg.value())
    finally:
        c.close()


def wait_committed_past(lsn: int, timeout: float = 60) -> int:
    # offset.flush.interval.ms = 5 s; 60 s leaves room for a task restart in between
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
        "pg_current_wal_lsn()::text, wal_status, restart_lsn::text "
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
        "restart_lsn": row[6],
    }


def slot_pending_relations(
    dbz: psycopg.Connection, upto: str, slot: str = "debezium_slot"
) -> set[tuple[str, str]]:
    """(schema, table) of every relation with a change in the slot between its
    confirmed_flush_lsn and `upto`, read with pg_logical_slot_peek_binary_changes (nothing
    is consumed; the slot must be inactive). pgoutput sends a Relation message ('R': int32
    oid, namespace, name, ...) before the first change of every relation in a decoding
    session, so the set of 'R' messages names every table the changes touch."""
    rows = dbz.execute(
        "SELECT data FROM pg_logical_slot_peek_binary_changes(%s, %s::pg_lsn, NULL, "
        "'proto_version', '1', 'publication_names', 'dbz_publication')",
        (slot, upto),
    ).fetchall()
    relations = set()
    for (data,) in rows:
        data = bytes(data)
        if data[:1] == b"R":
            namespace, name = data[5:].split(b"\0")[:2]
            relations.add((namespace.decode(), name.decode()))
        elif data[:1] == b"T":  # TRUNCATE carries relation oids only: report it as such
            relations.add(("?", "truncate"))
    return relations


def advance_slot_past_stored_offset(
    dbz: psycopg.Connection, stored_lsn_proc: int, timeout: float = 60
) -> dict[str, Any]:
    """Recreate the state of the 08 §14 incident on a STOPPED connector: the slot's
    restart_lsn ahead of the stored offset, with only heartbeat changes in between.

    As `debezium` (REPLICATION; the role that owns the slot) upsert dbz.heartbeat the way
    heartbeat.action.query does, check that the WAL up to the current position carries
    no change of any other table, and advance the slot there. restart_lsn only moves past
    an xl_running_xacts record (the background writer logs one every 15 s while there is
    WAL activity), so repeat until restart_lsn > the stored lsn_proc."""

    def attempt() -> dict[str, Any] | None:
        dbz.execute(
            "INSERT INTO dbz.heartbeat (id, ts) VALUES (1, now()) "
            "ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
        )
        target = dbz.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
        touched = slot_pending_relations(dbz, target)
        if not touched <= {("dbz", "heartbeat")}:
            return {"unsafe": touched}  # never skip a captured change: stop and fail
        dbz.execute("SELECT pg_replication_slot_advance('debezium_slot', %s::pg_lsn)", (target,))
        st = slot_state(dbz)
        return st if lsn_to_int(st["restart_lsn"]) > stored_lsn_proc else None

    st = wait_until(
        attempt, timeout=timeout, every=3, desc="slot restart_lsn past the stored offset"
    )
    assert "unsafe" not in st, f"the WAL to skip holds changes of {st['unsafe']}"
    return st


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


# --- broker / worker internals read through `docker compose` -----------------------------
def cleaned_offsets(stack) -> dict[str, int]:
    """`<topic>-<partition>` → offset up to which the log cleaner compacted it, from the
    broker's cleaner-offset-checkpoint. A partition missing here was never compacted, so
    every record ever written to it is still in the log."""
    out = stack.exec(
        "kafka",
        "sh",
        "-c",
        "cat /var/lib/kafka/data/cleaner-offset-checkpoint 2>/dev/null",
        check=False,
    ).stdout
    cleaned = {}
    for line in out.splitlines()[2:]:  # line 1 = format version, line 2 = entry count
        parts = line.split()
        if len(parts) == 3:
            cleaned[f"{parts[0]}-{parts[1]}"] = int(parts[2])
    return cleaned


def producer_configs(logs: str, client_id: str) -> list[dict[str, str]]:
    """Every `ProducerConfig values:` block that the Connect worker logged for `client_id`
    (one per task start), as dicts, oldest first. `logs` is `docker compose logs` output
    (`connect  | <TAB>acks = -1`)."""
    blocks: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in logs.splitlines():
        body = line.split("| ", 1)[1] if "| " in line else line
        if "ProducerConfig values:" in body:
            current = {}
            blocks.append(current)
        elif current is not None and body.startswith("\t") and " = " in body:
            k, v = body.strip().split(" = ", 1)
            current[k] = v
        else:
            current = None
    return [b for b in blocks if b.get("client.id") == client_id]


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


# --- probe shop: mirrors of the business tables for the probe lifecycle -------------------
# Integration tests never write into the business tables of the live stack (00-overview,
# "Pozostało do zrobienia"): Spark turns every committed change of shop.orders into gold
# history (status transitions, funnel), which a later DELETE does not undo. The probe
# orders live in per-session copies `shop.zz_probe_<hex>_<table>` instead: captured by
# Debezium like any shop table (topic olist.shop.zz_probe_<hex>_<table>, own subjects),
# processed by Spark into bronze/silver, never into gold (not in GOLD_SOURCES), and
# validated by the consumer with the rules of the table they mirror (consumer.rules
# .table_role). The topics are created up front with the partition count and cleanup
# policy of the business topic, so the partitioning claims hold for them too.
PROBE_SHOP_PREFIX = "zz_probe_"
PROBE_SHOP_TABLES = ("customers", "orders", "order_items", "order_payments")


@dataclass(frozen=True)
class ProbeShop:
    tag: str

    def table(self, role: str) -> str:
        """`orders` → `zz_probe_<tag>_orders`."""
        return f"{PROBE_SHOP_PREFIX}{self.tag}_{role}"

    def qualified(self, role: str) -> str:
        return f"shop.{self.table(role)}"

    def topic(self, role: str) -> str:
        return topic(self.table(role))

    def role_of(self, table_or_topic: str) -> str:
        name = table_or_topic.rsplit(".", 1)[-1]
        prefix = f"{PROBE_SHOP_PREFIX}{self.tag}_"
        return name[len(prefix) :] if name.startswith(prefix) else name

    @classmethod
    def create(cls, app: psycopg.Connection, admin) -> ProbeShop:
        from confluent_kafka.admin import ConfigResource, NewTopic

        shop = cls(uuid.uuid4().hex[:8])
        for role in PROBE_SHOP_TABLES:
            real = topic(role)
            parts = len(admin.list_topics(real, timeout=10).topics[real].partitions)
            res = ConfigResource(ConfigResource.Type.TOPIC, real)
            described = admin.describe_configs([res])[res].result(timeout=15)
            config = {k: described[k].value for k in ("cleanup.policy", "segment.ms")}
            t = shop.topic(role)
            admin.create_topics([NewTopic(t, parts, 1, config=config)])[t].result(timeout=30)
        with app.transaction():
            for role in PROBE_SHOP_TABLES:
                # same columns, types, NOT NULL and PK as the business table; no FKs (the
                # event trigger makes it REPLICA IDENTITY FULL like every shop table)
                app.execute(
                    f"CREATE TABLE {shop.qualified(role)} (LIKE shop.{role} "
                    "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                )
        return shop

    def teardown(self, app, connect: Http, registry: Http, admin) -> None:
        """Drop the tables once the connector committed past every change they had
        (03 §13), then their subjects and topics. On a timeout they stay and the next
        session's sweep removes them."""
        now = lsn_to_int(app.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])
        wait_committed_past(now)
        for role in reversed(PROBE_SHOP_TABLES):
            drop_captured_table(app, self.table(role), connect, registry, admin)


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
    app: psycopg.Connection, shop: ProbeShop, tag: str, status: str, purchase_ts: datetime
) -> ProbeOrder:
    """customer + order + item + payment in ONE transaction (02 §6), as the simulator
    does, into the probe shop's tables (real product/seller ids, read only)."""
    oid, cid = probe_ids(tag)
    product = app.execute("SELECT product_id FROM shop.products LIMIT 1").fetchone()[0]
    seller = app.execute("SELECT seller_id FROM shop.sellers LIMIT 1").fetchone()[0]
    price, freight, payment = Decimal("58.90"), Decimal("13.29"), Decimal("72.19")
    with app.transaction():
        app.execute(
            f"INSERT INTO {shop.qualified('customers')} (customer_id, customer_unique_id, "
            "customer_zip_code_prefix, customer_city, customer_state) "
            "VALUES (%s, %s, '01037', 'sao paulo', 'SP')",
            (cid, cid),
        )
        app.execute(
            f"INSERT INTO {shop.qualified('orders')} (order_id, customer_id, order_status, "
            "order_purchase_timestamp, order_estimated_delivery_date) "
            "VALUES (%s, %s, %s, %s, %s + interval '30 days')",
            (oid, cid, status, purchase_ts, purchase_ts),
        )
        app.execute(
            f"INSERT INTO {shop.qualified('order_items')} (order_id, order_item_id, product_id, "
            "seller_id, shipping_limit_date, price, freight_value) "
            "VALUES (%s, 1, %s, %s, %s, %s, %s)",
            (oid, product, seller, purchase_ts, price, freight),
        )
        app.execute(
            f"INSERT INTO {shop.qualified('order_payments')} (order_id, payment_sequential, "
            "payment_type, payment_installments, payment_value) "
            "VALUES (%s, 1, 'credit_card', 1, %s)",
            (oid, payment),
        )
    return ProbeOrder(oid, cid, purchase_ts, price, freight, payment)


def probe_set_status(
    app: psycopg.Connection, shop: ProbeShop, order: ProbeOrder, status: str, **cols: Any
) -> None:
    sets = ", ".join(["order_status = %s"] + [f"{c} = %s" for c in cols])
    app.execute(
        f"UPDATE {shop.qualified('orders')} SET {sets} WHERE order_id = %s",
        (status, *cols.values(), order.order_id),
    )


def probe_delete(app: psycopg.Connection, shop: ProbeShop, order: ProbeOrder) -> None:
    """Remove the probe rows in one transaction (children first)."""
    with app.transaction():
        for role in ("order_payments", "order_items", "orders"):
            app.execute(
                f"DELETE FROM {shop.qualified(role)} WHERE order_id = %s", (order.order_id,)
            )
        app.execute(
            f"DELETE FROM {shop.qualified('customers')} WHERE customer_id = %s",
            (order.customer_id,),
        )


@dataclass
class Lifecycle:
    """What one run of the probe lifecycle produced (see conftest `probe_lifecycle`)."""

    shop: ProbeShop
    clean: ProbeOrder  # created → approved → deleted
    bad: ProbeOrder  # delivered → created (forbidden transition, 04 §6) → deleted
    start: dict[str, Watermarks]
    records: dict[str, list[Record]]  # new records per topic since `start`

    def of(self, role: str, order: ProbeOrder) -> list[Record]:
        """Records of `order` in the probe table that mirrors `role` (e.g. `orders`)."""
        return [
            r
            for r in self.records[self.shop.topic(role)]
            if (r.key or {}).get("order_id") == order.order_id
            or (role == "customers" and (r.key or {}).get("customer_id") == order.customer_id)
        ]


def run_probe_lifecycle(app: psycopg.Connection, reader: TopicReader, shop: ProbeShop) -> Lifecycle:
    topics = [shop.topic(t) for t in PROBE_SHOP_TABLES] + [TRANSACTION_TOPIC]
    start = {t: reader.watermarks(t) for t in topics}
    clock = sim_state(app)["sim_clock"] or datetime(2017, 1, 5)
    clean = probe_purchase(app, shop, "clean", "created", clock)
    probe_set_status(app, shop, clean, "approved", order_approved_at=clock)
    probe_delete(app, shop, clean)
    bad = probe_purchase(app, shop, "bad", "delivered", clock)
    probe_set_status(app, shop, bad, "created")
    probe_delete(app, shop, bad)

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
        done = complete if t == shop.topic("orders") else _has_tombstones(clean, bad, t)
        records[t] = reader.read_new(start[t], done, timeout=90)
    # the END record of every probe transaction carries the same id as its data events
    tx_ids = {
        tx_prefix(r.value["transaction"]["id"])
        for r in records[shop.topic("orders")]
        if r.value
        and r.value.get("transaction")
        and (r.key or {}).get("order_id") in (clean.order_id, bad.order_id)
    }
    records[TRANSACTION_TOPIC] = reader.read_new(
        start[TRANSACTION_TOPIC], lambda recs: _transactions_done(recs, tx_ids), timeout=90
    )
    return Lifecycle(shop=shop, clean=clean, bad=bad, start=start, records=records)


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
        # Never drop a captured table before its changes are behind the committed offset
        # (03 §13): after a task restart Debezium re-reads the WAL from that offset and looks
        # the table's PK up in the live catalog under the name the WAL carries, so a dropped
        # table yields keyless records that the compacted topic rejects (task FAILED for
        # good). Renaming it to `_…` would not help: pgoutput describes the relation with
        # the name it had when the change was written. On a timeout the table is therefore
        # left in place (it is harmless while it exists) and the next session sweeps it.
        wait_committed_past(self.last_lsn)
        drop_captured_table(app, self.table, connect, registry, admin)


def drop_captured_table(app, table: str, connect: Http, registry: Http, admin) -> None:
    """DROP a captured probe table whose changes are already committed by the connector,
    then remove its subject config, subjects (permanently) and topic."""
    t = topic(table)
    app.execute(f"DROP TABLE IF EXISTS shop.{table}")
    wait_running(connect, timeout=120)
    registry.delete(f"/config/{t}-value")
    for s in (f"{t}-key", f"{t}-value"):
        if registry.delete(f"/subjects/{s}").status_code == 200:
            registry.delete(f"/subjects/{s}", params={"permanent": "true"})
    if t in admin.list_topics(timeout=10).topics:
        admin.delete_topics([t])[t].result(timeout=30)


def sweep_leftover_probe_tables(
    app, connect: Http, registry: Http, admin, prefix: str = PROBE_PREFIX
) -> list[str]:
    """Drop captured probe tables (`prefix`*) an earlier session had to leave behind
    (teardown timed out waiting for the offset commit, or the run was interrupted). Waits
    until the connector committed past the current WAL position first, which covers every
    change those tables ever had."""
    rows = app.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'shop' AND tablename LIKE %s",
        (prefix.replace("_", "\\_") + "%",),
    ).fetchall()
    leftovers = [r[0] for r in rows]
    if leftovers:
        now = lsn_to_int(app.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])
        wait_committed_past(now)
        for table in leftovers:
            drop_captured_table(app, table, connect, registry, admin)
    return leftovers
