"""Debezium PostgreSQL connector (docs/design/03-debezium.md §11, 02 §5, 08 §14).

Runs against the live stack: the connector `olist-postgres` registered by
`connect-init` after the simulator's preload, streaming `olist.shop.*` as AVRO.
Tests that change state (probe rows, DDL on shop.orders, a failing probe table)
restore it before they return, so the suite is re-runnable on the same stack.
"""

from __future__ import annotations

import json
import statistics
import time
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from psycopg import errors

from tests.helpers import cdc
from tests.helpers.cdc import CONNECTOR, HEARTBEAT_TOPIC, SNAPSHOT_FLAGS, TRANSACTION_TOPIC, topic
from tests.helpers.kafka import Record, Watermarks, by_key
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration


# --- registration, secrets ---------------------------------------------------------------
def test_connector_registered_by_connect_init_and_running(connect, stack, pg_reader):
    """03 §8: connect-init registered olist-postgres with PUT and waited for RUNNING; the
    replication slot is active and uses the pre-created publication (02 §3)."""
    st = cdc.connector_status(connect)
    assert cdc.states(st) == ("RUNNING", ["RUNNING"]), st
    assert st["tasks"][0]["version"] == "3.6.3.Final"
    logs = stack.logs("connect-init")
    assert f"PUT http://connect:8083/connectors/{CONNECTOR}/config" in logs
    assert f"{CONNECTOR} RUNNING" in logs
    slot = cdc.slot_state(pg_reader)
    assert slot is not None and slot["active"] is True, slot
    assert slot["wal_status"] == "reserved", slot
    cfg = connect.json(f"/connectors/{CONNECTOR}/config")
    assert cfg["publication.name"] == "dbz_publication"
    assert cfg["publication.autocreate.mode"] == "disabled"
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["snapshot.mode"] == "initial"


def test_register_twice_keeps_one_connector_with_the_current_config(connect):
    """03 §11: registering again (what connect-init does on every start) is a no-op PUT:
    no error, still one connector, the stored config equals the file."""
    file_cfg = cdc.connector_config()
    r = connect.put(
        f"/connectors/{CONNECTOR}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(file_cfg),
    )
    # 200 = updated an existing connector; 201 would mean it had to be created again
    assert r.status_code == 200, (r.status_code, r.text)
    assert connect.json("/connectors") == [CONNECTOR]
    stored = connect.json(f"/connectors/{CONNECTOR}/config")
    assert {k: v for k, v in stored.items() if k != "name"} == file_cfg
    cdc.wait_running(connect)


def test_password_is_never_exposed_by_the_rest_api(connect, settings):
    """03 §8/§11: the config holds `${env:DEBEZIUM_DB_PASSWORD}`; the clear password of
    .env appears nowhere in the REST responses."""
    secret = settings["DEBEZIUM_DB_PASSWORD"]
    cfg = connect.json(f"/connectors/{CONNECTOR}/config")
    assert cfg["database.password"] == "${env:DEBEZIUM_DB_PASSWORD}"
    for path in (
        f"/connectors/{CONNECTOR}",
        f"/connectors/{CONNECTOR}/config",
        f"/connectors/{CONNECTOR}/tasks",
        "/connectors?expand=info&expand=status",
    ):
        assert secret not in connect.get(path).text, path


# --- envelope: c / u / d with full before, keys, transaction metadata ---------------------
def test_insert_update_delete_give_c_u_d_with_full_before(probe_lifecycle):
    """03 §11: INSERT, UPDATE, DELETE in shop.orders → c, u, d; u and d carry the full
    old row (REPLICA IDENTITY FULL), every event has source.lsn, ts_ms and a
    transaction block; d is followed by a tombstone (04 §2)."""
    lc = probe_lifecycle
    recs = lc.of("orders", lc.clean)
    assert [r.op for r in recs] == ["c", "u", "d", "t"], [r.op for r in recs]
    c, u, d, t = recs
    assert c.before == {} and c.after["order_status"] == "created"
    assert c.after["order_id"] == lc.clean.order_id
    assert c.after["order_approved_at"] is None
    assert u.before == c.after, "UPDATE must carry the complete previous row"
    assert u.after["order_status"] == "approved" and u.after["order_approved_at"] is not None
    assert d.before == u.after, "DELETE must carry the complete deleted row"
    assert d.value["after"] is None
    assert t.value is None and t.key == c.key
    for r in (c, u, d):
        assert r.source["snapshot"] == "false"
        assert r.source["schema"] == "shop" and r.source["table"] == "orders"
        assert isinstance(r.source["lsn"], int) and r.source["lsn"] > 0
        assert isinstance(r.value["ts_ms"], int)
        assert r.value["transaction"]["id"].startswith(f"{r.source['txId']}:")
    assert c.source["lsn"] < u.source["lsn"] < d.source["lsn"]
    # the other tables of the purchase saw c and d (+ tombstone) too
    for table in ("order_items", "order_payments", "customers"):
        ops = [r.op for r in lc.of(table, lc.clean)]
        assert ops == ["c", "d", "t"], (table, ops)
    items = lc.of("order_items", lc.clean)
    assert items[0].after["price"] == Decimal("58.90")
    assert items[1].before["price"] == Decimal("58.90"), "DELETE of an item carries price"


def test_purchase_transaction_metadata(probe_lifecycle):
    """03 §7: olist.transaction carries BEGIN/END per transaction; the END of a purchase
    counts 4 events over customers/orders/order_items/order_payments and shares its txId
    with the data events (`transaction.id` is `txId:lsn` of each record, so only the txId
    prefix is common; `total_order`, `data_collection_order` number the events)."""
    lc = probe_lifecycle
    c = lc.of("orders", lc.clean)[0]
    tx_id = cdc.tx_prefix(c.value["transaction"]["id"])
    assert c.source["txId"] == int(tx_id)
    assert c.value["transaction"]["total_order"] == 2  # customers insert came first
    assert c.value["transaction"]["data_collection_order"] == 1
    tx = {
        (r.value["status"], cdc.tx_prefix(r.value["id"])): r
        for r in lc.records[TRANSACTION_TOPIC]
        if r.value
    }
    assert ("BEGIN", tx_id) in tx and ("END", tx_id) in tx
    begin, end = tx[("BEGIN", tx_id)], tx[("END", tx_id)]
    assert begin.value["event_count"] is None and begin.value["data_collections"] is None
    assert end.value["event_count"] == 4
    collections = {d["data_collection"]: d["event_count"] for d in end.value["data_collections"]}
    assert collections == {
        "shop.customers": 1,
        "shop.orders": 1,
        "shop.order_items": 1,
        "shop.order_payments": 1,
    }
    assert end.key == {"id": end.value["id"]}
    assert begin.offset < end.offset and end.value["ts_ms"] >= begin.value["ts_ms"]
    # the whole lifecycle of the clean probe is exactly 3 transactions, in order
    ids: list[str] = []
    for r in lc.of("orders", lc.clean):
        if r.value and cdc.tx_prefix(r.value["transaction"]["id"]) not in ids:
            ids.append(cdc.tx_prefix(r.value["transaction"]["id"]))
    assert len(ids) == 3
    assert [tx[("END", i)].value["event_count"] for i in ids] == [4, 1, 4]


def test_message_key_is_the_table_pk_with_its_own_subject(schema_registry, pg_reader, topic_reader):
    """03 §11, §4: key = PK columns of the table (no message.key.columns); the key schema is
    registered under <topic>-key, e.g. order_items-key = (order_id, order_item_id)."""
    for table in cdc.SHOP_TABLES:
        pk = cdc.primary_key(pg_reader, table)
        schema = json.loads(
            schema_registry.json(f"/subjects/{topic(table)}-key/versions/latest")["schema"]
        )
        assert schema["type"] == "record" and schema["name"] == "Key", (table, schema)
        assert schema["namespace"] == topic(table)
        assert [f["name"] for f in schema["fields"]] == pk, (table, pk, schema["fields"])
    key_fields = {
        f["name"]: f["type"]
        for f in json.loads(
            schema_registry.json(f"/subjects/{topic('order_items')}-key/versions/latest")["schema"]
        )["fields"]
    }
    assert key_fields == {"order_id": "string", "order_item_id": "int"}
    # a decoded key equals the PK columns of its own `after`
    start = topic_reader.watermarks(topic("order_items"))
    first = topic_reader.read_range(
        Watermarks(start.topic, dict.fromkeys(start.high, 0)),
        Watermarks(start.topic, {p: min(2, h) for p, h in start.high.items()}),
    )
    assert first, "no order_items messages yet"
    for r in first:
        assert r.key == {"order_id": r.after["order_id"], "order_item_id": r.after["order_item_id"]}


# --- types ---------------------------------------------------------------------------------
def test_timestamps_and_decimals_are_logical_types(schema_registry, pg_reader, topic_reader):
    """03 §11, §6: time.precision.mode=connect → TIMESTAMP as `long` timestamp-millis;
    decimal.handling.mode=precise → NUMERIC(10,2) as `bytes` decimal(10,2). A decoded
    message carries exactly the Postgres values (58.90 stays 58.90)."""
    value = json.loads(
        schema_registry.json(f"/subjects/{topic('order_items')}-value/versions/latest")["schema"]
    )
    fields = {f["name"]: f["type"] for f in _value_record(value)["fields"]}
    assert fields["price"] == {
        "type": "bytes",
        "scale": 2,
        "precision": 10,
        "connect.version": 1,
        "connect.parameters": {"scale": "2", "connect.decimal.precision": "10"},
        "connect.name": "org.apache.kafka.connect.data.Decimal",
        "logicalType": "decimal",
    }
    assert fields["shipping_limit_date"] == {
        "type": "long",
        "connect.version": 1,
        "connect.name": "org.apache.kafka.connect.data.Timestamp",
        "logicalType": "timestamp-millis",
    }
    orders = json.loads(
        schema_registry.json(f"/subjects/{topic('orders')}-value/versions/latest")["schema"]
    )
    ofields = {f["name"]: f for f in _value_record(orders)["fields"]}
    assert ofields["order_purchase_timestamp"]["type"]["logicalType"] == "timestamp-millis"
    # nullable milestone dates: union with null first and default null (BACKWARD-friendly)
    approved = ofields["order_approved_at"]
    assert approved["type"][0] == "null" and approved["default"] is None
    assert approved["type"][1]["logicalType"] == "timestamp-millis"

    # a real row: Kafka value == Postgres value
    wm = topic_reader.watermarks(topic("order_items"))
    recs = topic_reader.read_range(
        Watermarks(wm.topic, dict.fromkeys(wm.high, 0)),
        Watermarks(wm.topic, {p: min(1, h) for p, h in wm.high.items()}),
    )
    rec = next(r for r in recs if r.value and r.after)
    row = pg_reader.execute(
        "SELECT price, freight_value, shipping_limit_date FROM shop.order_items "
        "WHERE order_id = %s AND order_item_id = %s",
        (rec.after["order_id"], rec.after["order_item_id"]),
    ).fetchone()
    assert row is not None, "row vanished from Postgres (probe rows are excluded by design)"
    assert isinstance(rec.after["price"], Decimal)
    assert rec.after["price"] == row[0] and rec.after["price"].as_tuple().exponent == -2
    assert rec.after["freight_value"] == row[1]
    ts = rec.after["shipping_limit_date"]
    assert isinstance(ts, datetime)
    assert ts.astimezone(UTC).replace(tzinfo=None) == row[2]


def _value_record(envelope: dict) -> dict:
    before = next(f for f in envelope["fields"] if f["name"] == "before")
    return next(t for t in before["type"] if isinstance(t, dict))


# --- snapshot then stream -------------------------------------------------------------------
class TopicDump(list):
    """Records of a topic from offset 0 up to `end` (the watermarks when it was read)."""

    end: Watermarks


@pytest.fixture(scope="module")
def orders_topic(topic_reader) -> TopicDump:
    """Every message of olist.shop.orders up to the watermarks at the time of the call."""
    t = topic("orders")
    end = topic_reader.watermarks(t)
    dump = TopicDump(topic_reader.read_range(Watermarks(t, dict.fromkeys(end.high, 0)), end, 180))
    dump.end = end
    return dump


def test_preload_rows_arrive_as_snapshot_reads_and_the_stream_continues_without_gap_or_duplicate(
    orders_topic, pg_reader, stack
):
    """03 §11, §2 and 08 §14: rows loaded before the connector started (the simulator's
    preload, everything before SIM_START) arrive as op=r in one consistent snapshot;
    afterwards the stream delivers c/u/d from the slot's consistent point. No key is both
    snapshotted and re-created (duplicate) and no Postgres row is missing (gap)."""
    state = cdc.sim_state(pg_reader)
    assert state["phase"] in ("replay", "done") and state["preload_finished_at"] is not None
    preload_ids = {
        r[0]
        for r in pg_reader.execute(
            "SELECT order_id FROM shop.orders WHERE order_purchase_timestamp < %s",
            (state["sim_start"],),
        )
    }
    assert preload_ids, "no preload orders: SIM_START must leave history before it"
    groups = by_key(orders_topic)
    reads = {k for k, recs in groups.items() if any(r.op == "r" for r in recs)}
    creates = {k for k, recs in groups.items() if any(r.op == "c" for r in recs)}
    # The topic is a compacted mirror (segment.ms = 1 h): on a stack older than an hour the
    # cleaner may have removed every record of a key but its latest one, so the checks
    # below only use properties compaction preserves (it removes records, never reorders
    # or adds them).
    preload = {(oid,) for oid in preload_ids}
    assert preload <= set(groups), f"{len(preload - set(groups))} preload orders not in Kafka"
    # no duplicate at the boundary: a preload row is never re-created by the stream,
    # a row created by the stream was never in the snapshot, nothing is read twice
    assert preload & creates == set(), f"preload rows re-created: {sorted(preload & creates)[:5]}"
    assert (reads - preload) == set(), (
        f"snapshot reads of non-preload rows: {sorted(reads - preload)[:5]}"
    )
    assert reads & creates == set(), (
        f"keys both snapshotted and created: {sorted(reads & creates)[:5]}"
    )
    for k, recs in groups.items():
        ops = [r.op for r in recs]
        assert ops.count("r") <= 1 and ops.count("c") <= 1, (k, ops)
        if "r" in ops:
            assert ops.index("r") == 0, (k, ops)
        if "c" in ops:
            assert ops.index("c") == 0, (k, ops)
    # exactly one op=r per preload order: provable only where the cleaner never ran (it may
    # have dropped the `r` of a row updated later). The checkpoint is read AFTER the dump,
    # and it only grows, so "never cleaned now" implies "complete when dumped". On a fresh
    # stack (< segment.ms = 1 h) that is every partition, so reads == preload exactly.
    cleaned = cdc.cleaned_offsets(stack)
    t = orders_topic.end.topic
    intact = {p for p in orders_topic.end.high if cleaned.get(f"{t}-{p}", 0) == 0}
    exact = {k for k in preload if groups[k][0].partition in intact}
    print(
        f"\nsnapshot: {len(reads)} op=r keys, {len(preload)} preload orders; exact check on "
        f"never-compacted partitions {sorted(intact)}: {len(exact)} orders"
    )
    not_read = sorted(k for k in exact if [r.op for r in groups[k]].count("r") != 1)
    assert not_read == [], (
        f"{len(not_read)} preload orders without exactly one op=r: {not_read[:5]}"
    )
    if intact == set(orders_topic.end.high):
        assert reads == preload
    # the snapshot phase precedes the stream in every partition, with the right flags
    for partition in {r.partition for r in orders_topic}:
        ops = [r for r in orders_topic if r.partition == partition]
        last_r = max((i for i, r in enumerate(ops) if r.op == "r"), default=-1)
        first_stream = next((i for i, r in enumerate(ops) if r.op in ("c", "u", "d")), None)
        if first_stream is not None:
            assert last_r < first_stream, f"partition {partition}: snapshot read after stream"
        for r in ops:
            if r.op == "r":
                assert r.source["snapshot"] in SNAPSHOT_FLAGS, r.source
            elif r.value is not None:
                assert r.source["snapshot"] == "false", r.source
    # no gap: every order in Postgres (snapshot taken AFTER the topic read) is in the topic
    # once the stream caught up; probe rows deleted in between are allowed to be gone
    pg_ids = {r[0] for r in pg_reader.execute("SELECT order_id FROM shop.orders")}
    missing = pg_ids - {k[0] for k in groups}
    if missing:
        # rows written after the dump was taken: they must show up right behind it
        from tests.helpers.kafka import TopicReader

        late = TopicReader().read_new(
            orders_topic.end,
            lambda rs: missing <= {(r.key or {}).get("order_id") for r in rs},
            timeout=60,
        )
        missing -= {(r.key or {}).get("order_id") for r in late}
    assert missing == set(), (
        f"{len(missing)} Postgres orders never reached Kafka: {sorted(missing)[:5]}"
    )


def _multi_event_orders(records) -> list[list[Record]]:
    """Simulator orders (not probes) with at least three data events (c|r, u, u…)."""
    return [
        recs
        for k, recs in by_key(records).items()
        if not str(k[0]).startswith("test-stage2-") and sum(1 for r in recs if r.value) >= 3
    ]


# slow: polls a full read of olist.shop.orders every 20 s (62 s measured)
@pytest.mark.slow
def test_changes_of_one_row_stay_in_one_partition_in_lsn_order(topic_reader):
    """04 §8/§4: all events of one key hash to the same partition and keep their order
    (offsets increase with source.lsn); an INSERT or snapshot read only ever comes first.
    On a fresh stack the test waits until the replay produced >= 10 orders with three or
    more events (approval and shipment follow the purchase within minutes at x1440)."""
    t = topic("orders")

    def enough():
        end = topic_reader.watermarks(t)
        recs = topic_reader.read_range(Watermarks(t, dict.fromkeys(end.high, 0)), end, 180)
        multi = _multi_event_orders(recs)
        return multi if len(multi) >= 10 else None

    multi = wait_until(enough, timeout=420, every=20, desc=">= 10 orders with 3+ events")
    rank = {"created": 0, "approved": 1, "shipped": 2, "delivered": 3}
    checked = 0
    for recs in multi:
        data = [r for r in recs if r.value]
        assert len({r.partition for r in data}) == 1, [(r.partition, r.offset) for r in data]
        lsns = [r.source["lsn"] for r in data]
        assert lsns == sorted(lsns), lsns
        # an insert/snapshot read can only be the first event of a key (compaction may
        # have removed it, never moved it)
        assert all(r.op in ("u", "d") for r in data[1:]), [r.op for r in data]
        statuses = [r.after["order_status"] for r in data if r.after]
        if all(st in rank for st in statuses):
            assert [rank[x] for x in statuses] == sorted(rank[x] for x in statuses), statuses
            checked += 1
    assert checked >= 10


# --- schema evolution ---------------------------------------------------------------------
EVOLUTION_COLUMN = "delivery_channel"


def test_add_nullable_column_gives_a_new_schema_version_and_the_connector_stays_running(
    pg_app, pg_reader, connect, schema_registry, topic_reader
):
    """03 §11, §5: ALTER TABLE shop.orders ADD COLUMN delivery_channel TEXT (as shop_app,
    01 §7 stands in for the simulator's synthetic event) → the next change of the table is
    written with a new AVRO schema that has the nullable field, registered as a new version
    of olist.shop.orders-value (BACKWARD passes), connector RUNNING. The column is dropped
    again and the writer schema returns to the previous one."""
    subject = f"{topic('orders')}-value"
    pg_app.execute(f"ALTER TABLE shop.orders DROP COLUMN IF EXISTS {EVOLUTION_COLUMN}")
    before_versions = schema_registry.json(f"/subjects/{subject}/versions")
    order_id = pg_reader.execute(
        "SELECT order_id FROM shop.orders WHERE order_id NOT LIKE 'test-stage2-%%' "
        "ORDER BY order_purchase_timestamp DESC LIMIT 1"
    ).fetchone()[0]

    def mine(recs: list[Record], predicate) -> Record | None:
        return next(
            (
                r
                for r in recs
                if (r.key or {}).get("order_id") == order_id and r.value and predicate(r)
            ),
            None,
        )

    # the writer schema in use right now (a no-op update; the subject may already hold
    # the evolved version from an earlier run, so `latest` is not the reference)
    start0 = topic_reader.watermarks(topic("orders"))
    pg_app.execute(
        "UPDATE shop.orders SET order_status = order_status WHERE order_id = %s", (order_id,)
    )
    recs = topic_reader.read_new(
        start0, lambda rs: mine(rs, lambda r: r.op == "u") is not None, timeout=90
    )
    current = mine(recs, lambda r: r.op == "u")
    id_without = current.value_schema_id
    assert EVOLUTION_COLUMN not in schema_registry.json(f"/schemas/ids/{id_without}")["schema"]
    start = topic_reader.watermarks(topic("orders"))
    try:
        pg_app.execute(f"ALTER TABLE shop.orders ADD COLUMN {EVOLUTION_COLUMN} text")
        pg_app.execute(
            f"UPDATE shop.orders SET {EVOLUTION_COLUMN} = 'web' WHERE order_id = %s", (order_id,)
        )
        recs = topic_reader.read_new(
            start,
            lambda rs: mine(rs, lambda r: r.after.get(EVOLUTION_COLUMN) == "web") is not None,
            timeout=90,
        )
        evolved = mine(recs, lambda r: r.after.get(EVOLUTION_COLUMN) == "web")
        assert evolved.op == "u" and evolved.before.get(EVOLUTION_COLUMN) is None
        assert evolved.value_schema_id != id_without, "writer schema did not change"
        versions = schema_registry.json(f"/subjects/{subject}/versions")
        new_version = schema_registry.json(f"/schemas/ids/{evolved.value_schema_id}/versions")
        assert any(v["subject"] == subject for v in new_version), new_version
        with_col = json.loads(
            schema_registry.json(f"/schemas/ids/{evolved.value_schema_id}")["schema"]
        )
        field = next(f for f in _value_record(with_col)["fields"] if f["name"] == EVOLUTION_COLUMN)
        assert field["type"] == ["null", "string"] and field["default"] is None
        assert set(versions) >= set(before_versions) and len(versions) >= 2
        ids = {schema_registry.json(f"/subjects/{subject}/versions/{v}")["id"] for v in versions}
        assert evolved.value_schema_id in ids and id_without in ids
        # the registry agrees that the change is BACKWARD compatible with the old version
        compat = schema_registry.post(
            f"/compatibility/subjects/{subject}/versions/latest",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"schema": json.dumps(with_col)}),
        ).json()
        assert compat["is_compatible"] is True, compat
        assert cdc.states(cdc.connector_status(connect)) == ("RUNNING", ["RUNNING"])
    finally:
        pg_app.execute(f"ALTER TABLE shop.orders DROP COLUMN IF EXISTS {EVOLUTION_COLUMN}")
    # after the DROP the writer schema is the old one again (no-op update, same values)
    start2 = topic_reader.watermarks(topic("orders"))
    pg_app.execute(
        "UPDATE shop.orders SET order_status = order_status WHERE order_id = %s", (order_id,)
    )
    recs = topic_reader.read_new(
        start2,
        lambda rs: mine(rs, lambda r: EVOLUTION_COLUMN not in r.after) is not None,
        timeout=90,
    )
    back = mine(recs, lambda r: EVOLUTION_COLUMN not in r.after)
    assert back.value_schema_id == id_without
    assert cdc.states(cdc.connector_status(connect)) == ("RUNNING", ["RUNNING"])


@pytest.mark.slow
def test_incompatible_type_change_is_rejected_and_the_connector_fails_then_recovers(
    captured_probe_table, pg_app, connect, schema_registry, topic_reader
):
    """03 §11, §5 and 04 §8: a column type change that is not BACKWARD compatible
    (text → integer) is rejected by Schema Registry (`is_compatible: false`, no new
    version) and the connector task goes FAILED with the registry's error. Recovery: the
    operator relaxes compatibility for that one subject, restarts the failed task, the
    event flows. Done on the throw-away captured table so the simulator's tables are
    untouched; the fixture removes table, topic and subjects at the end of the session."""
    probe = captured_probe_table
    table, t, subject = probe.table, probe.topic, probe.subject
    assert schema_registry.json(f"/subjects/{subject}/versions") == [1]
    v1 = json.loads(schema_registry.json(f"/subjects/{subject}/versions/1")["schema"])
    start = topic_reader.watermarks(t)

    pg_app.execute(f"ALTER TABLE shop.{table} ALTER COLUMN v TYPE integer USING v::integer")
    pg_app.execute(f"INSERT INTO shop.{table} VALUES (2, 2)")
    try:
        st = cdc.wait_connector(connect, "RUNNING", ["FAILED"], timeout=120)
        trace = st["tasks"][0]["trace"]
        assert "incompatible" in trace.lower() and subject in trace, trace[:500]
        assert schema_registry.json(f"/subjects/{subject}/versions") == [1]
        # the registry's own verdict on the schema Debezium tried to register
        # (the column stays nullable, so the union becomes ["null", "int"])
        v_int = json.loads(json.dumps(v1))
        for f in _value_record(v_int)["fields"]:
            if f["name"] == "v":
                f["type"] = ["null", "int"]
        compat = schema_registry.post(
            f"/compatibility/subjects/{subject}/versions/latest",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"schema": json.dumps(v_int)}),
        ).json()
        assert compat["is_compatible"] is False, compat
    finally:
        # recovery (operator decision): compatibility NONE for this subject only; the
        # pending event can only pass this way, so it runs even if an assertion failed
        r = schema_registry.put(
            f"/config/{subject}",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            data=json.dumps({"compatibility": "NONE"}),
        )
        assert r.status_code == 200, r.text
        cdc.restart_failed_tasks(connect)
        cdc.wait_running(connect, timeout=120)
    recs = topic_reader.read_new(
        start, lambda rs: any(r.after.get("id") == 2 for r in rs if r.value), timeout=90
    )
    second = next(r for r in recs if r.value and r.after.get("id") == 2)
    probe.note(second)
    assert second.after == {"id": 2, "v": 2}
    assert schema_registry.json(f"/subjects/{subject}/versions") == [1, 2]
    assert schema_registry.json("/config") == {"compatibilityLevel": "BACKWARD"}
    assert schema_registry.json(f"/config/{subject}") == {"compatibilityLevel": "NONE"}


# --- heartbeat ------------------------------------------------------------------------------
# slow: needs two heartbeat records 10 s apart after an upsert (24 s measured)
@pytest.mark.slow
def test_heartbeat_writes_the_sim_table_and_feeds_the_heartbeat_topic(pg_reader, topic_reader):
    """03 §7, 02 §5: every 10 s the connector upserts dbz.heartbeat (heartbeat.action.query;
    the test id predates the move out of schema sim) and emits a record to
    __debezium-heartbeat.olist (key serverName=olist, value ts_ms)."""
    ts0 = pg_reader.execute("SELECT ts FROM dbz.heartbeat WHERE id = 1").fetchone()[0]
    wait_until(
        lambda: pg_reader.execute("SELECT ts FROM dbz.heartbeat WHERE id = 1").fetchone()[0] > ts0,
        timeout=30,
        desc="heartbeat.action.query ran",
    )
    start = topic_reader.watermarks(HEARTBEAT_TOPIC)
    recs = topic_reader.read_new(start, lambda rs: len(rs) >= 2, timeout=40)
    assert all(r.key == {"serverName": "olist"} for r in recs)
    ts = [r.value["ts_ms"] for r in recs]
    assert ts == sorted(ts) and 5000 <= ts[1] - ts[0] <= 20000, ts


@pytest.mark.slow
def test_heartbeat_advances_the_slot_while_shop_is_idle(stack, pg_reader, settings):
    """03 §11, 02 §5: with the simulator stopped (no traffic in shop.*) the slot's
    confirmed_flush_lsn still advances, because the heartbeat table is in the publication
    and its filtered change forces an offset commit. The simulator is started again."""
    stack.run("stop", "simulator")
    try:
        # let in-flight events drain, then measure during silence
        first = wait_until(lambda: cdc.slot_state(pg_reader), 10, desc="slot visible")
        wait_until(
            lambda: cdc.slot_state(pg_reader)["lag_bytes"] < 1_000_000, 60, desc="slot caught up"
        )
        # every shop change was written before this WAL position (simulator stopped), so
        # confirming anything past it can only come from WAL written during the silence,
        # i.e. the heartbeat upsert; merely draining the backlog cannot get there
        silent_from = cdc.lsn_to_int(cdc.slot_state(pg_reader)["current_lsn"])
        hb0 = pg_reader.execute("SELECT ts FROM dbz.heartbeat").fetchone()[0]
        advanced = wait_until(
            lambda: cdc.lsn_to_int(cdc.slot_state(pg_reader)["confirmed_flush_lsn"]) > silent_from,
            timeout=60,  # heartbeat every 10 s + offset flush every 5 s, with margin
            every=2,
            desc="confirmed_flush_lsn passed the WAL position of the last shop change",
        )
        assert advanced
        assert pg_reader.execute("SELECT ts FROM dbz.heartbeat").fetchone()[0] > hb0
        assert cdc.slot_state(pg_reader)["active"] is True, first
    finally:
        stack.run("up", "-d", "--no-deps", "simulator")
        wait_until(
            lambda: next(r for r in stack.ps() if r.service == "simulator").health == "healthy",
            timeout=180,
            every=3,
            desc="simulator healthy again",
        )


# --- latency and lag (measurements for 03/08) -------------------------------------------
def test_cdc_latency_and_slot_lag_at_defaults(topic_reader, pg_reader):
    """03 §9/§13: end-to-end latency of the stream at default settings, over the last
    (at most) 200 streamed orders events committed within five minutes of the newest one:
    Debezium delay = ts_ms − source.ts_ms (commit → connector), Kafka delay = record
    timestamp − source.ts_ms (commit → appended to the log). Waits until 50 such events
    exist (fresh stack). The slot lag stays small (offsets flushed every 5 s). A task
    restart shortly before (re-delivery of up to ~5 s of WAL, 03 §9) would show up as
    seconds, which is why only fresh commits are measured. Values are printed for the docs."""
    t = topic("orders")

    def fresh():
        end = topic_reader.watermarks(t)
        start = Watermarks(t, {p: max(0, h - 300) for p, h in end.high.items()})
        recs = [r for r in topic_reader.read_range(start, end, 120) if r.value]
        streamed = [r for r in recs if r.op in ("c", "u", "d")]
        if not streamed:
            return None
        newest = max(r.source["ts_ms"] for r in streamed)
        window = sorted(
            (r for r in streamed if r.source["ts_ms"] >= newest - 300_000),
            key=lambda r: r.source["ts_ms"],
        )[-200:]
        return window if len(window) >= 50 else None

    streamed = wait_until(fresh, timeout=420, every=20, desc=">= 50 fresh streamed events")
    dbz = [r.value["ts_ms"] - r.source["ts_ms"] for r in streamed]
    e2e = [r.timestamp_ms - r.source["ts_ms"] for r in streamed]
    p = lambda xs, q: sorted(xs)[int(len(xs) * q) - 1]  # noqa: E731
    slot = cdc.slot_state(pg_reader)
    print(
        f"\nCDC latency over {len(streamed)} events: "
        f"debezium median={statistics.median(dbz):.0f}ms "
        f"p95={p(dbz, 0.95)}ms; commit→kafka median={statistics.median(e2e):.0f}ms "
        f"p95={p(e2e, 0.95)}ms max={max(e2e)}ms; slot lag={slot['lag_bytes']} bytes"
    )
    assert statistics.median(e2e) < 2000 and p(e2e, 0.95) < 5000, (
        statistics.median(e2e),
        p(e2e, 0.95),
    )
    assert all(x >= 0 for x in e2e)
    assert slot["lag_bytes"] < 16 * 1024 * 1024, slot


def test_snapshot_time_is_recorded_in_source(orders_topic):
    """03 §2: snapshot reads are tagged (source.snapshot) and carry the snapshot's own
    ts_ms, which lies after the simulator's preload finished (connect-init waited)."""
    reads = [r for r in orders_topic if r.op == "r"]
    assert reads
    ts = {r.source["ts_ms"] for r in reads}
    assert max(ts) - min(ts) < 60_000, "one snapshot, not several"
    assert datetime.fromtimestamp(min(ts) / 1000, tz=UTC) > datetime(2026, 1, 1, tzinfo=UTC)
    assert all(r.value["transaction"] is None for r in reads)


def test_probe_rows_were_removed(pg_reader, probe_lifecycle):
    """The probe lifecycle leaves no row behind (truth views unchanged for other tests)."""
    for table, col in (("orders", "order_id"), ("customers", "customer_id")):
        n = pg_reader.execute(
            f"SELECT count(*) FROM shop.{table} WHERE {col} LIKE 'test-stage2-%%'"
        ).fetchone()[0]
        assert n == 0, (table, n)


def test_debezium_role_cannot_read_sim_state_even_with_heartbeat_access(pg_debezium):
    """02 §4/§5: the heartbeat grant is minimal: the table lives in schema dbz (owned by
    postgres, so shop_app cannot replace it with a trigger that runs as debezium);
    debezium has USAGE on dbz and SELECT/INSERT/UPDATE on dbz.heartbeat only, and no
    USAGE on schema sim at all."""
    privs = pg_debezium.execute(
        "SELECT has_schema_privilege('sim', 'USAGE'), has_schema_privilege('dbz', 'USAGE'), "
        "has_schema_privilege('dbz', 'CREATE'), "
        "(SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'dbz')"
    ).fetchone()
    assert privs == (False, True, False, "postgres"), privs
    assert pg_debezium.execute("SELECT id FROM dbz.heartbeat").fetchone() == (1,)
    with pytest.raises(errors.InsufficientPrivilege):
        pg_debezium.execute("SELECT phase FROM sim.sim_state")
    with pytest.raises(errors.InsufficientPrivilege):
        pg_debezium.execute("DELETE FROM dbz.heartbeat")
    # the connector's own statement works for the role (same text as the connector config)
    query = cdc.connector_config()["heartbeat.action.query"]
    assert "dbz.heartbeat" in query
    pg_debezium.execute(query)
    assert pg_debezium.execute(
        "SELECT now() - ts < interval '5 seconds' FROM dbz.heartbeat"
    ).fetchone() == (True,)


def test_stream_keeps_up_with_the_simulator(pg_reader, topic_reader):
    """Sanity for the measurements above: the newest Postgres order is in Kafka within
    seconds (the simulator writes continuously during replay)."""
    state = cdc.sim_state(pg_reader)
    if state["phase"] != "replay":
        pytest.skip("simulator not replaying (phase done): nothing new to wait for")
    newest = pg_reader.execute(
        "SELECT order_id FROM shop.orders ORDER BY order_purchase_timestamp DESC LIMIT 1"
    ).fetchone()[0]
    t0 = time.monotonic()
    wm = topic_reader.watermarks(topic("orders"))
    back = Watermarks(wm.topic, {p: max(0, h - 200) for p, h in wm.high.items()})
    found = topic_reader.read_new(
        back, lambda rs: any((r.key or {}).get("order_id") == newest for r in rs), timeout=30
    )
    assert found and time.monotonic() - t0 < 30
