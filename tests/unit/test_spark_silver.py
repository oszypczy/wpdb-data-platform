"""05 §10 unit rows on the silver MERGE (Delta on the local filesystem)."""

from __future__ import annotations

import datetime as dt
import json

import pytest

from lakehouse import avro, bronze, delta_io, silver
from lakehouse.registry import static_resolver
from lakehouse.tables import SILVER_PROPERTIES
from tests.helpers import debezium_fixtures as fx

pytestmark = pytest.mark.unit

ORDERS = "olist.shop.orders"
PK = ["order_id"]


def apply(spark, path: str, msgs: list[dict], schemas: dict[int, str] | None = None) -> None:
    """One micro-batch through decode -> bronze rows -> changes -> dedup -> MERGE."""
    resolve = static_resolver(schemas or fx.schema_texts())
    decoded = avro.decode(fx.kafka_df(spark, msgs), resolve)[msgs[0]["topic"]]
    latest = silver.latest_per_key(silver.changes(bronze.envelope_rows(decoded)), PK)
    delta_io.merge_silver(spark, path, latest, PK, SILVER_PROPERTIES)


def state(spark, path: str) -> list[dict]:
    rows = spark.read.format("delta").load(path).drop("_updated_at", "_ingested_at")
    return sorted((r.asDict() for r in rows.collect()), key=lambda r: r["order_id"])


def c(order, lsn, off):
    return fx.message(ORDERS, 1, {"order_id": order["order_id"]}, 2, "c", None, order, lsn, off)


def u(before, after, lsn, off):
    return fx.message(ORDERS, 1, {"order_id": after["order_id"]}, 2, "u", before, after, lsn, off)


def d(before, lsn, off):
    return fx.message(ORDERS, 1, {"order_id": before["order_id"]}, 2, "d", before, None, lsn, off)


def test_merge_c_u_d_gives_a_soft_deleted_row(spark, tmp_path):
    """05 §10: "MERGE sekwencji c → u → d" → wiersz z `_is_deleted = true`."""
    path = str(tmp_path / "orders")
    o = fx.order_row("o1")
    o2 = fx.order_row("o1", "approved", order_approved_at=dt.datetime(2017, 1, 5, 10, 30))
    apply(spark, path, [c(o, 100, 0)])
    assert [(r["order_status"], r["_is_deleted"]) for r in state(spark, path)] == [
        ("created", False)
    ]
    apply(spark, path, [u(o, o2, 110, 1)])
    apply(spark, path, [d(o2, 120, 2)])
    rows = state(spark, path)
    assert len(rows) == 1
    r = rows[0]
    assert (r["_is_deleted"], r["_lsn"], r["_op"], r["order_status"]) == (
        True,
        120,
        "d",
        "approved",
    )
    # CDF is on (06 §5) and the PK is not in the data columns twice
    props = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["properties"]
    assert props["delta.enableChangeDataFeed"] == "true"
    assert props["delta.enableDeletionVectors"] == "false"


def test_same_batch_twice_gives_the_same_state(spark, tmp_path):
    """05 §10: "ta sama paczka dwa razy" → stan identyczny (idempotencja)."""
    path = str(tmp_path / "orders")
    a, b = fx.order_row("o1"), fx.order_row("o2")
    b2 = fx.order_row("o2", "approved", order_approved_at=dt.datetime(2017, 1, 5, 11))
    batch = [c(a, 100, 0), c(b, 101, 1), u(b, b2, 105, 2)]
    apply(spark, path, batch)
    first = state(spark, path)
    apply(spark, path, batch)
    assert state(spark, path) == first
    assert [r["order_status"] for r in first] == ["created", "approved"]


def test_older_event_after_newer_and_create_after_delete_are_ignored(spark, tmp_path):
    """05 §10: "starsze zdarzenie po nowszym; c po d" → ignorowane; brak „wskrzeszenia”."""
    path = str(tmp_path / "orders")
    o = fx.order_row("o1")
    o_appr = fx.order_row("o1", "approved", order_approved_at=dt.datetime(2017, 1, 5, 10, 30))
    o_ship = fx.order_row(
        "o1",
        "shipped",
        order_approved_at=dt.datetime(2017, 1, 5, 10, 30),
        order_delivered_carrier_date=dt.datetime(2017, 1, 6),
    )
    apply(spark, path, [c(o, 100, 0), u(o_appr, o_ship, 120, 2)])
    apply(spark, path, [u(o, o_appr, 110, 1)])  # late, older
    assert [(r["order_status"], r["_lsn"]) for r in state(spark, path)] == [("shipped", 120)]

    # second row: deleted, then a late duplicate of its create arrives
    x = fx.order_row("x1")
    apply(spark, path, [c(x, 200, 3)])
    apply(spark, path, [d(x, 210, 4)])
    apply(spark, path, [c(x, 200, 3)])  # re-delivery (at-least-once)
    rows = {r["order_id"]: r for r in state(spark, path)}
    assert (rows["x1"]["_is_deleted"], rows["x1"]["_lsn"]) == (True, 210)

    # a delete of a row silver has never seen is inserted as deleted (no later resurrection)
    y = fx.order_row("y1")
    apply(spark, path, [d(y, 310, 5)])
    apply(spark, path, [c(y, 300, 6)])
    rows = {r["order_id"]: r for r in state(spark, path)}
    assert (rows["y1"]["_is_deleted"], rows["y1"]["_lsn"]) == (True, 310)


def test_add_column_reaches_silver_through_schema_evolution(spark, tmp_path):
    """05 §2 step 4, 06 §6: a v2 row (new nullable column) is merged with schema
    evolution; existing rows get null."""
    path = str(tmp_path / "orders")
    apply(spark, path, [c(fx.order_row("o1"), 100, 0)])
    v2 = fx.order_row("o2", delivery_channel="pickup_point")
    msg = fx.message(ORDERS, 1, {"order_id": "o2"}, 3, "c", None, v2, 110, 1)
    apply(spark, path, [msg])
    rows = {r["order_id"]: r for r in state(spark, path)}
    assert rows["o1"]["delivery_channel"] is None
    assert rows["o2"]["delivery_channel"] == "pickup_point"


def test_forced_type_change_is_cast_to_the_existing_column_type(spark, tmp_path):
    """03 §5 recovery path (compatibility NONE, text -> integer): the stream does not fail;
    the new values are cast to the type silver already has."""
    path = str(tmp_path / "probe")
    topic = "olist.shop.zz_probe"
    key = fx.key_schema("zz_probe", [{"name": "order_id", "type": "string"}])
    v_text = fx.envelope(
        "zz_probe",
        [{"name": "order_id", "type": "string"}, {"name": "v", "type": ["null", "string"]}],
    )
    v_int = fx.envelope(
        "zz_probe", [{"name": "order_id", "type": "string"}, {"name": "v", "type": ["null", "int"]}]
    )
    ids = {91: key, 92: v_text, 93: v_int}
    old = fx.IDS.copy()
    fx.IDS.update(ids)
    try:
        texts = {i: json.dumps(s) for i, s in ids.items()}
        m1 = fx.message(
            topic, 91, {"order_id": "a"}, 92, "c", None, {"order_id": "a", "v": "x"}, 10, 0
        )
        m2 = fx.message(
            topic, 91, {"order_id": "b"}, 93, "c", None, {"order_id": "b", "v": 2}, 20, 1
        )
        apply(spark, path, [m1], texts)
        apply(spark, path, [m2], texts)
        # both schemas inside one batch as well
        m3 = fx.message(
            topic, 91, {"order_id": "c"}, 93, "c", None, {"order_id": "c", "v": 3}, 30, 2
        )
        m4 = fx.message(
            topic, 91, {"order_id": "d"}, 92, "c", None, {"order_id": "d", "v": "y"}, 40, 3
        )
        apply(spark, path, [m3, m4], texts)
    finally:
        fx.IDS.clear()
        fx.IDS.update(old)
    rows = {r["order_id"]: r["v"] for r in spark.read.format("delta").load(path).collect()}
    assert rows == {"a": "x", "b": "2", "c": "3", "d": "y"}
    assert dict(spark.read.format("delta").load(path).dtypes)["v"] == "string"
