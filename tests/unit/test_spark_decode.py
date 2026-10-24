"""05 §10 unit rows on decoding and deduplication (local SparkSession, no Kafka)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from lakehouse import avro, bronze, silver
from lakehouse.registry import static_resolver
from tests.helpers import debezium_fixtures as fx

pytestmark = pytest.mark.unit

ORDERS = "olist.shop.orders"
ITEMS = "olist.shop.order_items"


def _resolver():
    return static_resolver(fx.schema_texts())


def test_decoding_confluent_header_v1_and_v2_in_one_batch(spark):
    """05 §10: "dekodowanie z nagłówkiem Confluenta, schematy v1 i v2 w jednej paczce" →
    every row decoded, the old ones with the new column = null."""
    v1 = fx.order_row("o1")
    v2 = fx.order_row("o2", delivery_channel="courier")
    msgs = [
        fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "c", None, v1, lsn=100, offset=0),
        fx.message(ORDERS, 1, {"order_id": "o2"}, 3, "c", None, v2, lsn=110, offset=1),
        fx.message(
            ITEMS,
            4,
            {"order_id": "o1", "order_item_id": 1},
            5,
            "c",
            None,
            fx.item_row("o1"),
            lsn=101,
            offset=0,
        ),
    ]
    decoded = avro.decode(fx.kafka_df(spark, msgs), _resolver())
    assert set(decoded) == {ORDERS, ITEMS}
    rows = {r["key"]["order_id"]: r for r in decoded[ORDERS].collect()}
    assert set(rows) == {"o1", "o2"}
    assert {r["value_schema_id"] for r in rows.values()} == {2, 3}
    assert rows["o1"]["value"]["after"]["delivery_channel"] is None
    assert rows["o2"]["value"]["after"]["delivery_channel"] == "courier"
    # logical types: timestamp-millis -> timestamp (UTC session), decimal(10,2) -> Decimal
    assert rows["o1"]["value"]["after"]["order_purchase_timestamp"] == dt.datetime(2017, 1, 5, 10)
    item = decoded[ITEMS].collect()[0]
    assert item["value"]["after"]["price"] == Decimal("58.90")
    types = dict(decoded[ITEMS].select("value.after.*").dtypes)
    assert types["price"] == "decimal(10,2)" and types["shipping_limit_date"] == "timestamp"
    b = bronze.envelope_rows(decoded[ORDERS])
    assert {r["source_lsn"] for r in b.collect()} == {100, 110}
    assert {c for c in b.columns} >= {
        "op",
        "before",
        "after",
        "source_lsn",
        "source_tx_id",
        "source_ts",
        "kafka_partition",
        "kafka_offset",
        "value_schema_id",
        "ingested_at",
        "ingest_date",
    }


def test_tombstone_in_batch_is_filtered(spark):
    """05 §10: "tombstone w paczce" → odfiltrowany."""
    row = fx.order_row("o1")
    msgs = [
        fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "d", row, None, lsn=200, offset=0),
        fx.message(ORDERS, 1, {"order_id": "o1"}, None, None, None, None, lsn=200, offset=1),
    ]
    df = fx.kafka_df(spark, msgs)
    assert df.filter("value IS NULL").count() == 1
    decoded = avro.decode(df, _resolver())
    out = decoded[ORDERS].collect()
    assert [(r["kafka_offset"], r["value"]["op"]) for r in out] == [(0, "d")]


def test_undecodable_value_goes_to_quarantine_rows(spark):
    """05 §6: a value without the Confluent header does not stop the batch; it is reported."""
    good = fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "c", None, fx.order_row("o1"), 1, 0)
    bad = dict(good, value=b"not-avro", offset=1)
    decoded = avro.decode(fx.kafka_df(spark, [good, bad]), _resolver())[ORDERS]
    assert bronze.envelope_rows(decoded).count() == 1
    q = bronze.undecodable_rows(decoded).collect()
    assert [(r["rule"], r["record_key"]) for r in q] == [("avro_decodable", f"{ORDERS}/0/1")]


def test_three_changes_of_one_row_keep_the_highest_lsn(spark):
    """05 §10: "3 zmiany jednego wiersza w paczce" → zostaje zdarzenie z najwyższym LSN."""
    r0 = fx.order_row("o1")
    r1 = fx.order_row("o1", "approved", order_approved_at=dt.datetime(2017, 1, 5, 10, 20))
    r2 = fx.order_row(
        "o1",
        "shipped",
        order_approved_at=dt.datetime(2017, 1, 5, 10, 20),
        order_delivered_carrier_date=dt.datetime(2017, 1, 6, 9),
    )
    # offsets deliberately not in LSN order (the guard is the LSN, not the offset)
    msgs = [
        fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "u", r0, r1, lsn=310, offset=5),
        fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "c", None, r0, lsn=300, offset=4),
        fx.message(ORDERS, 1, {"order_id": "o1"}, 2, "u", r1, r2, lsn=320, offset=3),
    ]
    decoded = avro.decode(fx.kafka_df(spark, msgs), _resolver())[ORDERS]
    ch = silver.changes(bronze.envelope_rows(decoded))
    out = silver.latest_per_key(ch, ["order_id"]).collect()
    assert len(out) == 1
    assert (out[0]["_lsn"], out[0]["order_status"], out[0]["_op"]) == (320, "shipped", "u")
