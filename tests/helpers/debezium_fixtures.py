"""Sample Debezium messages in the Confluent AVRO wire format, without Kafka (05 §10).

The schemas mirror the ones the connector registers (03 §13): an ``Envelope`` with
``before``/``after`` of the table's ``Value`` record, the Postgres ``Source`` block, the
``transaction`` block, ``op`` and ``ts_ms``; decimals are ``bytes`` with logical type
``decimal(10,2)`` and timestamps ``long`` ``timestamp-millis``. ``message(...)`` returns
a dict shaped like a row of Spark's Kafka source (key, value, topic, partition,
offset, timestamp), so tests can build a micro-batch with ``spark.createDataFrame``.
Shared with qa-engineer as fixtures.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import struct
from decimal import Decimal
from typing import Any

import fastavro

TS = {
    "type": "long",
    "connect.version": 1,
    "connect.name": "org.apache.kafka.connect.data.Timestamp",
    "logicalType": "timestamp-millis",
}
DEC = {
    "type": "bytes",
    "scale": 2,
    "precision": 10,
    "connect.version": 1,
    "connect.parameters": {"scale": "2", "connect.decimal.precision": "10"},
    "connect.name": "org.apache.kafka.connect.data.Decimal",
    "logicalType": "decimal",
}
SOURCE = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium.connector.postgresql",
    "fields": [
        {"name": "version", "type": "string"},
        {"name": "connector", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "ts_ms", "type": "long"},
        {
            "name": "snapshot",
            "type": [{"type": "string", "connect.name": "io.debezium.data.Enum"}, "null"],
            "default": "false",
        },
        {"name": "db", "type": "string"},
        {"name": "sequence", "type": ["null", "string"], "default": None},
        {"name": "schema", "type": "string"},
        {"name": "table", "type": "string"},
        {"name": "txId", "type": ["null", "long"], "default": None},
        {"name": "lsn", "type": ["null", "long"], "default": None},
        {"name": "xmin", "type": ["null", "long"], "default": None},
    ],
}
BLOCK = {
    "type": "record",
    "name": "block",
    "namespace": "event",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "total_order", "type": "long"},
        {"name": "data_collection_order", "type": "long"},
    ],
}

ORDERS_FIELDS = [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "order_status", "type": "string"},
    {"name": "order_purchase_timestamp", "type": TS},
    {"name": "order_approved_at", "type": ["null", TS], "default": None},
    {"name": "order_delivered_carrier_date", "type": ["null", TS], "default": None},
    {"name": "order_delivered_customer_date", "type": ["null", TS], "default": None},
    {"name": "order_estimated_delivery_date", "type": TS},
]
ORDER_ITEMS_FIELDS = [
    {"name": "order_id", "type": "string"},
    {"name": "order_item_id", "type": "int"},
    {"name": "product_id", "type": "string"},
    {"name": "seller_id", "type": "string"},
    {"name": "shipping_limit_date", "type": TS},
    {"name": "price", "type": DEC},
    {"name": "freight_value", "type": DEC},
]
EVOLVED_COLUMN = {"name": "delivery_channel", "type": ["null", "string"], "default": None}


def envelope(table: str, fields: list[dict[str, Any]]) -> dict[str, Any]:
    value = {
        "type": "record",
        "name": "Value",
        "namespace": f"olist.shop.{table}",
        "fields": fields,
        "connect.name": f"olist.shop.{table}.Value",
    }
    return {
        "type": "record",
        "name": "Envelope",
        "namespace": f"olist.shop.{table}",
        "fields": [
            {"name": "before", "type": ["null", value], "default": None},
            {"name": "after", "type": ["null", "Value"], "default": None},
            {"name": "source", "type": SOURCE},
            {"name": "transaction", "type": ["null", BLOCK], "default": None},
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": ["null", "long"], "default": None},
        ],
        "connect.name": f"olist.shop.{table}.Envelope",
    }


def key_schema(table: str, fields: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "record",
        "name": "Key",
        "namespace": f"olist.shop.{table}",
        "fields": fields,
        "connect.name": f"olist.shop.{table}.Key",
    }


ORDERS_V1 = envelope("orders", ORDERS_FIELDS)
ORDERS_V2 = envelope("orders", ORDERS_FIELDS + [EVOLVED_COLUMN])
ORDERS_KEY = key_schema("orders", [{"name": "order_id", "type": "string"}])
ITEMS_V1 = envelope("order_items", ORDER_ITEMS_FIELDS)
ITEMS_KEY = key_schema(
    "order_items",
    [{"name": "order_id", "type": "string"}, {"name": "order_item_id", "type": "int"}],
)

# schema ids as a registry would hand them out
IDS = {1: ORDERS_KEY, 2: ORDERS_V1, 3: ORDERS_V2, 4: ITEMS_KEY, 5: ITEMS_V1}


def schema_texts() -> dict[int, str]:
    return {i: json.dumps(s) for i, s in IDS.items()}


def confluent(schema_id: int, record: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    buf.write(b"\x00" + struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, fastavro.parse_schema(IDS[schema_id]), record)
    return buf.getvalue()


EPOCH = dt.datetime(2017, 1, 5, 10, 0, 0, tzinfo=dt.UTC)


def source(lsn: int, table: str = "orders", ts: dt.datetime | None = None, snapshot="false"):
    return {
        "version": "3.6.3.Final",
        "connector": "postgresql",
        "name": "olist",
        "ts_ms": int((ts or EPOCH).timestamp() * 1000),
        "snapshot": snapshot,
        "db": "shop",
        "sequence": None,
        "schema": "shop",
        "table": table,
        "txId": lsn // 10,
        "lsn": lsn,
        "xmin": None,
    }


def order_row(order_id: str, status: str = "created", **cols: Any) -> dict[str, Any]:
    purchase = cols.pop("purchase", dt.datetime(2017, 1, 5, 10, 0, tzinfo=dt.UTC))
    row = {
        "order_id": order_id,
        "customer_id": cols.pop("customer_id", f"c-{order_id}"),
        "order_status": status,
        "order_purchase_timestamp": purchase,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": purchase + dt.timedelta(days=10),
    }
    row.update(cols)
    return row


def item_row(order_id: str, item: int = 1, price: str = "58.90", **cols: Any) -> dict[str, Any]:
    row = {
        "order_id": order_id,
        "order_item_id": item,
        "product_id": cols.pop("product_id", "p-1"),
        "seller_id": cols.pop("seller_id", "s-1"),
        "shipping_limit_date": dt.datetime(2017, 1, 10, tzinfo=dt.UTC),
        "price": Decimal(price),
        "freight_value": Decimal(cols.pop("freight", "10.00")),
    }
    row.update(cols)
    return row


def message(
    topic: str,
    key_id: int,
    key: dict[str, Any],
    value_id: int | None,
    op: str | None,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    lsn: int,
    offset: int,
    partition: int = 0,
) -> dict[str, Any]:
    """One Kafka-source row; ``op=None`` builds a tombstone (value null)."""
    table = topic.rsplit(".", 1)[-1]
    value = None
    if op is not None:
        value = confluent(
            value_id,
            {
                "before": before,
                "after": after,
                "source": source(lsn, table, snapshot="true" if op == "r" else "false"),
                "transaction": None
                if op == "r"
                else {"id": f"{lsn // 10}:{lsn}", "total_order": 1, "data_collection_order": 1},
                "op": op,
                "ts_ms": int(EPOCH.timestamp() * 1000) + 100,
            },
        )
    return {
        "key": confluent(key_id, key),
        "value": value,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "timestamp": dt.datetime(2026, 10, 23, 12, 0, 0),
        "timestampType": 0,
    }


KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, "
    "timestamp timestamp, timestampType int"
)


def kafka_df(spark, messages: list[dict[str, Any]]):
    rows = [
        (
            m["key"],
            m["value"],
            m["topic"],
            m["partition"],
            m["offset"],
            m["timestamp"],
            m["timestampType"],
        )
        for m in messages
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)
