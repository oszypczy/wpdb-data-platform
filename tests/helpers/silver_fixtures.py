"""Small silver tables on the local filesystem for the gold unit tests (05 §10).

Rows are plain dicts with the Olist column names; the silver metadata columns
(``_lsn``, ``_op``, ``_is_deleted``, ...) are filled in. Shared with qa-engineer.
"""

from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import delta_io
from lakehouse.tables import SILVER_PROPERTIES

SCHEMAS = {
    "orders": (
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp, order_approved_at timestamp, "
        "order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp, "
        "order_estimated_delivery_date timestamp"
    ),
    "order_items": (
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date timestamp, price decimal(10,2), freight_value decimal(10,2)"
    ),
    "order_payments": (
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value decimal(10,2)"
    ),
    "customers": (
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string"
    ),
    "products": (
        "product_id string, product_category_name string, product_name_lenght int, "
        "product_description_lenght int, product_photos_qty int, product_weight_g int, "
        "product_length_cm int, product_height_cm int, product_width_cm int"
    ),
    "sellers": (
        "seller_id string, seller_zip_code_prefix string, seller_city string, seller_state string"
    ),
    "product_category_name_translation": (
        "product_category_name string, product_category_name_english string"
    ),
}
PKS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}
META = (
    "_lsn long, _op string, _is_deleted boolean, _source_ts timestamp, _tx_id long, "
    "_kafka_partition int, _kafka_offset long, _ingested_at timestamp"
)


def T(s: str) -> dt.datetime:  # noqa: N802 - short helper for readable fixtures
    return dt.datetime.fromisoformat(s)


def rows_df(spark: SparkSession, table: str, rows: list[dict[str, Any]], lsn: int, op="c"):
    cols = [c.strip().split(" ")[0] for c in re.split(r",(?![^()]*\))", SCHEMAS[table])]
    data = []
    for i, r in enumerate(rows):
        values = [r.get(c) for c in cols]
        data.append(
            (
                *values,
                lsn + i,
                r.get("_op", op),
                r.get("_is_deleted", op == "d"),
                dt.datetime(2026, 10, 23, 12),
                lsn,
                0,
                lsn + i,
                dt.datetime(2026, 10, 23, 12),
            )
        )
    return spark.createDataFrame(data, f"{SCHEMAS[table]}, {META}")


def merge(spark: SparkSession, base: str, table: str, rows: list[dict], lsn: int, op="c"):
    """Apply rows to silver/<table> through the real silver MERGE."""
    df = rows_df(spark, table, rows, lsn, op)
    delta_io.merge_silver(spark, f"{base}silver/{table}", df, PKS[table], SILVER_PROPERTIES)


def empty_silver(spark: SparkSession, base: str) -> None:
    for t in SCHEMAS:
        path = f"{base}silver/{t}"
        if not delta_io.is_delta(spark, path):
            df = rows_df(spark, t, [], 0).withColumn("_updated_at", F.current_timestamp())
            delta_io.ensure_table(spark, path, df.schema, properties=SILVER_PROPERTIES)


def cdf_since(spark: SparkSession, base: str, versions: dict[str, int]) -> DataFrame:
    """The union of the silver CDFs (as the gold query projects them) since ``versions``."""
    from delta.tables import DeltaTable

    from lakehouse.config import GOLD_SOURCES
    from lakehouse.jobs import cdf_projection

    out = None
    for t in GOLD_SOURCES:
        latest = DeltaTable.forPath(spark, f"{base}silver/{t}").history(1).first()["version"]
        if versions.get(t, 0) > latest:
            continue
        df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", versions.get(t, 0))
            .load(f"{base}silver/{t}")
        )
        p = cdf_projection(df, t)
        out = p if out is None else out.unionByName(p)
    return out


def versions(spark: SparkSession, base: str) -> dict[str, int]:
    from delta.tables import DeltaTable

    from lakehouse.config import GOLD_SOURCES

    return {
        t: DeltaTable.forPath(spark, f"{base}silver/{t}").history(1).first()["version"] + 1
        for t in GOLD_SOURCES
    }


def money(x: str) -> Decimal:
    return Decimal(x)
