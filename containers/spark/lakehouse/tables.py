"""Gold and ops tables: the public interface of the dashboard (05 §5-§7, 06 §2).

Names, columns, partitioning, properties and CHECK constraints are frozen here and in
06-minio-delta.md; changing them is a contract change. Only table features that
delta-rs and DuckDB read are used (06 §9): CHECK constraints (writer feature) and
``appendOnly``; no deletion vectors, column mapping, identity columns, timestampNtz,
liquid clustering or CDF on gold/ops.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STR, INT, LONG, DBL, BOOL, TS, DT = (
    StringType(),
    IntegerType(),
    LongType(),
    DoubleType(),
    BooleanType(),
    TimestampType(),
    DateType(),
)
MONEY = DecimalType(10, 2)
SUM = DecimalType(14, 2)


def schema(*cols: tuple[str, DataType]) -> StructType:
    return StructType([StructField(n, t, True) for n, t in cols])


@dataclass(frozen=True)
class TableSpec:
    layer: str
    name: str
    schema: StructType
    keys: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    properties: dict[str, str] = field(default_factory=dict)
    constraints: dict[str, str] = field(default_factory=dict)
    zorder: tuple[str, ...] = ()
    comment: str = ""


UPDATED = ("_updated_at", TS)

GOLD = {
    t.name: t
    for t in [
        TableSpec(
            "gold",
            "dim_date",
            schema(
                ("date_key", INT),
                ("date", DT),
                ("year", INT),
                ("quarter", INT),
                ("month", INT),
                ("month_name", STR),
                ("day", INT),
                ("day_of_week", INT),
                ("day_name", STR),
                ("week_of_year", INT),
                ("is_weekend", BOOL),
            ),
            keys=("date_key",),
            comment="calendar 2016-01-01..2019-12-31; date_key = yyyymmdd",
        ),
        TableSpec(
            "gold",
            "dim_geography",
            schema(("state", STR), ("state_name", STR), ("region", STR)),
            keys=("state",),
            comment="27 Brazilian federative units (customer_state / seller_state)",
        ),
        TableSpec(
            "gold",
            "dim_customer",
            schema(
                ("customer_sk", LONG),
                ("customer_unique_id", STR),
                ("customer_zip_code_prefix", STR),
                ("customer_city", STR),
                ("customer_state", STR),
                ("version", INT),
                ("valid_from", TS),
                ("valid_to", TS),
                ("is_current", BOOL),
                UPDATED,
            ),
            keys=("customer_sk",),
            constraints={"valid_range": "valid_from < valid_to"},
            comment="SCD2 per customer_unique_id; valid_to = 9999-12-31 for the current version",
        ),
        TableSpec(
            "gold",
            "dim_product",
            schema(
                ("product_id", STR),
                ("product_category_name", STR),
                ("product_category_name_english", STR),
                ("product_photos_qty", INT),
                ("product_weight_g", INT),
                ("product_length_cm", INT),
                ("product_height_cm", INT),
                ("product_width_cm", INT),
                UPDATED,
            ),
            keys=("product_id",),
        ),
        TableSpec(
            "gold",
            "dim_seller",
            schema(
                ("seller_id", STR),
                ("seller_zip_code_prefix", STR),
                ("seller_city", STR),
                ("seller_state", STR),
                UPDATED,
            ),
            keys=("seller_id",),
        ),
        TableSpec(
            "gold",
            "fact_order_items",
            schema(
                ("order_id", STR),
                ("order_item_id", INT),
                ("product_id", STR),
                ("seller_id", STR),
                ("customer_id", STR),
                ("customer_sk", LONG),
                ("customer_unique_id", STR),
                ("customer_state", STR),
                ("seller_state", STR),
                ("order_status", STR),
                ("purchased_at", TS),
                ("purchase_date", DT),
                ("purchase_date_key", INT),
                ("shipping_limit_at", TS),
                ("price", MONEY),
                ("freight_value", MONEY),
                UPDATED,
            ),
            keys=("order_id", "order_item_id"),
            constraints={"price_nonneg": "price >= 0", "freight_nonneg": "freight_value >= 0"},
            zorder=("customer_state", "purchase_date"),
            comment="grain: order item (silver order_items not deleted, quality rules passed)",
        ),
        TableSpec(
            "gold",
            "fact_orders_lifecycle",
            schema(
                ("order_id", STR),
                ("customer_id", STR),
                ("customer_sk", LONG),
                ("customer_unique_id", STR),
                ("customer_state", STR),
                ("order_status", STR),
                ("purchased_at", TS),
                ("approved_at", TS),
                ("shipped_at", TS),
                ("delivered_at", TS),
                ("estimated_delivery_at", TS),
                ("purchase_date_key", INT),
                ("h_to_approve", DBL),
                ("d_to_ship", DBL),
                ("d_to_deliver", DBL),
                ("is_delivered", BOOL),
                ("is_late", BOOL),
                ("items", INT),
                ("items_value", SUM),
                ("freight_value", SUM),
                ("payment_value", SUM),
                ("payments", INT),
                UPDATED,
            ),
            keys=("order_id",),
            constraints={"items_nonneg": "items >= 0"},
            zorder=("customer_state", "purchased_at"),
            comment="accumulating snapshot: one row per order, milestones filled as it progresses",
        ),
        TableSpec(
            "gold",
            "fact_payments",
            schema(
                ("order_id", STR),
                ("payment_sequential", INT),
                ("payment_type", STR),
                ("payment_installments", INT),
                ("payment_value", MONEY),
                ("customer_state", STR),
                ("order_status", STR),
                ("purchased_at", TS),
                ("purchase_date_key", INT),
                UPDATED,
            ),
            keys=("order_id", "payment_sequential"),
            # no CHECK on the value or type: payment_value_positive / payment_type_defined
            # are warn rules (the row stays in gold), a CHECK would fail the whole batch
            zorder=("purchased_at", "payment_type"),
            comment="grain: payment of an order (silver order_payments not deleted)",
        ),
        TableSpec(
            "gold",
            "fact_reviews",
            schema(
                ("review_id", STR),
                ("order_id", STR),
                ("review_score", INT),
                ("has_comment", BOOL),
                ("review_created_at", TS),
                ("review_answered_at", TS),
                ("customer_state", STR),
                ("purchased_at", TS),
                ("is_delivered", BOOL),
                ("is_late", BOOL),
                ("d_to_deliver", DBL),
                UPDATED,
            ),
            keys=("review_id", "order_id"),
            constraints={"score_range": "review_score BETWEEN 1 AND 5"},
            zorder=("review_created_at", "customer_state"),
            comment="grain: review of an order; delivery attributes from fact_orders_lifecycle",
        ),
        TableSpec(
            "gold",
            "revenue_events",
            schema(
                ("event_id", STR),
                ("order_id", STR),
                ("order_item_id", INT),
                ("customer_state", STR),
                ("event_time", TS),
                ("kind", STR),
                ("amount", MONEY),
                ("freight", MONEY),
                ("items", INT),
                ("gold_batch_id", LONG),
                ("recorded_at", TS),
            ),
            properties={"delta.appendOnly": "true"},
            comment="revenue movements = changes of fact_order_items (sale/correction/adjustment)",
        ),
        TableSpec(
            "gold",
            "status_transitions",
            schema(
                ("order_id", STR),
                ("source_lsn", LONG),
                ("from_status", STR),
                ("to_status", STR),
                ("event_time", TS),
                ("event_time_source", STR),
                ("source_ts", TS),
                ("recorded_at", TS),
            ),
            keys=("order_id", "source_lsn"),
            comment="order status changes from the before/after pairs of the CDC stream",
        ),
        TableSpec(
            "gold",
            "agg_revenue_hourly",
            schema(
                ("window_start", TS),
                ("window_end", TS),
                ("customer_state", STR),
                ("revenue", SUM),
                ("sales", SUM),
                ("corrections", SUM),
                ("freight", SUM),
                ("items_sold", LONG),
                ("items_canceled", LONG),
                ("events", LONG),
                UPDATED,
            ),
            keys=("window_start", "customer_state"),
            constraints={"corrections_nonpos": "corrections <= 0"},
            comment="1 h event-time windows (simulated time) per customer state, with watermark",
        ),
        TableSpec(
            "gold",
            "agg_status_funnel",
            schema(
                ("window_start", TS),
                ("window_end", TS),
                ("from_status", STR),
                ("to_status", STR),
                ("transitions", LONG),
                UPDATED,
            ),
            keys=("window_start", "from_status", "to_status"),
            comment="status transitions per 1 h event-time window; from_status '(new)' = created",
        ),
    ]
}

OPS = {
    t.name: t
    for t in [
        TableSpec(
            "ops",
            "stream_metrics",
            schema(
                ("query_name", STR),
                ("query_id", STR),
                ("run_id", STR),
                ("batch_id", LONG),
                ("progress_at", TS),
                ("num_input_rows", LONG),
                ("input_rows_per_second", DBL),
                ("processed_rows_per_second", DBL),
                ("batch_duration_ms", LONG),
                ("add_batch_ms", LONG),
                ("offsets_behind_latest", LONG),
                ("source", STR),
                ("recorded_at", TS),
            ),
            properties={"delta.appendOnly": "true"},
            comment="StreamingQueryListener progress per micro-batch (05 §7)",
        ),
        TableSpec(
            "ops",
            "e2e_latency",
            schema(
                ("layer", STR),
                ("table_name", STR),
                ("batch_id", LONG),
                ("measured_at", TS),
                ("rows", LONG),
                ("latency_ms_min", DBL),
                ("latency_ms_p50", DBL),
                ("latency_ms_p95", DBL),
                ("latency_ms_max", DBL),
            ),
            properties={"delta.appendOnly": "true"},
            comment="write time in silver/gold minus commit time in Postgres (source.ts_ms)",
        ),
        TableSpec(
            "ops",
            "cdc_heartbeat",
            schema(
                ("server_name", STR),
                ("heartbeat_ts", TS),
                ("kafka_partition", INT),
                ("kafka_offset", LONG),
                ("kafka_timestamp", TS),
                ("ingested_at", TS),
            ),
            properties={"delta.appendOnly": "true"},
            comment="Debezium heartbeat topic (__debezium-heartbeat.olist), every 10 s",
        ),
        TableSpec(
            "ops",
            "quarantine",
            schema(
                ("rule", STR),
                ("severity", STR),
                ("source_table", STR),
                ("record_key", STR),
                ("payload", STR),
                ("first_seen_at", TS),
                ("last_seen_at", TS),
            ),
            keys=("rule", "source_table", "record_key"),
            comment="rows that broke a quality rule; severity reject = kept out of gold",
        ),
        TableSpec(
            "ops",
            "maintenance_log",
            schema(
                ("run_id", STR),
                ("table_path", STR),
                ("operation", STR),
                ("started_at", TS),
                ("duration_ms", LONG),
                ("files_before", LONG),
                ("files_after", LONG),
                ("bytes_before", LONG),
                ("bytes_after", LONG),
                ("query_ms_before", DBL),
                ("query_ms_after", DBL),
                ("details", STR),
            ),
            properties={"delta.appendOnly": "true"},
            comment="OPTIMIZE / Z-ORDER / VACUUM runs with before/after measurements (06 §7)",
        ),
    ]
}

ALL = {**{f"gold/{k}": v for k, v in GOLD.items()}, **{f"ops/{k}": v for k, v in OPS.items()}}

# silver tables: CDF on (06 §5), the PK from the key schema is recorded as a property
SILVER_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.autoCompact": "true",
}
BRONZE_PROPERTIES = {
    "delta.appendOnly": "true",
    # file statistics only for the Kafka/key columns: the nested before/after structs
    # made every write generate and format a large stats projection (05 §8, JFR)
    "delta.dataSkippingNumIndexedCols": "8",
    "delta.autoOptimize.autoCompact": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 30 days",
}
PK_PROPERTY = "wpdb.primaryKey"

# VACUUM retention per layer (06 §7): bronze keeps history longer
RETENTION_HOURS = {"bronze": 720, "silver": 168, "gold": 168, "ops": 168}
