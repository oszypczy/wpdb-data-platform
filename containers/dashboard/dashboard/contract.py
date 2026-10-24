"""The gold and ops tables the dashboard reads, as Arrow schemas (07 §2, 06 §9).

This mirrors ``containers/spark/lakehouse/tables.py`` (the writer's contract) for the
columns the dashboard uses; ``tests/unit/test_dashboard_contract.py`` checks that every
column here exists in the writer's spec with the matching type. A table that does not
exist yet (fresh stack) is read as an empty table with this schema, so every query and
page works on "no data" without special cases.
"""

from __future__ import annotations

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")
MONEY = pa.decimal128(10, 2)
SUM = pa.decimal128(14, 2)
STR, INT, LONG, DBL, BOOL, DATE = (
    pa.string(),
    pa.int32(),
    pa.int64(),
    pa.float64(),
    pa.bool_(),
    pa.date32(),
)


def _schema(*cols: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema([pa.field(n, t, nullable=True) for n, t in cols])


SCHEMAS: dict[str, pa.Schema] = {
    "gold/dim_geography": _schema(("state", STR), ("state_name", STR), ("region", STR)),
    "gold/dim_customer": _schema(
        ("customer_sk", LONG),
        ("customer_unique_id", STR),
        ("customer_zip_code_prefix", STR),
        ("customer_city", STR),
        ("customer_state", STR),
        ("version", INT),
        ("valid_from", TS),
        ("valid_to", TS),
        ("is_current", BOOL),
    ),
    "gold/dim_product": _schema(
        ("product_id", STR),
        ("product_category_name", STR),
        ("product_category_name_english", STR),
    ),
    "gold/fact_order_items": _schema(
        ("order_id", STR),
        ("order_item_id", INT),
        ("product_id", STR),
        ("seller_id", STR),
        ("customer_unique_id", STR),
        ("customer_state", STR),
        ("seller_state", STR),
        ("order_status", STR),
        ("purchased_at", TS),
        ("purchase_date", DATE),
        ("price", MONEY),
        ("freight_value", MONEY),
    ),
    "gold/fact_orders_lifecycle": _schema(
        ("order_id", STR),
        ("customer_unique_id", STR),
        ("customer_state", STR),
        ("order_status", STR),
        ("purchased_at", TS),
        ("approved_at", TS),
        ("shipped_at", TS),
        ("delivered_at", TS),
        ("estimated_delivery_at", TS),
        ("h_to_approve", DBL),
        ("d_to_ship", DBL),
        ("d_to_deliver", DBL),
        ("is_delivered", BOOL),
        ("is_late", BOOL),
        ("items", INT),
        ("items_value", SUM),
        ("freight_value", SUM),
        ("payment_value", SUM),
    ),
    "gold/revenue_events": _schema(
        ("event_id", STR),
        ("order_id", STR),
        ("customer_state", STR),
        ("event_time", TS),
        ("kind", STR),
        ("amount", MONEY),
        ("freight", MONEY),
        ("items", INT),
    ),
    "gold/status_transitions": _schema(
        ("order_id", STR),
        ("source_lsn", LONG),
        ("from_status", STR),
        ("to_status", STR),
        ("event_time", TS),
    ),
    "gold/agg_revenue_hourly": _schema(
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
    ),
    "gold/agg_status_funnel": _schema(
        ("window_start", TS),
        ("window_end", TS),
        ("from_status", STR),
        ("to_status", STR),
        ("transitions", LONG),
    ),
    "ops/stream_metrics": _schema(
        ("query_name", STR),
        ("batch_id", LONG),
        ("progress_at", TS),
        ("num_input_rows", LONG),
        ("input_rows_per_second", DBL),
        ("processed_rows_per_second", DBL),
        ("batch_duration_ms", LONG),
        ("add_batch_ms", LONG),
        ("offsets_behind_latest", LONG),
        ("recorded_at", TS),
    ),
    "ops/e2e_latency": _schema(
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
    "ops/cdc_heartbeat": _schema(
        ("server_name", STR),
        ("heartbeat_ts", TS),
        ("kafka_offset", LONG),
        ("ingested_at", TS),
    ),
    "ops/quarantine": _schema(
        ("rule", STR),
        ("severity", STR),
        ("source_table", STR),
        ("record_key", STR),
        ("payload", STR),
        ("first_seen_at", TS),
        ("last_seen_at", TS),
    ),
    "ops/maintenance_log": _schema(
        ("run_id", STR),
        ("table_path", STR),
        ("operation", STR),
        ("started_at", TS),
        ("duration_ms", LONG),
        ("files_before", LONG),
        ("files_after", LONG),
    ),
}

# Gold tables offered on the time-travel page, with the measures compared there.
TIME_TRAVEL_MEASURES: dict[str, tuple[str, ...]] = {
    "gold/fact_order_items": ("price", "freight_value"),
    "gold/fact_orders_lifecycle": ("items_value", "payment_value"),
    "gold/revenue_events": ("amount",),
    "gold/agg_revenue_hourly": ("revenue", "sales", "corrections"),
    "gold/agg_status_funnel": ("transitions",),
    "gold/status_transitions": (),
    "gold/dim_customer": (),
    "gold/dim_product": (),
    "gold/dim_geography": (),
}


def empty(table: str) -> pa.Table:
    return SCHEMAS[table].empty_table()


def conform(table: str, data: pa.Table) -> pa.Table:
    """Project ``data`` onto the contract schema: contract columns in order, missing
    columns as nulls (an older table version), extra columns dropped (a newer one)."""
    schema = SCHEMAS[table]
    cols = []
    for f in schema:
        if f.name in data.column_names:
            cols.append(data.column(f.name).cast(f.type))
        else:
            cols.append(pa.nulls(data.num_rows, f.type))
    return pa.Table.from_arrays(cols, schema=schema)
