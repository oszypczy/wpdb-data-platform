"""Small gold/ops Delta tables for the dashboard tests (07 §7), written with ``deltalake``.

``write_small_lake(root)`` writes a hand-computable data set into ``<root>/gold/...`` and
``<root>/ops/...`` (local paths, the layout of the MinIO buckets). The expected values
are in ``EXPECTED``; QA can point the app at it with ``DASHBOARD_LAKE_ROOT=<root>``.

The orders:
- o1 (person u1, SP): 2 items 100.00 + 50.00, delivered in 4 days, on time.
- o2 (person u2, RJ): 1 item 30.00, delivered in 8 days, late.
- o3 (person u1, SP, after moving): 1 item 40.00 sold, then canceled: the item is deleted
  (gone from fact_order_items) and a -40.00 correction is recorded.
- o4 (person u3, MG): approved, no items yet.

Payments and reviews (``fact_payments``, ``fact_reviews``): see the comments at the rows;
expected page KPIs in ``EXPECTED_PAYMENTS_REVIEWS``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake

from dashboard import contract


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


D = Decimal


def _pay(order, seq, kind, inst, value, state, status, purchased) -> dict[str, Any]:
    t = ts(purchased)
    return {
        "order_id": order, "payment_sequential": seq, "payment_type": kind,
        "payment_installments": inst, "payment_value": D(value), "customer_state": state,
        "order_status": status, "purchased_at": t, "purchase_date_key": int(t.strftime("%Y%m%d")),
    }  # fmt: skip


def _review(rid, order, score, comment, created, state, delivered, late, days) -> dict[str, Any]:
    return {
        "review_id": rid, "order_id": order, "review_score": score, "has_comment": comment,
        "review_created_at": ts(created), "review_answered_at": ts(created),
        "customer_state": state, "purchased_at": None, "is_delivered": delivered,
        "is_late": late, "d_to_deliver": days,
    }  # fmt: skip


ROWS: dict[str, list[dict[str, Any]]] = {
    "gold/dim_geography": [
        {"state": "SP", "state_name": "São Paulo", "region": "Sudeste"},
        {"state": "RJ", "state_name": "Rio de Janeiro", "region": "Sudeste"},
        {"state": "MG", "state_name": "Minas Gerais", "region": "Sudeste"},
        {"state": "AC", "state_name": "Acre", "region": "Norte"},
    ],
    "gold/dim_product": [
        {
            "product_id": "p1",
            "product_category_name": "cama_mesa_banho",
            "product_category_name_english": "bed_bath_table",
        },
        {
            "product_id": "p2",
            "product_category_name": "beleza_saude",
            "product_category_name_english": "health_beauty",
        },
        {"product_id": "p3", "product_category_name": None, "product_category_name_english": None},
    ],
    "gold/dim_customer": [
        {
            "customer_sk": 1,
            "customer_unique_id": "u1",
            "customer_zip_code_prefix": "01001",
            "customer_city": "sao paulo",
            "customer_state": "SP",
            "version": 1,
            "valid_from": ts("2017-01-01 10:00:00"),
            "valid_to": ts("2017-01-03 09:00:00"),
            "is_current": False,
        },
        {
            "customer_sk": 2,
            "customer_unique_id": "u1",
            "customer_zip_code_prefix": "13330",
            "customer_city": "indaiatuba",
            "customer_state": "SP",
            "version": 2,
            "valid_from": ts("2017-01-03 09:00:00"),
            "valid_to": ts("9999-12-31 00:00:00"),
            "is_current": True,
        },
        {
            "customer_sk": 3,
            "customer_unique_id": "u2",
            "customer_zip_code_prefix": "20000",
            "customer_city": "rio de janeiro",
            "customer_state": "RJ",
            "version": 1,
            "valid_from": ts("2017-01-02 12:00:00"),
            "valid_to": ts("9999-12-31 00:00:00"),
            "is_current": True,
        },
        {
            "customer_sk": 4,
            "customer_unique_id": "u3",
            "customer_zip_code_prefix": "30000",
            "customer_city": "belo horizonte",
            "customer_state": "MG",
            "version": 1,
            "valid_from": ts("2017-01-04 08:00:00"),
            "valid_to": ts("9999-12-31 00:00:00"),
            "is_current": True,
        },
    ],
    "gold/fact_order_items": [
        {
            "order_id": "o1",
            "order_item_id": 1,
            "product_id": "p1",
            "seller_id": "s1",
            "customer_unique_id": "u1",
            "customer_state": "SP",
            "seller_state": "SP",
            "order_status": "delivered",
            "purchased_at": ts("2017-01-01 10:00:00"),
            "purchase_date": datetime(2017, 1, 1).date(),
            "price": D("100.00"),
            "freight_value": D("10.00"),
        },
        {
            "order_id": "o1",
            "order_item_id": 2,
            "product_id": "p2",
            "seller_id": "s1",
            "customer_unique_id": "u1",
            "customer_state": "SP",
            "seller_state": "SP",
            "order_status": "delivered",
            "purchased_at": ts("2017-01-01 10:00:00"),
            "purchase_date": datetime(2017, 1, 1).date(),
            "price": D("50.00"),
            "freight_value": D("5.00"),
        },
        {
            "order_id": "o2",
            "order_item_id": 1,
            "product_id": "p1",
            "seller_id": "s2",
            "customer_unique_id": "u2",
            "customer_state": "RJ",
            "seller_state": "SP",
            "order_status": "delivered",
            "purchased_at": ts("2017-01-02 12:00:00"),
            "purchase_date": datetime(2017, 1, 2).date(),
            "price": D("30.00"),
            "freight_value": D("3.00"),
        },
    ],
    "gold/fact_orders_lifecycle": [
        {
            "order_id": "o1",
            "customer_unique_id": "u1",
            "customer_state": "SP",
            "order_status": "delivered",
            "purchased_at": ts("2017-01-01 10:00:00"),
            "approved_at": ts("2017-01-01 10:30:00"),
            "shipped_at": ts("2017-01-02 10:00:00"),
            "delivered_at": ts("2017-01-05 10:00:00"),
            "estimated_delivery_at": ts("2017-01-10 00:00:00"),
            "h_to_approve": 0.5,
            "d_to_ship": 1.0,
            "d_to_deliver": 4.0,
            "is_delivered": True,
            "is_late": False,
            "items": 2,
            "items_value": D("150.00"),
            "freight_value": D("15.00"),
            "payment_value": D("165.00"),
        },
        {
            "order_id": "o2",
            "customer_unique_id": "u2",
            "customer_state": "RJ",
            "order_status": "delivered",
            "purchased_at": ts("2017-01-02 12:00:00"),
            "approved_at": ts("2017-01-02 14:00:00"),
            "shipped_at": ts("2017-01-04 12:00:00"),
            "delivered_at": ts("2017-01-10 12:00:00"),
            "estimated_delivery_at": ts("2017-01-08 00:00:00"),
            "h_to_approve": 2.0,
            "d_to_ship": 2.0,
            "d_to_deliver": 8.0,
            "is_delivered": True,
            "is_late": True,
            "items": 1,
            "items_value": D("30.00"),
            "freight_value": D("3.00"),
            "payment_value": D("33.00"),
        },
        {
            "order_id": "o3",
            "customer_unique_id": "u1",
            "customer_state": "SP",
            "order_status": "canceled",
            "purchased_at": ts("2017-01-03 09:00:00"),
            "approved_at": ts("2017-01-03 09:15:00"),
            "shipped_at": None,
            "delivered_at": None,
            "estimated_delivery_at": ts("2017-01-12 00:00:00"),
            "h_to_approve": 0.25,
            "d_to_ship": None,
            "d_to_deliver": None,
            "is_delivered": False,
            "is_late": False,
            "items": 0,
            "items_value": D("0.00"),
            "freight_value": D("0.00"),
            "payment_value": D("44.00"),
        },
        {
            "order_id": "o4",
            "customer_unique_id": "u3",
            "customer_state": "MG",
            "order_status": "approved",
            "purchased_at": ts("2017-01-04 08:00:00"),
            "approved_at": ts("2017-01-04 09:00:00"),
            "shipped_at": None,
            "delivered_at": None,
            "estimated_delivery_at": ts("2017-01-20 00:00:00"),
            "h_to_approve": 1.0,
            "d_to_ship": None,
            "d_to_deliver": None,
            "is_delivered": False,
            "is_late": None,
            "items": 0,
            "items_value": D("0.00"),
            "freight_value": D("0.00"),
            "payment_value": D("20.00"),
        },
    ],
    "gold/revenue_events": [
        {
            "event_id": "e1",
            "order_id": "o1",
            "customer_state": "SP",
            "event_time": ts("2017-01-01 10:00:00"),
            "kind": "sale",
            "amount": D("100.00"),
            "freight": D("10.00"),
            "items": 1,
        },
        {
            "event_id": "e2",
            "order_id": "o1",
            "customer_state": "SP",
            "event_time": ts("2017-01-01 10:00:00"),
            "kind": "sale",
            "amount": D("50.00"),
            "freight": D("5.00"),
            "items": 1,
        },
        {
            "event_id": "e3",
            "order_id": "o2",
            "customer_state": "RJ",
            "event_time": ts("2017-01-02 12:00:00"),
            "kind": "sale",
            "amount": D("30.00"),
            "freight": D("3.00"),
            "items": 1,
        },
        {
            "event_id": "e4",
            "order_id": "o3",
            "customer_state": "SP",
            "event_time": ts("2017-01-03 09:00:00"),
            "kind": "sale",
            "amount": D("40.00"),
            "freight": D("4.00"),
            "items": 1,
        },
        {
            "event_id": "e5",
            "order_id": "o3",
            "customer_state": "SP",
            "event_time": ts("2017-01-03 10:30:00"),
            "kind": "correction",
            "amount": D("-40.00"),
            "freight": D("-4.00"),
            "items": -1,
        },
    ],  # fmt: skip
    "gold/agg_revenue_hourly": [
        {
            "window_start": ts("2017-01-01 10:00:00"),
            "window_end": ts("2017-01-01 11:00:00"),
            "customer_state": "SP",
            "revenue": D("150.00"),
            "sales": D("150.00"),
            "corrections": D("0.00"),
            "freight": D("15.00"),
            "items_sold": 2,
            "items_canceled": 0,
            "events": 2,
        },
        {
            "window_start": ts("2017-01-02 12:00:00"),
            "window_end": ts("2017-01-02 13:00:00"),
            "customer_state": "RJ",
            "revenue": D("30.00"),
            "sales": D("30.00"),
            "corrections": D("0.00"),
            "freight": D("3.00"),
            "items_sold": 1,
            "items_canceled": 0,
            "events": 1,
        },
        {
            "window_start": ts("2017-01-03 09:00:00"),
            "window_end": ts("2017-01-03 10:00:00"),
            "customer_state": "SP",
            "revenue": D("40.00"),
            "sales": D("40.00"),
            "corrections": D("0.00"),
            "freight": D("4.00"),
            "items_sold": 1,
            "items_canceled": 0,
            "events": 1,
        },
        {
            "window_start": ts("2017-01-03 10:00:00"),
            "window_end": ts("2017-01-03 11:00:00"),
            "customer_state": "SP",
            "revenue": D("-40.00"),
            "sales": D("0.00"),
            "corrections": D("-40.00"),
            "freight": D("-4.00"),
            "items_sold": 0,
            "items_canceled": 1,
            "events": 1,
        },
    ],  # fmt: skip
    "gold/agg_status_funnel": [
        {
            "window_start": ts("2017-01-01 10:00:00"),
            "window_end": ts("2017-01-01 11:00:00"),
            "from_status": "(new)",
            "to_status": "created",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-01 10:00:00"),
            "window_end": ts("2017-01-01 11:00:00"),
            "from_status": "created",
            "to_status": "approved",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-02 12:00:00"),
            "window_end": ts("2017-01-02 13:00:00"),
            "from_status": "(new)",
            "to_status": "created",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-02 14:00:00"),
            "window_end": ts("2017-01-02 15:00:00"),
            "from_status": "created",
            "to_status": "approved",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-02 10:00:00"),
            "window_end": ts("2017-01-02 11:00:00"),
            "from_status": "approved",
            "to_status": "shipped",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-03 09:00:00"),
            "window_end": ts("2017-01-03 10:00:00"),
            "from_status": "(new)",
            "to_status": "created",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-03 09:00:00"),
            "window_end": ts("2017-01-03 10:00:00"),
            "from_status": "created",
            "to_status": "approved",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-03 10:00:00"),
            "window_end": ts("2017-01-03 11:00:00"),
            "from_status": "approved",
            "to_status": "canceled",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-04 08:00:00"),
            "window_end": ts("2017-01-04 09:00:00"),
            "from_status": "(new)",
            "to_status": "created",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-04 09:00:00"),
            "window_end": ts("2017-01-04 10:00:00"),
            "from_status": "created",
            "to_status": "approved",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-04 12:00:00"),
            "window_end": ts("2017-01-04 13:00:00"),
            "from_status": "approved",
            "to_status": "shipped",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-05 10:00:00"),
            "window_end": ts("2017-01-05 11:00:00"),
            "from_status": "shipped",
            "to_status": "delivered",
            "transitions": 1,
        },
        {
            "window_start": ts("2017-01-10 12:00:00"),
            "window_end": ts("2017-01-10 13:00:00"),
            "from_status": "shipped",
            "to_status": "delivered",
            "transitions": 1,
        },
    ],  # fmt: skip
    # o1 paid by credit card in 3 installments plus a voucher; o3 (canceled) keeps its
    # payment like in Olist
    "gold/fact_payments": [
        _pay("o1", 1, "credit_card", 3, "100.00", "SP", "delivered", "2017-01-01 10:00:00"),
        _pay("o1", 2, "voucher", 1, "65.00", "SP", "delivered", "2017-01-01 10:00:00"),
        _pay("o2", 1, "boleto", 1, "33.00", "RJ", "delivered", "2017-01-02 12:00:00"),
        _pay("o3", 1, "credit_card", 1, "44.00", "SP", "canceled", "2017-01-03 09:00:00"),
        _pay("o4", 1, "debit_card", 1, "20.00", "MG", "approved", "2017-01-04 08:00:00"),
    ],
    # o1 (on time) has two reviews (5 and 4), o2 (late) a 2, o3 (canceled, never
    # delivered) a 1; the week of 2017-01-16 has no review (a gap in the weekly series)
    "gold/fact_reviews": [
        _review("r1", "o1", 5, True, "2017-01-06 00:00:00", "SP", True, False, 4.0),
        _review("r2", "o2", 2, True, "2017-01-11 00:00:00", "RJ", True, True, 8.0),
        _review("r3", "o3", 1, False, "2017-01-04 00:00:00", "SP", False, False, None),
        _review("r4", "o1", 4, False, "2017-01-25 00:00:00", "SP", True, False, 4.0),
    ],
    "ops/cdc_heartbeat": [
        {
            "server_name": "olist",
            "heartbeat_ts": ts("2026-10-24 10:00:00"),
            "kafka_offset": 1,
            "ingested_at": ts("2026-10-24 10:00:05"),
        },
        {
            "server_name": "olist",
            "heartbeat_ts": ts("2026-10-24 10:00:10"),
            "kafka_offset": 2,
            "ingested_at": ts("2026-10-24 10:00:15"),
        },
    ],  # fmt: skip
    "ops/e2e_latency": [
        {
            "layer": "silver",
            "table_name": "orders",
            "batch_id": 1,
            "measured_at": ts("2026-10-24 09:00:00"),
            "rows": 10,
            "latency_ms_min": 1000.0,
            "latency_ms_p50": 10000.0,
            "latency_ms_p95": 20000.0,
            "latency_ms_max": 25000.0,
        },
        {
            "layer": "silver",
            "table_name": "orders",
            "batch_id": 2,
            "measured_at": ts("2026-10-24 09:30:00"),
            "rows": 10,
            "latency_ms_min": 1000.0,
            "latency_ms_p50": 12000.0,
            "latency_ms_p95": 22000.0,
            "latency_ms_max": 30000.0,
        },
        {
            "layer": "silver",
            "table_name": "orders",
            "batch_id": 0,
            "measured_at": ts("2026-10-24 07:00:00"),
            "rows": 10,
            "latency_ms_min": 1000.0,
            "latency_ms_p50": 90000.0,
            "latency_ms_p95": 99000.0,
            "latency_ms_max": 99000.0,
        },
        {
            "layer": "gold",
            "table_name": "star",
            "batch_id": 1,
            "measured_at": ts("2026-10-24 09:31:00"),
            "rows": 5,
            "latency_ms_min": 30000.0,
            "latency_ms_p50": 40000.0,
            "latency_ms_p95": 50000.0,
            "latency_ms_max": 55000.0,
        },
    ],  # fmt: skip
    "ops/stream_metrics": [
        {
            "query_name": "kafka_to_bronze",
            "batch_id": b,
            "progress_at": ts(f"2026-10-24 10:0{b}:00"),
            "num_input_rows": 10 * b,
            "input_rows_per_second": float(b),
            "processed_rows_per_second": float(b),
            "batch_duration_ms": 1000 * b,
            "add_batch_ms": 900 * b,
            "offsets_behind_latest": 0,
            "recorded_at": ts("2026-10-24 10:10:00"),
        }
        for b in (1, 2, 3)
    ],  # fmt: skip
    "ops/quarantine": [
        {
            "rule": "price_nonneg",
            "severity": "reject",
            "source_table": "order_items",
            "record_key": "o9|1",
            "payload": "{}",
            "first_seen_at": ts("2026-10-24 09:00:00"),
            "last_seen_at": ts("2026-10-24 09:00:00"),
        },
        {
            "rule": "price_nonneg",
            "severity": "reject",
            "source_table": "order_items",
            "record_key": "o9|2",
            "payload": "{}",
            "first_seen_at": ts("2026-10-24 09:00:00"),
            "last_seen_at": ts("2026-10-24 09:05:00"),
        },
        {
            "rule": "payment_type_defined",
            "severity": "warn",
            "source_table": "order_payments",
            "record_key": "o8|1",
            "payload": "{}",
            "first_seen_at": ts("2026-10-24 09:00:00"),
            "last_seen_at": ts("2026-10-24 09:00:00"),
        },
    ],  # fmt: skip
    "ops/maintenance_log": [
        {
            "run_id": "r1",
            "table_path": "s3a://gold/fact_order_items",
            "operation": "OPTIMIZE",
            "started_at": ts("2026-10-24 09:00:00"),
            "duration_ms": 1200,
            "files_before": 8,
            "files_after": 1,
        },
    ],  # fmt: skip
}

EXPECTED = {
    "revenue": D("180.00"),
    "gross_sales": D("220.00"),
    "corrections": D("-40.00"),
    "freight": D("18.00"),
    "items": 3,
    "orders": 4,
    "orders_with_items": 2,
    "avg_basket": D("90.00"),
    "delivered": 2,
    "late": 1,
    "late_pct": 50.0,
    "median_delivery_days": 6.0,
    "sim_clock": datetime(2017, 1, 10, 12, 0),
}

EXPECTED_PAYMENTS_REVIEWS = {
    "payments": 5,
    "payment_value": D("262.00"),
    "credit_card_value": D("144.00"),
    "credit_card_payments": 2,
    "avg_credit_card_installments": 2.0,
    "reviews": 4,
    "avg_score": 3.0,
    "five_star_pct": 25.0,
    "comment_pct": 50.0,
}


def arrow(table: str, rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=contract.SCHEMAS[table])


def write(root: Path | str, table: str, rows: list[dict[str, Any]], mode: str = "append") -> str:
    """Write ``rows`` (contract columns) as a Delta table ``<root>/<table>``; returns the path."""
    path = str(Path(root) / table)
    write_deltalake(path, arrow(table, rows), mode=mode)
    return path


def write_small_lake(root: Path | str, tables: list[str] | None = None) -> Path:
    """Write the whole data set (or only ``tables``) under ``root``; returns ``root``."""
    for table in tables or list(ROWS):
        write(root, table, ROWS[table], mode="overwrite")
    return Path(root)
