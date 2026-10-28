"""The dashboard's query layer on live gold/ops vs the Postgres truth views (07 §5, §7).

The query layer (``dashboard.queries`` over ``dashboard.lake``: delta-rs → Arrow →
DuckDB, account ``dashboard``) runs on the host exactly as in the container; the truth
side is ``sim.v_truth_*`` (plus plain SQL on ``shop.*`` for the medians and the clock).
Shared by the integration test of the "Płatności i opinie" page and the e2e KPI test.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import psycopg

from dashboard import queries as q
from dashboard.lake import Lake, LakeConfig, VersionedReader

ENDPOINT = "http://127.0.0.1:9000"
CENT = Decimal("0.01")
ITEMS, LIFECYCLE = "gold/fact_order_items", "gold/fact_orders_lifecycle"
REVENUE_HOURLY, REVENUE_EVENTS = "gold/agg_revenue_hourly", "gold/revenue_events"
GEOGRAPHY = "gold/dim_geography"
PAYMENTS, REVIEWS = "gold/fact_payments", "gold/fact_reviews"
HEARTBEAT, LATENCY = "ops/cdc_heartbeat", "ops/e2e_latency"


def lake_config(settings: dict[str, str]) -> LakeConfig:
    return LakeConfig(
        root="s3://",
        endpoint=ENDPOINT,
        access_key=settings["MINIO_DASHBOARD_ACCESS_KEY"],
        secret_key=settings["MINIO_DASHBOARD_SECRET_KEY"],
    )


def dashboard_reader(settings: dict[str, str]) -> VersionedReader:
    return VersionedReader(Lake(lake_config(settings)))


def money(v: float) -> Decimal:
    return Decimal(f"{v:.2f}")


# ------------------------------------------------------------------ "Przegląd" + map
def overview_of_the_query_layer(reader: VersionedReader) -> dict[str, Any]:
    """KPIs of the overview page and the map's per-state numbers (BRL, before the
    USD conversion of the view layer)."""
    k = reader.derive("kpis", [ITEMS, LIFECYCLE, REVENUE_HOURLY], q.kpis)
    rb = reader.derive("revenue_breakdown", [REVENUE_EVENTS], q.revenue_breakdown)
    states = reader.derive("state_metrics", [REVENUE_HOURLY, LIFECYCLE, GEOGRAPHY], q.state_metrics)
    return {
        "revenue": k.revenue,
        "gross_sales + corrections": k.gross_sales + k.corrections,
        "revenue_events net": rb.net,
        "freight": k.freight,
        "items": k.items,
        "orders": k.orders,
        "orders_with_items": k.orders_with_items,
        "avg_basket": k.avg_basket,
        "delivered": k.delivered,
        "late": k.late,
        "late_pct": k.late_pct,
        "median_delivery_days": k.median_delivery_days,
        "sim_clock": k.sim_clock,
        "per_state (orders, revenue)": {
            s: (int(o), money(r))
            for s, o, r in states[["state", "orders", "revenue"]].values
            if o > 0 or r != 0
        },
    }


def overview_truth(pg: psycopg.Connection) -> dict[str, Any]:
    with pg.transaction():
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        t = pg.execute(
            "SELECT orders, order_items, revenue, freight FROM sim.v_truth_totals"
        ).fetchone()
        lc = pg.execute("SELECT delivered, late FROM sim.v_truth_order_lifecycle").fetchone()
        with_items = pg.execute("SELECT count(DISTINCT order_id) FROM shop.order_items").fetchone()[
            0
        ]
        median, clock = pg.execute(
            """SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM
                          order_delivered_customer_date - order_purchase_timestamp) / 86400.0)
                          FILTER (WHERE order_delivered_customer_date IS NOT NULL),
                      max(greatest(order_purchase_timestamp, order_approved_at,
                                   order_delivered_carrier_date, order_delivered_customer_date))
               FROM shop.orders"""
        ).fetchone()
        per_state = pg.execute(
            "SELECT customer_state, orders, revenue FROM sim.v_truth_revenue_by_state"
        ).fetchall()
    orders, items, revenue, freight = t
    delivered, late = lc
    return {
        "revenue": revenue,
        "gross_sales + corrections": revenue,
        "revenue_events net": revenue,
        "freight": freight,
        "items": items,
        "orders": orders,
        "orders_with_items": with_items,
        "avg_basket": (revenue / with_items).quantize(CENT, ROUND_HALF_UP) if with_items else None,
        "delivered": delivered,
        "late": late,
        "late_pct": (100.0 * late / delivered) if delivered else None,
        "median_delivery_days": None if median is None else float(median),
        "sim_clock": clock,
        "per_state (orders, revenue)": {s: (int(o), r) for s, o, r in per_state},
    }


# ------------------------------------------------------------- "Płatności i opinie"
def payments_reviews_of_the_query_layer(reader: VersionedReader) -> dict[str, dict]:
    """The page's queries on live gold, shaped like the truth views (BRL, before the
    USD conversion of the view layer)."""
    dist = reader.derive("score_distribution", [REVIEWS], q.score_distribution, None)
    by = reader.derive("reviews_by_delivery", [REVIEWS], q.reviews_by_delivery, None)
    types = reader.derive("payment_types", [PAYMENTS], q.payment_types, None)
    inst = reader.derive("payment_installments", [PAYMENTS], q.payment_installments, None)
    k = reader.derive("payments_reviews_kpis", [PAYMENTS, REVIEWS], q.payments_reviews_kpis, None)
    return {
        "v_truth_reviews": {
            int(s): (int(n), int(o))
            for s, n, o in dist[["review_score", "reviews", "orders"]].values
            if n > 0
        },
        "v_truth_reviews_by_delivery": {
            bool(t): (int(n), Decimal(f"{a:.6f}").quantize(CENT, ROUND_HALF_UP))
            for t, n, a in by[["on_time", "reviews", "avg_score"]].values
        },
        "v_truth_payments": {
            t: (int(n), money(v)) for t, n, v in types[["payment_type", "payments", "value"]].values
        },
        "v_truth_payment_installments": {
            int(i): (int(n), money(v))
            for i, n, v in inst[["payment_installments", "payments", "value"]].values
        },
        "kpis": {"payments": k.payments, "value": k.payment_value, "reviews": k.reviews},
    }


def payments_reviews_truth(pg: psycopg.Connection) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for view in (
        "v_truth_reviews",
        "v_truth_reviews_by_delivery",
        "v_truth_payments",
        "v_truth_payment_installments",
    ):
        rows = pg.execute(f"SELECT * FROM sim.{view}").fetchall()
        out[view] = {k: (int(a), b if isinstance(b, Decimal) else int(b)) for k, a, b in rows}
    pays = out["v_truth_payments"].values()
    out["kpis"] = {
        "payments": sum(n for n, _ in pays),
        "value": sum((v for _, v in pays), Decimal("0.00")),
        "reviews": sum(n for n, _ in out["v_truth_reviews"].values()),
    }
    return out
