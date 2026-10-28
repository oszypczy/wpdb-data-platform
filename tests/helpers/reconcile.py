"""Reconciliation Postgres (truth) ↔ Delta silver ↔ gold (fault-injection skill).

- ``truth_aggregates(pg)`` / ``silver_aggregates(lake)``: every ``sim.v_truth_*`` view
  (02 §8) and the same aggregate computed over silver (``NOT _is_deleted``) in DuckDB.
- ``mirror_diffs(pg, lake)``: row by row, every column of every table in ``shop``
  against its silver table (missing, extra, changed rows by PK), silver PK duplicates
  (also among deleted rows) and bronze ``(topic, partition, offset)`` duplicates.
- ``gold_vs_silver(lake)``: the gold facts and aggregates against the same sums over
  silver (05 §10 "sumy gold = sumy silver").

Only test code reads Postgres (role ``reader``); the lake is read as the read-only
``agent`` account.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import psycopg

from tests.helpers.lake import Lake

BUSINESS_TABLES = (
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
)


def silver(table: str) -> str:
    return f"(SELECT * FROM delta_scan('s3://silver/{table}') WHERE NOT _is_deleted)"


def gold(table: str) -> str:
    return f"delta_scan('s3://gold/{table}')"


def norm(v: Any) -> Any:
    """Postgres `timestamp` is naive; the lake stores it as UTC (DuckDB: tz-aware)."""
    if isinstance(v, datetime) and v.tzinfo is not None:
        return v.astimezone(UTC).replace(tzinfo=None)
    return v


def avg2(total: Any, n: Any) -> Decimal:
    """avg(...)::numeric(4,2) as Postgres rounds it (half away from zero)."""
    return (Decimal(int(total)) / Decimal(int(n))).quantize(Decimal("0.01"), ROUND_HALF_UP)


def _keyed(rows: list[tuple], nkey: int = 1) -> dict:
    out = {}
    for r in rows:
        r = tuple(norm(v) for v in r)
        key = r[0] if nkey == 1 else r[:nkey]
        out[key] = r[nkey:]
    return out


# ---------------------------------------------------------------- truth views vs silver
TRUTH_VIEWS = (
    "v_truth_row_counts",
    "v_truth_orders",
    "v_truth_totals",
    "v_truth_revenue_by_state",
    "v_truth_payments",
    "v_truth_order_lifecycle",
    "v_truth_reviews",
    "v_truth_reviews_by_delivery",
    "v_truth_payment_installments",
)

SILVER_SQL = {
    "v_truth_orders": f"""
        SELECT o.order_status, count(DISTINCT o.order_id), count(i.order_id),
               coalesce(sum(i.price), 0), coalesce(sum(i.freight_value), 0)
        FROM {silver("orders")} o LEFT JOIN {silver("order_items")} i USING (order_id)
        GROUP BY 1""",
    "v_truth_totals": f"""
        SELECT (SELECT count(*) FROM {silver("orders")}),
               (SELECT count(*) FROM {silver("customers")}),
               (SELECT count(*) FROM {silver("order_items")}),
               (SELECT count(*) FROM {silver("order_payments")}),
               (SELECT count(*) FROM {silver("order_reviews")}),
               (SELECT coalesce(sum(price), 0) FROM {silver("order_items")}),
               (SELECT coalesce(sum(freight_value), 0) FROM {silver("order_items")}),
               (SELECT coalesce(sum(payment_value), 0) FROM {silver("order_payments")})""",
    "v_truth_revenue_by_state": f"""
        SELECT c.customer_state, count(DISTINCT o.order_id), count(i.order_id),
               coalesce(sum(i.price), 0)
        FROM {silver("orders")} o JOIN {silver("customers")} c USING (customer_id)
        LEFT JOIN {silver("order_items")} i USING (order_id)
        GROUP BY 1""",
    "v_truth_payments": f"""
        SELECT payment_type, count(*), coalesce(sum(payment_value), 0)
        FROM {silver("order_payments")} GROUP BY 1""",
    "v_truth_order_lifecycle": f"""
        SELECT count(*), count(order_approved_at), count(order_delivered_carrier_date),
               count(order_delivered_customer_date),
               count(*) FILTER (WHERE order_delivered_customer_date
                                      > order_estimated_delivery_date)
        FROM {silver("orders")}""",
    "v_truth_reviews": f"""
        SELECT review_score, count(*), count(DISTINCT order_id)
        FROM {silver("order_reviews")} GROUP BY 1""",
    "v_truth_reviews_by_delivery": f"""
        SELECT NOT coalesce(o.order_delivered_customer_date > o.order_estimated_delivery_date,
                            false),
               count(*), sum(r.review_score)
        FROM {silver("order_reviews")} r JOIN {silver("orders")} o USING (order_id)
        WHERE o.order_delivered_customer_date IS NOT NULL
        GROUP BY 1""",
    "v_truth_payment_installments": f"""
        SELECT payment_installments, count(*), coalesce(sum(payment_value), 0)
        FROM {silver("order_payments")} GROUP BY 1""",
}
SINGLE_ROW = {"v_truth_totals", "v_truth_order_lifecycle"}


def truth_aggregates(pg: psycopg.Connection) -> dict[str, Any]:
    """Every truth view, keyed by its first column (single-row views: the row), read in
    one REPEATABLE READ snapshot."""
    out: dict[str, Any] = {}
    with pg.transaction():
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        for view in TRUTH_VIEWS:
            rows = pg.execute(f"SELECT * FROM sim.{view}").fetchall()
            out[view] = tuple(rows[0]) if view in SINGLE_ROW else _keyed(rows)
    return out


def silver_aggregates(lake: Lake, tables: list[str]) -> dict[str, Any]:
    """The truth views computed over silver. ``tables`` = the tables of
    ``v_truth_row_counts`` (every table in shop)."""
    out: dict[str, Any] = {
        "v_truth_row_counts": {
            t: (lake.scalar(f"SELECT count(*) FROM {silver(t)}"),) for t in tables
        }
    }
    for view, sql in SILVER_SQL.items():
        rows = lake.sql(sql)
        if view == "v_truth_reviews_by_delivery":
            out[view] = {bool(k): (int(n), avg2(s, n)) for k, n, s in rows}
        elif view in SINGLE_ROW:
            out[view] = tuple(rows[0])
        else:
            out[view] = _keyed(rows)
    return out


def aggregate_diff(truth: dict, lake_side: dict) -> dict[str, tuple]:
    return {v: (truth.get(v), lake_side.get(v)) for v in truth if truth.get(v) != lake_side.get(v)}


# ------------------------------------------------------------------ row-level mirror
@dataclass
class TableDiff:
    table: str
    pg_rows: int
    silver_rows: int
    missing: list = field(default_factory=list)  # PK in Postgres, not alive in silver
    extra: list = field(default_factory=list)  # alive in silver, not in Postgres
    changed: list = field(default_factory=list)  # same PK, other values
    silver_pk_duplicates: int = 0  # among all silver rows, deleted ones included
    bronze_offset_duplicates: int = 0

    @property
    def ok(self) -> bool:
        return not (
            self.missing
            or self.extra
            or self.changed
            or self.silver_pk_duplicates
            or self.bronze_offset_duplicates
        )

    def summary(self) -> str:
        return (
            f"{self.table}: pg={self.pg_rows} silver={self.silver_rows} "
            f"missing={len(self.missing)} {self.missing[:3]} extra={len(self.extra)} "
            f"{self.extra[:3]} changed={len(self.changed)} {self.changed[:2]} "
            f"silver_pk_dups={self.silver_pk_duplicates} "
            f"bronze_offset_dups={self.bronze_offset_duplicates}"
        )


def shop_tables(pg: psycopg.Connection) -> list[str]:
    return [r[0] for r in pg.execute("SELECT table_name FROM sim.v_truth_row_counts")]


def columns(pg: psycopg.Connection, table: str) -> list[str]:
    return [
        r[0]
        for r in pg.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'shop' AND table_name = %s ORDER BY ordinal_position",
            (table,),
        )
    ]


def primary_key(pg: psycopg.Connection, table: str) -> list[str]:
    from tests.helpers.cdc import primary_key as pk

    return pk(pg, table)


def mirror_diff(pg: psycopg.Connection, lake: Lake, table: str) -> TableDiff:
    """Every column of `shop.<table>` equals its silver mirror (alive rows), by PK."""
    cols = columns(pg, table)
    pk = primary_key(pg, table)
    others = [c for c in cols if c not in pk]
    select = ", ".join(f'"{c}"' for c in pk + others)
    pg_rows = _keyed(pg.execute(f"SELECT {select} FROM shop.{table}").fetchall(), len(pk))
    lake_rows = _keyed(lake.sql(f"SELECT {select} FROM {silver(table)}"), len(pk))
    diff = TableDiff(table, len(pg_rows), len(lake_rows))
    diff.missing = sorted(k for k in pg_rows if k not in lake_rows)
    diff.extra = sorted(k for k in lake_rows if k not in pg_rows)
    diff.changed = sorted(
        (k, pg_rows[k], lake_rows[k])
        for k in pg_rows
        if k in lake_rows and pg_rows[k] != lake_rows[k]
    )
    pk_list = ", ".join(f'"{c}"' for c in pk)
    diff.silver_pk_duplicates = lake.scalar(
        f"SELECT count(*) - count(DISTINCT ({pk_list})) FROM delta_scan('s3://silver/{table}')"
    )
    diff.bronze_offset_duplicates = lake.scalar(
        "SELECT count(*) - count(DISTINCT (kafka_topic, kafka_partition, kafka_offset)) "
        f"FROM delta_scan('s3://bronze/{table}')"
    )
    return diff


def mirror_diffs(pg: psycopg.Connection, lake: Lake) -> list[TableDiff]:
    """All tables of shop in one REPEATABLE READ snapshot of Postgres."""
    with pg.transaction():
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return [mirror_diff(pg, lake, t) for t in shop_tables(pg)]


# ---------------------------------------------------------------------- gold vs silver
GOLD_VS_SILVER = {
    # name: (gold SQL, silver SQL), both keyed by the first column
    "fact_order_items per status (items, revenue, freight)": (
        f"""SELECT order_status, count(*), sum(price), sum(freight_value)
            FROM {gold("fact_order_items")} GROUP BY 1""",
        f"""SELECT o.order_status, count(*), sum(i.price), sum(i.freight_value)
            FROM {silver("order_items")} i JOIN {silver("orders")} o USING (order_id)
            GROUP BY 1""",
    ),
    "fact_orders_lifecycle per status (orders, items, value, payments, delivered, late)": (
        f"""SELECT order_status, count(*), sum(items), sum(items_value), sum(payment_value),
                   count(*) FILTER (WHERE is_delivered), count(*) FILTER (WHERE is_late)
            FROM {gold("fact_orders_lifecycle")} GROUP BY 1""",
        f"""WITH it AS (SELECT order_id, count(*) AS n, sum(price) AS v
                        FROM {silver("order_items")} GROUP BY 1),
                 pay AS (SELECT order_id, sum(payment_value) AS v
                         FROM {silver("order_payments")} GROUP BY 1)
            SELECT o.order_status, count(*), coalesce(sum(it.n), 0),
                   coalesce(sum(it.v), 0), coalesce(sum(pay.v), 0),
                   count(*) FILTER (WHERE o.order_delivered_customer_date IS NOT NULL),
                   count(*) FILTER (WHERE o.order_delivered_customer_date
                                          > o.order_estimated_delivery_date)
            FROM {silver("orders")} o LEFT JOIN it USING (order_id)
            LEFT JOIN pay USING (order_id) GROUP BY 1""",
    ),
    "fact_payments per type (payments, value)": (
        f"""SELECT payment_type, count(*), sum(payment_value)
            FROM {gold("fact_payments")} GROUP BY 1""",
        f"""SELECT payment_type, count(*), sum(payment_value)
            FROM {silver("order_payments")} GROUP BY 1""",
    ),
    "fact_reviews per score (reviews, orders)": (
        f"""SELECT review_score, count(*), count(DISTINCT order_id)
            FROM {gold("fact_reviews")} GROUP BY 1""",
        f"""SELECT review_score, count(*), count(DISTINCT order_id)
            FROM {silver("order_reviews")} GROUP BY 1""",
    ),
    "revenue: revenue_events = agg_revenue_hourly = silver order_items (sum price)": (
        f"""SELECT 'revenue',
                   (SELECT sum(amount) FROM {gold("revenue_events")}),
                   (SELECT sum(revenue) FROM {gold("agg_revenue_hourly")})""",
        f"""SELECT 'revenue', sum(price), sum(price) FROM {silver("order_items")}""",
    ),
    "agg_revenue_hourly: sales + corrections = revenue, freight": (
        f"""SELECT 'agg', sum(sales) + sum(corrections), sum(freight)
            FROM {gold("agg_revenue_hourly")}""",
        f"""SELECT 'agg', sum(price), sum(freight_value) FROM {silver("order_items")}""",
    ),
}


def gold_vs_silver(lake: Lake) -> dict[str, tuple[dict, dict]]:
    """name -> (gold, silver) for every comparison that differs (empty = all equal)."""
    out = {}
    for name, (g, s) in GOLD_VS_SILVER.items():
        gv, sv = _keyed(lake.sql(g)), _keyed(lake.sql(s))
        if gv != sv:
            out[name] = (gv, sv)
    return out
