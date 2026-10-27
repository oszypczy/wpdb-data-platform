"""Query layer: pure functions ``table(s) -> DataFrame`` (07 §5).

Inputs are Arrow tables (or pandas frames) with the contract columns of
``dashboard.contract``; the SQL runs in an in-memory DuckDB over Arrow (no S3, no
extensions). No Streamlit, no I/O: every function is unit-tested on small Delta tables.

Conventions:
- Business times are simulated Olist times stored as UTC; outputs are naive UTC
  ``TIMESTAMP`` (as the source shows them). Ops times are wall-clock UTC. Every time
  leaving DuckDB is cast to ``TIMESTAMP``: a ``TIMESTAMPTZ`` result needs ``pytz``,
  which the image does not ship (the unit tests block it to keep this true).
- Money in KPI results is ``Decimal`` (exact, comparable with the Postgres truth views);
  in frames for charts it is ``float``.
- Empty inputs give empty frames with the same columns (or zero/None KPIs), never errors.

KPI definitions (07 §2 "Przegląd"):
- revenue = sum of ``fact_order_items.price``: the items that exist now. Items of a
  canceled order are deleted in the source, so this is already net of corrections and
  equals ``sim.v_truth_totals.revenue``; ``gross_sales`` and ``corrections`` (< 0) come
  from ``agg_revenue_hourly`` and ``gross_sales + corrections = revenue``.
- orders = rows of ``fact_orders_lifecycle`` (one per order, any status).
- average basket = revenue / orders that have at least one item.
- % late = late deliveries / delivered orders (``is_late`` / ``is_delivered``).
- median delivery = median ``d_to_deliver`` (days purchase -> delivery) of delivered orders.
- simulation clock = latest business time in the lifecycle (purchase, approval,
  shipment or delivery): the simulator fills each milestone when it happens.

KPI definitions (07 §2 "Płatności i opinie", ``fact_payments`` / ``fact_reviews``):
- average score = mean ``review_score`` of all reviews; 5-star share and comment share
  (title or message not blank) = share of review rows.
- average installments = mean ``payment_installments`` of ``credit_card`` payments.
- credit card share = credit-card ``payment_value`` / all ``payment_value`` (by value).
- on time vs late (reviews of delivered orders only) = ``not is_late`` as in
  ``sim.v_truth_reviews_by_delivery``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import duckdb
import pandas as pd

FREQS = {"hour": "hour", "day": "day", "week": "week"}
FUNNEL_PATH = ("created", "approved", "shipped", "delivered")
FUNNEL_EXITS = ("canceled", "unavailable", "processing", "invoiced")
CENT = Decimal("0.01")


def _con(**tables: Any) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for name, data in tables.items():
        con.register(name, data)
    return con


def sql(query: str, **tables: Any) -> pd.DataFrame:
    """Run ``query`` over the given tables (registered under their keyword names)."""
    con = _con(**tables)
    try:
        return con.execute(query).df()
    finally:
        con.close()


def _row(query: str, **tables: Any) -> tuple:
    con = _con(**tables)
    try:
        return con.execute(query).fetchone()
    finally:
        con.close()


def _freq(freq: str) -> str:
    if freq not in FREQS:
        raise ValueError(f"freq must be one of {sorted(FREQS)}, not {freq!r}")
    return FREQS[freq]


def _in_list(values: Iterable[str] | None) -> str:
    """SQL ``IN (...)`` list of string literals (values come from widgets, so quote them)."""
    vals = [str(v).replace("'", "''") for v in values or ()]
    return "(" + ", ".join(f"'{v}'" for v in vals) + ")"


def _naive(ts: Any) -> datetime | None:
    if ts is None:
        return None
    return ts.replace(tzinfo=None) if getattr(ts, "tzinfo", None) else ts


# --------------------------------------------------------------------------- KPIs
@dataclass(frozen=True)
class Kpis:
    revenue: Decimal
    gross_sales: Decimal
    corrections: Decimal
    freight: Decimal
    items: int
    orders: int
    orders_with_items: int
    avg_basket: Decimal | None
    delivered: int
    late: int
    late_pct: float | None
    median_delivery_days: float | None
    sim_clock: datetime | None

    @property
    def is_empty(self) -> bool:
        return self.orders == 0 and self.items == 0


def kpis(items: Any, lifecycle: Any, revenue_hourly: Any) -> Kpis:
    """Headline KPIs of the overview page (definitions in the module docstring)."""
    revenue, freight, n_items, with_items = _row(
        """SELECT coalesce(sum(price), 0)::DECIMAL(18,2),
                  coalesce(sum(freight_value), 0)::DECIMAL(18,2),
                  count(*), count(DISTINCT order_id)
           FROM items""",
        items=items,
    )
    orders, delivered, late, median_days, clock = _row(
        """SELECT count(*),
                  count(*) FILTER (WHERE is_delivered),
                  count(*) FILTER (WHERE is_late),
                  median(d_to_deliver) FILTER (WHERE is_delivered),
                  max(greatest(purchased_at, approved_at, shipped_at, delivered_at))::TIMESTAMP
           FROM lifecycle""",
        lifecycle=lifecycle,
    )
    sales, corrections = _row(
        """SELECT coalesce(sum(sales), 0)::DECIMAL(18,2),
                  coalesce(sum(corrections), 0)::DECIMAL(18,2)
           FROM agg""",
        agg=revenue_hourly,
    )
    avg = (revenue / with_items).quantize(CENT, ROUND_HALF_UP) if with_items else None
    return Kpis(
        revenue=revenue,
        gross_sales=sales,
        corrections=corrections,
        freight=freight,
        items=int(n_items),
        orders=int(orders),
        orders_with_items=int(with_items),
        avg_basket=avg,
        delivered=int(delivered),
        late=int(late),
        late_pct=(100.0 * late / delivered) if delivered else None,
        median_delivery_days=None if median_days is None else float(median_days),
        sim_clock=_naive(clock),
    )


@dataclass(frozen=True)
class RevenueBreakdown:
    sales: Decimal
    corrections: Decimal
    adjustments: Decimal

    @property
    def net(self) -> Decimal:
        return self.sales + self.corrections + self.adjustments


def revenue_breakdown(revenue_events: Any) -> RevenueBreakdown:
    """Revenue movements of ``gold/revenue_events``: sales (+), corrections after a
    cancellation (-), price adjustments; ``net`` = what ``fact_order_items`` holds."""
    sales, corr, adj = _row(
        """SELECT coalesce(sum(amount) FILTER (WHERE kind = 'sale'), 0)::DECIMAL(18,2),
                  coalesce(sum(amount) FILTER (WHERE kind = 'correction'), 0)::DECIMAL(18,2),
                  coalesce(sum(amount) FILTER (WHERE kind NOT IN ('sale', 'correction')), 0)
                      ::DECIMAL(18,2)
           FROM ev""",
        ev=revenue_events,
    )
    return RevenueBreakdown(sales, corr, adj)


# ----------------------------------------------------------------- sales over time
def revenue_timeseries(
    revenue_hourly: Any, freq: str = "hour", states: tuple[str, ...] | None = None
) -> pd.DataFrame:
    """Revenue per hour/day/week (simulated time) from ``agg_revenue_hourly``:
    ``sales`` (+), ``corrections`` (<= 0) and ``revenue`` = their sum. Periods without
    events between the first and the last one are present with zeros (no bridged gaps)."""
    unit = _freq(freq)
    where = f"WHERE customer_state IN {_in_list(states)}" if states else ""
    return sql(
        f"""WITH a AS (
                SELECT date_trunc('{unit}', window_start::TIMESTAMP) AS period,
                       sum(sales) AS sales, sum(corrections) AS corrections,
                       sum(revenue) AS revenue, sum(freight) AS freight,
                       sum(items_sold) AS items_sold, sum(items_canceled) AS items_canceled
                FROM agg {where} GROUP BY 1),
            r AS (
                SELECT unnest(generate_series(min(period), max(period), INTERVAL 1 {unit}))
                       AS period
                FROM a)
            SELECT r.period,
                   coalesce(a.sales, 0)::DOUBLE AS sales,
                   coalesce(a.corrections, 0)::DOUBLE AS corrections,
                   coalesce(a.revenue, 0)::DOUBLE AS revenue,
                   coalesce(a.freight, 0)::DOUBLE AS freight,
                   coalesce(a.items_sold, 0)::BIGINT AS items_sold,
                   coalesce(a.items_canceled, 0)::BIGINT AS items_canceled
            FROM r LEFT JOIN a USING (period)
            ORDER BY r.period""",
        agg=revenue_hourly,
    )


# ---------------------------------------------------------------------------- map
def state_metrics(revenue_hourly: Any, lifecycle: Any, geography: Any) -> pd.DataFrame:
    """One row per Brazilian state (all 27 of ``dim_geography``, plus any state seen in
    the facts but missing there): revenue, orders, delivered, late, % late."""
    return sql(
        """WITH rev AS (
               SELECT customer_state AS state, sum(revenue)::DOUBLE AS revenue,
                      sum(corrections)::DOUBLE AS corrections
               FROM agg GROUP BY 1),
           lc AS (
               SELECT customer_state AS state, count(*) AS orders,
                      count(*) FILTER (WHERE is_delivered) AS delivered,
                      count(*) FILTER (WHERE is_late) AS late
               FROM lifecycle GROUP BY 1),
           states AS (
               SELECT state FROM geo UNION SELECT state FROM rev UNION SELECT state FROM lc)
           SELECT s.state, g.state_name, g.region,
                  coalesce(rev.revenue, 0) AS revenue,
                  coalesce(rev.corrections, 0) AS corrections,
                  coalesce(lc.orders, 0)::BIGINT AS orders,
                  coalesce(lc.delivered, 0)::BIGINT AS delivered,
                  coalesce(lc.late, 0)::BIGINT AS late,
                  CASE WHEN lc.delivered > 0 THEN 100.0 * lc.late / lc.delivered END AS late_pct
           FROM states s
           LEFT JOIN geo g USING (state)
           LEFT JOIN rev USING (state)
           LEFT JOIN lc USING (state)
           WHERE s.state IS NOT NULL
           ORDER BY s.state""",
        agg=revenue_hourly,
        lifecycle=lifecycle,
        geo=geography,
    )


# ------------------------------------------------------------------------- funnel
def funnel_transitions(funnel: Any) -> pd.DataFrame:
    """Total transitions per ``from -> to`` pair (``(new)`` = order created)."""
    return sql(
        """SELECT from_status, to_status, from_status || ' → ' || to_status AS transition,
                  sum(transitions)::BIGINT AS transitions
           FROM f GROUP BY ALL ORDER BY transitions DESC, transition""",
        f=funnel,
    )


def funnel_stages(funnel: Any) -> pd.DataFrame:
    """How many orders entered each status: the main path created -> approved ->
    shipped -> delivered (``kind = 'path'``) and the exits (canceled, unavailable, ...)."""
    stages = [(s, "path", i) for i, s in enumerate(FUNNEL_PATH)] + [
        (s, "exit", len(FUNNEL_PATH) + i) for i, s in enumerate(FUNNEL_EXITS)
    ]
    order = pd.DataFrame(stages, columns=["stage", "kind", "pos"])
    return sql(
        """SELECT o.stage, o.kind,
                  coalesce(sum(f.transitions), 0)::BIGINT AS entered
           FROM o LEFT JOIN f ON f.to_status = o.stage
           GROUP BY o.stage, o.kind, o.pos ORDER BY o.pos""",
        f=funnel,
        o=order,
    )


def funnel_timeseries(
    funnel: Any, freq: str = "day", to_statuses: tuple[str, ...] | None = None
) -> pd.DataFrame:
    """Transitions per period and pair; ``to_statuses`` keeps only those targets
    (e.g. canceled, unavailable)."""
    where = f"WHERE to_status IN {_in_list(to_statuses)}" if to_statuses else ""
    return sql(
        f"""SELECT date_trunc('{_freq(freq)}', window_start::TIMESTAMP) AS period,
                   from_status || ' → ' || to_status AS transition,
                   sum(transitions)::BIGINT AS transitions
            FROM f {where}
            GROUP BY ALL ORDER BY period, transition""",
        f=funnel,
    )


# ---------------------------------------------------------------------- logistics
def logistics_summary(lifecycle: Any) -> pd.DataFrame:
    """One row: delivered orders, median/p90 days to deliver, median days to ship,
    median hours to approve, % late."""
    return sql(
        """SELECT count(*) FILTER (WHERE is_delivered)::BIGINT AS delivered,
                  count(*) FILTER (WHERE is_late)::BIGINT AS late,
                  100.0 * count(*) FILTER (WHERE is_late)
                      / nullif(count(*) FILTER (WHERE is_delivered), 0) AS late_pct,
                  median(d_to_deliver) FILTER (WHERE is_delivered) AS median_d_to_deliver,
                  quantile_cont(d_to_deliver, 0.9) FILTER (WHERE is_delivered)
                      AS p90_d_to_deliver,
                  median(d_to_ship) AS median_d_to_ship,
                  median(h_to_approve) AS median_h_to_approve
           FROM lc""",
        lc=lifecycle,
    )


def delivery_times(lifecycle: Any) -> pd.DataFrame:
    """Delivered orders with their delivery time in days and the late flag."""
    return sql(
        """SELECT order_id, customer_state, purchased_at::TIMESTAMP AS purchased_at,
                  d_to_deliver, coalesce(is_late, false) AS is_late
           FROM lc WHERE is_delivered AND d_to_deliver IS NOT NULL
           ORDER BY purchased_at, order_id""",
        lc=lifecycle,
    )


def late_by_state(lifecycle: Any) -> pd.DataFrame:
    """Per customer state: delivered, late, % late, median delivery days."""
    return sql(
        """SELECT customer_state AS state,
                  count(*)::BIGINT AS delivered,
                  count(*) FILTER (WHERE is_late)::BIGINT AS late,
                  100.0 * count(*) FILTER (WHERE is_late) / count(*) AS late_pct,
                  median(d_to_deliver) AS median_days
           FROM lc WHERE is_delivered
           GROUP BY 1 ORDER BY late_pct DESC, delivered DESC, state""",
        lc=lifecycle,
    )


# ----------------------------------------------------------------------- products
def top_categories(items: Any, products: Any, n: int = 15) -> pd.DataFrame:
    """Top ``n`` product categories (English names) by revenue, with freight, items
    and orders. Products without a category are ``(no category)``."""
    return sql(
        f"""SELECT coalesce(p.product_category_name_english, p.product_category_name,
                            '(no category)') AS category,
                   sum(i.price)::DOUBLE AS revenue,
                   sum(i.freight_value)::DOUBLE AS freight,
                   count(*)::BIGINT AS items,
                   count(DISTINCT i.order_id)::BIGINT AS orders
            FROM i LEFT JOIN p USING (product_id)
            GROUP BY 1 ORDER BY revenue DESC, category LIMIT {int(n)}""",
        i=items,
        p=products,
    )


# ---------------------------------------------------------------------- customers
def customer_summary(dim_customer: Any, lifecycle: Any) -> pd.DataFrame:
    """One row: customers (people = ``customer_unique_id``), returning customers (more
    than one order), customers who moved (more than one SCD2 version), versions."""
    return sql(
        """WITH per AS (SELECT customer_unique_id, count(*) AS orders FROM lc
                        WHERE customer_unique_id IS NOT NULL GROUP BY 1),
                ver AS (SELECT customer_unique_id, count(*) AS versions FROM dc GROUP BY 1)
           SELECT (SELECT count(*) FROM ver)::BIGINT AS customers,
                  (SELECT count(*) FROM per WHERE orders > 1)::BIGINT AS returning,
                  (SELECT count(*) FROM ver WHERE versions > 1)::BIGINT AS movers,
                  (SELECT count(*) FROM dc)::BIGINT AS versions""",
        dc=dim_customer,
        lc=lifecycle,
    )


def orders_per_customer(lifecycle: Any) -> pd.DataFrame:
    """Distribution: how many customers placed 1, 2, 3, ... orders."""
    return sql(
        """SELECT orders, count(*)::BIGINT AS customers FROM (
               SELECT customer_unique_id, count(*)::BIGINT AS orders FROM lc
               WHERE customer_unique_id IS NOT NULL GROUP BY 1)
           GROUP BY 1 ORDER BY 1""",
        lc=lifecycle,
    )


def customer_moves(dim_customer: Any) -> pd.DataFrame:
    """The full SCD2 history of every customer with more than one version (moves);
    the current version has ``valid_to`` = 9999-12-31."""
    return sql(
        """SELECT customer_unique_id, version, customer_zip_code_prefix AS zip,
                  customer_city AS city, customer_state AS state,
                  valid_from::TIMESTAMP AS valid_from, valid_to::TIMESTAMP AS valid_to,
                  is_current
           FROM dc
           WHERE customer_unique_id IN (
               SELECT customer_unique_id FROM dc GROUP BY 1 HAVING count(*) > 1)
           ORDER BY customer_unique_id, version""",
        dc=dim_customer,
    )


# ------------------------------------------------------------------ pipeline (ops)
def cdc_freshness(heartbeat: Any, now: datetime) -> pd.DataFrame:
    """One row: last Debezium heartbeat (ops/cdc_heartbeat), its age at ``now`` (CDC
    freshness: Postgres -> Debezium -> Kafka -> Spark is alive) and when Spark wrote it."""
    df = sql(
        """SELECT max(heartbeat_ts)::TIMESTAMP AS last_heartbeat,
                  max(ingested_at)::TIMESTAMP AS last_ingested,
                  count(*)::BIGINT AS heartbeats
           FROM hb""",
        hb=heartbeat,
    )
    now = _naive(now)
    for col, out in (("last_heartbeat", "freshness_s"), ("last_ingested", "ingest_age_s")):
        ts = df.at[0, col]
        df[out] = None if pd.isna(ts) else (now - ts.to_pydatetime()).total_seconds()
    return df


def latency_timeseries(e2e: Any, since: datetime | None = None) -> pd.DataFrame:
    """End-to-end latency (write time - Postgres commit) per layer and batch, seconds."""
    where = f"WHERE measured_at::TIMESTAMP >= TIMESTAMP '{_naive(since)}'" if since else ""
    return sql(
        f"""SELECT measured_at::TIMESTAMP AS measured_at, layer,
                   max(latency_ms_p50) / 1000 AS p50_s,
                   max(latency_ms_p95) / 1000 AS p95_s,
                   max(latency_ms_max) / 1000 AS max_s,
                   sum(rows)::BIGINT AS rows
            FROM e {where}
            GROUP BY ALL ORDER BY measured_at, layer""",
        e=e2e,
    )


def latency_summary(e2e: Any, window_minutes: int = 60) -> pd.DataFrame:
    """Per layer, over its last ``window_minutes`` of measurements: median of the batch
    p50 and p95, the maximum, batches and the last measurement time (seconds)."""
    return sql(
        f"""WITH last AS (SELECT layer, max(measured_at) AS until FROM e GROUP BY 1)
            SELECT e.layer,
                   count(*)::BIGINT AS batches,
                   max(e.measured_at)::TIMESTAMP AS last_measured,
                   median(e.latency_ms_p50) / 1000 AS p50_s,
                   median(e.latency_ms_p95) / 1000 AS p95_s,
                   max(e.latency_ms_max) / 1000 AS max_s
            FROM e JOIN last USING (layer)
            WHERE e.measured_at >= last.until - INTERVAL {int(window_minutes)} MINUTE
            GROUP BY 1 ORDER BY 1""",
        e=e2e,
    )


def quarantine_by_rule(quarantine: Any) -> pd.DataFrame:
    """Rows in quarantine per rule, severity and source table."""
    return sql(
        """SELECT rule, severity, source_table, count(*)::BIGINT AS rows,
                  max(last_seen_at)::TIMESTAMP AS last_seen
           FROM q GROUP BY ALL ORDER BY rows DESC, rule, source_table""",
        q=quarantine,
    )


# ------------------------------------------------------------ payments and reviews
REVIEW_SCORES = (1, 2, 3, 4, 5)


def _states(states: Iterable[str] | None, *conditions: str) -> str:
    """``WHERE`` clause: the customer-state filter (if any) and the extra conditions."""
    conds = [f"customer_state IN {_in_list(states)}"] if states else []
    conds += conditions
    return f"WHERE {' AND '.join(conds)}" if conds else ""


@dataclass(frozen=True)
class PaymentsReviewsKpis:
    payments: int
    payment_value: Decimal
    credit_card_value: Decimal
    credit_card_value_pct: float | None
    credit_card_payments: int
    avg_credit_card_installments: float | None
    reviews: int
    avg_score: float | None
    five_star_pct: float | None
    comment_pct: float | None

    @property
    def is_empty(self) -> bool:
        return self.payments == 0 and self.reviews == 0


def payments_reviews_kpis(
    payments: Any, reviews: Any, states: tuple[str, ...] | None = None
) -> PaymentsReviewsKpis:
    """KPIs of the "Płatności i opinie" page (07 §2): average score 1..5, share of
    5-star reviews, share of reviews with a comment (title or message), average number
    of installments of credit-card payments, credit card's share of the paid value."""
    where = _states(states)
    n, value, cc_value, cc_n, cc_inst = _row(
        f"""SELECT count(*),
                   coalesce(sum(payment_value), 0)::DECIMAL(18,2),
                   coalesce(sum(payment_value) FILTER (WHERE payment_type = 'credit_card'), 0)
                       ::DECIMAL(18,2),
                   count(*) FILTER (WHERE payment_type = 'credit_card'),
                   avg(payment_installments) FILTER (WHERE payment_type = 'credit_card')
            FROM p {where}""",
        p=payments,
    )
    r, avg, five, comments = _row(
        f"""SELECT count(*), avg(review_score),
                   count(*) FILTER (WHERE review_score = 5),
                   count(*) FILTER (WHERE has_comment)
            FROM r {where}""",
        r=reviews,
    )
    return PaymentsReviewsKpis(
        payments=int(n),
        payment_value=value,
        credit_card_value=cc_value,
        credit_card_value_pct=float(100 * cc_value / value) if value else None,
        credit_card_payments=int(cc_n),
        avg_credit_card_installments=None if cc_inst is None else float(cc_inst),
        reviews=int(r),
        avg_score=None if avg is None else float(avg),
        five_star_pct=(100.0 * five / r) if r else None,
        comment_pct=(100.0 * comments / r) if r else None,
    )


def payment_types(payments: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Per payment type: payments, value (BRL) and their shares (%), by value desc."""
    return sql(
        f"""SELECT payment_type,
                   count(*)::BIGINT AS payments,
                   sum(payment_value)::DOUBLE AS value,
                   100.0 * count(*) / sum(count(*)) OVER () AS payments_pct,
                   (100.0 * sum(payment_value) / sum(sum(payment_value)) OVER ())::DOUBLE
                       AS value_pct
            FROM p {_states(states)}
            GROUP BY 1 ORDER BY value DESC, payment_type""",
        p=payments,
    )


def payment_installments(payments: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Payments and their value (BRL) per number of installments (all payment types)."""
    return sql(
        f"""SELECT payment_installments,
                   count(*)::BIGINT AS payments,
                   sum(payment_value)::DOUBLE AS value
            FROM p {_states(states)}
            GROUP BY 1 ORDER BY 1""",
        p=payments,
    )


def score_distribution(reviews: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Reviews (and distinct orders) per score; all five scores, zeros included.
    Empty when there are no reviews."""
    return sql(
        f"""WITH r AS (SELECT * FROM rv {_states(states)}),
                 s AS (SELECT unnest({list(REVIEW_SCORES)}) AS review_score)
            SELECT s.review_score::INTEGER AS review_score,
                   count(r.review_id)::BIGINT AS reviews,
                   count(DISTINCT r.order_id)::BIGINT AS orders,
                   100.0 * count(r.review_id) / (SELECT count(*) FROM r) AS share_pct
            FROM s LEFT JOIN r USING (review_score)
            WHERE (SELECT count(*) FROM r) > 0
            GROUP BY 1 ORDER BY 1""",
        rv=reviews,
    )


def weekly_scores(reviews: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Average score per week of ``review_created_at`` (simulated time) with the number
    of reviews; weeks without reviews are present with 0 reviews and no average, so a
    line breaks there instead of bridging the gap."""
    return sql(
        f"""WITH a AS (
                SELECT date_trunc('week', review_created_at::TIMESTAMP) AS week,
                       count(*) AS reviews, avg(review_score) AS avg_score
                FROM r {_states(states, "review_created_at IS NOT NULL")}
                GROUP BY 1),
            w AS (
                SELECT unnest(generate_series(min(week), max(week), INTERVAL 1 week)) AS week
                FROM a)
            SELECT w.week, coalesce(a.reviews, 0)::BIGINT AS reviews,
                   a.avg_score::DOUBLE AS avg_score
            FROM w LEFT JOIN a USING (week) ORDER BY w.week""",
        r=reviews,
    )


def reviews_by_delivery(reviews: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Reviews of delivered orders, on time (``not is_late``) vs late: reviews, sum and
    average of the scores (= ``sim.v_truth_reviews_by_delivery``); on time first."""
    where = _states(states, "is_delivered")
    return sql(
        f"""SELECT NOT coalesce(is_late, false) AS on_time,
                   count(*)::BIGINT AS reviews,
                   sum(review_score)::BIGINT AS score_sum,
                   avg(review_score)::DOUBLE AS avg_score
            FROM r {where}
            GROUP BY 1 ORDER BY 1 DESC""",
        r=reviews,
    )


def score_shares_by_delivery(reviews: Any, states: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Score distribution per delivery group (delivered orders): for each of on time /
    late with any review, all five scores with their count and share of the group (%),
    so groups of different size compare."""
    where = _states(states, "is_delivered")
    return sql(
        f"""WITH r AS (SELECT NOT coalesce(is_late, false) AS on_time, review_score
                       FROM rv {where}),
                 g AS (SELECT on_time, count(*) AS total FROM r GROUP BY 1),
                 s AS (SELECT unnest({list(REVIEW_SCORES)}) AS review_score)
            SELECT g.on_time, s.review_score::INTEGER AS review_score,
                   count(r.review_score)::BIGINT AS reviews,
                   100.0 * count(r.review_score) / g.total AS share_pct
            FROM g CROSS JOIN s
            LEFT JOIN r ON r.on_time = g.on_time AND r.review_score = s.review_score
            GROUP BY g.on_time, s.review_score, g.total
            ORDER BY g.on_time DESC, s.review_score""",
        rv=reviews,
    )
