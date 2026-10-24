"""Gold: star schema, events and windowed aggregates (05 §5), pure functions.

Gold is computed from the *state* in silver (read through its Change Data Feed), not
from the raw stream: a micro-batch of silver changes only tells *which* orders and
customers are affected; their gold rows are recomputed from the current silver tables
and synchronised (``delta_io.sync_subset``). No stream-stream joins, and the order in
which items and orders arrive does not matter.

Time: every business timestamp is *simulated* time (the replayed Olist history).
Spark cannot see the simulator's clock, so where an event has no timestamp column of
its own (a cancellation, a final status) its time is estimated as the latest business
timestamp seen so far (``sim_clock``); its precision is the gap between two events.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.tables import MONEY, SUM

END_OF_TIME = dt.datetime(9999, 12, 31)
ORDER_TS = (
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
)

# 27 federative units of Brazil: code -> (name, region)
STATES = {
    "AC": ("Acre", "Norte"),
    "AL": ("Alagoas", "Nordeste"),
    "AM": ("Amazonas", "Norte"),
    "AP": ("Amapá", "Norte"),
    "BA": ("Bahia", "Nordeste"),
    "CE": ("Ceará", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"),
    "ES": ("Espírito Santo", "Sudeste"),
    "GO": ("Goiás", "Centro-Oeste"),
    "MA": ("Maranhão", "Nordeste"),
    "MG": ("Minas Gerais", "Sudeste"),
    "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"),
    "PA": ("Pará", "Norte"),
    "PB": ("Paraíba", "Nordeste"),
    "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"),
    "PR": ("Paraná", "Sul"),
    "RJ": ("Rio de Janeiro", "Sudeste"),
    "RN": ("Rio Grande do Norte", "Nordeste"),
    "RO": ("Rondônia", "Norte"),
    "RR": ("Roraima", "Norte"),
    "RS": ("Rio Grande do Sul", "Sul"),
    "SC": ("Santa Catarina", "Sul"),
    "SE": ("Sergipe", "Nordeste"),
    "SP": ("São Paulo", "Sudeste"),
    "TO": ("Tocantins", "Norte"),
}


def alive(df: DataFrame) -> DataFrame:
    return df.filter(~F.col("_is_deleted"))


def latest_business_ts(df: DataFrame, prefix: str = "") -> Column:
    """greatest() of the lifecycle timestamps of an orders row (null-safe)."""
    return F.greatest(*[F.col(f"{prefix}{c}") for c in ORDER_TS])


def sim_clock(orders: DataFrame) -> dt.datetime | None:
    """The latest business timestamp in silver orders = the pipeline's estimate of the
    simulator's clock."""
    return orders.select(F.max(latest_business_ts(orders)).alias("t")).first()["t"]


# ---------------------------------------------------------------------- dimensions
def dim_date(spark: SparkSession, start: str = "2016-01-01", end: str = "2019-12-31"):
    d = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date"
    )
    return d.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        # ISO day of week: 1 = Monday .. 7 = Sunday
        (((F.dayofweek("date") + 5) % 7) + 1).alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def dim_geography(spark: SparkSession) -> DataFrame:
    rows = [(k, v[0], v[1]) for k, v in sorted(STATES.items())]
    return spark.createDataFrame(rows, "state string, state_name string, region string")


def dim_product(products: DataFrame, translation: DataFrame) -> DataFrame:
    t = alive(translation).select("product_category_name", "product_category_name_english")
    p = alive(products)
    return p.join(t, "product_category_name", "left").select(
        "product_id",
        "product_category_name",
        F.coalesce(
            "product_category_name_english", "product_category_name", F.lit("unknown")
        ).alias("product_category_name_english"),
        F.col("product_photos_qty").cast("int"),
        F.col("product_weight_g").cast("int"),
        F.col("product_length_cm").cast("int"),
        F.col("product_height_cm").cast("int"),
        F.col("product_width_cm").cast("int"),
    )


def dim_seller(sellers: DataFrame) -> DataFrame:
    return alive(sellers).select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    )


def customer_scd2(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """SCD type 2 per ``customer_unique_id`` (05 §5, 01 §6: 250 real movers).

    Olist creates a new ``customer_id`` for every order, so the history of a person is
    the sequence of their orders (by purchase time) with the address of each order; a
    new version starts where (zip, city, state) differs from the previous order.
    ``valid_to`` of the current version is 9999-12-31; ``customer_sk`` is deterministic
    (xxhash64 of the person and ``valid_from``), so a recomputation keeps the keys."""
    attrs = ["customer_zip_code_prefix", "customer_city", "customer_state"]
    base = alive(customers).join(
        alive(orders).select("customer_id", F.col("order_purchase_timestamp").alias("ts")),
        "customer_id",
    )
    w = Window.partitionBy("customer_unique_id").orderBy("ts", "customer_id")
    same = None
    for a in attrs:
        cond = F.col(a).eqNullSafe(F.lag(a).over(w))
        same = cond if same is None else same & cond
    flagged = base.withColumn("_new", F.when(same, 0).otherwise(1))
    versioned = flagged.withColumn("version", F.sum("_new").over(w).cast("int"))
    # a version starts at its first order: valid_from = that purchase, attributes = its
    runs = (
        versioned.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_unique_id", "version").orderBy("ts", "customer_id")
            ),
        )
        .filter("_rn = 1")
        .select("customer_unique_id", "version", F.col("ts").alias("valid_from"), *attrs)
    )
    wv = Window.partitionBy("customer_unique_id").orderBy("version")
    valid_to = F.coalesce(F.lead("valid_from").over(wv), F.lit(END_OF_TIME).cast("timestamp"))
    return runs.select(
        F.xxhash64("customer_unique_id", "valid_from").alias("customer_sk"),
        "customer_unique_id",
        *attrs,
        "version",
        "valid_from",
        valid_to.alias("valid_to"),
        (valid_to == F.lit(END_OF_TIME).cast("timestamp")).alias("is_current"),
    )


def _with_customer(orders: DataFrame, customers: DataFrame, dim_customer: DataFrame):
    """orders (alive) + customer attributes + the SCD2 version valid at purchase time."""
    o = alive(orders).join(
        alive(customers).select("customer_id", "customer_unique_id", "customer_state"),
        "customer_id",
    )
    d = dim_customer.select(
        F.col("customer_unique_id").alias("_u"), "customer_sk", "valid_from", "valid_to"
    )
    cond = (
        (o["customer_unique_id"] == d["_u"])
        & (o["order_purchase_timestamp"] >= d["valid_from"])
        & (o["order_purchase_timestamp"] < d["valid_to"])
    )
    return o.join(d, cond, "left").drop("_u", "valid_from", "valid_to")


# --------------------------------------------------------------------------- facts
def fact_order_items(
    items: DataFrame,
    orders: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    sellers: DataFrame,
) -> DataFrame:
    """Grain: order item (05 §5); ``items`` are the rows that passed the quality rules."""
    o = _with_customer(orders, customers, dim_customer).select(
        "order_id",
        "customer_id",
        "customer_sk",
        "customer_unique_id",
        "customer_state",
        "order_status",
        F.col("order_purchase_timestamp").alias("purchased_at"),
    )
    s = alive(sellers).select("seller_id", "seller_state")
    f = alive(items).join(o, "order_id").join(s, "seller_id", "left")
    return f.select(
        "order_id",
        F.col("order_item_id").cast("int"),
        "product_id",
        "seller_id",
        "customer_id",
        "customer_sk",
        "customer_unique_id",
        "customer_state",
        "seller_state",
        "order_status",
        "purchased_at",
        F.to_date("purchased_at").alias("purchase_date"),
        F.date_format("purchased_at", "yyyyMMdd").cast("int").alias("purchase_date_key"),
        F.col("shipping_limit_date").alias("shipping_limit_at"),
        F.col("price").cast(MONEY),
        F.col("freight_value").cast(MONEY),
    )


def _hours(a: str, b: str) -> Column:
    return (F.col(b).cast("long") - F.col(a).cast("long")) / 3600.0


def fact_orders_lifecycle(
    orders: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    items: DataFrame,
    payments: DataFrame,
) -> DataFrame:
    """Accumulating snapshot (Kimball): one row per order, milestones filled in as the
    order progresses (05 §5). ``is_late`` = delivered after the estimate (01 §6)."""
    o = _with_customer(orders, customers, dim_customer)
    it = (
        alive(items)
        .groupBy("order_id")
        .agg(
            F.count(F.lit(1)).cast("int").alias("items"),
            F.sum("price").cast(SUM).alias("items_value"),
            F.sum("freight_value").cast(SUM).alias("freight_value"),
        )
    )
    pay = (
        alive(payments)
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").cast(SUM).alias("payment_value"),
            F.count(F.lit(1)).cast("int").alias("payments"),
        )
    )
    j = o.join(it, "order_id", "left").join(pay, "order_id", "left")
    delivered = F.col("order_delivered_customer_date")
    return j.select(
        "order_id",
        "customer_id",
        "customer_sk",
        "customer_unique_id",
        "customer_state",
        "order_status",
        F.col("order_purchase_timestamp").alias("purchased_at"),
        F.col("order_approved_at").alias("approved_at"),
        F.col("order_delivered_carrier_date").alias("shipped_at"),
        delivered.alias("delivered_at"),
        F.col("order_estimated_delivery_date").alias("estimated_delivery_at"),
        F.date_format("order_purchase_timestamp", "yyyyMMdd")
        .cast("int")
        .alias("purchase_date_key"),
        _hours("order_purchase_timestamp", "order_approved_at").alias("h_to_approve"),
        (_hours("order_purchase_timestamp", "order_delivered_carrier_date") / 24).alias(
            "d_to_ship"
        ),
        (_hours("order_purchase_timestamp", "order_delivered_customer_date") / 24).alias(
            "d_to_deliver"
        ),
        delivered.isNotNull().alias("is_delivered"),
        F.coalesce(delivered > F.col("order_estimated_delivery_date"), F.lit(False)).alias(
            "is_late"
        ),
        F.coalesce("items", F.lit(0)).alias("items"),
        F.coalesce("items_value", F.lit(0).cast(SUM)).alias("items_value"),
        F.coalesce("freight_value", F.lit(0).cast(SUM)).alias("freight_value"),
        F.coalesce("payment_value", F.lit(0).cast(SUM)).alias("payment_value"),
        F.coalesce("payments", F.lit(0)).alias("payments"),
    )


# -------------------------------------------------------------------------- events
def revenue_events(changes: DataFrame, clock: dt.datetime | None, batch_id: int) -> DataFrame:
    """Revenue movements from the differences of ``fact_order_items`` (``delta_io.diff``):
    a new item is a sale at its purchase time; a removed item (DELETE of the items of
    a canceled order, full ``before`` thanks to REPLICA IDENTITY FULL) is a negative
    correction at the time of the cancellation (``clock``); a changed price is an
    adjustment. The sum of all events always equals the sum of the fact table."""
    clock_col = F.lit(clock).cast("timestamp") if clock else F.lit(None).cast("timestamp")
    new, old = F.col("_new"), F.col("_old")
    act = F.col("_action")
    kind = (
        F.when(act == "insert", "sale")
        .when(act == "delete", "correction")
        .when(
            ~new["price"].eqNullSafe(old["price"])
            | ~new["freight_value"].eqNullSafe(old["freight_value"]),
            "adjustment",
        )
    )
    zero = F.lit(0).cast(MONEY)
    amount = (
        F.when(act == "insert", new["price"])
        .when(act == "delete", -old["price"])
        .otherwise(new["price"] - old["price"])
    )
    freight = (
        F.when(act == "insert", new["freight_value"])
        .when(act == "delete", -old["freight_value"])
        .otherwise(new["freight_value"] - old["freight_value"])
    )
    later = F.greatest(clock_col, F.coalesce(new["purchased_at"], old["purchased_at"]))
    ev = changes.select(
        "order_id",
        "order_item_id",
        F.coalesce(new["customer_state"], old["customer_state"]).alias("customer_state"),
        F.when(act == "insert", new["purchased_at"]).otherwise(later).alias("event_time"),
        kind.alias("kind"),
        F.coalesce(amount, zero).cast(MONEY).alias("amount"),
        F.coalesce(freight, zero).cast(MONEY).alias("freight"),
        F.when(act == "insert", 1).when(act == "delete", -1).otherwise(0).alias("items"),
    ).filter(F.col("kind").isNotNull())
    return ev.select(
        F.sha2(
            F.concat_ws(
                "|", "order_id", F.col("order_item_id").cast("string"), "kind", F.lit(str(batch_id))
            ),
            256,
        ).alias("event_id"),
        "*",
        F.lit(batch_id).cast("long").alias("gold_batch_id"),
        F.current_timestamp().alias("recorded_at"),
    )


def status_transitions(bronze_orders: DataFrame, clock: dt.datetime | None) -> DataFrame:
    """Status changes from the ``before -> after`` pair of every orders event (05 §5).

    Silver keeps only the latest state per batch, so the funnel is read from bronze,
    where every event is kept. INSERT (``c``) = ``(new) -> created`` at purchase time;
    UPDATE with a changed status: the event time is the milestone column it set
    (approved / shipped / delivered) or, for statuses without a column (canceled,
    unavailable, invoiced, ...), the estimated clock. Snapshot reads (``r``) are states,
    not transitions, and are skipped; so are deletes."""
    b, a = F.col("before"), F.col("after")
    newly = {
        c: a[c].isNotNull() & ~a[c].eqNullSafe(b[c])
        for c in (
            "order_delivered_customer_date",
            "order_delivered_carrier_date",
            "order_approved_at",
        )
    }
    own_latest = F.greatest(*[a[c] for c in ORDER_TS])
    clock_col = F.lit(clock).cast("timestamp") if clock else F.lit(None).cast("timestamp")
    estimated = F.greatest(clock_col, own_latest)
    when_col = (
        F.when(F.col("op") == "c", a["order_purchase_timestamp"])
        .when(newly["order_delivered_customer_date"], a["order_delivered_customer_date"])
        .when(newly["order_delivered_carrier_date"], a["order_delivered_carrier_date"])
        .when(newly["order_approved_at"], a["order_approved_at"])
    )
    changed = (F.col("op") == "c") | (
        (F.col("op") == "u") & ~a["order_status"].eqNullSafe(b["order_status"])
    )
    return (
        bronze_orders.filter(changed & a.isNotNull())
        .select(
            a["order_id"].alias("order_id"),
            F.col("source_lsn").cast("long").alias("source_lsn"),
            F.when(F.col("op") == "c", F.lit(None).cast("string"))
            .otherwise(b["order_status"])
            .alias("from_status"),
            a["order_status"].alias("to_status"),
            F.coalesce(when_col, estimated).alias("event_time"),
            F.when(when_col.isNotNull(), "column")
            .otherwise("estimated")
            .alias("event_time_source"),
            F.col("source_ts"),
            F.current_timestamp().alias("recorded_at"),
        )
        .dropDuplicates(["order_id", "source_lsn"])
    )


# ------------------------------------------------------------------------- windows
def revenue_windows(events: DataFrame, watermark: str, window: str = "1 hour") -> DataFrame:
    """Revenue per hour and customer state in event (simulated) time with a watermark:
    an event older than the watermark is dropped and a closed window never changes."""
    amt = F.col("amount")
    agg = (
        events.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window).alias("w"), "customer_state")
        .agg(
            F.sum(amt).alias("revenue"),
            F.sum(F.when(amt > 0, amt).otherwise(0)).alias("sales"),
            F.sum(F.when(amt < 0, amt).otherwise(0)).alias("corrections"),
            F.sum("freight").alias("freight"),
            F.sum(F.when(F.col("items") > 0, F.col("items")).otherwise(0)).alias("items_sold"),
            F.sum(F.when(F.col("items") < 0, -F.col("items")).otherwise(0)).alias("items_canceled"),
            F.count(F.lit(1)).alias("events"),
        )
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "customer_state",
        F.col("revenue").cast(SUM),
        F.col("sales").cast(SUM),
        F.col("corrections").cast(SUM),
        F.col("freight").cast(SUM),
        F.col("items_sold").cast("long"),
        F.col("items_canceled").cast("long"),
        F.col("events").cast("long"),
    )


NEW_STATUS = "(new)"


def funnel_windows(transitions: DataFrame, watermark: str, window: str = "1 hour") -> DataFrame:
    """Number of status transitions per hour (event time, watermark)."""
    agg = (
        transitions.withWatermark("event_time", watermark)
        .groupBy(
            F.window("event_time", window).alias("w"),
            F.coalesce("from_status", F.lit(NEW_STATUS)).alias("from_status"),
            "to_status",
        )
        .agg(F.count(F.lit(1)).alias("transitions"))
    )
    return agg.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "from_status",
        "to_status",
        F.col("transitions").cast("long"),
    )


# ---------------------------------------------------------------- affected keys
def affected(
    cdf: DataFrame, orders: DataFrame, items: DataFrame, customers: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """(order_ids, customer_unique_ids) whose gold rows must be recomputed for a batch of
    silver changes. ``orders``/``items``/``customers`` are the whole silver tables
    (deleted rows included, so deletes propagate)."""
    direct = cdf.filter(F.col("src").isin("orders", "order_items", "order_payments")).select(
        "order_id"
    )
    products = cdf.filter(F.col("src") == "products").select("product_id").distinct()
    via_products = items.join(products, "product_id", "left_semi").select("order_id")
    first = direct.unionByName(via_products).filter(F.col("order_id").isNotNull()).distinct()
    cust = (
        cdf.filter(F.col("src") == "customers")
        .select("customer_id")
        .unionByName(orders.join(first, "order_id", "left_semi").select("customer_id"))
        .distinct()
    )
    uniq = customers.join(cust, "customer_id", "left_semi").select("customer_unique_id").distinct()
    all_cust = customers.join(uniq, "customer_unique_id", "left_semi").select("customer_id")
    more = orders.join(all_cust, "customer_id", "left_semi").select("order_id")
    return first.unionByName(more).distinct(), uniq
