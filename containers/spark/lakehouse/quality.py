"""Data-quality rules on the silver -> gold transition (05 §6), pure functions.

Each rule returns the violating rows in the quarantine shape
``(rule, severity, source_table, record_key, payload)``. Severity ``reject`` keeps the
row out of gold (the CHECK constraints of 06 §6 are the last line of defence behind
these rules); ``warn`` lets it through and only reports it. Real Olist examples: 9
payments of value 0 and 3 payments of type ``not_defined`` (01 §6).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # reject | warn
    table: str
    keys: tuple[str, ...]
    ok: str  # SQL predicate that a valid row satisfies (null counts as valid)


ITEM_RULES = (
    Rule("price_nonneg", "reject", "order_items", ("order_id", "order_item_id"), "price >= 0"),
    Rule(
        "freight_nonneg",
        "reject",
        "order_items",
        ("order_id", "order_item_id"),
        "freight_value >= 0",
    ),
)
ORDER_RULES = (
    Rule(
        "delivered_after_purchase",
        "warn",
        "orders",
        ("order_id",),
        "order_delivered_customer_date >= order_purchase_timestamp",
    ),
    Rule(
        "shipped_after_purchase",
        "warn",
        "orders",
        ("order_id",),
        "order_delivered_carrier_date >= order_purchase_timestamp",
    ),
)
PAYMENT_RULES = (
    Rule(
        "payment_value_positive",
        "warn",
        "order_payments",
        ("order_id", "payment_sequential"),
        "payment_value > 0",
    ),
    Rule(
        "payment_type_defined",
        "warn",
        "order_payments",
        ("order_id", "payment_sequential"),
        "payment_type <> 'not_defined'",
    ),
)
REVIEW_RULES = (
    # the CHECK score_range of gold/fact_reviews (06 §6) is the last line of defence;
    # Delta's CHECK fails on NULL, so here a missing score is a violation too
    Rule(
        "review_score_range",
        "reject",
        "order_reviews",
        ("review_id", "order_id"),
        "coalesce(review_score BETWEEN 1 AND 5, false)",
    ),
)
PRODUCT_EXISTS = Rule(
    "product_exists", "reject", "order_items", ("order_id", "order_item_id"), "product exists"
)


def _key(keys: tuple[str, ...]) -> Column:
    return F.concat_ws("|", *[F.col(k).cast("string") for k in keys])


def _payload(df: DataFrame) -> Column:
    cols = [c for c in df.columns if not c.startswith("_")]
    return F.to_json(F.struct(*cols))


def violations(df: DataFrame, rule: Rule) -> DataFrame:
    bad = df.filter(~F.coalesce(F.expr(rule.ok), F.lit(True)))
    return bad.select(
        F.lit(rule.name).alias("rule"),
        F.lit(rule.severity).alias("severity"),
        F.lit(rule.table).alias("source_table"),
        _key(rule.keys).alias("record_key"),
        _payload(df).alias("payload"),
    )


def check(df: DataFrame, rules: tuple[Rule, ...]) -> DataFrame | None:
    out = None
    for r in rules:
        v = violations(df, r)
        out = v if out is None else out.unionByName(v)
    return out


def check_items(items: DataFrame, products: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid items, violations): price/freight rules plus "the product exists"."""
    v = check(items, ITEM_RULES)
    missing = items.join(products.select("product_id"), "product_id", "left_anti")
    v_missing = missing.select(
        F.lit(PRODUCT_EXISTS.name).alias("rule"),
        F.lit(PRODUCT_EXISTS.severity).alias("severity"),
        F.lit(PRODUCT_EXISTS.table).alias("source_table"),
        _key(PRODUCT_EXISTS.keys).alias("record_key"),
        _payload(items).alias("payload"),
    )
    all_v = v.unionByName(v_missing)
    rejected = all_v.filter(F.col("severity") == "reject").select(
        F.split("record_key", r"\|").getItem(0).alias("order_id"),
        F.split("record_key", r"\|").getItem(1).cast("int").alias("order_item_id"),
    )
    valid = items.join(rejected, ["order_id", "order_item_id"], "left_anti")
    return valid, all_v


def check_orders(orders: DataFrame) -> DataFrame:
    return check(orders, ORDER_RULES)


def check_payments(payments: DataFrame) -> DataFrame:
    return check(payments, PAYMENT_RULES)


def check_reviews(reviews: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid reviews, violations): a score outside 1..5 is kept out of gold."""
    v = check(reviews, REVIEW_RULES)
    rule = REVIEW_RULES[0]
    valid = reviews.filter(F.coalesce(F.expr(rule.ok), F.lit(True)))
    return valid, v
