"""05 §10 unit rows on gold: SCD2, accumulating snapshot, watermark, negative correction,
quality rules (local SparkSession, Delta on the local filesystem)."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from lakehouse import delta_io, gold, quality
from lakehouse.config import Lake, Settings
from lakehouse.jobs import GoldJob, ensure_gold_and_ops
from tests.helpers import silver_fixtures as sf
from tests.helpers.silver_fixtures import T

pytestmark = pytest.mark.unit


def _df(spark, table, rows, op="c"):
    return sf.rows_df(spark, table, rows, 100, op)


# ------------------------------------------------------------------------------ SCD2
def test_scd2_customer_moves_and_the_fact_points_to_the_version_valid_at_purchase(spark):
    """05 §10: "SCD2: klient zmienia zip" → dwie wersje; fakt wskazuje wersję ważną w
    chwili zakupu."""
    customers = _df(
        spark,
        "customers",
        [
            {
                "customer_id": "c1",
                "customer_unique_id": "u1",
                "customer_zip_code_prefix": "06233",
                "customer_city": "osasco",
                "customer_state": "SP",
            },
            {
                "customer_id": "c2",
                "customer_unique_id": "u1",
                "customer_zip_code_prefix": "06233",
                "customer_city": "osasco",
                "customer_state": "SP",
            },
            {
                "customer_id": "c3",
                "customer_unique_id": "u1",
                "customer_zip_code_prefix": "13330",
                "customer_city": "indaiatuba",
                "customer_state": "SP",
            },
        ],
    )
    orders = _df(
        spark,
        "orders",
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "delivered",
                "order_purchase_timestamp": T("2017-03-14 10:00:00"),
                "order_estimated_delivery_date": T("2017-03-30 00:00:00"),
            },
            {
                "order_id": "o2",
                "customer_id": "c2",
                "order_status": "delivered",
                "order_purchase_timestamp": T("2017-06-01 09:00:00"),
                "order_estimated_delivery_date": T("2017-06-20 00:00:00"),
            },
            {
                "order_id": "o3",
                "customer_id": "c3",
                "order_status": "created",
                "order_purchase_timestamp": T("2018-01-20 08:00:00"),
                "order_estimated_delivery_date": T("2018-02-10 00:00:00"),
            },
        ],
    )
    dim = gold.customer_scd2(customers, orders)
    versions = sorted(dim.collect(), key=lambda r: r["version"])
    assert [(v["version"], v["customer_zip_code_prefix"], v["is_current"]) for v in versions] == [
        (1, "06233", False),
        (2, "13330", True),
    ]
    assert versions[0]["valid_from"] == T("2017-03-14 10:00:00")
    assert versions[0]["valid_to"] == versions[1]["valid_from"] == T("2018-01-20 08:00:00")
    assert versions[1]["valid_to"] == gold.END_OF_TIME

    items = _df(
        spark,
        "order_items",
        [
            {
                "order_id": o,
                "order_item_id": 1,
                "product_id": "p1",
                "seller_id": "s1",
                "shipping_limit_date": T("2018-01-25 00:00:00"),
                "price": Decimal("10.00"),
                "freight_value": Decimal("1.00"),
            }
            for o in ("o1", "o2", "o3")
        ],
    )
    sellers = _df(spark, "sellers", [{"seller_id": "s1", "seller_state": "RJ"}])
    fact = {
        r["order_id"]: r
        for r in gold.fact_order_items(items, orders, customers, dim, sellers).collect()
    }
    sk = {v["version"]: v["customer_sk"] for v in versions}
    assert {o: fact[o]["customer_sk"] for o in fact} == {"o1": sk[1], "o2": sk[1], "o3": sk[2]}
    assert fact["o1"]["seller_state"] == "RJ" and fact["o1"]["purchase_date_key"] == 20170314


# ------------------------------------------------------------ accumulating snapshot
def test_accumulating_snapshot_fills_the_milestones_and_is_late(spark, tmp_path):
    """05 §10: "fakt z akumulującym snapshotem" → poprawne czasy etapów i `is_late`;
    one row per order, updated in place as the order progresses."""
    path = f"{tmp_path}/fact_orders_lifecycle"
    from lakehouse.tables import GOLD

    delta_io.ensure_table(spark, path, GOLD["fact_orders_lifecycle"].schema)
    customers = _df(
        spark,
        "customers",
        [{"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"}],
    )
    base = {
        "order_id": "o1",
        "customer_id": "c1",
        "order_purchase_timestamp": T("2017-10-02 10:56:00"),
        "order_estimated_delivery_date": T("2017-10-09 00:00:00"),
    }
    steps = [
        {"order_status": "created"},
        {"order_status": "approved", "order_approved_at": T("2017-10-02 11:08:00")},
        {
            "order_status": "shipped",
            "order_approved_at": T("2017-10-02 11:08:00"),
            "order_delivered_carrier_date": T("2017-10-04 19:55:00"),
        },
        {
            "order_status": "delivered",
            "order_approved_at": T("2017-10-02 11:08:00"),
            "order_delivered_carrier_date": T("2017-10-04 19:55:00"),
            "order_delivered_customer_date": T("2017-10-10 21:25:00"),
        },
    ]
    items = _df(
        spark,
        "order_items",
        [
            {
                "order_id": "o1",
                "order_item_id": 1,
                "price": Decimal("29.99"),
                "freight_value": Decimal("8.72"),
            }
        ],
    )
    pays = _df(
        spark,
        "order_payments",
        [{"order_id": "o1", "payment_sequential": 1, "payment_value": Decimal("38.71")}],
    )
    for step in steps:
        orders = _df(spark, "orders", [{**base, **step}])
        dim = gold.customer_scd2(customers, orders)
        desired = gold.fact_orders_lifecycle(orders, customers, dim, items, pays)
        delta_io.sync_subset(spark, path, desired, ["order_id"])
    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 1
    r = rows[0]
    assert (r["order_status"], r["approved_at"], r["shipped_at"], r["delivered_at"]) == (
        "delivered",
        T("2017-10-02 11:08:00"),
        T("2017-10-04 19:55:00"),
        T("2017-10-10 21:25:00"),
    )
    assert round(r["h_to_approve"], 1) == 0.2
    assert round(r["d_to_deliver"], 1) == 8.4
    assert r["is_late"] is True and r["is_delivered"] is True
    assert (r["items"], r["items_value"], r["payment_value"]) == (
        1,
        Decimal("29.99"),
        Decimal("38.71"),
    )
    # four syncs, one row: the second and later syncs were updates of the same row
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").filter("operation = 'MERGE'").count()
    assert hist == 4


# ------------------------------------------------------------------------- watermark
def test_late_event_beyond_the_watermark_does_not_change_a_closed_window(spark, tmp_path):
    """05 §10: "okno z watermarkiem" → zdarzenie spóźnione poza watermark nie zmienia
    zamkniętego okna (Structured Streaming, update mode, as in the container)."""
    from lakehouse.tables import GOLD

    src = f"{tmp_path}/revenue_events"
    out = f"{tmp_path}/agg"
    delta_io.ensure_table(spark, src, GOLD["revenue_events"].schema)
    delta_io.ensure_table(spark, out, GOLD["agg_revenue_hourly"].schema)

    def events(rows):
        return spark.createDataFrame(
            [
                (
                    "e" + str(i),
                    f"o{i}",
                    1,
                    st,
                    t,
                    "sale",
                    Decimal(a),
                    Decimal("0.00"),
                    1,
                    0,
                    dt.datetime(2026, 10, 23),
                )
                for i, (st, t, a) in enumerate(rows)
            ],
            GOLD["revenue_events"].schema,
        )

    def run_stream():
        agg = gold.revenue_windows(spark.readStream.format("delta").load(src), "2 hours")

        def sink(df, _):
            delta_io.upsert(
                spark,
                out,
                df.withColumn("_updated_at", F.current_timestamp()),
                ["window_start", "customer_state"],
            )

        q = (
            agg.writeStream.outputMode("update")
            .foreachBatch(sink)
            .option("checkpointLocation", f"{tmp_path}/ckpt")
            .trigger(availableNow=True)
            .start()
        )
        q.awaitTermination()

    def window(start):
        r = (
            spark.read.format("delta")
            .load(out)
            .filter((F.col("window_start") == T(start)) & (F.col("customer_state") == "SP"))
            .collect()
        )
        return (r[0]["revenue"], r[0]["events"]) if r else None

    events(
        [("SP", T("2017-01-05 09:10:00"), "10.00"), ("SP", T("2017-01-05 09:50:00"), "5.00")]
    ).write.format("delta").mode("append").save(src)
    run_stream()
    assert window("2017-01-05 09:00:00") == (Decimal("15.00"), 2)
    # time moves on to 13:00 -> watermark = 13:00 - 2 h = 11:00, window 09-10 is closed
    events([("SP", T("2017-01-05 13:00:00"), "7.00")]).write.format("delta").mode("append").save(
        src
    )
    run_stream()
    # late event for 09:30 (older than the watermark) and one inside the watermark (11:30)
    events(
        [("SP", T("2017-01-05 09:30:00"), "100.00"), ("SP", T("2017-01-05 11:30:00"), "3.00")]
    ).write.format("delta").mode("append").save(src)
    run_stream()
    assert window("2017-01-05 09:00:00") == (Decimal("15.00"), 2)  # unchanged
    assert window("2017-01-05 11:00:00") == (Decimal("3.00"), 1)
    assert window("2017-01-05 13:00:00") == (Decimal("7.00"), 1)


# ------------------------------------------------------------- negative correction
def test_delete_of_a_canceled_orders_item_is_a_negative_correction(spark):
    """05 §10: "DELETE pozycji anulowanego zamówienia" → ujemna korekta w agregacie
    przychodu (from the full `before` image of the removed fact row)."""
    from lakehouse.tables import GOLD

    cols = [c.name for c in GOLD["fact_order_items"].schema if c.name != "_updated_at"]
    row = {c: None for c in cols} | {
        "order_id": "o1",
        "order_item_id": 1,
        "customer_state": "SP",
        "price": Decimal("58.90"),
        "freight_value": Decimal("12.00"),
        "purchased_at": T("2017-01-05 10:00:00"),
    }
    schema = ", ".join(
        f"{f.name} {f.dataType.simpleString()}"
        for f in GOLD["fact_order_items"].schema
        if f.name != "_updated_at"
    )
    current = spark.createDataFrame([tuple(row[c] for c in cols)], schema)
    empty = current.limit(0)
    keys = ["order_id", "order_item_id"]
    values = [c for c in cols if c not in keys]
    sale = gold.revenue_events(
        delta_io.diff(empty, current, keys, values), T("2017-01-05 10:00:00"), 1
    )
    # three days later the order is canceled: its items are deleted (desired = empty)
    corr = gold.revenue_events(
        delta_io.diff(current, empty, keys, values), T("2017-01-08 14:20:00"), 2
    )
    ev = sale.unionByName(corr)
    got = sorted((r["kind"], r["amount"], r["items"], r["event_time"]) for r in ev.collect())
    assert got == [
        ("correction", Decimal("-58.90"), -1, T("2017-01-08 14:20:00")),
        ("sale", Decimal("58.90"), 1, T("2017-01-05 10:00:00")),
    ]
    agg = {r["window_start"]: r for r in gold.revenue_windows(ev, "24 hours").collect()}
    corr_w = agg[T("2017-01-08 14:00:00")]
    assert (corr_w["revenue"], corr_w["corrections"], corr_w["items_canceled"]) == (
        Decimal("-58.90"),
        Decimal("-58.90"),
        1,
    )
    assert agg[T("2017-01-05 10:00:00")]["sales"] == Decimal("58.90")
    assert sum(r["revenue"] for r in agg.values()) == Decimal("0.00")


# -------------------------------------------------------------------- quality rules
@pytest.mark.slow
def test_quality_rules_send_bad_rows_to_quarantine_and_the_batch_goes_on(spark, tmp_path):
    """05 §10: "reguły jakości" → `price < 0` w kwarantannie, strumień działa dalej: a
    whole gold micro-batch (GoldJob.process) over local silver tables writes the good
    rows, keeps the rejected item out of gold and reports every rule violation."""
    base = f"{tmp_path}/"
    lake = Lake(base)
    settings = Settings(base_uri=base)
    sf.empty_silver(spark, base)
    ensure_gold_and_ops(spark, lake)
    sf.merge(
        spark,
        base,
        "customers",
        [
            {
                "customer_id": "c1",
                "customer_unique_id": "u1",
                "customer_state": "SP",
                "customer_city": "sao paulo",
                "customer_zip_code_prefix": "01037",
            }
        ],
        10,
    )
    sf.merge(spark, base, "products", [{"product_id": "p1", "product_category_name": "beleza"}], 10)
    sf.merge(
        spark,
        base,
        "product_category_name_translation",
        [{"product_category_name": "beleza", "product_category_name_english": "health_beauty"}],
        10,
    )
    sf.merge(spark, base, "sellers", [{"seller_id": "s1", "seller_state": "SP"}], 10)
    sf.merge(
        spark,
        base,
        "orders",
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "created",
                "order_purchase_timestamp": T("2017-01-05 10:00:00"),
                "order_estimated_delivery_date": T("2017-01-20 00:00:00"),
            }
        ],
        20,
    )
    sf.merge(
        spark,
        base,
        "order_items",
        [
            {
                "order_id": "o1",
                "order_item_id": 1,
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal("58.90"),
                "freight_value": Decimal("10.00"),
            },
            {
                "order_id": "o1",
                "order_item_id": 2,
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal("-1.00"),
                "freight_value": Decimal("1.00"),
            },
            {
                "order_id": "o1",
                "order_item_id": 3,
                "product_id": "p-missing",
                "seller_id": "s1",
                "price": Decimal("5.00"),
                "freight_value": Decimal("1.00"),
            },
        ],
        30,
    )
    sf.merge(
        spark,
        base,
        "order_payments",
        [
            {
                "order_id": "o1",
                "payment_sequential": 1,
                "payment_type": "not_defined",
                "payment_value": Decimal("0.00"),
            }
        ],
        40,
    )

    job = GoldJob(spark, settings, lake)
    job.process(sf.cdf_since(spark, base, {}), 0)

    fact = spark.read.format("delta").load(lake.gold("fact_order_items")).collect()
    assert [(r["order_item_id"], r["price"]) for r in fact] == [(1, Decimal("58.90"))]
    q = {
        (r["rule"], r["record_key"], r["severity"])
        for r in spark.read.format("delta").load(lake.ops("quarantine")).collect()
    }
    assert q == {
        ("price_nonneg", "o1|2", "reject"),
        ("product_exists", "o1|3", "reject"),
        ("payment_value_positive", "o1|1", "warn"),
        ("payment_type_defined", "o1|1", "warn"),
    }
    lc = spark.read.format("delta").load(lake.gold("fact_orders_lifecycle")).collect()
    assert [(r["order_id"], r["items"], r["items_value"]) for r in lc] == [
        ("o1", 1, Decimal("58.90"))
    ]
    dp = spark.read.format("delta").load(lake.gold("dim_product")).collect()
    assert [(r["product_id"], r["product_category_name_english"]) for r in dp] == [
        ("p1", "health_beauty")
    ]
    ev = spark.read.format("delta").load(lake.gold("revenue_events")).collect()
    assert [(r["kind"], r["amount"]) for r in ev] == [("sale", Decimal("58.90"))]

    # next batch: the item with the bad price is fixed, the order is canceled and its
    # items deleted -> facts follow, revenue events carry the corrections
    v = sf.versions(spark, base)
    sf.merge(
        spark,
        base,
        "order_items",
        [
            {
                "order_id": "o1",
                "order_item_id": 2,
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal("4.00"),
                "freight_value": Decimal("1.00"),
            }
        ],
        50,
        op="u",
    )
    job.process(sf.cdf_since(spark, base, v), 1)
    fact = spark.read.format("delta").load(lake.gold("fact_order_items")).collect()
    assert sorted(r["order_item_id"] for r in fact) == [1, 2]
    v = sf.versions(spark, base)
    sf.merge(
        spark,
        base,
        "orders",
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "canceled",
                "order_purchase_timestamp": T("2017-01-05 10:00:00"),
                "order_estimated_delivery_date": T("2017-01-20 00:00:00"),
            }
        ],
        60,
        op="u",
    )
    sf.merge(
        spark,
        base,
        "order_items",
        [
            {
                "order_id": "o1",
                "order_item_id": i,
                "product_id": "p1",
                "seller_id": "s1",
                "price": Decimal("0"),
                "freight_value": Decimal("0"),
            }
            for i in (1, 2)
        ],
        61,
        op="d",
    )
    job.process(sf.cdf_since(spark, base, v), 2)
    assert spark.read.format("delta").load(lake.gold("fact_order_items")).count() == 0
    ev = spark.read.format("delta").load(lake.gold("revenue_events"))
    assert ev.agg(F.sum("amount")).first()[0] == Decimal("0.00")
    lc = spark.read.format("delta").load(lake.gold("fact_orders_lifecycle")).first()
    assert (lc["order_status"], lc["items"]) == ("canceled", 0)


def test_quality_rule_predicates(spark):
    """05 §6: each rule on its own (a NULL date is valid for a warn rule; warn rows stay
    in gold; NULL against a CHECK is covered by the parametrised test below)."""
    items = _df(
        spark,
        "order_items",
        [
            {
                "order_id": "a",
                "order_item_id": 1,
                "product_id": "p1",
                "price": Decimal("0.00"),
                "freight_value": Decimal("0.00"),
            },
            {
                "order_id": "a",
                "order_item_id": 2,
                "product_id": "p1",
                "price": Decimal("-0.01"),
                "freight_value": Decimal("0.00"),
            },
            {  # NULL fails the CHECK freight_nonneg, so the rule rejects it too
                "order_id": "a",
                "order_item_id": 3,
                "product_id": "p1",
                "price": Decimal("1.00"),
                "freight_value": None,
            },
        ],
    )
    products = _df(spark, "products", [{"product_id": "p1"}])
    valid, v = quality.check_items(items, products)
    assert [r["order_item_id"] for r in valid.collect()] == [1]
    assert sorted((r["rule"], r["record_key"]) for r in v.collect()) == [
        ("freight_nonneg", "a|3"),
        ("price_nonneg", "a|2"),
    ]
    orders = _df(
        spark,
        "orders",
        [
            {
                "order_id": "x",
                "order_purchase_timestamp": T("2017-01-05 10:00:00"),
                "order_delivered_customer_date": T("2017-01-04 10:00:00"),
            },
            {"order_id": "y", "order_purchase_timestamp": T("2017-01-05 10:00:00")},
        ],
    )
    assert [(r["rule"], r["record_key"]) for r in quality.check_orders(orders).collect()] == [
        ("delivered_after_purchase", "x")
    ]


RULES = (*quality.ITEM_RULES, *quality.ORDER_RULES, *quality.PAYMENT_RULES, *quality.REVIEW_RULES)
# CHECKs on columns gold computes itself (coalesced count, sum of negatives, lead of
# valid_from): no source value reaches them directly, so no rule guards them
COMPUTED_CHECKS = {
    "dim_customer.valid_range",
    "fact_orders_lifecycle.items_nonneg",
    "agg_revenue_hourly.corrections_nonpos",
}
# values probed per guarded CHECK: below, on and inside the bound, and NULL
CHECK_PROBES = {
    "fact_order_items.price_nonneg": ("price", (Decimal("-0.01"), Decimal("0"), None)),
    "fact_order_items.freight_nonneg": ("freight_value", (Decimal("-1"), Decimal("2.5"), None)),
    "fact_reviews.score_range": ("review_score", (0, 1, 5, 6, None)),
}


def test_every_gold_check_is_guarded_by_a_reject_rule_with_the_same_expression():
    """06 §6 / 05 §6: each gold CHECK on a source column has a reject rule with the very
    same expression (and a probe below); the others are on columns gold computes."""
    from lakehouse.tables import GOLD

    checks = {f"{t}.{c}": e for t, spec in GOLD.items() for c, e in spec.constraints.items()}
    guarded = {r.guards: r for r in RULES if r.guards}
    assert set(checks) == set(guarded) | COMPUTED_CHECKS
    assert set(guarded) == set(CHECK_PROBES)
    for name, rule in guarded.items():
        assert (rule.severity, rule.ok) == ("reject", checks[name])


@pytest.mark.parametrize("check", sorted(CHECK_PROBES))
def test_reject_rule_rejects_exactly_the_rows_its_check_rejects_including_null(
    spark, tmp_path, check
):
    """Delta fails a CHECK that evaluates to NULL; the guarding rule must quarantine the
    same rows (NULL included), so the CHECK never fires and the gold batch goes on."""
    from lakehouse.tables import GOLD

    table, constraint = check.split(".")
    spec = GOLD[table]
    rule = next(r for r in RULES if r.guards == check)
    column, values = CHECK_PROBES[check]
    path = f"{tmp_path}/{table}"
    delta_io.ensure_table(
        spark, path, spec.schema, constraints={constraint: spec.constraints[constraint]}
    )
    types = {f.name: f.dataType.simpleString() for f in spec.schema}
    rows = []
    for i, value in enumerate(values):
        keys = {k: i if types[k] == "int" else f"k{i}" for k in rule.keys}
        row = dict.fromkeys(types) | keys | {column: value}
        rows.append(tuple(row[c] for c in types))
    df = spark.createDataFrame(rows, spec.schema)
    key = F.concat_ws("|", *[F.col(k).cast("string") for k in rule.keys])
    check_rejects = set()
    for r in df.select(key.alias("k")).collect():
        one = df.filter(key == r["k"])
        try:
            one.write.format("delta").mode("append").save(path)
        except Exception as e:  # noqa: BLE001 - Delta raises a JVM-wrapped error
            assert "check constraint" in str(e).lower(), e
            check_rejects.add(r["k"])
    assert None in values and check_rejects  # the probe covers NULL and a real violation
    rule_rejects = {r["record_key"] for r in quality.violations(df, rule).collect()}
    assert rule_rejects == check_rejects
    passed = {r["k"] for r in df.filter(quality.passes(rule)).select(key.alias("k")).collect()}
    assert passed == {
        r["k"] for r in spark.read.format("delta").load(path).select(key.alias("k")).collect()
    }


@pytest.mark.slow
def test_gold_batch_replayed_after_a_crash_between_events_and_fact_is_exactly_once(
    spark, tmp_path, monkeypatch
):
    """05 §3 / §5: exactly-once of the derived revenue events. The batch crashes after
    the events were committed and before the fact MERGE; silver moves on meanwhile; the
    replay reads the pinned silver versions, skips the events (txnVersion) and applies
    the same fact change, so sum(revenue_events) = sum(fact_order_items) afterwards."""
    base = f"{tmp_path}/"
    lake = Lake(base)
    settings = Settings(base_uri=base)
    sf.empty_silver(spark, base)
    ensure_gold_and_ops(spark, lake)
    sf.merge(
        spark,
        base,
        "customers",
        [{"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"}],
        10,
    )
    sf.merge(spark, base, "products", [{"product_id": "p1"}], 10)
    sf.merge(spark, base, "sellers", [{"seller_id": "s1", "seller_state": "SP"}], 10)
    sf.merge(
        spark,
        base,
        "orders",
        [
            {
                "order_id": "o1",
                "customer_id": "c1",
                "order_status": "created",
                "order_purchase_timestamp": T("2017-01-05 10:00:00"),
                "order_estimated_delivery_date": T("2017-01-20 00:00:00"),
            }
        ],
        20,
    )
    item = {
        "order_id": "o1",
        "order_item_id": 1,
        "product_id": "p1",
        "seller_id": "s1",
        "price": Decimal("10.00"),
        "freight_value": Decimal("1.00"),
    }
    sf.merge(spark, base, "order_items", [item], 30)
    batch0 = sf.cdf_since(spark, base, {}).localCheckpoint()

    job = GoldJob(spark, settings, lake)
    real_apply = delta_io.apply_diff

    def crash_on_fact(spark_, path, changes, keys):
        if path.endswith("fact_order_items"):
            raise RuntimeError("simulated crash after the events commit")
        return real_apply(spark_, path, changes, keys)

    monkeypatch.setattr(delta_io, "apply_diff", crash_on_fact)
    with pytest.raises(RuntimeError, match="simulated crash"):
        job.process(batch0, 0)
    monkeypatch.setattr(delta_io, "apply_diff", real_apply)
    ev = spark.read.format("delta").load(lake.gold("revenue_events"))
    assert ev.count() == 1  # the events of batch 0 are committed
    assert spark.read.format("delta").load(lake.gold("fact_order_items")).count() == 0

    # silver moves on before the replay: a second item arrives
    v = sf.versions(spark, base)
    sf.merge(
        spark, base, "order_items", [{**item, "order_item_id": 2, "price": Decimal("5.00")}], 40
    )
    job.process(batch0, 0)  # replay of batch 0: pinned versions -> the same diff
    fact = spark.read.format("delta").load(lake.gold("fact_order_items"))
    assert [r["order_item_id"] for r in fact.collect()] == [1]
    assert ev.count() == 1
    job.process(sf.cdf_since(spark, base, v), 1)  # the next batch brings item 2
    total_ev = ev.agg(F.sum("amount")).first()[0]
    total_fact = fact.agg(F.sum("price")).first()[0]
    assert total_ev == total_fact == Decimal("15.00")
