"""05 §10 unit rows on gold/fact_payments and gold/fact_reviews: grain, deletes, order
attributes that change later, a review before its order, replay idempotence and the
backfill after the gold sources change (local SparkSession, Delta on the local fs)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from delta.tables import DeltaTable

from lakehouse import delta_io, gold, quality
from lakehouse.config import GOLD_SOURCES, Lake, Settings
from lakehouse.jobs import SOURCES_MARKER, GoldJob, ensure_gold_and_ops, reset_on_new_sources
from tests.helpers import silver_fixtures as sf
from tests.helpers.silver_fixtures import T

pytestmark = pytest.mark.unit


def _df(spark, table, rows, op="c"):
    return sf.rows_df(spark, table, rows, 100, op)


ORDER = {
    "order_id": "o1",
    "customer_id": "c1",
    "order_status": "shipped",
    "order_purchase_timestamp": T("2017-01-05 10:00:00"),
    "order_approved_at": T("2017-01-05 10:20:00"),
    "order_delivered_carrier_date": T("2017-01-07 09:00:00"),
    "order_estimated_delivery_date": T("2017-01-20 00:00:00"),
}


def _lifecycle(spark, orders, customers):
    orders = _df(spark, "orders", orders)
    customers = _df(spark, "customers", customers)
    dim = gold.customer_scd2(customers, orders)
    empty_items = _df(spark, "order_items", [])
    empty_pay = _df(spark, "order_payments", [])
    return gold.fact_orders_lifecycle(orders, customers, dim, empty_items, empty_pay)


# --------------------------------------------------------------------- pure functions
def test_fact_payments_grain_is_one_row_per_payment_with_order_attributes(spark):
    """Grain (order_id, payment_sequential); a payment waits for its order; the warn
    rules (value 0, type not_defined) report the row and keep it in gold (05 §6)."""
    lc = _lifecycle(
        spark, [ORDER], [{"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"}]
    )
    pays = _df(
        spark,
        "order_payments",
        [
            {
                "order_id": "o1",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_installments": 3,
                "payment_value": Decimal("30.00"),
            },
            {
                "order_id": "o1",
                "payment_sequential": 2,
                "payment_type": "not_defined",
                "payment_installments": 1,
                "payment_value": Decimal("0.00"),
            },
            {  # its order is not in silver yet
                "order_id": "o9",
                "payment_sequential": 1,
                "payment_type": "boleto",
                "payment_installments": 1,
                "payment_value": Decimal("5.00"),
            },
        ],
    )
    rows = sorted(gold.fact_payments(pays, lc).collect(), key=lambda r: r["payment_sequential"])
    assert [
        (r["order_id"], r["payment_sequential"], r["payment_type"], r["payment_installments"])
        for r in rows
    ] == [("o1", 1, "credit_card", 3), ("o1", 2, "not_defined", 1)]
    r = rows[0]
    assert (r["payment_value"], r["customer_state"], r["order_status"]) == (
        Decimal("30.00"),
        "SP",
        "shipped",
    )
    assert (r["purchased_at"], r["purchase_date_key"]) == (T("2017-01-05 10:00:00"), 20170105)
    v = {(x["rule"], x["severity"]) for x in quality.check_payments(pays).collect()}
    assert v == {("payment_value_positive", "warn"), ("payment_type_defined", "warn")}


def test_fact_reviews_grain_comment_flag_and_delivery_attributes_of_the_lifecycle(spark):
    """Grain (review_id, order_id): one review_id on two orders gives two rows;
    has_comment = title or message non-blank; is_late / is_delivered / d_to_deliver
    are exactly the values of fact_orders_lifecycle."""
    late = {
        **ORDER,
        "order_status": "delivered",
        "order_delivered_customer_date": T("2017-01-21 15:00:00"),
    }
    on_time = {
        **late,
        "order_id": "o2",
        "customer_id": "c2",
        "order_delivered_customer_date": T("2017-01-19 15:00:00"),
    }
    lc = _lifecycle(
        spark,
        [late, on_time],
        [
            {"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"},
            {"customer_id": "c2", "customer_unique_id": "u2", "customer_state": "RJ"},
        ],
    )
    reviews = _df(
        spark,
        "order_reviews",
        [
            {"review_id": "r1", "order_id": "o1", "review_score": 1, "review_comment_title": "x"},
            {"review_id": "r1", "order_id": "o2", "review_score": 5, "review_comment_message": " "},
            {
                "review_id": "r2",
                "order_id": "o2",
                "review_score": 4,
                "review_comment_message": "ok",
                "review_creation_date": T("2017-01-20 00:00:00"),
                "review_answer_timestamp": T("2017-01-21 10:00:00"),
            },
        ],
    )
    got = {(r["review_id"], r["order_id"]): r for r in gold.fact_reviews(reviews, lc).collect()}
    assert set(got) == {("r1", "o1"), ("r1", "o2"), ("r2", "o2")}
    assert [got[k]["has_comment"] for k in sorted(got)] == [True, False, True]
    lcr = {r["order_id"]: r for r in lc.collect()}
    for (_, o), r in got.items():
        for c in ("is_delivered", "is_late", "d_to_deliver", "customer_state", "purchased_at"):
            assert r[c] == lcr[o][c], (o, c)
    assert (got[("r1", "o1")]["is_late"], got[("r1", "o2")]["is_late"]) == (True, False)
    assert got[("r2", "o2")]["review_created_at"] == T("2017-01-20 00:00:00")
    assert got[("r2", "o2")]["review_answered_at"] == T("2017-01-21 10:00:00")


# ------------------------------------------------------------ whole gold micro-batch
def _setup(spark, tmp_path):
    base = f"{tmp_path}/"
    lake = Lake(base)
    sf.empty_silver(spark, base)
    ensure_gold_and_ops(spark, lake)
    return base, lake, GoldJob(spark, Settings(base_uri=base), lake)


def _read(spark, lake, table, keys):
    rows = spark.read.format("delta").load(lake.gold(table)).collect()
    return {tuple(r[k] for k in keys): r for r in rows}


def _version(spark, path):
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


@pytest.mark.slow
def test_payments_and_reviews_follow_silver_deletes_late_orders_and_replays(spark, tmp_path):
    """GoldJob.process over local silver: a review before its order appears when the
    order arrives; delivery after the estimate turns is_late on in the review row; a
    customer's state change reaches both facts; deleted payments/reviews disappear; a
    score outside 1..5 is quarantined; a replayed batch commits nothing new."""
    base, lake, job = _setup(spark, tmp_path)
    sf.merge(
        spark,
        base,
        "customers",
        [
            {"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"},
            {"customer_id": "c2", "customer_unique_id": "u2", "customer_state": "MG"},
        ],
        10,
    )
    sf.merge(spark, base, "orders", [ORDER], 20)
    pay = {
        "order_id": "o1",
        "payment_type": "credit_card",
        "payment_installments": 2,
        "payment_value": Decimal("20.00"),
    }
    sf.merge(
        spark,
        base,
        "order_payments",
        [{**pay, "payment_sequential": 1}, {**pay, "payment_sequential": 2}],
        30,
    )
    sf.merge(
        spark,
        base,
        "order_reviews",
        [
            {"review_id": "rA", "order_id": "o1", "review_score": 5},
            {"review_id": "rB", "order_id": "o2", "review_score": 2},  # o2 not in silver
        ],
        40,
    )
    job.process(sf.cdf_since(spark, base, {}), 0)
    fp = _read(spark, lake, "fact_payments", ["order_id", "payment_sequential"])
    assert set(fp) == {("o1", 1), ("o1", 2)}
    assert {r["order_status"] for r in fp.values()} == {"shipped"}
    fr = _read(spark, lake, "fact_reviews", ["review_id", "order_id"])
    assert set(fr) == {("rA", "o1")}
    assert (fr[("rA", "o1")]["is_delivered"], fr[("rA", "o1")]["is_late"]) == (False, False)

    # batch 1: o1 delivered one day after the estimate, payment 2 deleted, o2 arrives
    v = sf.versions(spark, base)
    delivered = {
        **ORDER,
        "order_status": "delivered",
        "order_delivered_customer_date": T("2017-01-21 12:00:00"),
    }
    sf.merge(spark, base, "orders", [delivered], 50, op="u")
    sf.merge(spark, base, "order_payments", [{**pay, "payment_sequential": 2}], 51, op="d")
    sf.merge(spark, base, "orders", [{**ORDER, "order_id": "o2", "customer_id": "c2"}], 52)
    job.process(sf.cdf_since(spark, base, v), 1)
    fp = _read(spark, lake, "fact_payments", ["order_id", "payment_sequential"])
    assert set(fp) == {("o1", 1)} and fp[("o1", 1)]["order_status"] == "delivered"
    fr = _read(spark, lake, "fact_reviews", ["review_id", "order_id"])
    assert set(fr) == {("rA", "o1"), ("rB", "o2")}
    a = fr[("rA", "o1")]
    assert (a["is_delivered"], a["is_late"], round(a["d_to_deliver"], 2)) == (True, True, 16.08)
    lc = _read(spark, lake, "fact_orders_lifecycle", ["order_id"])
    assert a["is_late"] == lc[("o1",)]["is_late"]
    assert fr[("rB", "o2")]["customer_state"] == "MG"

    # batch 2: c1 moves to RJ (customers only), rB deleted, a review with score 7
    v = sf.versions(spark, base)
    sf.merge(
        spark,
        base,
        "customers",
        [{"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "RJ"}],
        60,
        op="u",
    )
    sf.merge(
        spark,
        base,
        "order_reviews",
        [
            {"review_id": "rB", "order_id": "o2", "review_score": 2},
        ],
        61,
        op="d",
    )
    sf.merge(
        spark, base, "order_reviews", [{"review_id": "rC", "order_id": "o2", "review_score": 7}], 62
    )
    batch2 = sf.cdf_since(spark, base, v).localCheckpoint()
    job.process(batch2, 2)
    fp = _read(spark, lake, "fact_payments", ["order_id", "payment_sequential"])
    fr = _read(spark, lake, "fact_reviews", ["review_id", "order_id"])
    assert fp[("o1", 1)]["customer_state"] == "RJ"
    assert set(fr) == {("rA", "o1")} and fr[("rA", "o1")]["customer_state"] == "RJ"
    q = {
        (r["rule"], r["record_key"], r["severity"])
        for r in spark.read.format("delta").load(lake.ops("quarantine")).collect()
    }
    assert ("review_score_range", "rC|o2", "reject") in q

    # replay of batch 2 (pinned silver versions): same state, no new commits
    before = {t: _version(spark, lake.gold(t)) for t in ("fact_payments", "fact_reviews")}
    job.process(batch2, 2)
    after = {t: _version(spark, lake.gold(t)) for t in ("fact_payments", "fact_reviews")}
    assert after == before
    assert set(_read(spark, lake, "fact_reviews", ["review_id", "order_id"])) == {("rA", "o1")}


def test_review_score_rule_matches_the_check_constraint(spark, tmp_path):
    """06 §6: CHECK score_range on gold/fact_reviews rejects a score outside 1..5 and a
    NULL score (Delta fails a CHECK that evaluates to NULL); the reject rule
    review_score_range keeps exactly those rows out of gold, so the CHECK never fires."""
    from lakehouse.tables import GOLD

    spec = GOLD["fact_reviews"]
    path = f"{tmp_path}/fact_reviews"
    delta_io.ensure_table(spark, path, spec.schema, constraints=spec.constraints)
    cols = [f.name for f in spec.schema]
    for score in (0, 6, None):
        row = dict.fromkeys(cols) | {"review_id": "r", "order_id": "o", "review_score": score}
        df = spark.createDataFrame([tuple(row[c] for c in cols)], spec.schema)
        with pytest.raises(Exception, match="(?i)check constraint"):
            df.write.format("delta").mode("append").save(path)
    reviews = _df(
        spark,
        "order_reviews",
        [{"review_id": f"r{s}", "order_id": "o", "review_score": s} for s in (0, 1, 5, 6, None)],
    )
    valid, v = quality.check_reviews(reviews)
    assert sorted(r["review_score"] for r in valid.collect()) == [1, 5]
    assert sorted(r["record_key"] for r in v.collect()) == ["r0|o", "r6|o", "rNone|o"]


# --------------------------------------------------------------------------- backfill
@pytest.mark.slow
def test_new_gold_source_clears_the_checkpoint_and_backfills_from_silver(spark, tmp_path):
    """Adding order_reviews to GOLD_SOURCES: the old checkpoint (7 sources) is cleared,
    the fresh query starts from a snapshot of silver, so fact_payments/fact_reviews get
    every existing row, not only new changes; the next start keeps the checkpoint."""
    base, lake, _ = _setup(spark, tmp_path)
    ckpt = lake.checkpoint("silver_to_gold")
    old = ",".join(t for t in GOLD_SOURCES if t != "order_reviews")
    delta_io.write_text(spark, f"{ckpt}/offsets/41", "v1")
    delta_io.write_text(spark, f"{ckpt}/{SOURCES_MARKER}", old)
    assert reset_on_new_sources(spark, ckpt, GOLD_SOURCES) is True
    assert delta_io.list_dirs(spark, ckpt) == []
    assert delta_io.read_text(spark, f"{ckpt}/{SOURCES_MARKER}") == ",".join(GOLD_SOURCES)
    assert reset_on_new_sources(spark, ckpt, GOLD_SOURCES) is False
    # a checkpoint of a run before the marker existed (offsets, no marker) is cleared too
    delta_io.delete_path(spark, ckpt)
    delta_io.write_text(spark, f"{ckpt}/offsets/7", "v1")
    job = GoldJob(spark, Settings(base_uri=base), lake)
    assert job.latency_from is not None
    assert "offsets" not in delta_io.list_dirs(spark, ckpt)

    # silver that existed before gold knew about reviews: the snapshot batch fills both
    sf.merge(
        spark,
        base,
        "customers",
        [{"customer_id": f"c{i}", "customer_unique_id": f"u{i}", "customer_state": "SP"}
         for i in range(3)],
        10,
    )  # fmt: skip
    sf.merge(
        spark,
        base,
        "orders",
        [{**ORDER, "order_id": f"o{i}", "customer_id": f"c{i}"} for i in range(3)],
        20,
    )
    sf.merge(
        spark,
        base,
        "order_payments",
        [
            {"order_id": f"o{i}", "payment_sequential": 1, "payment_value": Decimal("9.00")}
            for i in range(3)
        ],
        30,
    )
    sf.merge(
        spark,
        base,
        "order_reviews",
        [{"review_id": f"r{i}", "order_id": f"o{i}", "review_score": 3} for i in range(3)],
        40,
    )
    job.process(sf.cdf_since(spark, base, {}), 0)
    assert spark.read.format("delta").load(lake.gold("fact_payments")).count() == 3
    assert spark.read.format("delta").load(lake.gold("fact_reviews")).count() == 3
