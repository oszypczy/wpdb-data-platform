"""07 §7 unit rows on the query layer: KPIs, correction after a cancellation, funnel,
empty tables, plus the other page queries. The inputs are small Delta tables written with
``deltalake`` (tests/helpers/dashboard_fixtures.py) and read back through the dashboard's
own reader, exactly as the app reads MinIO."""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from dashboard import contract
from dashboard import queries as q
from dashboard.lake import Lake, LakeConfig
from tests.helpers import dashboard_fixtures as fx

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def no_pytz(monkeypatch):
    """The image has no pytz: a TIMESTAMPTZ leaving DuckDB would fail there (07 §5)."""
    monkeypatch.setitem(sys.modules, "pytz", None)


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = fx.write_small_lake(tmp_path_factory.mktemp("lake"))
    reader = Lake(LakeConfig(root=str(root)))
    return {t: reader.read(t) for t in contract.SCHEMAS}


@pytest.fixture(scope="module")
def empty_lake(tmp_path_factory):
    """No table exists yet (fresh stack): the reader returns empty contract tables."""
    reader = Lake(LakeConfig(root=str(tmp_path_factory.mktemp("empty"))))
    return {t: reader.read(t) for t in contract.SCHEMAS}


def test_kpis_on_small_delta_tables(lake):
    """07 §7: "KPI na małych tabelach Delta" → poprawny przychód, liczba zamówień, średni
    koszyk, % spóźnień, mediana dostawy."""
    k = q.kpis(lake["gold/fact_order_items"], lake["gold/fact_orders_lifecycle"],
               lake["gold/agg_revenue_hourly"])  # fmt: skip
    for name, expected in fx.EXPECTED.items():
        assert getattr(k, name) == expected, name
    assert isinstance(k.revenue, Decimal) and isinstance(k.avg_basket, Decimal)
    assert not k.is_empty


def test_negative_correction_reduces_revenue(lake):
    """07 §7: "przychód z korektą z anulacji" → ujemna korekta pomniejsza przychód.

    o3 sold 40.00 and was canceled: revenue events, the hourly aggregate and the KPI all
    give 220.00 sales - 40.00 correction = 180.00 net (= the items that still exist)."""
    b = q.revenue_breakdown(lake["gold/revenue_events"])
    assert (b.sales, b.corrections, b.adjustments, b.net) == (
        Decimal("220.00"), Decimal("-40.00"), Decimal("0.00"), Decimal("180.00"))  # fmt: skip

    hourly = q.revenue_timeseries(lake["gold/agg_revenue_hourly"], "hour")
    cancel = hourly[hourly["period"] == pd.Timestamp("2017-01-03 10:00")].iloc[0]
    assert (cancel["sales"], cancel["corrections"], cancel["revenue"]) == (0.0, -40.0, -40.0)
    assert hourly["revenue"].sum() == 180.0
    # every hour from the first to the last window, empty hours as zeros (no bridged gaps)
    assert len(hourly) == 49 and hourly["period"].diff().dropna().nunique() == 1
    assert (hourly.set_index("period").loc["2017-01-02 00:00", ["sales", "revenue"]] == 0).all()
    assert (hourly["sales"] + hourly["corrections"] == hourly["revenue"]).all()

    daily = q.revenue_timeseries(lake["gold/agg_revenue_hourly"], "day")
    assert daily.set_index("period")["revenue"].to_dict() == {
        pd.Timestamp("2017-01-01"): 150.0,
        pd.Timestamp("2017-01-02"): 30.0,
        pd.Timestamp("2017-01-03"): 0.0,  # 40.00 sold and corrected the same day
    }
    sp = q.revenue_timeseries(lake["gold/agg_revenue_hourly"], "day", ("SP",))
    assert sp["revenue"].sum() == 150.0 and sp["corrections"].sum() == -40.0

    k = q.kpis(lake["gold/fact_order_items"], lake["gold/fact_orders_lifecycle"],
               lake["gold/agg_revenue_hourly"])  # fmt: skip
    assert k.gross_sales + k.corrections == k.revenue == b.net


def test_funnel_counts_match_the_input(lake):
    """07 §7: "lejek" → liczby przejść zgodne z danymi wejściowymi."""
    f = lake["gold/agg_status_funnel"]
    pairs = q.funnel_transitions(f).set_index("transition")["transitions"].to_dict()
    assert pairs == {
        "(new) → created": 4,
        "created → approved": 4,
        "approved → shipped": 2,
        "shipped → delivered": 2,
        "approved → canceled": 1,
    }
    stages = q.funnel_stages(f)
    assert stages["stage"].tolist() == list(q.FUNNEL_PATH + q.FUNNEL_EXITS)
    assert stages.set_index("stage")["entered"].to_dict() == {
        "created": 4, "approved": 4, "shipped": 2, "delivered": 2,
        "canceled": 1, "unavailable": 0, "processing": 0, "invoiced": 0,
    }  # fmt: skip
    daily = q.funnel_timeseries(f, "day", ("canceled", "delivered"))
    assert daily.to_dict("records") == [
        {"period": pd.Timestamp("2017-01-03"), "transition": "approved → canceled",
         "transitions": 1},
        {"period": pd.Timestamp("2017-01-05"), "transition": "shipped → delivered",
         "transitions": 1},
        {"period": pd.Timestamp("2017-01-10"), "transition": "shipped → delivered",
         "transitions": 1},
    ]  # fmt: skip
    assert q.funnel_timeseries(f, "week")["transitions"].sum() == 13


def test_state_metrics_cover_every_state_of_the_dimension(lake):
    df = q.state_metrics(lake["gold/agg_revenue_hourly"], lake["gold/fact_orders_lifecycle"],
                         lake["gold/dim_geography"]).set_index("state")  # fmt: skip
    assert sorted(df.index) == ["AC", "MG", "RJ", "SP"]
    assert df.loc["SP", ["revenue", "corrections", "orders", "delivered", "late"]].tolist() == [
        150.0, -40.0, 2, 1, 0]  # fmt: skip
    assert df.loc["RJ", "late_pct"] == 100.0 and df.loc["SP", "late_pct"] == 0.0
    assert df.loc["AC", "orders"] == 0 and pd.isna(df.loc["AC", "late_pct"])
    assert df.loc["MG", "state_name"] == "Minas Gerais"


def test_logistics_queries(lake):
    lc = lake["gold/fact_orders_lifecycle"]
    s = q.logistics_summary(lc).iloc[0]
    assert (s["delivered"], s["late"], s["late_pct"], s["median_d_to_deliver"]) == (2, 1, 50.0, 6.0)
    assert s["median_d_to_ship"] == 1.5 and s["median_h_to_approve"] == 0.75
    dt = q.delivery_times(lc)
    assert dt[["order_id", "d_to_deliver", "is_late"]].values.tolist() == [
        ["o1", 4.0, False], ["o2", 8.0, True]]  # fmt: skip
    late = q.late_by_state(lc)
    assert late[["state", "delivered", "late", "late_pct"]].values.tolist() == [
        ["RJ", 1, 1, 100.0], ["SP", 1, 0, 0.0]]  # fmt: skip
    assert q.approval_times(lc)["h_to_approve"].tolist() == [0.5, 2.0, 0.25, 1.0]


def test_top_categories_use_english_names(lake):
    top = q.top_categories(lake["gold/fact_order_items"], lake["gold/dim_product"], 15)
    assert top.values.tolist() == [
        ["bed_bath_table", 130.0, 13.0, 2, 2],
        ["health_beauty", 50.0, 5.0, 1, 1],
    ]
    assert len(q.top_categories(lake["gold/fact_order_items"], lake["gold/dim_product"], 1)) == 1


def test_customers_returning_and_moves_from_scd2(lake):
    s = q.customer_summary(lake["gold/dim_customer"], lake["gold/fact_orders_lifecycle"])
    assert s.iloc[0].to_dict() == {"customers": 3, "returning": 1, "movers": 1, "versions": 4}
    assert q.orders_per_customer(lake["gold/fact_orders_lifecycle"]).values.tolist() == [
        [1, 2], [2, 1]]  # fmt: skip
    moves = q.customer_moves(lake["gold/dim_customer"])
    assert moves[["customer_unique_id", "version", "city", "is_current"]].values.tolist() == [
        ["u1", 1, "sao paulo", False], ["u1", 2, "indaiatuba", True]]  # fmt: skip
    assert moves["valid_to"].iloc[1] == pd.Timestamp("9999-12-31")


def test_pipeline_health_queries(lake):
    fresh = q.cdc_freshness(lake["ops/cdc_heartbeat"], datetime(2026, 10, 24, 10, 1, 10)).iloc[0]
    assert fresh["last_heartbeat"] == pd.Timestamp("2026-10-24 10:00:10")
    assert (fresh["freshness_s"], fresh["ingest_age_s"], fresh["heartbeats"]) == (60.0, 55.0, 2)

    lat = q.latency_summary(lake["ops/e2e_latency"], 60).set_index("layer")
    # silver: the 07:00 batch is outside the last hour of measurements
    assert lat.loc["silver", ["batches", "p50_s", "p95_s", "max_s"]].tolist() == [
        2,
        11.0,
        21.0,
        30.0,
    ]
    assert lat.loc["gold", "p95_s"] == 50.0
    ts = q.latency_timeseries(lake["ops/e2e_latency"], datetime(2026, 10, 24, 8, 0))
    assert len(ts) == 3 and ts["p95_s"].max() == 50.0

    sm = q.stream_summary(lake["ops/stream_metrics"], 2).iloc[0]
    assert (sm["query_name"], sm["last_batch"], sm["last_duration_s"], sm["rows"]) == (
        "kafka_to_bronze", 3, 3.0, 50)  # fmt: skip
    assert sm["median_duration_s"] == 2.5
    assert q.stream_timeseries(lake["ops/stream_metrics"])["duration_s"].tolist() == [1, 2, 3]

    qr = q.quarantine_by_rule(lake["ops/quarantine"])
    assert qr[["rule", "severity", "rows"]].values.tolist() == [
        ["price_nonneg", "reject", 2], ["payment_type_defined", "warn", 1]]  # fmt: skip
    assert len(q.maintenance_recent(lake["ops/maintenance_log"])) == 1


def test_compare_versions_and_history_frame(lake):
    items = lake["gold/fact_order_items"]
    cmp = q.compare_versions("gold/fact_order_items", items, items.slice(0, 1),
                             ("price", "freight_value"), 7)  # fmt: skip
    assert cmp.values.tolist() == [
        ["rows", 3.0, 1.0, 2.0],
        ["sum(price)", 180.0, 100.0, 80.0],
        ["sum(freight_value)", 18.0, 10.0, 8.0],
    ]
    assert list(cmp.columns) == ["metric", "now", "version 7", "diff"]
    h = q.history_frame([
        {"version": 0, "timestamp": 1_700_000_000_000, "operation": "CREATE TABLE"},
        {"version": 1, "timestamp": 1_700_000_010_000, "operation": "MERGE",
         "operationMetrics": {"numTargetRowsInserted": "3", "numTargetRowsUpdated": 1}},
    ])  # fmt: skip
    assert h["version"].tolist() == [1, 0]
    assert h.iloc[0]["numTargetRowsInserted"] == 3.0 and pd.isna(h.iloc[1]["numOutputRows"])


def test_every_query_on_empty_tables_returns_empty_results(empty_lake):
    """07 §7: "puste tabele" → puste wyniki, brak wyjątków (fresh stack, gold empty)."""
    e = empty_lake
    assert all(t.num_rows == 0 for t in e.values())
    k = q.kpis(e["gold/fact_order_items"], e["gold/fact_orders_lifecycle"],
               e["gold/agg_revenue_hourly"])  # fmt: skip
    assert k.is_empty and k.revenue == Decimal("0.00") and k.orders == 0
    assert (k.avg_basket, k.late_pct, k.median_delivery_days, k.sim_clock) == (None,) * 4
    assert q.revenue_breakdown(e["gold/revenue_events"]).net == 0

    frames = {
        "revenue_timeseries": q.revenue_timeseries(e["gold/agg_revenue_hourly"], "day"),
        "state_metrics": q.state_metrics(e["gold/agg_revenue_hourly"],
                                         e["gold/fact_orders_lifecycle"],
                                         e["gold/dim_geography"]),
        "funnel_transitions": q.funnel_transitions(e["gold/agg_status_funnel"]),
        "funnel_timeseries": q.funnel_timeseries(e["gold/agg_status_funnel"], "day",
                                                 ("canceled",)),
        "delivery_times": q.delivery_times(e["gold/fact_orders_lifecycle"]),
        "late_by_state": q.late_by_state(e["gold/fact_orders_lifecycle"]),
        "approval_times": q.approval_times(e["gold/fact_orders_lifecycle"]),
        "top_categories": q.top_categories(e["gold/fact_order_items"], e["gold/dim_product"]),
        "orders_per_customer": q.orders_per_customer(e["gold/fact_orders_lifecycle"]),
        "customer_moves": q.customer_moves(e["gold/dim_customer"]),
        "latency_timeseries": q.latency_timeseries(e["ops/e2e_latency"]),
        "latency_summary": q.latency_summary(e["ops/e2e_latency"]),
        "stream_summary": q.stream_summary(e["ops/stream_metrics"]),
        "stream_timeseries": q.stream_timeseries(e["ops/stream_metrics"]),
        "quarantine_by_rule": q.quarantine_by_rule(e["ops/quarantine"]),
        "maintenance_recent": q.maintenance_recent(e["ops/maintenance_log"]),
    }  # fmt: skip
    for name, df in frames.items():
        assert df.empty and len(df.columns) > 0, name

    assert q.funnel_stages(e["gold/agg_status_funnel"])["entered"].sum() == 0
    assert q.logistics_summary(e["gold/fact_orders_lifecycle"]).iloc[0]["delivered"] == 0
    assert q.customer_summary(e["gold/dim_customer"], e["gold/fact_orders_lifecycle"]).iloc[0][
        "customers"] == 0  # fmt: skip
    fresh = q.cdc_freshness(e["ops/cdc_heartbeat"], datetime(2026, 1, 1)).iloc[0]
    assert fresh["heartbeats"] == 0 and fresh["freshness_s"] is None
    cmp = q.compare_versions("gold/fact_order_items", e["gold/fact_order_items"],
                             e["gold/fact_order_items"], ("price",), 0)  # fmt: skip
    assert cmp["now"].tolist() == [0.0, 0.0]
    assert q.history_frame([]).empty


def test_widget_values_are_quoted_in_sql(lake):
    """Filter values from widgets end up in SQL literals: quotes cannot break out."""
    df = q.revenue_timeseries(lake["gold/agg_revenue_hourly"], "day", ("SP' OR '1'='1",))
    assert df.empty
    with pytest.raises(ValueError):
        q.revenue_timeseries(lake["gold/agg_revenue_hourly"], "minute; DROP")
