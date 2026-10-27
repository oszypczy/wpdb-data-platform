"""07 §7 integration rows: every page renders on the live MinIO data
(``streamlit.testing.AppTest``), the `dashboard` account's permissions through the
dashboard's own readers (delta-rs, DuckDB), and the container (healthy, also with an
empty gold). The e2e rows (KPIs = Postgres truth views, freshness thresholds) are in
the final stage (08 §10)."""

from __future__ import annotations

import json
import subprocess
import uuid
from decimal import ROUND_HALF_UP, Decimal

import duckdb
import pyarrow as pa
import pytest
import requests

from dashboard import queries as q
from dashboard.lake import Lake, LakeConfig, VersionedReader
from tests.helpers.dashboard_app import PAGES, problems, render, texts
from tests.helpers.lake import Lake as AgentLake
from tests.helpers.lake import duck, storage_options
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration

ENDPOINT = "http://127.0.0.1:9000"


def _config(settings) -> LakeConfig:
    return LakeConfig(
        root="s3://",
        endpoint=ENDPOINT,
        access_key=settings["MINIO_DASHBOARD_ACCESS_KEY"],
        secret_key=settings["MINIO_DASHBOARD_SECRET_KEY"],
    )


@pytest.fixture(scope="module")
def gold_ready(stack, settings):
    """Gold has orders (on a fresh stack Spark needs about a minute after healthy)."""
    lake = Lake(_config(settings))
    wait_until(
        lambda: (
            (lake.version("gold/fact_orders_lifecycle") is not None)
            and lake.read("gold/fact_orders_lifecycle").num_rows > 0
        ),
        timeout=300,
        every=5,
        desc="orders in gold/fact_orders_lifecycle",
    )
    return lake


@pytest.fixture
def dashboard_env(monkeypatch, settings):
    monkeypatch.setenv("S3_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("MINIO_DASHBOARD_ACCESS_KEY", settings["MINIO_DASHBOARD_ACCESS_KEY"])
    monkeypatch.setenv("MINIO_DASHBOARD_SECRET_KEY", settings["MINIO_DASHBOARD_SECRET_KEY"])
    monkeypatch.delenv("DASHBOARD_LAKE_ROOT", raising=False)


# ------------------------------------------------------------------ pages on MinIO
@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders_on_the_live_minio_data(page, gold_ready, dashboard_env):
    """07 §7: "wszystkie strony w streamlit.testing.AppTest na danych w MinIO" → każda
    strona renderuje się bez wyjątku (and shows data, not the waiting message)."""
    at = render(page)
    assert problems(at) == []
    assert "Czekam na dane" not in texts(at), page


def test_query_layer_on_live_gold_equals_duckdb_delta_scan(gold_ready, settings):
    """The KPIs of the query layer (delta-rs → Arrow → DuckDB) equal plain SQL over
    ``delta_scan`` of the same tables (DuckDB delta extension, read-only `agent`)."""
    reader = VersionedReader(gold_ready)
    agent = AgentLake(settings, "agent")

    def both():
        vs = reader.versions(["gold/fact_order_items", "gold/fact_orders_lifecycle"])
        k = reader.derive(
            "kpis",
            ["gold/fact_order_items", "gold/fact_orders_lifecycle", "gold/agg_revenue_hourly"],
            q.kpis,
        )
        ref = agent.sql(
            """SELECT (SELECT coalesce(sum(price), 0) FROM delta_scan('s3://gold/fact_order_items')),
                      (SELECT count(*) FROM delta_scan('s3://gold/fact_orders_lifecycle')),
                      (SELECT count(*) FILTER (WHERE is_late)
                       FROM delta_scan('s3://gold/fact_orders_lifecycle'))"""
        )[0]
        same_versions = vs == reader.versions(list(vs))
        return same_versions and (k, ref)

    # a Spark commit between the two reads would make them differ: retry until stable
    k, ref = wait_until(lambda: (lambda r: r and r[0].revenue == r[1][0] and r)(both()),
                        timeout=120, every=2, desc="a stable pair of reads")  # fmt: skip
    assert (k.revenue, k.orders, k.late) == (Decimal(ref[0]), ref[1], ref[2])
    assert k.gross_sales + k.corrections == k.revenue


def _payments_reviews_of_the_query_layer(reader: VersionedReader) -> dict[str, dict]:
    """The page's queries on live gold, shaped like the truth views (BRL, before the
    USD conversion of the view layer)."""
    pay, rev = "gold/fact_payments", "gold/fact_reviews"

    def money(v: float) -> Decimal:
        return Decimal(f"{v:.2f}")

    dist = reader.derive("score_distribution", [rev], q.score_distribution, None)
    by = reader.derive("reviews_by_delivery", [rev], q.reviews_by_delivery, None)
    types = reader.derive("payment_types", [pay], q.payment_types, None)
    inst = reader.derive("payment_installments", [pay], q.payment_installments, None)
    k = reader.derive("payments_reviews_kpis", [pay, rev], q.payments_reviews_kpis, None)
    return {
        "v_truth_reviews": {
            int(s): (int(n), int(o))
            for s, n, o in dist[["review_score", "reviews", "orders"]].values
            if n > 0
        },
        "v_truth_reviews_by_delivery": {
            bool(t): (int(n), Decimal(f"{a:.6f}").quantize(Decimal("0.01"), ROUND_HALF_UP))
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


def _payments_reviews_truth(pg) -> dict[str, dict]:
    out = {}
    for view in ("v_truth_reviews", "v_truth_reviews_by_delivery", "v_truth_payments",
                 "v_truth_payment_installments"):  # fmt: skip
        rows = pg.execute(f"SELECT * FROM sim.{view}").fetchall()
        out[view] = {k: (int(a), b if isinstance(b, Decimal) else int(b)) for k, a, b in rows}
    pays = out["v_truth_payments"].values()
    out["kpis"] = {
        "payments": sum(n for n, _ in pays),
        "value": sum((v for _, v in pays), Decimal("0.00")),
        "reviews": sum(n for n, _ in out["v_truth_reviews"].values()),
    }
    return out


def test_payments_reviews_queries_on_live_gold_equal_the_truth_views(gold_ready, pg_reader):
    """07 §7 "Płatności i opinie": the page's queries on live gold (account `dashboard`)
    equal the Postgres truth views: reviews and distinct orders per score, reviews and
    average score (rounded like ``numeric(4,2)``) of delivered orders on time vs late,
    payments and BRL value per type and per number of installments, and the KPI totals.
    Both sides are re-read every round, so a running simulator only delays equality."""
    reader = VersionedReader(gold_ready)
    state: dict[str, dict] = {}

    def equal() -> bool:
        state["truth"] = _payments_reviews_truth(pg_reader)
        state["gold"] = _payments_reviews_of_the_query_layer(reader)
        return state["gold"] == state["truth"]

    try:
        wait_until(equal, 300, every=5, desc="dashboard payments/reviews = sim.v_truth_*")
    except TimeoutError:
        diff = {v: (state["gold"][v], state["truth"][v]) for v in state["truth"]
                if state["gold"][v] != state["truth"][v]}  # fmt: skip
        pytest.fail(f"query layer != truth after 300 s (gold, truth): {diff}")
    assert state["truth"]["kpis"]["reviews"] > 0 and state["truth"]["kpis"]["payments"] > 0
    assert set(state["truth"]["v_truth_reviews_by_delivery"]) == {True, False}


# ------------------------------------------------------------------- permissions
def test_dashboard_account_reads_gold_and_ops_but_not_bronze_or_silver(gold_ready, settings):
    """07 §7: "uprawnienia" → konto `dashboard` czyta `gold` i `ops`, nie ma dostępu do
    `bronze`/`silver` (through delta-rs and DuckDB, the readers the dashboard uses),
    and cannot write anywhere."""
    opts = storage_options(settings, "dashboard")
    from deltalake import DeltaTable, write_deltalake

    for uri in ("s3://gold/fact_order_items", "s3://gold/dim_customer", "s3://ops/cdc_heartbeat"):
        assert DeltaTable(uri, storage_options=opts).version() >= 0, uri
    # SCD2 valid_to = 9999-12-31 is stored as Parquet INT96 by Spark: readable (07 §5)
    dc = gold_ready.read("gold/dim_customer")
    assert dc.num_rows > 0
    assert max(v.year for v in dc.column("valid_to").to_pylist()) == 9999

    for uri in ("s3://silver/orders", "s3://bronze/orders"):
        with pytest.raises(Exception) as exc:  # noqa: B017 - delta-rs wraps the S3 error
            DeltaTable(uri, storage_options=opts).version()
        assert "403" in str(exc.value) or "Access Denied" in str(exc.value), str(exc.value)

    con = duck(settings, "dashboard")
    assert con.execute("SELECT count(*) FROM delta_scan('s3://gold/dim_geography')").fetchone() == (
        27,
    )
    for uri in ("s3://silver/orders", "s3://bronze/orders"):
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT count(*) FROM delta_scan('{uri}')").fetchone()

    target = f"s3://gold/_it_dash_{uuid.uuid4().hex[:8]}"
    with pytest.raises(Exception) as exc:  # noqa: B017
        write_deltalake(target, pa.table({"x": [1]}), storage_options=opts)
    assert "403" in str(exc.value) or "Access Denied" in str(exc.value), str(exc.value)


# ---------------------------------------------------------------------- container
def test_dashboard_container_is_healthy_and_serves_the_app(stack):
    """07 §7: "kontener" → healthcheck zdrowy po starcie; port 8501 only on 127.0.0.1;
    no Postgres, Kafka or Spark settings in its environment."""
    state = {r.service: r for r in stack.ps()}["dashboard"]
    assert (state.state, state.health) == ("running", "healthy")
    assert requests.get("http://127.0.0.1:8501/_stcore/health", timeout=5).text == "ok"
    assert "<title>Streamlit</title>" in requests.get("http://127.0.0.1:8501/", timeout=5).text
    names = {line.split("=", 1)[0] for line in stack.exec("dashboard", "env").stdout.splitlines()}
    banned = ("PG_", "POSTGRES", "KAFKA", "SCHEMA_REGISTRY", "MINIO_SPARK", "MINIO_ROOT")
    assert not [n for n in names if n.startswith(banned)], names
    assert {"S3_ENDPOINT", "MINIO_DASHBOARD_ACCESS_KEY"} <= names
    assert stack.exec("dashboard", "python", "/app/healthcheck.py").returncode == 0


# Renders every page with AppTest inside a container of the image (its own dependencies,
# its own environment) and prints {page: [problems, shows "czekam na dane"]} as JSON.
IN_CONTAINER_APPTEST = """
import json
from streamlit.testing.v1 import AppTest
out = {}
for page in %s:
    at = AppTest.from_file("/app/app.py", default_timeout=120)
    at.run()
    at.switch_page(f"views/{page}.py").run()
    problems = [e.value for e in at.exception] + [e.value for e in at.error]
    waiting = any("Czekam na dane" in e.value for e in at.info)
    out[page] = [problems, waiting]
print(json.dumps(out))
"""


def test_every_page_renders_inside_the_container_image(stack, gold_ready):
    """The pages run with the image's own dependencies (no host-only packages such as
    pytz) against minio:9000 inside the compose network."""
    script = IN_CONTAINER_APPTEST % json.dumps(PAGES)
    res = stack.exec("dashboard", "python", "-c", script)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    assert out == {p: [[], False] for p in PAGES}


@pytest.mark.slow
def test_dashboard_container_is_healthy_with_an_empty_gold(stack):
    """07 §7: "kontener" → zdrowy także przy pustym gold: a second container of the same
    image whose lake has no tables yet (as right after `make reset && make up`) becomes
    healthy and every page shows "czekam na dane" instead of an error."""
    name = f"dashboard-empty-{uuid.uuid4().hex[:6]}"
    stack.run(
        "run", "-d", "--no-deps", "--name", name,
        "-e", "DASHBOARD_LAKE_ROOT=/tmp/empty-lake", "dashboard",
    )  # fmt: skip
    try:

        def health() -> bool:
            out = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Health.Status}}", name],
                capture_output=True, text=True, check=True,
            ).stdout.strip()  # fmt: skip
            assert out != "unhealthy", stack.docker("logs", name).stdout
            return out == "healthy"

        wait_until(health, timeout=120, every=2, desc=f"{name} healthy")
        script = IN_CONTAINER_APPTEST % json.dumps(PAGES)
        res = stack.docker("exec", name, "python", "-c", script)
        out = json.loads(res.stdout.strip().splitlines()[-1])
        assert out == {p: [[], True] for p in PAGES}
    finally:
        stack.docker("rm", "-f", name, check=False)
