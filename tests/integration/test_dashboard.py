"""07 §7 integration rows: every page renders on the live MinIO data
(``streamlit.testing.AppTest``), the `dashboard` account's permissions through the
dashboard's own readers (delta-rs, DuckDB), and the container (healthy, also with an
empty gold). The e2e rows (KPIs = Postgres truth views, freshness thresholds) are in
the final stage (08 §10)."""

from __future__ import annotations

import json
import re
import subprocess
import uuid
from datetime import datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest
import requests

from dashboard import queries as q
from dashboard.lake import Lake, LakeConfig, VersionedReader
from tests.helpers import kpis
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


def test_query_layer_on_live_gold_equals_duckdb_delta_scan(gold_ready, settings, paused_simulator):
    """The KPIs of the query layer (delta-rs → Arrow → DuckDB) equal plain SQL over
    ``delta_scan`` of the same tables (DuckDB delta extension, read-only `agent`), and
    gross sales + corrections of ``agg_revenue_hourly`` = net revenue of the fact. The
    aggregate is written by another query one trigger after the fact, so with purchases
    every second the two never agree at one moment (failed 2026-10-27 on a fresh stack):
    the simulator is paused (fixture ``paused_simulator``) until gold has caught up."""
    reader = VersionedReader(gold_ready)
    agent = AgentLake(settings, "agent")

    def both():
        vs = reader.versions(
            ["gold/fact_order_items", "gold/fact_orders_lifecycle", "gold/agg_revenue_hourly"]
        )
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
    def caught_up(r):
        return r and r[0].revenue == r[1][0] and r[0].gross_sales + r[0].corrections == r[0].revenue

    k, ref = wait_until(
        lambda: (lambda r: caught_up(r) and r)(both()),
        timeout=180,
        every=2,
        desc="a stable pair of reads, gold caught up",
    )
    assert (k.revenue, k.orders, k.late) == (Decimal(ref[0]), ref[1], ref[2])
    assert k.gross_sales + k.corrections == k.revenue


def test_payments_reviews_queries_on_live_gold_equal_the_truth_views(
    gold_ready, pg_reader, paused_simulator
):
    """07 §7 "Płatności i opinie": the page's queries on live gold (account `dashboard`)
    equal the Postgres truth views: reviews and distinct orders per score, reviews and
    average score (rounded like ``numeric(4,2)``) of delivered orders on time vs late,
    payments and BRL value per type and per number of installments, and the KPI totals.
    The simulator is paused meanwhile (fixture `paused_simulator`): gold trails Postgres
    by ~45 s, so with purchases every few seconds the two sides rarely meet."""
    reader = VersionedReader(gold_ready)
    state: dict[str, dict] = {}

    def equal() -> bool:
        state["truth"] = kpis.payments_reviews_truth(pg_reader)
        state["gold"] = kpis.payments_reviews_of_the_query_layer(reader)
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


# Every page inside the container with MinIO down: {page: [exceptions, errors]}.
IN_CONTAINER_ERRORS = """
import json
from streamlit.testing.v1 import AppTest
out = {}
for page in %s:
    at = AppTest.from_file("/app/app.py", default_timeout=120)
    at.run()
    at.switch_page(f"views/{page}.py").run()
    out[page] = [[e.value for e in at.exception], [e.value for e in at.error]]
print(json.dumps(out))
"""
MINIO_ERROR = "Nie udało się odczytać danych z MinIO: "


def _docker_ts(value: str) -> datetime:
    """A Docker timestamp (RFC 3339, nanoseconds, `Z` or an offset) as a datetime."""
    m = re.fullmatch(r"(.{19})(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", value)
    assert m, value
    frac = (m.group(2) or "0")[:6].ljust(6, "0")
    return datetime.fromisoformat(f"{m.group(1)}.{frac}{m.group(3).replace('Z', '+00:00')}")


@pytest.mark.slow
def test_dashboard_shows_an_error_while_minio_is_down_and_recovers(
    stack, gold_ready, dashboard_env
):
    """07 §3: "Błąd odczytu (np. restart MinIO) jest pokazywany w miejscu danych, a
    następne odświeżenie ponawia odczyt." With `minio` stopped: the overview page (host,
    a reader that already cached the data) and every page inside the dashboard container
    render without an uncaught exception and show the MinIO read error in place of the
    data; the container keeps running, healthy, not restarted. After `minio` is started
    again the next render shows the KPIs with no error (the stack is left healthy)."""
    at = render("overview")
    assert problems(at) == [] and at.metric, texts(at)
    dash0 = stack.inspect("dashboard")
    minio0 = stack.inspect("minio")["State"]["StartedAt"]
    stack.run("stop", "minio")
    try:
        state = stack.inspect("minio")["State"]
        assert state["Status"] == "exited", state
        stopped_at = _docker_ts(state["FinishedAt"])
        at = render("overview")
        assert [e.value for e in at.exception] == []
        errors = [e.value for e in at.error]
        assert errors and all(e.startswith(MINIO_ERROR) for e in errors), errors

        res = stack.exec("dashboard", "python", "-c", IN_CONTAINER_ERRORS % json.dumps(PAGES))
        pages = json.loads(res.stdout.strip().splitlines()[-1])
        bad = {p: v for p, v in pages.items()
               if v[0] or not v[1] or not all(e.startswith(MINIO_ERROR) for e in v[1])}  # fmt: skip
        assert bad == {}, bad

        # a healthcheck that ran while MinIO was down passed; same container, no restart
        def checked_after_stop() -> list | None:
            log = stack.inspect("dashboard")["State"]["Health"]["Log"]
            return [h for h in log if _docker_ts(h["Start"]) > stopped_at] or None

        runs = wait_until(checked_after_stop, 60, every=2, desc="a dashboard healthcheck")
        assert all(h["ExitCode"] == 0 for h in runs), runs
        dash = stack.inspect("dashboard")
        assert dash["State"]["StartedAt"] == dash0["State"]["StartedAt"]
        assert dash["RestartCount"] == dash0["RestartCount"]
        assert dash["State"]["Health"]["Status"] == "healthy"
    finally:
        stack.run("start", "minio")
        wait_until(lambda: stack.health("minio") == "healthy", 120, every=2,
                   desc="minio healthy again")  # fmt: skip
    assert stack.inspect("minio")["State"]["StartedAt"] != minio0

    def recovered():
        a = render("overview")
        return a if problems(a) == [] and a.metric else None

    at = wait_until(recovered, 120, every=5, desc="overview shows the KPIs again")
    assert "Czekam na dane" not in texts(at)
    stack.wait_healthy(timeout=600)  # spark resumes its writes by itself (08 §12.8)
