"""07 §7 integration rows: every page renders on the live MinIO data
(``streamlit.testing.AppTest``), the `dashboard` account's permissions through the
dashboard's own readers (delta-rs, DuckDB), and the container (healthy, also with an
empty gold). The e2e rows (KPIs = Postgres truth views, freshness thresholds) are in
the final stage (08 §10)."""

from __future__ import annotations

import json
import subprocess
import uuid
from decimal import Decimal

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
