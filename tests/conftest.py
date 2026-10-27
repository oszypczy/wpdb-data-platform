"""Shared fixtures (docs/design/08-operations.md §9).

Levels: `unit` tests never touch these Docker fixtures; `integration` tests ask
for `stack` (session-scoped: starts the default compose services once and waits
for healthy); `e2e` tests additionally expect WPDB_FRESH=1 (volumes wiped first).
The stack is never torn down by the tests: `make down` / `make reset` do that.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from dotenv import dotenv_values

from tests.helpers import clients
from tests.helpers.compose import ComposeStack

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def settings() -> dict[str, str]:
    """Secrets and parameters: .env if present, else .env.example; env vars win."""
    env_file = ROOT / ".env"
    values = dotenv_values(env_file if env_file.exists() else ROOT / ".env.example")
    merged = {k: v for k, v in values.items() if v is not None}
    merged.update({k: v for k, v in os.environ.items() if k in values})
    return merged


@pytest.fixture(scope="session")
def compose() -> ComposeStack:
    return ComposeStack(root=ROOT)


@pytest.fixture(scope="session")
def stack(compose: ComposeStack) -> ComposeStack:
    """The default compose services, healthy. WPDB_FRESH=1 wipes volumes first."""
    fresh = os.environ.get("WPDB_FRESH") == "1"
    if fresh:
        compose.down(volumes=True)
    compose.up_and_wait(timeout=float(os.environ.get("WPDB_HEALTHY_TIMEOUT", "300")))
    compose.fresh = fresh  # type: ignore[attr-defined]
    return compose


# --- PostgreSQL ---------------------------------------------------------------
def _pg(settings: dict[str, str], user: str, pw_var: str):
    conn = clients.pg_connect(user, settings[pw_var])
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def pg_reader(stack, settings):
    yield from _pg(settings, "reader", "READER_PASSWORD")


@pytest.fixture
def pg_app(stack, settings):
    yield from _pg(settings, "shop_app", "SHOP_APP_PASSWORD")


@pytest.fixture
def pg_debezium(stack, settings):
    yield from _pg(settings, "debezium", "DEBEZIUM_DB_PASSWORD")


# --- Kafka / Schema Registry / Connect ------------------------------------------
@pytest.fixture
def kafka_admin(stack):
    return clients.kafka_admin()


@pytest.fixture
def schema_registry(stack) -> clients.Http:
    return clients.Http(clients.SCHEMA_REGISTRY_URL)


@pytest.fixture
def connect(stack) -> clients.Http:
    return clients.Http(clients.CONNECT_URL)


# --- MinIO ----------------------------------------------------------------------
@pytest.fixture
def minio(stack, settings):
    """Factory: minio("spark"|"dashboard"|"agent"|"root") -> boto3 S3 client."""

    def factory(account: str):
        keys = {
            "spark": ("MINIO_SPARK_ACCESS_KEY", "MINIO_SPARK_SECRET_KEY"),
            "dashboard": ("MINIO_DASHBOARD_ACCESS_KEY", "MINIO_DASHBOARD_SECRET_KEY"),
            "agent": ("MINIO_AGENT_ACCESS_KEY", "MINIO_AGENT_SECRET_KEY"),
            "root": ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"),
        }[account]
        return clients.minio_client(settings[keys[0]], settings[keys[1]])

    return factory


# --- stage 2: CDC (Debezium → Kafka) ----------------------------------------------
@pytest.fixture(scope="session")
def topic_reader(stack):
    """AVRO-decoding reader of the olist.* topics (tests/helpers/kafka.py)."""
    from tests.helpers.kafka import TopicReader

    return TopicReader()


@pytest.fixture(scope="session")
def probe_shop(stack, settings):
    """Per-session mirrors `shop.zz_probe_<hex>_{customers,orders,order_items,
    order_payments}` of the business tables (same columns, types and PKs; topics with the
    business topics' partitions and cleanup policy), written instead of the live tables
    by every test that needs probe orders: committed changes of the business tables would
    stay in Spark's gold history (tests/helpers/cdc.py::ProbeShop). Leftovers of an
    interrupted session are swept first; at the end the tables are dropped once the
    connector's offset passed their last change, then their subjects and topics."""
    from tests.helpers import cdc

    app = clients.pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    app.autocommit = True
    admin = clients.kafka_admin()
    registry = clients.Http(clients.SCHEMA_REGISTRY_URL)
    connect = clients.Http(clients.CONNECT_URL)
    leftovers = cdc.sweep_leftover_probe_tables(
        app, connect, registry, admin, prefix=cdc.PROBE_SHOP_PREFIX
    )
    if leftovers:
        print(f"\nswept probe shop tables left by an earlier session: {leftovers}")
    shop = cdc.ProbeShop.create(app, admin)
    try:
        yield shop
    finally:
        shop.teardown(app, connect, registry, admin)
        app.close()


@pytest.fixture(scope="session")
def probe_lifecycle(stack, settings, topic_reader, probe_shop):
    """Two synthetic orders written as shop_app through their whole lifecycle (03 §11,
    04 §6/§8) in the probe shop's tables, and the records Debezium produced for them
    (tests/helpers/cdc.py). Runs once per session; the rows are deleted again inside the
    lifecycle."""
    from tests.helpers import cdc

    app = clients.pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    app.autocommit = True
    try:
        cdc.wait_running(clients.Http(clients.CONNECT_URL))
        return cdc.run_probe_lifecycle(app, topic_reader, probe_shop)
    finally:
        app.close()


@pytest.fixture(scope="module")
def captured_probe_table(stack, settings, topic_reader):
    """A throw-away captured table `shop.zz_stage2_probe_<hex>` (id int PK, v text) with one
    row, its topic created by the connector's default topic.creation group. Shared by the
    topic-design and schema-evolution tests. Torn down only after the connector committed
    its offset past the table's last change (see tests/helpers/cdc.py::committed_lsn), then
    table, subject config, subjects and topic are removed; if that wait times out the table
    stays and the next session's fixture sweeps it (cdc.sweep_leftover_probe_tables)."""
    from tests.helpers import cdc

    app = clients.pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    app.autocommit = True
    admin = clients.kafka_admin()
    registry = clients.Http(clients.SCHEMA_REGISTRY_URL)
    connect = clients.Http(clients.CONNECT_URL)
    leftovers = cdc.sweep_leftover_probe_tables(app, connect, registry, admin)
    if leftovers:
        print(f"\nswept captured probe tables left by an earlier session: {leftovers}")
    probe = cdc.CapturedProbe.create(app, topic_reader, admin)
    try:
        yield probe
    finally:
        probe.teardown(app, connect, registry, admin, topic_reader)
        app.close()
