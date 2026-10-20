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
    """Factory: minio("spark") / minio("dashboard") / minio("root") -> boto3 S3 client."""

    def factory(account: str):
        keys = {
            "spark": ("MINIO_SPARK_ACCESS_KEY", "MINIO_SPARK_SECRET_KEY"),
            "dashboard": ("MINIO_DASHBOARD_ACCESS_KEY", "MINIO_DASHBOARD_SECRET_KEY"),
            "root": ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"),
        }[account]
        return clients.minio_client(settings[keys[0]], settings[keys[1]])

    return factory
