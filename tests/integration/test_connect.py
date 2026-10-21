"""Kafka Connect worker image and connect-init (docs/design/03-debezium.md §1, §8)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def plugin_classes(connect) -> set[str]:
    return {
        p["class"] for p in connect.json("/connector-plugins", params={"connectorsOnly": "false"})
    }


def test_plugins_include_avro_converter(connect):
    """03 §1: the image contains the Confluent AVRO converter.

    The plugin list hides converters unless connectorsOnly=false is passed.
    """
    classes = plugin_classes(connect)
    assert "io.confluent.connect.avro.AvroConverter" in classes, sorted(classes)


def test_plugins_include_debezium_postgres(connect):
    """03 §1: the Debezium PostgreSQL connector is available."""
    plugins = {
        p["class"]: p
        for p in connect.json("/connector-plugins", params={"connectorsOnly": "false"})
    }
    assert "io.debezium.connector.postgresql.PostgresConnector" in plugins
    assert plugins["io.debezium.connector.postgresql.PostgresConnector"]["version"].startswith(
        "3.6.3"
    )


def test_connect_init_registered_the_connector(connect, stack):
    """03 §8: connect-init registered every containers/connect/connectors/*.json with PUT
    (stage 2: olist-postgres), waited for RUNNING and exited 0."""
    rows = {r.service: r for r in stack.ps()}
    assert rows["connect-init"].state == "exited" and rows["connect-init"].exit_code == 0
    logs = stack.logs("connect-init")
    assert "olist-postgres RUNNING" in logs and "connect-init: done" in logs
    assert connect.json("/connectors") == ["olist-postgres"]


def test_worker_uses_env_config_provider(connect, stack):
    """03 §8: the worker resolves ${env:...} placeholders (config.providers=env)."""
    out = stack.exec("connect", "cat", "/kafka/config/connect-distributed.properties").stdout
    props = dict(
        line.split("=", 1) for line in out.splitlines() if "=" in line and not line.startswith("#")
    )
    assert props.get("config.providers") == "env"
    assert (
        props.get("config.providers.env.class")
        == "org.apache.kafka.common.config.provider.EnvVarConfigProvider"
    )
    assert props.get("connector.client.config.override.policy") == "All"
    assert props.get("key.converter.schema.registry.url") == "http://schema-registry:8081"
    assert props.get("value.converter.schema.registry.url") == "http://schema-registry:8081"


def test_worker_kafka_version_matches_broker_line(connect):
    """08 §1 version matrix: Connect runs on the Kafka 4.3 line (CP 8.3 broker + converter)."""
    root = connect.json("/")
    assert root["version"].startswith("4.3."), root
