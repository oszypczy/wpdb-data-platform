"""Schema Registry (docs/design/04-kafka.md §1, §5)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_subjects_endpoint(schema_registry):
    """04 §1: GET /subjects answers with the subjects Debezium registered (stage 2)."""
    subjects = schema_registry.json("/subjects")
    assert isinstance(subjects, list)
    assert all(isinstance(s, str) for s in subjects), subjects
    assert "olist.shop.orders-value" in subjects


def test_global_compatibility_backward(schema_registry):
    """04 §5: global compatibility level is BACKWARD (explicit, not the image default)."""
    assert schema_registry.json("/config") == {"compatibilityLevel": "BACKWARD"}


def test_registry_stores_schemas_in__schemas_topic(kafka_admin):
    """04 §1: the registry's store is the pre-created compacted topic _schemas (topics as code)."""
    md = kafka_admin.list_topics(timeout=10)
    assert "_schemas" in md.topics and len(md.topics["_schemas"].partitions) == 1


def test_mode_is_readwrite(schema_registry):
    """Registry accepts new schemas (mode READWRITE), as Debezium needs in stage 2."""
    assert schema_registry.json("/mode") == {"mode": "READWRITE"}


# --- stage 2 (04 §1, §5, §8) -------------------------------------------------------------
import json  # noqa: E402

from tests.helpers import cdc  # noqa: E402
from tests.helpers.cdc import HEARTBEAT_TOPIC, TRANSACTION_TOPIC, topic  # noqa: E402


def test_subjects_follow_the_topic_name_strategy(schema_registry, kafka_admin):
    """04 §1: one `<topic>-key` and one `<topic>-value` subject per olist.* topic (and the
    heartbeat topic), nothing else; the key record is named Key, the value Envelope."""
    topics = {t for t in kafka_admin.list_topics(timeout=10).topics if t.startswith("olist.")}
    topics.add(HEARTBEAT_TOPIC)
    expected = {f"{t}-key" for t in topics} | {f"{t}-value" for t in topics}
    subjects = set(schema_registry.json("/subjects"))
    assert subjects == expected, sorted(subjects ^ expected)
    for table in cdc.SHOP_TABLES:
        key = json.loads(schema_registry.json(f"/subjects/{topic(table)}-key/versions/1")["schema"])
        value = json.loads(
            schema_registry.json(f"/subjects/{topic(table)}-value/versions/1")["schema"]
        )
        assert (key["name"], key["namespace"]) == ("Key", topic(table))
        assert (value["name"], value["namespace"]) == ("Envelope", topic(table))
        assert [f["name"] for f in value["fields"]] == [
            "before",
            "after",
            "source",
            "transaction",
            "op",
            "ts_ms",
            "ts_us",
            "ts_ns",
        ]
    tx = json.loads(
        schema_registry.json(f"/subjects/{TRANSACTION_TOPIC}-value/versions/1")["schema"]
    )
    assert [f["name"] for f in tx["fields"]] == [
        "status",
        "id",
        "event_count",
        "data_collections",
        "ts_ms",
    ]


def _post_compat(schema_registry, subject: str, schema: dict) -> dict:
    r = schema_registry.post(
        f"/compatibility/subjects/{subject}/versions/latest",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": json.dumps(schema)}),
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_compatibility_endpoint_accepts_nullable_field_and_rejects_type_change(schema_registry):
    """04 §8: against olist.shop.orders-value (BACKWARD): adding a nullable field with a
    default is compatible; changing order_status from string to int is not; the registry
    answers `is_compatible` without registering anything."""
    subject = f"{topic('orders')}-value"
    versions_before = schema_registry.json(f"/subjects/{subject}/versions")
    latest = json.loads(schema_registry.json(f"/subjects/{subject}/versions/latest")["schema"])
    before = next(f for f in latest["fields"] if f["name"] == "before")
    value = next(t for t in before["type"] if isinstance(t, dict))

    added = json.loads(json.dumps(latest))
    rec = next(
        t
        for t in next(f for f in added["fields"] if f["name"] == "before")["type"]
        if isinstance(t, dict)
    )
    rec["fields"].append(
        {"name": "delivery_channel_probe", "type": ["null", "string"], "default": None}
    )
    assert _post_compat(schema_registry, subject, added)["is_compatible"] is True

    changed = json.loads(json.dumps(latest))
    rec = next(
        t
        for t in next(f for f in changed["fields"] if f["name"] == "before")["type"]
        if isinstance(t, dict)
    )
    next(f for f in rec["fields"] if f["name"] == "order_status")["type"] = "int"
    verdict = _post_compat(schema_registry, subject, changed)
    assert verdict["is_compatible"] is False, verdict

    # a required field without default is also rejected under BACKWARD
    required = json.loads(json.dumps(latest))
    rec = next(
        t
        for t in next(f for f in required["fields"] if f["name"] == "before")["type"]
        if isinstance(t, dict)
    )
    rec["fields"].append({"name": "must_have", "type": "string"})
    assert _post_compat(schema_registry, subject, required)["is_compatible"] is False
    assert schema_registry.json(f"/subjects/{subject}/versions") == versions_before
    assert value["name"] == "Value"
