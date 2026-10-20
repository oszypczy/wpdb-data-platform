"""Schema Registry (docs/design/04-kafka.md §1, §5)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_subjects_endpoint(schema_registry):
    """04 §1: GET /subjects answers (empty list before any connector registers a schema)."""
    subjects = schema_registry.json("/subjects")
    assert isinstance(subjects, list)
    assert all(isinstance(s, str) for s in subjects), subjects


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
