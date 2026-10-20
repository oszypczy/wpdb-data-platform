"""Read-only MCP server for the local Kafka broker and Schema Registry.

Exposes a handful of inspection tools (topics, configs, AVRO-decoded messages,
subjects, schemas). Nothing here creates topics, registers schemas or commits
offsets: consumption uses a throw-away consumer group with auto-commit off.
"""

from __future__ import annotations

import json
import os
import struct
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from mcp.server import MCPServer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
MAGIC_BYTE = 0

mcp = MCPServer("kafka")


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP, "socket.timeout.ms": 5000})


def _registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def _jsonable(value: Any) -> Any:
    """Make AVRO-decoded payloads (dicts with bytes/Decimal/datetime) JSON-safe."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


@mcp.tool()
def list_topics() -> list[dict[str, Any]]:
    """List topics on the broker with their partition counts."""
    md = _admin().list_topics(timeout=10)
    return sorted(
        ({"topic": name, "partitions": len(t.partitions)} for name, t in md.topics.items()),
        key=lambda r: r["topic"],
    )


@mcp.tool()
def describe_topic(topic: str) -> dict[str, Any]:
    """Partitions (leader, offsets) and non-default configs of one topic."""
    admin = _admin()
    md = admin.list_topics(topic=topic, timeout=10)
    t = md.topics.get(topic)
    if t is None or t.error is not None:
        raise ValueError(f"topic {topic!r} not found: {t.error if t else 'missing'}")
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"kafka-mcp-{uuid.uuid4()}"})
    partitions = []
    try:
        for pid, p in sorted(t.partitions.items()):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
            partitions.append({"partition": pid, "leader": p.leader, "low": low, "high": high})
    finally:
        consumer.close()
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    configs = admin.describe_configs([resource])[resource].result(timeout=10)
    non_default = {
        k: v.value for k, v in configs.items() if not v.is_default and v.value is not None
    }
    return {"topic": topic, "partitions": partitions, "config": non_default}


def _decode(
    data: bytes | None,
    deserializer: AvroDeserializer,
    ctx: SerializationContext,
) -> Any:
    if data is None:
        return None
    if len(data) >= 5 and data[0] == MAGIC_BYTE:
        try:
            return _jsonable(deserializer(data, ctx))
        except Exception as exc:  # noqa: BLE001 - surface the decoding problem in the output
            schema_id = struct.unpack(">I", data[1:5])[0]
            return {"_undecodable": str(exc), "schema_id": schema_id, "raw_hex": data.hex()}
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return {"raw_hex": data.hex()}


@mcp.tool()
def consume(
    topic: str, n: int = 10, from_beginning: bool = True, timeout_s: float = 10.0
) -> list[dict[str, Any]]:
    """Read up to n messages from a topic, AVRO-decoded via Schema Registry.

    from_beginning=True reads the first messages of each partition; otherwise the
    last n per partition. Tombstones have value null. Offsets are never committed.
    """
    registry = _registry()
    key_deser = AvroDeserializer(registry)
    value_deser = AvroDeserializer(registry)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"kafka-mcp-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    out: list[dict[str, Any]] = []
    try:
        md = consumer.list_topics(topic=topic, timeout=10)
        t = md.topics.get(topic)
        if t is None or t.error is not None:
            raise ValueError(f"topic {topic!r} not found")
        assignment = []
        for pid in t.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
            start = low if from_beginning else max(low, high - n)
            assignment.append(TopicPartition(topic, pid, start))
        consumer.assign(assignment)
        remaining = timeout_s
        while len(out) < n and remaining > 0:
            msg = consumer.poll(min(1.0, remaining))
            remaining -= 1.0
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            schema_id = None
            value = msg.value()
            if value is not None and len(value) >= 5 and value[0] == MAGIC_BYTE:
                schema_id = struct.unpack(">I", value[1:5])[0]
            out.append(
                {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp_ms": msg.timestamp()[1],
                    "schema_id": schema_id,
                    "key": _decode(
                        msg.key(), key_deser, SerializationContext(topic, MessageField.KEY)
                    ),
                    "value": _decode(
                        value, value_deser, SerializationContext(topic, MessageField.VALUE)
                    ),
                }
            )
    finally:
        consumer.close()
    return out


@mcp.tool()
def list_subjects() -> list[dict[str, Any]]:
    """Subjects in Schema Registry with their version numbers."""
    registry = _registry()
    rows = []
    for subject in sorted(registry.get_subjects()):
        rows.append({"subject": subject, "versions": registry.get_versions(subject)})
    return rows


@mcp.tool()
def get_schema(subject: str, version: str = "latest") -> dict[str, Any]:
    """One version of a subject: schema id, version and the parsed schema."""
    registry = _registry()
    if version == "latest":
        rv = registry.get_latest_version(subject)
    else:
        rv = registry.get_version(subject, int(version))
    schema_str = rv.schema.schema_str
    try:
        parsed = json.loads(schema_str)
    except json.JSONDecodeError:
        parsed = schema_str
    return {
        "subject": subject,
        "version": rv.version,
        "schema_id": rv.schema_id,
        "schema_type": rv.schema.schema_type,
        "schema": parsed,
    }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
