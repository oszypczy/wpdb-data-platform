"""Kafka/Schema Registry helpers for the integration tests (stage 2).

- `TopicReader`: reads a topic (all partitions) with AVRO decoding through Schema
  Registry, either everything up to the current high watermarks (`read_all`) or new
  messages after a watermark snapshot (`read_new`), with a throw-away consumer group
  and no commits.
- `Record`: one decoded message with its raw bytes and schema ids.
- `Watermarks`: per-partition high offsets, used as "start reading from here".
"""

from __future__ import annotations

import struct
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from tests.helpers.clients import KAFKA_BOOTSTRAP_HOST, SCHEMA_REGISTRY_URL

MAGIC_BYTE = 0


def schema_id_of(data: bytes | None) -> int | None:
    if data is None or len(data) < 5 or data[0] != MAGIC_BYTE:
        return None
    return struct.unpack(">I", data[1:5])[0]


@dataclass(slots=True)
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: dict[str, Any] | None
    value: dict[str, Any] | None
    key_raw: bytes | None
    value_raw: bytes | None

    @property
    def op(self) -> str:
        return "t" if self.value is None else str(self.value.get("op"))

    @property
    def key_schema_id(self) -> int | None:
        return schema_id_of(self.key_raw)

    @property
    def value_schema_id(self) -> int | None:
        return schema_id_of(self.value_raw)

    @property
    def table(self) -> str:
        return self.topic.rsplit(".", 1)[-1]

    @property
    def after(self) -> dict[str, Any]:
        return (self.value or {}).get("after") or {}

    @property
    def before(self) -> dict[str, Any]:
        return (self.value or {}).get("before") or {}

    @property
    def source(self) -> dict[str, Any]:
        return (self.value or {}).get("source") or {}

    def key_tuple(self) -> tuple:
        return tuple((self.key or {}).values())


@dataclass
class Watermarks:
    topic: str
    high: dict[int, int] = field(default_factory=dict)  # partition -> next offset


class TopicReader:
    """AVRO-decoding reader with a throw-away group (no commits, auto.offset.reset=earliest)."""

    def __init__(
        self, bootstrap: str = KAFKA_BOOTSTRAP_HOST, registry_url: str = SCHEMA_REGISTRY_URL
    ):
        self.bootstrap = bootstrap
        self.registry = SchemaRegistryClient({"url": registry_url})
        self.key_deser = AvroDeserializer(self.registry)
        self.value_deser = AvroDeserializer(self.registry)

    def _consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"wpdb-tests-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "broker.address.family": "v4",
                "socket.timeout.ms": 10000,
            }
        )

    def partitions(self, topic: str) -> list[int]:
        c = self._consumer()
        try:
            md = c.list_topics(topic=topic, timeout=10)
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                raise LookupError(f"topic {topic!r} not found")
            return sorted(t.partitions)
        finally:
            c.close()

    def watermarks(self, topic: str) -> Watermarks:
        c = self._consumer()
        try:
            wm = Watermarks(topic)
            md = c.list_topics(topic=topic, timeout=10)
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                raise LookupError(f"topic {topic!r} not found")
            for pid in t.partitions:
                _, high = c.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
                wm.high[pid] = high
            return wm
        finally:
            c.close()

    def decode(self, msg) -> Record:
        topic = msg.topic()
        key_raw, value_raw = msg.key(), msg.value()
        key = (
            self.key_deser(key_raw, SerializationContext(topic, MessageField.KEY))
            if key_raw is not None
            else None
        )
        value = (
            self.value_deser(value_raw, SerializationContext(topic, MessageField.VALUE))
            if value_raw is not None
            else None
        )
        return Record(
            topic=topic,
            partition=msg.partition(),
            offset=msg.offset(),
            timestamp_ms=msg.timestamp()[1],
            key=key,
            value=value,
            key_raw=key_raw,
            value_raw=value_raw,
        )

    def read_all(self, topic: str, timeout: float = 120) -> list[Record]:
        """Every message from offset 0 up to the high watermarks taken now."""
        start = Watermarks(topic, {p: 0 for p in self.partitions(topic)})
        end = self.watermarks(topic)
        return self.read_range(start, end, timeout)

    def read_range(self, start: Watermarks, end: Watermarks, timeout: float = 120) -> list[Record]:
        """Messages with start.high[p] <= offset < end.high[p] for every partition."""
        topic = start.topic
        c = self._consumer()
        out: list[Record] = []
        try:
            pending = {
                p: end.high.get(p, 0) for p in end.high if end.high[p] > start.high.get(p, 0)
            }
            if not pending:
                return out
            c.assign([TopicPartition(topic, p, start.high.get(p, 0)) for p in pending])
            deadline = time.monotonic() + timeout
            while pending and time.monotonic() < deadline:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                p = msg.partition()
                if p not in pending:
                    continue
                if msg.offset() < pending[p]:
                    out.append(self.decode(msg))
                if msg.offset() + 1 >= pending[p]:
                    del pending[p]
            if pending:
                left = sorted(pending)
                raise TimeoutError(f"{topic}: partitions {left} not drained in {timeout}s")
        finally:
            c.close()
        out.sort(key=lambda r: (r.partition, r.offset))
        return out

    def read_new(
        self,
        since: Watermarks,
        until: Callable[[list[Record]], bool],
        timeout: float = 60,
    ) -> list[Record]:
        """Poll from `since` until `until(records)` is true (or timeout). Returns all read."""
        topic = since.topic
        c = self._consumer()
        out: list[Record] = []
        try:
            c.assign([TopicPartition(topic, p, off) for p, off in since.high.items()])
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                msg = c.poll(1.0)
                if msg is None:
                    if until(out):
                        return out
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                out.append(self.decode(msg))
                if until(out):
                    return out
            raise TimeoutError(f"{topic}: condition not met in {timeout}s ({len(out)} new records)")
        finally:
            c.close()


def by_key(records: Iterable[Record]) -> dict[tuple, list[Record]]:
    groups: dict[tuple, list[Record]] = {}
    for r in records:
        groups.setdefault(r.key_tuple(), []).append(r)
    for recs in groups.values():
        recs.sort(key=lambda r: (r.partition, r.offset))
    return groups
