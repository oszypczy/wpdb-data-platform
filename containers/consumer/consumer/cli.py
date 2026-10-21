"""Command line of the validating AVRO consumer (04-kafka.md §6).

Modes (combinable):
  default            one line per event: `[orders] u e481f5 approved → shipped lsn=… tx=… schema v1`
  --stats            every --stats-interval seconds: events/s, c/u/d/r/t per topic, lag,
                     schema ids seen (per-event lines are suppressed unless --verbose)
  --validate         rules of consumer.rules, violations printed as they happen, final report
  --follow-order ID  (alias --trace) only the events of one order, then its timeline
  --json             machine-readable: one JSON object per event, a final {"summary": …}

One-shot use (tests, demos): --from-beginning reads every topic from offset 0 with a
throw-away consumer group (no commits), --max-messages N stops after N messages and
--idle-timeout S stops after S seconds without a message (i.e. when caught up).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from consumer import rules
from consumer.model import Message, OrderTracker, Stats, describe

DEFAULT_TOPICS = r"^olist\.shop\..*"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="consumer", description=__doc__.split("\n\n")[0])
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    p.add_argument(
        "--topics",
        default=DEFAULT_TOPICS,
        help="regex (^…) or comma-separated topic names; default: every olist.shop.* topic",
    )
    p.add_argument(
        "--group-id", default=None, help="default: wpdb-consumer (random if --from-beginning)"
    )
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument("--max-messages", type=int, default=None)
    p.add_argument("--idle-timeout", type=float, default=None, help="seconds without a message")
    p.add_argument(
        "--until-end",
        action="store_true",
        help="stop once every assigned partition reported its end (partition EOF)",
    )
    p.add_argument("--stats", action="store_true")
    p.add_argument("--stats-interval", type=float, default=5.0)
    p.add_argument("--validate", action="store_true")
    p.add_argument(
        "--follow-order", "--trace", dest="follow_order", default=None, metavar="ORDER_ID"
    )
    p.add_argument("--json", action="store_true")
    p.add_argument("--verbose", action="store_true", help="per-event lines even with --stats")
    return p


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class Registry:
    """Schema Registry lookups with caches: schema id → exists, (subject, id) → version."""

    def __init__(self, url: str) -> None:
        self.client = SchemaRegistryClient({"url": url})  # shared with the deserializers
        # separate client for metadata: once the deserializer fetched a schema by id, the
        # shared client's cache answers get_version() with schema_id=None (confluent-kafka 2.16)
        self.meta = SchemaRegistryClient({"url": url})
        self._known: dict[int, bool] = {}
        self._versions: dict[str, dict[int, int]] = {}

    def is_known(self, schema_id: int) -> bool:
        if schema_id not in self._known:
            try:
                self.meta.get_schema(schema_id)
                self._known[schema_id] = True
            except Exception:  # noqa: BLE001 - any registry error means "not resolvable"
                self._known[schema_id] = False
        return self._known[schema_id]

    def version_of(self, subject: str, schema_id: int) -> int | None:
        versions = self._versions.setdefault(subject, {})
        if schema_id not in versions:
            try:
                for v in self.meta.get_versions(subject):
                    rs = self.meta.get_version(subject, v)
                    versions[rs.schema_id] = rs.version
            except Exception:  # noqa: BLE001
                return None
        return versions.get(schema_id)


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.registry = Registry(args.schema_registry)
        self.key_deser = AvroDeserializer(self.registry.client)
        self.value_deser = AvroDeserializer(self.registry.client)
        group = args.group_id or (
            f"wpdb-consumer-{uuid.uuid4().hex[:8]}" if args.from_beginning else "wpdb-consumer"
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap,
                "group.id": group,
                "enable.auto.commit": not args.from_beginning,
                "auto.offset.reset": "earliest" if args.from_beginning else "latest",
                "topic.metadata.refresh.interval.ms": 10000,
                # the stack is IPv4 only; without this librdkafka first tries ::1 on the host
                "broker.address.family": "v4",
                # --until-end: librdkafka reports the end of each partition (exact even when
                # compaction left offset gaps at the tail)
                "enable.partition.eof": bool(args.until_end),
            }
        )
        self.stats = Stats()
        self.tracker = OrderTracker(args.follow_order) if args.follow_order else None
        self.clock: datetime | None = None
        self.violations: list[tuple[Message, rules.Violation]] = []
        self.stop = False
        self.decode_errors = 0
        self.at_eof: set[tuple[str, int]] = set()

    # -- output ---------------------------------------------------------------------------
    def emit(self, text: str) -> None:
        print(text, flush=True)

    def emit_event(self, msg: Message, violations: list[rules.Violation]) -> None:
        if self.args.json:
            self.emit(
                json.dumps(
                    {
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "table": msg.table,
                        "op": msg.op,
                        "key": _jsonable(msg.key),
                        "value": _jsonable(msg.value),
                        "key_schema_id": msg.key_schema_id,
                        "value_schema_id": msg.value_schema_id,
                        "violations": [str(v) for v in violations],
                    }
                )
            )
            return
        line = describe(msg, self.version_of(msg))
        if violations:
            line += "  !! " + "; ".join(str(v) for v in violations)
        self.emit(line)

    def version_of(self, msg: Message) -> int | None:
        if msg.value_schema_id is None:
            return None
        return self.registry.version_of(f"{msg.topic}-value", msg.value_schema_id)

    # -- decoding -------------------------------------------------------------------------
    def decode(self, raw) -> Message:
        topic = raw.topic()
        key_b, val_b = raw.key(), raw.value()
        key = (
            self.key_deser(key_b, SerializationContext(topic, MessageField.KEY))
            if key_b is not None
            else None
        )
        value = (
            self.value_deser(val_b, SerializationContext(topic, MessageField.VALUE))
            if val_b is not None
            else None
        )
        kh = rules.parse_wire_header(key_b)
        vh = rules.parse_wire_header(val_b)
        return Message(
            topic=topic,
            partition=raw.partition(),
            offset=raw.offset(),
            key=key,
            value=value,
            key_schema_id=kh[1] if kh else None,
            value_schema_id=vh[1] if vh else None,
            raw_bytes=len(key_b or b"") + len(val_b or b""),
        )

    # -- main loop ------------------------------------------------------------------------
    def subscribe(self) -> None:
        topics = self.args.topics
        names = [topics] if topics.startswith("^") else [t.strip() for t in topics.split(",") if t]

        self.consumer.subscribe(names)

    def reached_end(self) -> bool:
        """--until-end: every assigned partition reported EOF at least once."""
        assignment = self.consumer.assignment()
        return bool(assignment) and all(
            (tp.topic, tp.partition) in self.at_eof for tp in assignment
        )

    def update_lag(self) -> None:
        by_topic: dict[str, int] = {}
        for tp in self.consumer.assignment():
            try:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=5)
                (pos,) = self.consumer.position([tp])
                position = pos.offset if pos.offset >= 0 else 0
                by_topic[tp.topic] = by_topic.get(tp.topic, 0) + max(high - position, 0)
            except KafkaException:
                continue
        for topic, lag in by_topic.items():
            self.stats.set_lag(topic, lag)

    def handle(self, raw) -> None:
        try:
            msg = self.decode(raw)
        except Exception as exc:  # noqa: BLE001 - report and keep consuming
            self.decode_errors += 1
            self.emit(f"[{raw.topic()}] p={raw.partition()} off={raw.offset()} DECODE ERROR: {exc}")
            return
        violations: list[rules.Violation] = []
        if self.args.validate:
            violations = rules.validate_event(msg.table, msg.value, self.clock)
            for field, data in (("key", raw.key()), ("value", raw.value())):
                v = rules.check_schema_id_known(data, self.registry.is_known, field)
                if v is not None:
                    violations.append(v)
            self.violations.extend((msg, v) for v in violations)
        self.clock = rules.advance_clock(msg.table, msg.value or {}, self.clock)
        self.stats.add(msg, len(violations))
        if self.tracker is not None:
            if self.tracker.record(msg):
                self.emit_event(msg, violations)
        elif self.args.verbose or not self.args.stats or violations:
            self.emit_event(msg, violations)

    def run(self) -> int:
        a = self.args
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        self.subscribe()
        last_msg = time.monotonic()
        last_stats = time.monotonic()
        last_end_check = time.monotonic()
        try:
            while not self.stop:
                raw = self.consumer.poll(1.0)
                now = time.monotonic()
                if raw is not None:
                    if raw.error() and raw.error().code() == KafkaError._PARTITION_EOF:
                        self.at_eof.add((raw.topic(), raw.partition()))
                    elif raw.error():
                        self.emit(f"consumer error: {raw.error()}")
                    else:
                        self.handle(raw)
                        last_msg = now
                        if a.max_messages is not None and self.stats.total >= a.max_messages:
                            break
                elif a.idle_timeout is not None and now - last_msg >= a.idle_timeout:
                    break
                # --until-end: after every EOF event, and every 2 s for the watermark
                # fallback (under live traffic poll() rarely comes back empty)
                if a.until_end:
                    eof = raw is not None and raw.error() is not None
                    if eof or now - last_end_check >= 2.0:
                        last_end_check = now
                        if self.reached_end():
                            break
                if a.stats and now - last_stats >= a.stats_interval:
                    self.update_lag()
                    for line in self.stats.lines(now - last_stats):
                        self.emit(line)
                    last_stats = now
        finally:
            if a.stats:  # one last stats block for the final (partial) interval
                try:
                    self.update_lag()
                    for line in self.stats.lines(max(time.monotonic() - last_stats, 1e-9)):
                        self.emit(line)
                except KafkaException:
                    pass
            self.consumer.close()
        return self.finish()

    def finish(self) -> int:
        summary = self.stats.summary()
        summary["decode_errors"] = self.decode_errors
        summary["clock"] = self.clock.isoformat() if self.clock else None
        if self.tracker is not None:
            summary["order"] = self.tracker.order_id
            summary["timeline"] = self.tracker.timeline(self.version_of)
        if self.args.json:
            self.emit(json.dumps({"summary": _jsonable(summary)}))
        else:
            if self.tracker is not None:
                self.emit(f"--- lifecycle of order {self.tracker.order_id} ---")
                for line in self.tracker.timeline(self.version_of):
                    self.emit(line)
            self.emit(
                f"SUMMARY events={summary['events']} violations={summary['violations']} "
                f"decode_errors={self.decode_errors} clock={summary['clock']}"
            )
            if self.args.validate and self.violations:
                by_rule: dict[str, int] = {}
                for _, v in self.violations:
                    by_rule[v.rule] = by_rule.get(v.rule, 0) + 1
                self.emit(
                    "violations by rule: " + ", ".join(f"{k}={n}" for k, n in by_rule.items())
                )
        return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return Runner(args).run()


if __name__ == "__main__":
    sys.exit(main())
