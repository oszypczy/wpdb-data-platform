"""Kafka broker: listeners, topics as code, no auto-creation (04-kafka.md §1, §3, §8)."""

from __future__ import annotations

from pathlib import Path

import pytest
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import ConfigResource, NewTopic

from tests.helpers.compose import ComposeStack
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration

TOPICS_TXT = Path(__file__).resolve().parents[2] / "containers" / "kafka" / "topics.txt"


def parse_topics_txt() -> dict[str, tuple[int, dict[str, str]]]:
    topics = {}
    for line in TOPICS_TXT.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, partitions, *rest = line.split()
        cfg = dict(kv.split("=", 1) for kv in (rest[0].split(",") if rest else []))
        topics[name] = (int(partitions), cfg)
    return topics


def test_reachable_from_host(kafka_admin):
    """04 §8 listeners: a client on the host reaches the broker via localhost:9092."""
    md = kafka_admin.list_topics(timeout=10)
    assert len(md.brokers) == 1
    broker = next(iter(md.brokers.values()))
    assert (broker.host, broker.port) == ("localhost", 9092), broker


def test_reachable_from_container(stack: ComposeStack):
    """04 §8 listeners: a client inside the network reaches the broker via kafka:29092."""
    out = stack.exec(
        "kafka", "kafka-broker-api-versions", "--bootstrap-server", "kafka:29092"
    ).stdout
    assert "kafka:29092" in out.splitlines()[0], out[:200]


def test_auto_create_disabled(kafka_admin):
    """04 §3: auto.create.topics.enable=false.

    Producing to an unknown topic fails and creates nothing.
    """
    broker_id = next(iter(kafka_admin.list_topics(timeout=10).brokers))
    res = ConfigResource(ConfigResource.Type.BROKER, str(broker_id))
    cfg = kafka_admin.describe_configs([res])[res].result(timeout=10)
    assert cfg["auto.create.topics.enable"].value == "false"

    errors: list[KafkaError] = []
    # librdkafka reports UNKNOWN_TOPIC_OR_PART only after topic.metadata.propagation.max.ms
    # (default 30 s, meant for brokers that auto-create); shorten it so the real cause surfaces
    p = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "topic.metadata.propagation.max.ms": 1000,
            "message.timeout.ms": 15000,
        }
    )
    p.produce("stage0-does-not-exist", b"x", on_delivery=lambda err, msg: errors.append(err))
    p.flush(15)
    assert errors and errors[0] is not None
    assert errors[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART, errors[0]
    assert "stage0-does-not-exist" not in kafka_admin.list_topics(timeout=10).topics


def test_internal_topics_match_topics_txt(kafka_admin):
    """04 §3 topics as code: each line of containers/kafka/topics.txt exists with its
    partitions and configs, and no other connect-*/_schemas topic exists."""
    expected = parse_topics_txt()
    assert set(expected) == {"connect-configs", "connect-offsets", "connect-status", "_schemas"}
    md = kafka_admin.list_topics(timeout=10)
    internal = {t for t in md.topics if t.startswith("connect-") or t == "_schemas"}
    assert internal == set(expected), f"internal topics in Kafka: {sorted(internal)}"
    resources = [ConfigResource(ConfigResource.Type.TOPIC, t) for t in expected]
    described = kafka_admin.describe_configs(resources)
    for res in resources:
        name = res.name
        partitions, cfg = expected[name]
        assert len(md.topics[name].partitions) == partitions, name
        actual = described[res].result(timeout=10)
        for k, v in cfg.items():
            assert actual[k].value == v, f"{name}: {k}={actual[k].value!r}, expected {v!r}"


def test_producer_to_explicitly_created_topic_is_acked(kafka_admin):
    """Host listener end to end: an explicitly created topic accepts a produce (acks=all)."""
    topic = "stage0-probe"
    futures = kafka_admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result(timeout=15)
    try:
        wait_until(
            lambda: topic in kafka_admin.list_topics(timeout=10).topics, 15, desc="topic listed"
        )
        delivered = []
        p = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})
        p.produce(topic, key=b"k", value=b"v", on_delivery=lambda e, m: delivered.append((e, m)))
        p.flush(15)
        assert delivered and delivered[0][0] is None, delivered
        assert delivered[0][1].offset() == 0
    finally:
        kafka_admin.delete_topics([topic])[topic].result(timeout=15)


def test_unknown_topic_raises_for_admin_describe(kafka_admin):
    """Describing a non-existent topic is an error, not an implicit creation."""
    res = ConfigResource(ConfigResource.Type.TOPIC, "stage0-missing")
    with pytest.raises(KafkaException):
        kafka_admin.describe_configs([res])[res].result(timeout=10)


# --- stage 2: business topics created by Debezium (04 §2, §3, §7, §8) -------------------
import json  # noqa: E402
import time  # noqa: E402
from decimal import Decimal  # noqa: E402

from confluent_kafka.admin import AlterConfigOpType, ConfigEntry  # noqa: E402

from tests.helpers import cdc  # noqa: E402
from tests.helpers.cdc import HEARTBEAT_TOPIC, TRANSACTION_TOPIC, topic  # noqa: E402
from tests.helpers.kafka import Watermarks, by_key  # noqa: E402

LARGE = {"partitions": 3, "cleanup.policy": "compact", "segment.ms": "3600000"}
SMALL = {"partitions": 1, "cleanup.policy": "compact", "segment.ms": "3600000"}
TOPIC_DESIGN = {
    **{topic(t): LARGE for t in cdc.LARGE_TABLES},
    **{topic(t): SMALL for t in cdc.SHOP_TABLES if t not in cdc.LARGE_TABLES},
    TRANSACTION_TOPIC: {"partitions": 1, "cleanup.policy": "delete", "retention.ms": "86400000"},
    HEARTBEAT_TOPIC: {"partitions": 1, "cleanup.policy": "delete", "retention.ms": "3600000"},
}


def _topic_config(kafka_admin, name: str) -> dict[str, str]:
    res = ConfigResource(ConfigResource.Type.TOPIC, name)
    return {
        k: v.value for k, v in kafka_admin.describe_configs([res])[res].result(timeout=10).items()
    }


def test_business_topics_match_the_topic_design(kafka_admin):
    """04 §8: AdminClient sees every topic of the §2 table with the partitions,
    cleanup.policy and retention set by the connector's topic.creation groups (§3);
    replication factor 1 everywhere; no other olist.* topic exists."""
    md = kafka_admin.list_topics(timeout=10)
    olist = {
        t
        for t in md.topics
        if (t.startswith("olist.") or t == HEARTBEAT_TOPIC)
        and not t.startswith(f"olist.shop.{cdc.PROBE_PREFIX}")  # session probe, see conftest
    }
    assert olist == set(TOPIC_DESIGN), sorted(olist ^ set(TOPIC_DESIGN))
    for name, want in TOPIC_DESIGN.items():
        parts = md.topics[name].partitions
        assert len(parts) == want["partitions"], (name, len(parts))
        assert all(len(p.replicas) == 1 for p in parts.values()), name
        cfg = _topic_config(kafka_admin, name)
        for k, v in want.items():
            if k != "partitions":
                assert cfg[k] == v, f"{name}: {k}={cfg[k]!r}, expected {v!r}"
    # the connector config is the single source of these settings (topics as code)
    conn = cdc.connector_config()
    assert conn["topic.creation.groups"] == "large,transaction,heartbeat"
    assert conn["topic.creation.large.partitions"] == "3"
    assert conn["topic.creation.default.partitions"] == "1"
    assert conn["topic.creation.default.cleanup.policy"] == "compact"


def test_new_table_gets_a_topic_from_the_default_group(
    captured_probe_table, kafka_admin, schema_registry
):
    """04 §3: a table created later in `shop` (published automatically, 02 §3) gets its
    topic from the default topic.creation group: 1 partition, compact; and both subjects.
    Tables named `_…` are the exception: excluded from capture (03 §10, test probes)."""
    t = captured_probe_table.topic
    md = kafka_admin.list_topics(timeout=10)
    assert len(md.topics[t].partitions) == 1
    cfg = _topic_config(kafka_admin, t)
    assert cfg["cleanup.policy"] == "compact" and cfg["segment.ms"] == "3600000"
    assert {f"{t}-key", f"{t}-value"} <= set(schema_registry.json("/subjects"))
    assert captured_probe_table.first.op == "c"
    assert captured_probe_table.first.key == {"id": 1}
    assert not any(
        name.startswith("olist.shop._") for name in kafka_admin.list_topics(timeout=10).topics
    ), "underscore tables must not be captured"


def test_delete_gives_d_then_tombstone(probe_lifecycle):
    """04 §8: DELETE of a shop.orders row → `op=d` (full before) followed by a tombstone
    (same key, null value) so compaction can drop the key (04 §2)."""
    lc = probe_lifecycle
    recs = lc.of("orders", lc.clean)
    d, t = recs[-2], recs[-1]
    assert d.op == "d" and d.before["order_id"] == lc.clean.order_id and d.value["after"] is None
    assert t.value is None and t.value_raw is None
    assert t.key == d.key and t.partition == d.partition and t.offset == d.offset + 1
    assert t.key_raw == d.key_raw, "tombstone must use the identical serialized key"
    for table in ("order_items", "order_payments", "customers"):
        ops = [r.op for r in lc.of(table, lc.clean)]
        assert ops[-2:] == ["d", "t"], (table, ops)


def test_changes_of_one_row_stay_in_one_partition_in_order(probe_lifecycle):
    """04 §8/§4: the events of one key (c, u, d, tombstone) land in one partition with
    increasing offsets; two different probe keys may well hash elsewhere."""
    lc = probe_lifecycle
    for order in (lc.clean, lc.bad):
        recs = lc.of("orders", order)
        assert [r.op for r in recs] == ["c", "u", "d", "t"]
        assert len({r.partition for r in recs}) == 1, [(r.partition, r.offset) for r in recs]
        offsets = [r.offset for r in recs]
        assert offsets == sorted(offsets) and len(set(offsets)) == 4
        lsns = [r.source["lsn"] for r in recs if r.value]
        assert lsns == sorted(lsns)


def test_producer_overrides_are_applied_and_zstd_compresses(connect, stack, kafka_admin):
    """04 §7: the connector asks for acks=all, idempotence and zstd; the worker accepts
    the overrides (policy All) and the log on disk is smaller than the bytes produced."""
    cfg = connect.json(f"/connectors/{cdc.CONNECTOR}/config")
    assert cfg["producer.override.acks"] == "all"
    assert cfg["producer.override.enable.idempotence"] == "true"
    assert cfg["producer.override.compression.type"] == "zstd"
    logs = stack.logs("connect", tail=5000)
    assert "compression.type = zstd" in logs, "worker did not log a producer with zstd"
    assert "enable.idempotence = true" in logs
    # size on disk vs. uncompressed message bytes for olist.shop.orders
    from tests.helpers.kafka import TopicReader

    reader = TopicReader()
    t = topic("orders")
    end = reader.watermarks(t)
    recs = reader.read_range(Watermarks(t, dict.fromkeys(end.high, 0)), end, timeout=180)
    produced = sum(len(r.key_raw or b"") + len(r.value_raw or b"") for r in recs)
    out = stack.exec("kafka", "sh", "-c", f"du -cb /var/lib/kafka/data/{t}-*/*.log | tail -1")
    on_disk = int(out.stdout.split()[0])
    # every record batch written by the connector carries the zstd codec
    codecs = stack.exec(
        "kafka",
        "sh",
        "-c",
        # batches with records only: the log cleaner keeps empty batches (count: 0, codec
        # none) to preserve the idempotent producer's sequence numbers
        f"for f in /var/lib/kafka/data/{t}-*/*.log; do kafka-dump-log --files $f 2>/dev/null; "
        "done | grep -v ' count: 0 ' | grep -o 'compresscodec: [a-z]*' | sort | uniq -c",
    ).stdout.split("\n")
    counts = {line.split()[-1]: int(line.split()[0]) for line in codecs if line.strip()}
    ratio = produced / on_disk
    print(
        f"\n{t}: {len(recs)} records, {produced} bytes produced, {on_disk} bytes on disk, "
        f"zstd ratio {ratio:.2f}x, batch codecs {counts}"
    )
    assert set(counts) == {"zstd"} and counts["zstd"] > 0, counts
    # AVRO is already compact; zstd gains 1.04-1.4x depending on batch size (04 §7)
    assert produced > 100_000 and on_disk < produced, (produced, on_disk)


def test_avro_messages_are_smaller_than_json_with_schema(schema_registry):
    """04 §5: measured saving of AVRO (5-byte header + binary) against JSON payloads and
    against JSON with the schema embedded in every message (the JsonConverter default),
    on 500 streamed events of olist.shop.orders."""
    from tests.helpers.kafka import TopicReader

    reader = TopicReader()
    t = topic("orders")
    end = reader.watermarks(t)
    start = Watermarks(t, {p: max(0, h - 200) for p, h in end.high.items()})
    recs = [r for r in reader.read_range(start, end, timeout=120) if r.value][:500]
    assert len(recs) >= 100
    schema = schema_registry.json(f"/subjects/{t}-value/versions/latest")["schema"]

    def jsonable(v):
        if isinstance(v, dict):
            return {k: jsonable(x) for k, x in v.items()}
        if isinstance(v, Decimal):
            return str(v)
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return v

    avro = sum(len(r.value_raw) for r in recs)
    plain = sum(len(json.dumps(jsonable(r.value), separators=(",", ":"))) for r in recs)
    embedded = sum(
        len(
            json.dumps(
                {"schema": json.loads(schema), "payload": jsonable(r.value)}, separators=(",", ":")
            )
        )
        for r in recs
    )
    print(
        f"\norders value bytes over {len(recs)} events: avro={avro} json={plain} "
        f"json+schema={embedded} (avro/json={avro / plain:.2f}, "
        f"avro/json+schema={avro / embedded:.3f})"
    )
    assert avro < plain < embedded
    assert all(r.value_raw[0] == 0 and len(r.value_raw) > 5 for r in recs)


@pytest.mark.slow
def test_compaction_keeps_the_last_version_of_every_item(
    pg_app, pg_reader, kafka_admin, stack, probe_lifecycle
):
    """04 §8: after compaction of olist.shop.order_items every item is still present with
    its last version (mirror of the table). Compaction is forced within the test budget by
    temporarily setting segment.ms=1000 and min.cleanable.dirty.ratio=0.01 on the topic
    (restored afterwards); the extra versions are no-op updates (price = price) of three
    existing items, so Postgres and the truth views do not change."""
    from tests.helpers.kafka import TopicReader

    t = topic("order_items")
    reader = TopicReader()
    keys = pg_reader.execute(
        "SELECT order_id, order_item_id FROM shop.order_items "
        "WHERE order_id NOT LIKE 'test-stage2-%%' ORDER BY order_id LIMIT 3"
    ).fetchall()
    assert len(keys) == 3

    def alter(entries: list[ConfigEntry]) -> None:
        res_entries = ConfigResource(ConfigResource.Type.TOPIC, t, incremental_configs=entries)
        kafka_admin.incremental_alter_configs([res_entries])[res_entries].result(timeout=15)

    def segments() -> int:
        out = stack.exec("kafka", "sh", "-c", f"ls /var/lib/kafka/data/{t}-*/ | grep -c '\\.log$'")
        return int(out.stdout.strip() or 0)

    def versions_of(k) -> list:
        end = reader.watermarks(t)
        recs = reader.read_range(Watermarks(t, dict.fromkeys(end.high, 0)), end, timeout=180)
        return [r for r in recs if r.key == {"order_id": k[0], "order_item_id": k[1]}]

    def cleaned_offsets() -> dict[str, int]:
        out = stack.exec(
            "kafka", "sh", "-c", "cat /var/lib/kafka/data/cleaner-offset-checkpoint 2>/dev/null"
        ).stdout
        return {
            f"{line.split()[0]}-{line.split()[1]}": int(line.split()[2])
            for line in out.splitlines()[2:]
            if line.strip()
        }

    try:
        alter(
            [
                ConfigEntry("segment.ms", "1000", incremental_operation=AlterConfigOpType.SET),
                ConfigEntry(
                    "min.cleanable.dirty.ratio", "0.01", incremental_operation=AlterConfigOpType.SET
                ),
            ]
        )
        before = cleaned_offsets()
        # four rounds of no-op updates: a segment rolls on the first append after
        # segment.ms (1 s) has elapsed, so each round must start after that pause
        # (time.sleep here is the timing requirement of the roll, not synchronization)
        for _ in range(4):
            n = segments()
            time.sleep(1.5)
            for k in keys:
                pg_app.execute(
                    "UPDATE shop.order_items SET price = price "
                    "WHERE order_id = %s AND order_item_id = %s",
                    k,
                )
            wait_until(lambda n=n: segments() > n, 30, desc="segment rolled")
        wait_until(
            lambda: any(
                cleaned_offsets().get(f"{t}-{p}", 0) > before.get(f"{t}-{p}", 0) for p in range(3)
            ),
            timeout=120,
            every=3,
            desc="log cleaner compacted order_items",
        )

        def few_versions(k) -> list | None:
            vs = versions_of(k)
            return vs if len(vs) <= 2 else None

        for k in keys:
            # at least 1 + 4 versions were written; compaction leaves the last one (plus
            # whatever is still in the active, uncleanable segment)
            vs = wait_until(
                lambda k=k: few_versions(k), timeout=120, every=5, desc=f"{k} compacted"
            )
            last = vs[-1]
            row = pg_reader.execute(
                "SELECT price, freight_value FROM shop.order_items "
                "WHERE order_id = %s AND order_item_id = %s",
                k,
            ).fetchone()
            assert last.op in ("u", "c", "r") and last.after["price"] == row[0]
            assert last.after["freight_value"] == row[1]
        # mirror property: every Postgres item is still in the topic with its current values
        # (Postgres first; rows inserted while reading Kafka must show up right behind)
        with pg_reader.transaction():
            pg_reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            rows = pg_reader.execute(
                "SELECT order_id, order_item_id, price FROM shop.order_items"
            ).fetchall()
        end = reader.watermarks(t)
        recs = reader.read_range(Watermarks(t, dict.fromkeys(end.high, 0)), end, timeout=180)
        latest = {k: recs_[-1] for k, recs_ in by_key(recs).items()}
        missing = {(o, i) for o, i, _ in rows if (o, i) not in latest}
        if missing:
            late = reader.read_new(
                end, lambda rs: missing <= {r.key_tuple() for r in rs}, timeout=60
            )
            for r in late:
                latest[r.key_tuple()] = r
            missing -= set(latest)
        assert not missing, f"{len(missing)} items lost by compaction: {sorted(missing)[:3]}"
        wrong = [
            (o, i)
            for o, i, price in rows
            if latest[(o, i)].value is not None and latest[(o, i)].after["price"] != price
        ]
        assert not wrong, f"stale last version after compaction: {wrong[:3]}"
    finally:
        alter(
            [
                ConfigEntry("segment.ms", "3600000", incremental_operation=AlterConfigOpType.SET),
                ConfigEntry(
                    "min.cleanable.dirty.ratio", "", incremental_operation=AlterConfigOpType.DELETE
                ),
            ]
        )
        cfg = _topic_config(kafka_admin, t)
        assert cfg["segment.ms"] == "3600000" and cfg["min.cleanable.dirty.ratio"] == "0.5"
