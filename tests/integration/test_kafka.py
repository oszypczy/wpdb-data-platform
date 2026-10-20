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
