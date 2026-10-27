"""Settings from the environment (docker-compose.yml `spark` service, .env.example).

Secrets only come from the environment (06 §10); nothing here has a default secret.
Paths are URIs, so the same code runs on MinIO (``s3a://``) and on the local
filesystem in unit tests (``file:///tmp/...``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

LAYERS = ("bronze", "silver", "gold", "ops", "checkpoints")

# Debezium topics of the shop schema (03 §13) and the heartbeat topic (03 §7)
TOPIC_PREFIX = "olist.shop."
HEARTBEAT_TOPIC = "__debezium-heartbeat.olist"
SUBSCRIBE_PATTERN = r"olist\.shop\..*|__debezium-heartbeat\.olist"

# silver tables the star schema is built from (05 §5); gold waits until they exist
GOLD_SOURCES = (
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "product_category_name_translation",
)


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"environment variable {name} is required")
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "kafka:29092"
    schema_registry_url: str = "http://schema-registry:8081"
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    # root of every layer: "s3a://" -> s3a://bronze/..., or a local dir for tests
    base_uri: str = "s3a://"
    trigger: str = "10 seconds"
    # gold and the aggregates read already-processed layers: a slower trigger halves
    # their per-batch overhead without changing the latency much (05 §8)
    gold_trigger: str = "20 seconds"
    max_offsets_per_trigger: int = 50000
    shuffle_partitions: int = 4
    # event-time watermark of the windowed aggregates, in *simulated* time. At
    # SIM_SPEED=1440 one wall second is 24 simulated minutes, so the delay must cover
    # the pipeline's wall-clock lateness (a few triggers) times SIM_SPEED (05 §5).
    agg_watermark: str = "24 hours"
    agg_window: str = "1 hour"
    maintenance_interval_s: int = 1800
    health_file: str = "/tmp/spark/health.json"
    extra: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_env() -> Settings:
        return Settings(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "kafka:29092"),
            schema_registry_url=_env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            s3_endpoint=_env("S3_ENDPOINT", "http://minio:9000"),
            s3_access_key=_env("MINIO_SPARK_ACCESS_KEY"),
            s3_secret_key=_env("MINIO_SPARK_SECRET_KEY"),
            base_uri=_env("LAKE_BASE_URI", "s3a://"),
            trigger=_env("SPARK_TRIGGER", "10 seconds"),
            gold_trigger=_env("SPARK_GOLD_TRIGGER", "20 seconds"),
            max_offsets_per_trigger=int(_env("SPARK_MAX_OFFSETS_PER_TRIGGER", "50000")),
            shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "4")),
            agg_watermark=_env("AGG_WATERMARK", "24 hours"),
            maintenance_interval_s=int(_env("MAINTENANCE_INTERVAL_S", "1800")),
            health_file=_env("SPARK_HEALTH_FILE", "/tmp/spark/health.json"),
        )


class Lake:
    """Table and checkpoint locations: one bucket per layer (06 §2)."""

    def __init__(self, base_uri: str = "s3a://") -> None:
        self.base = base_uri if base_uri.endswith("/") else base_uri + "/"

    def path(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}")
        return f"{self.base}{layer}/{name}"

    def bronze(self, table: str) -> str:
        return self.path("bronze", table)

    def silver(self, table: str) -> str:
        return self.path("silver", table)

    def gold(self, table: str) -> str:
        return self.path("gold", table)

    def ops(self, table: str) -> str:
        return self.path("ops", table)

    def checkpoint(self, query: str) -> str:
        return self.path("checkpoints", query)

    def layer_root(self, layer: str) -> str:
        return f"{self.base}{layer}/"


def table_of_topic(topic: str) -> str | None:
    """`olist.shop.order_items` -> `order_items`; other topics -> None."""
    if topic.startswith(TOPIC_PREFIX):
        return topic[len(TOPIC_PREFIX) :]
    return None


def topic_of_table(table: str) -> str:
    return TOPIC_PREFIX + table
