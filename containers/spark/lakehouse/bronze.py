"""Bronze: decoded Debezium envelopes, append-only, one table per topic (05 §3).

Columns: the full envelope (``op``, ``before``, ``after``; ``before`` is needed by gold,
03 §3), source metadata (``source_lsn``, ``source_tx_id``, ``source_ts``,
``source_snapshot``, ``transaction_id``), Kafka and ingestion metadata
(``kafka_topic``, ``kafka_partition``, ``kafka_offset``, ``kafka_timestamp``,
``key_schema_id``, ``value_schema_id``, ``ingested_at``) plus the message ``key``
(the primary key, 03 §4) and the partition column ``ingest_date`` (06 §7).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ms_to_ts(col: str):
    return F.expr(f"timestamp_millis({col})")


def envelope_rows(decoded: DataFrame) -> DataFrame:
    """Decoded rows of one ``olist.shop.*`` topic with a valid envelope -> bronze rows."""
    ok = decoded.filter(F.col("value").isNotNull())
    return ok.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "key_schema_id",
        "value_schema_id",
        "key",
        F.col("value.op").alias("op"),
        F.col("value.before").alias("before"),
        F.col("value.after").alias("after"),
        F.col("value.source.lsn").alias("source_lsn"),
        F.col("value.source.txId").alias("source_tx_id"),
        ms_to_ts("value.source.ts_ms").alias("source_ts"),
        F.col("value.source.snapshot").alias("source_snapshot"),
        F.col("value.transaction.id").alias("transaction_id"),
        ms_to_ts("value.ts_ms").alias("dbz_ts"),
        F.current_timestamp().alias("ingested_at"),
    ).withColumn("ingest_date", F.to_date("ingested_at"))


def undecodable_rows(decoded: DataFrame) -> DataFrame:
    """Messages whose value could not be decoded (no Confluent header, corrupt Avro):
    the stream goes on, the record is reported in ops/quarantine (05 §6)."""
    bad = decoded.filter(F.col("value").isNull())
    return bad.select(
        F.lit("avro_decodable").alias("rule"),
        F.lit("reject").alias("severity"),
        F.col("kafka_topic").alias("source_table"),
        F.concat_ws(
            "/", "kafka_topic", F.col("kafka_partition").cast("string"), F.col("kafka_offset")
        ).alias("record_key"),
        F.to_json(
            F.struct("kafka_partition", "kafka_offset", "key_schema_id", "value_schema_id")
        ).alias("payload"),
    )


def heartbeat_rows(decoded: DataFrame) -> DataFrame:
    """``__debezium-heartbeat.olist`` -> ops/cdc_heartbeat (05 §7)."""
    ok = decoded.filter(F.col("value").isNotNull())
    return ok.select(
        F.col("key.serverName").alias("server_name"),
        ms_to_ts("value.ts_ms").alias("heartbeat_ts"),
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("ingested_at"),
    )
