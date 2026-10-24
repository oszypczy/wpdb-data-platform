"""Confluent AVRO wire format -> Spark structs (05 §2).

A Confluent message is ``0x00`` + 4-byte big-endian schema id + Avro binary. Spark's
``from_avro`` understands neither the header nor several schemas in one column, so a
micro-batch is grouped by ``(topic, key schema id, value schema id)``, each group is
decoded with its own writer schema (resolved by id, never ``latest``: after ``DROP
COLUMN`` the registry hands out the v1 id again, 03 §13) and the groups are unioned
into one schema per topic (``schema_align.union_conformed``). Tombstones (value
``null``, one after every delete, 04 §2) are dropped before decoding.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType

from lakehouse.schema_align import union_conformed

MAGIC = b"\x00"


def schema_id(col: str) -> Column:
    """Schema id from bytes 1-4 of a Confluent-framed column; null if not framed."""
    c = F.col(col)
    framed = (F.length(c) >= 5) & (F.hex(F.substring(c, 1, 1)) == F.lit("00"))
    return F.when(framed, F.conv(F.hex(F.substring(c, 2, 4)), 16, 10).cast("int"))


def payload(col: str) -> Column:
    """The Avro body after the 5-byte header."""
    return F.expr(f"substring(`{col}`, 6, length(`{col}`) - 5)")


def with_schema_ids(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.withColumn("key_schema_id", schema_id("key")).withColumn(
        "value_schema_id", schema_id("value")
    )


def drop_tombstones(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.filter(F.col("value").isNotNull())


def decode(
    kafka_df: DataFrame,
    resolve: Callable[[int], str],
    existing: dict[str, StructType] | None = None,
) -> dict[str, DataFrame]:
    """Decode a Kafka micro-batch into one DataFrame per topic.

    ``kafka_df`` has the Kafka source columns (key, value, topic, partition, offset,
    timestamp). ``resolve(id)`` returns the writer schema text. ``existing`` maps a
    topic to the ``value`` struct type already stored for it (its types win). Every
    result has: kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
    key_schema_id, value_schema_id, key (struct), value (struct; null = undecodable).
    """
    framed = with_schema_ids(drop_tombstones(kafka_df))
    combos = (
        framed.select("topic", "key_schema_id", "value_schema_id")
        .distinct()
        .orderBy("topic", "value_schema_id", "key_schema_id")
        .collect()
    )
    groups: dict[str, list[DataFrame]] = defaultdict(list)
    for row in combos:
        topic, kid, vid = row["topic"], row["key_schema_id"], row["value_schema_id"]
        part = framed.filter(
            (F.col("topic") == topic)
            & F.col("key_schema_id").eqNullSafe(F.lit(kid))
            & F.col("value_schema_id").eqNullSafe(F.lit(vid))
        )
        key = (
            from_avro(payload("key"), resolve(kid), {"mode": "PERMISSIVE"})
            if kid is not None
            else F.lit(None)
        )
        if vid is None:
            # not Confluent-framed: kept with value = null so it ends up in quarantine
            value = F.lit(None)
        else:
            value = from_avro(payload("value"), resolve(vid), {"mode": "PERMISSIVE"})
        groups[topic].append(
            part.select(
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                "key_schema_id",
                "value_schema_id",
                key.alias("key"),
                value.alias("value"),
            )
        )
    out: dict[str, DataFrame] = {}
    for topic, frames in groups.items():
        typed = [f for f in frames if not _is_null_type(f, "value")]
        if not typed:
            out[topic] = frames[0]
            continue
        first = None
        if existing and topic in existing:
            first = StructType(
                [f for f in typed[0].schema.fields if f.name != "value"]
                + [_field("value", existing[topic])]
            )
        aligned = union_conformed(typed, first)
        rest = [f for f in frames if _is_null_type(f, "value")]
        for f in rest:
            aligned = aligned.unionByName(
                f.withColumn(
                    "value", F.lit(None).cast(aligned.schema["value"].dataType)
                ).withColumn("key", F.lit(None).cast(aligned.schema["key"].dataType))
            )
        out[topic] = aligned
    return out


def _is_null_type(df: DataFrame, col: str) -> bool:
    return df.schema[col].dataType.typeName() == "void"


def _field(name: str, dt):
    from pyspark.sql.types import StructField

    return StructField(name, dt, True)
