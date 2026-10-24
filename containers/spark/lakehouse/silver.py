"""Silver: the current state of every source table, a mirror of the database (05 §4).

Pure parts: bronze rows -> changes (one row per event: the row image plus metadata),
deduplication within a batch (highest LSN per primary key) and the MERGE conditions.
The MERGE itself is in ``delta_io.merge_silver``.

Metadata columns (prefixed with ``_``): ``_lsn`` (Postgres LSN of the change, the
guard), ``_op`` (c/u/d/r), ``_is_deleted`` (soft delete: the tombstone row remembers
the LSN of the delete, so a late duplicate ``c`` cannot resurrect it), ``_source_ts``
(commit time in Postgres), ``_tx_id``, ``_kafka_partition``, ``_kafka_offset``,
``_ingested_at`` (bronze) and ``_updated_at`` (silver write time).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

META = [
    "_lsn",
    "_op",
    "_is_deleted",
    "_source_ts",
    "_tx_id",
    "_kafka_partition",
    "_kafka_offset",
    "_ingested_at",
]


def changes(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> one row per change: the row image (``after``; ``before`` for a
    delete, complete thanks to REPLICA IDENTITY FULL) flattened, plus metadata."""
    image = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    with_row = bronze.filter(F.col("op").isin("c", "u", "d", "r")).withColumn("_row", image)
    with_row = with_row.filter(F.col("_row").isNotNull())
    return with_row.select(
        "_row.*",
        F.coalesce(F.col("source_lsn"), F.lit(0)).cast("long").alias("_lsn"),
        F.col("op").alias("_op"),
        (F.col("op") == "d").alias("_is_deleted"),
        F.col("source_ts").alias("_source_ts"),
        F.col("source_tx_id").alias("_tx_id"),
        F.col("kafka_partition").alias("_kafka_partition"),
        F.col("kafka_offset").alias("_kafka_offset"),
        F.col("ingested_at").alias("_ingested_at"),
    )


def latest_per_key(df: DataFrame, pk: list[str]) -> DataFrame:
    """Step 1 (05 §4): per primary key keep the change with the highest LSN; equal
    LSNs are re-deliveries of the same change (at-least-once), the later offset wins."""
    w = Window.partitionBy(*[F.col(f"`{c}`") for c in pk]).orderBy(
        F.col("_lsn").desc(), F.col("_kafka_partition").desc(), F.col("_kafka_offset").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def merge_condition(pk: list[str], target: str = "t", source: str = "s") -> str:
    return " AND ".join(f"{target}.`{c}` = {source}.`{c}`" for c in pk)


# step 2 (05 §4): the LSN guard; duplicates and older events are ignored
GUARD = "s._lsn > t._lsn"
DELETE_CLAUSE = f"{GUARD} AND s._op = 'd'"


def value_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if not c.startswith("_")]
