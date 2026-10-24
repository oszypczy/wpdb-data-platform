"""Reading and writing Delta tables (IO, kept apart from the pure transforms).

- ``ensure_table``: create a table with its schema, partitioning, properties and CHECK
  constraints if it does not exist (06 §6).
- ``append_idempotent``: blind append with ``txnAppId``/``txnVersion`` so a re-executed
  micro-batch is ignored (05 §3).
- ``merge_silver``: MERGE with the LSN guard and soft delete (05 §4).
- ``diff`` / ``sync_subset``: make the rows of a gold table inside a scope equal to a
  desired DataFrame (insert, update, delete) and return the differences (05 §5).
- ``with_conflict_retry``: retry a Delta commit that lost a race with a concurrent
  writer (e.g. OPTIMIZE of the maintenance job, 06 §7).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from typing import TypeVar

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from lakehouse import silver
from lakehouse.schema_align import conform, merge_structs, nullable

log = logging.getLogger(__name__)
T = TypeVar("T")

# readers (delta-rs, DuckDB) must open every table: no deletion vectors, no column
# mapping, no identity columns, no timestampNtz (06 §9, §12)
MUTABLE_PROPERTIES = {
    "delta.dataSkippingNumIndexedCols",
    "delta.autoOptimize.autoCompact",
    "delta.deletedFileRetentionDuration",
    "delta.logRetentionDuration",
}
BASE_PROPERTIES = {
    "delta.enableDeletionVectors": "false",
    "delta.columnMapping.mode": "none",
}


def is_delta(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:  # noqa: BLE001 - missing bucket/prefix = not a table
        return False


def ensure_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    properties: dict[str, str] | None = None,
    partition_by: Iterable[str] = (),
    constraints: dict[str, str] | None = None,
    comment: str | None = None,
) -> DeltaTable:
    props = {**BASE_PROPERTIES, **(properties or {})}
    if not is_delta(spark, path):
        # a creation that died half-way (e.g. OOM) can leave a _delta_log/ without commit
        # 0, which Delta then takes for an existing table: remove it, nothing was committed
        _drop_uncommitted_log(spark, path)
        b = DeltaTable.create(spark).location(path).addColumns(schema)
        parts = list(partition_by)
        if parts:
            b = b.partitionedBy(*parts)
        for k, v in props.items():
            b = b.property(k, v)
        if comment:
            b = b.comment(comment)
        try:
            b.execute()
        except Exception:
            if not is_delta(spark, path):  # lost a creation race: the table exists now
                raise
    dt = DeltaTable.forPath(spark, path)
    current = dt.detail().select("properties").first()[0] or {}
    # properties that can change on an existing table without rewriting it
    drift = {k: v for k, v in props.items() if k in MUTABLE_PROPERTIES and current.get(k) != v}
    if drift:
        assigns = ", ".join(f"'{k}' = '{v}'" for k, v in drift.items())
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({assigns})")
    if constraints:
        for name, expr in constraints.items():
            if f"delta.constraints.{name.lower()}" not in current:
                spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {name} CHECK ({expr})")
    return dt


def _drop_uncommitted_log(spark: SparkSession, path: str) -> None:
    jvm = spark._jvm
    log_dir = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
    fs = log_dir.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(log_dir):
        return
    files = [st.getPath().getName() for st in fs.listStatus(log_dir)]
    if not any(f.endswith(".json") or f.endswith(".parquet") for f in files):
        log.warning("removing uncommitted %s (%s)", log_dir.toString(), files)
        fs.delete(log_dir, True)


def table_schema(spark: SparkSession, path: str) -> StructType | None:
    if not is_delta(spark, path):
        return None
    return spark.read.format("delta").load(path).schema


def is_conflict(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    return any(
        k in text
        for k in (
            "ConcurrentAppendException",
            "ConcurrentDeleteReadException",
            "ConcurrentDeleteDeleteException",
            "ConcurrentTransactionException",
            "MetadataChangedException",
            "ProtocolChangedException",
            "DELTA_CONCURRENT",
        )
    )


def with_conflict_retry(fn: Callable[[], T], attempts: int = 6, what: str = "commit") -> T:
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if i == attempts - 1 or not is_conflict(exc):
                raise
            log.warning("%s lost a concurrent commit race (%s), retry %d", what, exc, i + 1)
            time.sleep(0.5 * (i + 1))
    raise AssertionError("unreachable")


def append_idempotent(
    df: DataFrame,
    path: str,
    app_id: str,
    version: int,
    merge_schema: bool = False,
    partition_by: Iterable[str] = (),
) -> None:
    w = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", str(version))
    )
    if merge_schema:
        w = w.option("mergeSchema", "true")
    parts = list(partition_by)
    if parts:
        w = w.partitionBy(*parts)
    with_conflict_retry(lambda: w.save(path), what=f"append {path}")


# ---------------------------------------------------------------------------- silver
def merge_silver(
    spark: SparkSession, path: str, batch: DataFrame, pk: list[str], properties: dict[str, str]
) -> None:
    """MERGE of one deduplicated batch with the LSN guard (05 §4).

    The batch is conformed to the table's types first (a forced type change in the
    source is cast to the existing type); new columns are added by schema evolution."""
    batch = batch.withColumn("_updated_at", F.current_timestamp())
    current = table_schema(spark, path)
    if current is None:
        ensure_table(spark, path, nullable(batch.schema), properties=properties)
        current = table_schema(spark, path)
    merged = merge_structs(current, nullable(batch.schema))
    src = conform(batch, merged)

    def run() -> None:
        t = DeltaTable.forPath(spark, path)
        set_delete = {
            "_is_deleted": "true",
            "_lsn": "s._lsn",
            "_op": "s._op",
            "_source_ts": "s._source_ts",
            "_tx_id": "s._tx_id",
            "_kafka_partition": "s._kafka_partition",
            "_kafka_offset": "s._kafka_offset",
            "_ingested_at": "s._ingested_at",
            "_updated_at": "s._updated_at",
        }
        (
            t.alias("t")
            .merge(src.alias("s"), silver.merge_condition(pk))
            .withSchemaEvolution()
            .whenMatchedUpdate(condition=silver.DELETE_CLAUSE, set=set_delete)
            .whenMatchedUpdateAll(condition=silver.GUARD)
            .whenNotMatchedInsertAll()  # also for 'd': inserted as deleted
            .execute()
        )

    with_conflict_retry(run, what=f"merge {path}")


# ------------------------------------------------------------------------------ gold
def diff(
    current: DataFrame, desired: DataFrame, keys: list[str], value_cols: list[str]
) -> DataFrame:
    """Full outer join of the current and the desired rows on ``keys``.

    Result columns: keys, ``_old`` (struct of value_cols or null), ``_new`` (struct or
    null), ``_action`` in (insert, update, delete); unchanged rows are dropped."""
    c = current.select(*keys, F.struct(*value_cols).alias("_old"))
    d = desired.select(*keys, F.struct(*value_cols).alias("_new"))
    j = c.join(d, keys, "full_outer")
    action = (
        F.when(F.col("_old").isNull(), F.lit("insert"))
        .when(F.col("_new").isNull(), F.lit("delete"))
        .when(~F.col("_old").eqNullSafe(F.col("_new")), F.lit("update"))
    )
    return j.withColumn("_action", action).filter(F.col("_action").isNotNull())


def diff_subset(
    spark: SparkSession,
    path: str,
    desired: DataFrame,
    keys: list[str],
    scope: DataFrame | None = None,
    scope_cols: list[str] | None = None,
) -> DataFrame:
    """The differences between the target rows within ``scope`` (a DataFrame of
    ``scope_cols``; None = the whole table) and ``desired`` (see ``diff``), materialised
    without lineage (``localCheckpoint``)."""
    target = spark.read.format("delta").load(path)
    value_cols = [c for c in target.columns if c not in keys and c != "_updated_at"]
    desired = desired.select(*keys, *value_cols)
    current = target if scope is None else target.join(scope, scope_cols, "left_semi")
    return diff(current, desired, keys, value_cols).localCheckpoint(eager=True)


def apply_diff(spark: SparkSession, path: str, changes: DataFrame, keys: list[str]) -> None:
    """One MERGE that applies a ``diff_subset`` result (insert, update, delete)."""
    if changes.isEmpty():
        return
    value_cols = [f.name for f in changes.schema["_new"].dataType.fields]
    src = changes.select(*keys, "_action", *[F.col(f"_new.`{c}`").alias(c) for c in value_cols])
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    assign = {f"`{c}`": f"s.`{c}`" for c in value_cols}
    assign["_updated_at"] = "current_timestamp()"

    def run() -> None:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(src.alias("s"), cond)
            .whenMatchedDelete(condition="s._action = 'delete'")
            .whenMatchedUpdate(condition="s._action = 'update'", set=assign)
            .whenNotMatchedInsert(
                condition="s._action = 'insert'",
                values={**{f"`{k}`": f"s.`{k}`" for k in keys}, **assign},
            )
            .execute()
        )

    with_conflict_retry(run, what=f"sync {path}")


def sync_subset(
    spark: SparkSession,
    path: str,
    desired: DataFrame,
    keys: list[str],
    scope: DataFrame | None = None,
    scope_cols: list[str] | None = None,
) -> DataFrame:
    """Make the target rows within ``scope`` equal to ``desired``; returns the diff."""
    changes = diff_subset(spark, path, desired, keys, scope, scope_cols)
    apply_diff(spark, path, changes, keys)
    return changes


def upsert(
    spark: SparkSession,
    path: str,
    rows: DataFrame,
    keys: list[str],
    insert_only: bool = False,
    update_cols: list[str] | None = None,
) -> None:
    """MERGE rows into a table by ``keys``: insert new keys; update ``update_cols`` of
    existing ones (all columns when None) unless ``insert_only``."""
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)

    def run() -> None:
        m = DeltaTable.forPath(spark, path).alias("t").merge(rows.alias("s"), cond)
        if not insert_only:
            if update_cols is None:
                m = m.whenMatchedUpdateAll()
            else:
                m = m.whenMatchedUpdate(set={f"`{c}`": f"s.`{c}`" for c in update_cols})
        m.whenNotMatchedInsertAll().execute()

    with_conflict_retry(run, what=f"upsert {path}")


# --------------------------------------------------------------------- filesystem
def list_dirs(spark: SparkSession, uri: str) -> list[str]:
    """Names of the direct sub-directories of ``uri`` (Hadoop FS: s3a:// or file://)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = p.getFileSystem(conf)
    if not fs.exists(p):
        return []
    return sorted(s.getPath().getName() for s in fs.listStatus(p) if s.isDirectory())


def read_text(spark: SparkSession, uri: str) -> str | None:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = p.getFileSystem(conf)
    if not fs.exists(p):
        return None
    stream = fs.open(p)
    try:
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        return "\n".join(lines)
    finally:
        stream.close()


def write_text(spark: SparkSession, uri: str, text: str) -> None:
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = p.getFileSystem(conf)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode()))
    finally:
        out.close()


def delete_path(spark: SparkSession, uri: str) -> None:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(uri)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(p):
        fs.delete(p, True)
