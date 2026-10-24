"""Table maintenance (06 §7): OPTIMIZE (+ Z-ORDER on the facts) and VACUUM, measured.

Runs as a periodic batch job inside the Spark container (no extra component): the
supervisor starts it every ``MAINTENANCE_INTERVAL_S`` (default 30 min) or when the
file ``/tmp/spark/run-maintenance`` appears. Every run appends one row per table and
operation to ops/maintenance_log with files and bytes before/after and the time of a
typical query before/after. Concurrent streaming commits may win the race; OPTIMIZE is
retried (``delta_io.with_conflict_retry``) and the streaming MERGEs retry as well.

Never use MinIO lifecycle rules on Delta tables: only VACUUM removes Delta files.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
import uuid
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse import delta_io
from lakehouse.config import Lake
from lakehouse.tables import GOLD, OPS, RETENTION_HOURS

log = logging.getLogger(__name__)

# typical dashboard-like query per table, timed before and after OPTIMIZE
PROBE_QUERIES = {
    "fact_order_items": (
        "SELECT customer_state, sum(price) FROM delta.`{p}` "
        "WHERE customer_state = 'SP' GROUP BY customer_state"
    ),
    "fact_orders_lifecycle": (
        "SELECT customer_state, avg(d_to_deliver) FROM delta.`{p}` "
        "WHERE customer_state = 'RJ' GROUP BY customer_state"
    ),
}


def detail(spark: SparkSession, path: str) -> tuple[int, int]:
    d = DeltaTable.forPath(spark, path).detail().select("numFiles", "sizeInBytes").first()
    return int(d[0]), int(d[1])


def timed_query(spark: SparkSession, sql: str, repeat: int = 3) -> float:
    best = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        ms = (time.perf_counter() - t0) * 1000
        best = ms if best is None else min(best, ms)
    return float(best or 0.0)


def optimize(spark: SparkSession, path: str, zorder: tuple[str, ...] = ()) -> dict[str, Any]:
    name = path.rstrip("/").rsplit("/", 1)[-1]
    probe = PROBE_QUERIES.get(name)
    f0, b0 = detail(spark, path)
    q0 = timed_query(spark, probe.format(p=path)) if probe else None
    t0 = time.perf_counter()

    def run():
        opt = DeltaTable.forPath(spark, path).optimize()
        return opt.executeZOrderBy(*zorder) if zorder else opt.executeCompaction()

    metrics = delta_io.with_conflict_retry(run, what=f"optimize {path}")
    dur = int((time.perf_counter() - t0) * 1000)
    f1, b1 = detail(spark, path)
    q1 = timed_query(spark, probe.format(p=path)) if probe else None
    details = metrics.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
    return {
        "table_path": path,
        "operation": "OPTIMIZE ZORDER BY (" + ", ".join(zorder) + ")" if zorder else "OPTIMIZE",
        "duration_ms": dur,
        "files_before": f0,
        "files_after": f1,
        "bytes_before": b0,
        "bytes_after": b1,
        "query_ms_before": q0,
        "query_ms_after": q1,
        "details": f"added={details[0]} removed={details[1]}",
    }


def vacuum(spark: SparkSession, path: str, retention_hours: float) -> dict[str, Any]:
    f0, b0 = detail(spark, path)
    t0 = time.perf_counter()
    delta_io.with_conflict_retry(
        lambda: DeltaTable.forPath(spark, path).vacuum(retention_hours), what=f"vacuum {path}"
    )
    f1, b1 = detail(spark, path)
    return {
        "table_path": path,
        "operation": f"VACUUM RETAIN {retention_hours:g} HOURS",
        "duration_ms": int((time.perf_counter() - t0) * 1000),
        "files_before": f0,
        "files_after": f1,
        "bytes_before": b0,
        "bytes_after": b1,
        "query_ms_before": None,
        "query_ms_after": None,
        "details": "",
    }


def tables(spark: SparkSession, lake: Lake) -> list[tuple[str, str, tuple[str, ...]]]:
    out = []
    for layer in ("bronze", "silver"):
        for name in delta_io.list_dirs(spark, lake.layer_root(layer)):
            out.append((layer, lake.path(layer, name), ()))
    for spec in GOLD.values():
        out.append(("gold", lake.gold(spec.name), spec.zorder))
    for spec in OPS.values():
        out.append(("ops", lake.ops(spec.name), ()))
    return out


def run(spark: SparkSession, lake: Lake, vacuum_tables: bool = True) -> list[dict[str, Any]]:
    run_id = str(uuid.uuid4())
    started = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    rows: list[dict[str, Any]] = []
    for layer, path, zorder in tables(spark, lake):
        if not delta_io.is_delta(spark, path):
            continue
        try:
            # bronze and silver are compacted by autoCompact on write; gold and ops here
            if layer in ("gold", "ops"):
                rows.append(optimize(spark, path, zorder))
                log.info("maintenance %s: %s", path, rows[-1])
            if vacuum_tables:
                rows.append(vacuum(spark, path, RETENTION_HOURS[layer]))
                log.info("maintenance %s: %s", path, rows[-1])
        except Exception:  # noqa: BLE001 - one table must not stop the others
            log.exception("maintenance of %s failed", path)
    if rows:
        schema = OPS["maintenance_log"].schema
        recs = [{"run_id": run_id, "started_at": started, **r} for r in rows]
        df = spark.createDataFrame(
            [tuple(r.get(f.name) for f in schema.fields) for r in recs], schema
        )
        df.coalesce(1).write.format("delta").mode("append").save(lake.ops("maintenance_log"))
    log.info("maintenance run %s: %d operations", run_id, len(rows))
    return rows
