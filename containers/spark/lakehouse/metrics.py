"""StreamingQueryListener -> ops/stream_metrics (05 §7).

The listener only buffers progress events (it runs on Spark's listener bus thread and
must return quickly); the supervisor flushes the buffer every ``flush_every`` seconds
with one append. Idle triggers are recorded at most once a minute per query.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from lakehouse.tables import OPS

log = logging.getLogger(__name__)


def _ts(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    return dt.datetime.strptime(text.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")


def progress_row(p: Any) -> dict[str, Any]:
    behind = None
    for s in p.sources or []:
        metrics = getattr(s, "metrics", None) or {}
        v = metrics.get("maxOffsetsBehindLatest")
        if v is not None:
            behind = max(behind or 0, int(float(v)))
    durations = p.durationMs or {}
    return {
        "query_name": p.name,
        "query_id": str(p.id),
        "run_id": str(p.runId),
        "batch_id": int(p.batchId),
        "progress_at": _ts(p.timestamp),
        "num_input_rows": int(p.numInputRows or 0),
        "input_rows_per_second": float(p.inputRowsPerSecond or 0.0),
        "processed_rows_per_second": float(p.processedRowsPerSecond or 0.0),
        "batch_duration_ms": int(durations.get("triggerExecution", 0) or 0),
        "add_batch_ms": int(durations.get("addBatch", 0) or 0),
        "offsets_behind_latest": behind,
        "source": ";".join((s.description or "")[:120] for s in (p.sources or [])),
        "recorded_at": None,
    }


class MetricsListener(StreamingQueryListener):
    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()
        self.last_progress: dict[str, float] = {}
        self.last_input: dict[str, float] = {}

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        try:
            p = event.progress
            now = time.time()
            name = p.name or str(p.id)
            self.last_progress[name] = now
            idle = int(p.numInputRows or 0) == 0
            if not idle:
                self.last_input[name] = now
            if idle and now - self._last.get(name, 0) < 60:
                return
            row = progress_row(p)
            with self._lock:
                self._rows.append(row)
                self._last[name] = now
        except Exception:  # noqa: BLE001 - never break the listener bus
            log.exception("metrics listener failed")

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass

    def drain(self) -> list[dict[str, Any]]:
        with self._lock:
            rows, self._rows = self._rows, []
        return rows

    def flush(self, spark: SparkSession, path: str) -> int:
        rows = self.drain()
        if not rows:
            return 0
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        for r in rows:
            r["recorded_at"] = now
        schema = OPS["stream_metrics"].schema
        df = spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)
        df.coalesce(1).write.format("delta").mode("append").save(path)
        return len(rows)


class LatencyBuffer:
    """End-to-end latency summaries computed by the jobs (one row per batch and table),
    buffered and flushed with the stream metrics: one commit per 30 s instead of one
    per micro-batch and table."""

    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def add(self, rows: list[dict[str, Any]]) -> None:
        with self._lock:
            self._rows.extend(rows)

    def flush(self, spark: SparkSession, path: str) -> int:
        with self._lock:
            rows, self._rows = self._rows, []
        if not rows:
            return 0
        schema = OPS["e2e_latency"].schema
        df = spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)
        df.coalesce(1).write.format("delta").mode("append").save(path)
        return len(rows)


LATENCY = LatencyBuffer()
