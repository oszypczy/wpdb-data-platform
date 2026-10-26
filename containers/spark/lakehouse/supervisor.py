"""Driver main loop: starts the queries, discovers new tables, restarts failed queries.

- ``kafka_to_bronze`` starts at once; a ``silver_<table>`` query is started for every
  bronze table whose Kafka topic exists (a table added at runtime gets its query
  within one discovery round, its PK from the key schema in the registry, 05 §4).
- The gold queries start as soon as the silver tables they read exist.
- A Delta streaming source stops when its table's schema changes (``ADD COLUMN``
  reaching bronze/silver); the supervisor restarts that one query from its checkpoint
  (the container and the other queries keep running). A query failing more than
  ``MAX_FAILURES`` times in ``FAILURE_WINDOW_S`` stops the process with exit code 1
  so Docker restarts the container (``restart: unless-stopped``).
- The loop waits with ``spark.streams.awaitAnyTermination(timeout)`` (never on one
  query), writes the health file for the healthcheck, flushes the stream metrics and
  runs the maintenance job on schedule.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from lakehouse import delta_io, jobs, maintenance
from lakehouse.config import GOLD_SOURCES, Lake, Settings, topic_of_table
from lakehouse.metrics import MetricsListener
from lakehouse.registry import SchemaRegistry

log = logging.getLogger(__name__)

MAX_FAILURES = 10
FAILURE_WINDOW_S = 600
DISCOVERY_EVERY_S = 10
FLUSH_EVERY_S = 30
RETIRE_IDLE_S = 120
MAINTENANCE_TRIGGER = "/tmp/spark/run-maintenance"


@dataclass
class Managed:
    name: str
    starter: Callable[[], StreamingQuery]
    query: StreamingQuery | None = None
    failures: list[float] = field(default_factory=list)
    started_at: float = 0.0
    restarts: int = 0
    last_error: str = ""


class Supervisor:
    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        lake: Lake,
        registry: SchemaRegistry,
        listener: MetricsListener,
    ) -> None:
        self.spark, self.settings, self.lake = spark, settings, lake
        self.registry, self.listener = registry, listener
        self.managed: dict[str, Managed] = {}
        self._admin = None
        self._last_discovery = 0.0
        self._last_flush = time.time()
        self._last_maintenance = time.time()
        self._maintenance: threading.Thread | None = None

    # ------------------------------------------------------------ query control
    def add(self, name: str, starter: Callable[[], StreamingQuery]) -> None:
        if name not in self.managed:
            self.managed[name] = Managed(name, starter)
            self._start(self.managed[name])

    def _start(self, m: Managed) -> None:
        try:
            m.query = m.starter()
            m.started_at = time.time()
            log.info("started query %s (id %s)", m.name, m.query.id)
        except Exception as exc:  # noqa: BLE001 - recorded as a failure, retried
            m.query = None
            self._failed(m, f"start failed: {exc}")

    def _failed(self, m: Managed, error: str) -> None:
        now = time.time()
        m.failures = [t for t in m.failures if now - t < FAILURE_WINDOW_S] + [now]
        m.last_error = error[:2000]
        log.warning(
            "query %s stopped (%d in %d s): %s",
            m.name,
            len(m.failures),
            FAILURE_WINDOW_S,
            error[:500],
        )
        if len(m.failures) > MAX_FAILURES:
            raise SystemExit(f"query {m.name} keeps failing: {error[:500]}")

    def check_queries(self) -> None:
        for m in self.managed.values():
            q = m.query
            if q is not None and q.isActive:
                continue
            if q is not None:
                exc = q.exception()
                self._failed(m, str(exc) if exc else "terminated")
            m.restarts += 1
            self._start(m)

    # ---------------------------------------------------------------- discovery
    def kafka_topics(self) -> set[str] | None:
        try:
            if self._admin is None:
                jvm = self.spark._jvm
                props = jvm.java.util.Properties()
                props.put("bootstrap.servers", self.settings.kafka_bootstrap)
                props.put("request.timeout.ms", "10000")
                self._admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
            unit = self.spark._jvm.java.util.concurrent.TimeUnit.SECONDS
            return set(self._admin.listTopics().names().get(15, unit))
        except Exception as exc:  # noqa: BLE001 - discovery is retried next round
            log.warning("cannot list Kafka topics: %s", exc)
            return None

    def retire_dropped(self, topics: set[str]) -> None:
        """Stop the silver query of a table whose topic was deleted (a dropped test
        table) once it has been idle for RETIRE_IDLE_S: nothing can arrive any more, and
        idle queries would accumulate. Its silver table stays as the last state."""
        now = time.time()
        # silver_<table> queries only: silver_to_gold also starts with "silver_" but reads
        # the silver tables, not a topic, and was stopped and restarted every idle period
        for name in [n for n in self.managed if n.startswith("silver_") and n != "silver_to_gold"]:
            table = name[len("silver_") :]
            if table in GOLD_SOURCES or topic_of_table(table) in topics:
                continue
            m = self.managed[name]
            last = max(self.listener.last_input.get(name, 0.0), m.started_at)
            if now - last < RETIRE_IDLE_S:
                continue
            log.info("retiring %s: topic %s is gone", name, topic_of_table(table))
            try:
                if m.query is not None and m.query.isActive:
                    m.query.stop()
            finally:
                del self.managed[name]

    def discover(self) -> None:
        s, lake = self.spark, self.lake
        topics = self.kafka_topics()
        if topics is not None:
            self.retire_dropped(topics)
        for table in delta_io.list_dirs(s, lake.layer_root("bronze")):
            name = f"silver_{table}"
            if name in self.managed:
                continue
            if topics is not None and topic_of_table(table) not in topics:
                continue  # topic removed (dropped test table): its silver stays as it is
            if not delta_io.is_delta(s, lake.bronze(table)):
                continue
            try:
                pk = self.registry.key_fields(topic_of_table(table))
            except Exception as exc:  # noqa: BLE001 - registry not ready: next round
                log.warning("no key schema for %s yet: %s", table, exc)
                continue
            job = jobs.SilverJob(s, self.settings, lake, table, pk)
            self.add(name, job.start)
        have = all(delta_io.is_delta(s, lake.silver(t)) for t in GOLD_SOURCES)
        if have and "silver_to_gold" not in self.managed:
            self.add("silver_to_gold", jobs.GoldJob(s, self.settings, lake).start)
        if delta_io.is_delta(s, lake.bronze("orders")) and "gold_transitions" not in self.managed:
            self.add("gold_transitions", jobs.TransitionsJob(s, self.settings, lake).start)
        if "silver_to_gold" in self.managed and "gold_agg_revenue" not in self.managed:
            self.add("gold_agg_revenue", jobs.revenue_agg_job(s, self.settings, lake).start)
        if "gold_transitions" in self.managed and "gold_agg_funnel" not in self.managed:
            self.add("gold_agg_funnel", jobs.funnel_agg_job(s, self.settings, lake).start)

    # ------------------------------------------------------------ housekeeping
    def expected(self) -> list[str]:
        return list(self.managed)

    def write_health(self) -> None:
        now = time.time()
        queries = {}
        ok = "kafka_to_bronze" in self.managed
        for m in self.managed.values():
            active = bool(m.query is not None and m.query.isActive)
            ok = ok and active
            queries[m.name] = {
                "active": active,
                "restarts": m.restarts,
                "recent_failures": len(m.failures),
                "last_progress": self.listener.last_progress.get(m.name),
                "last_error": m.last_error[:300],
            }
        doc = {"ts": now, "status": "ok" if ok else "degraded", "queries": queries}
        path = self.settings.health_file
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(doc, f, indent=1)
        os.replace(tmp, path)

    def flush_metrics(self) -> None:
        try:
            from lakehouse.metrics import LATENCY

            self.listener.flush(self.spark, self.lake.ops("stream_metrics"))
            LATENCY.flush(self.spark, self.lake.ops("e2e_latency"))
        except Exception:  # noqa: BLE001 - metrics must not stop the pipeline
            log.exception("flushing stream metrics failed")

    def maybe_maintenance(self) -> None:
        if self._maintenance is not None and self._maintenance.is_alive():
            return
        due = time.time() - self._last_maintenance >= self.settings.maintenance_interval_s
        forced = os.path.exists(MAINTENANCE_TRIGGER)
        if not (due or forced):
            return
        if forced:
            os.remove(MAINTENANCE_TRIGGER)
        self._last_maintenance = time.time()

        def work() -> None:
            try:
                maintenance.run(self.spark, self.lake)
            except Exception:  # noqa: BLE001
                log.exception("maintenance failed")

        self._maintenance = threading.Thread(target=work, name="maintenance", daemon=True)
        self._maintenance.start()

    # --------------------------------------------------------------------- loop
    def run(self) -> None:
        bronze = jobs.BronzeJob(self.spark, self.settings, self.lake, self.registry)
        self.add("kafka_to_bronze", bronze.start)
        while True:
            now = time.time()
            if now - self._last_discovery >= DISCOVERY_EVERY_S:
                self._last_discovery = now
                try:
                    self.discover()
                except Exception:  # noqa: BLE001 - next round
                    log.exception("discovery failed")
            self.check_queries()
            self.write_health()
            if now - self._last_flush >= FLUSH_EVERY_S:
                self._last_flush = now
                self.flush_metrics()
            self.maybe_maintenance()
            # wake up as soon as any query terminates (never block on a single one);
            # a query that died with an error re-raises it here: handled by
            # check_queries() in the next round
            try:
                if self.spark.streams.awaitAnyTermination(5):
                    self.spark.streams.resetTerminated()
            except Exception as exc:  # noqa: BLE001
                log.info("a query terminated: %s", str(exc)[:300])
                self.spark.streams.resetTerminated()
