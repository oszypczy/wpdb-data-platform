"""The streaming queries (05 §1-§7): sources, sinks and the foreachBatch functions.

Every query has its own checkpoint in the ``checkpoints`` bucket (06 §8):

=====================  ===========================================  ======================
query                  source                                       sinks
=====================  ===========================================  ======================
kafka_to_bronze        Kafka ``olist.shop.*`` + heartbeat topic     bronze/<table>,
                                                                    ops/cdc_heartbeat
silver_<table>         bronze/<table> (Delta stream)                silver/<table>
silver_to_gold         CDF of the 8 silver tables of GOLD_SOURCES   gold dims + facts,
                                                                    gold/revenue_events,
                                                                    ops/quarantine
gold_transitions       bronze/orders (Delta stream)                 gold/status_transitions
gold_agg_revenue       gold/revenue_events (Delta stream)           gold/agg_revenue_hourly
gold_agg_funnel        gold/status_transitions (Delta stream)       gold/agg_status_funnel
=====================  ===========================================  ======================

Blind appends carry ``txnAppId``/``txnVersion`` = (query token, batch id), so a
re-executed batch is skipped by Delta (05 §3); the token is stored next to the
checkpoint and changes when the checkpoint is wiped (a fresh checkpoint restarts the
batch ids at 0). MERGEs are idempotent by construction (LSN guard, replacement).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from lakehouse import avro, bronze, delta_io, gold, quality, silver
from lakehouse.config import (
    GOLD_SOURCES,
    HEARTBEAT_TOPIC,
    SUBSCRIBE_PATTERN,
    Lake,
    Settings,
    table_of_topic,
)
from lakehouse.registry import SchemaRegistry
from lakehouse.schema_align import conform, merge_structs, nullable
from lakehouse.tables import (
    BRONZE_PROPERTIES,
    GOLD,
    OPS,
    PK_PROPERTY,
    SILVER_PROPERTIES,
    TableSpec,
)

log = logging.getLogger(__name__)


def txn_app_id(spark: SparkSession, checkpoint: str, query: str) -> str:
    """A token per checkpoint generation: ``<query>:<uuid>`` stored in the checkpoint."""
    marker = checkpoint.rstrip("/") + "/_wpdb_txn_app_id"
    token = delta_io.read_text(spark, marker)
    if not token:
        token = f"{query}:{uuid.uuid4()}"
        delta_io.write_text(spark, marker, token)
    return token.strip()


def ensure_spec(spark: SparkSession, lake: Lake, spec: TableSpec) -> None:
    delta_io.ensure_table(
        spark,
        lake.path(spec.layer, spec.name),
        spec.schema,
        properties=spec.properties,
        partition_by=spec.partition_by,
        constraints=spec.constraints,
        comment=spec.comment or None,
    )


def ensure_gold_and_ops(spark: SparkSession, lake: Lake) -> None:
    for spec in [*GOLD.values(), *OPS.values()]:
        ensure_spec(spark, lake, spec)
    # static dimensions: filled once
    for name, build in (("dim_date", gold.dim_date), ("dim_geography", gold.dim_geography)):
        path = lake.gold(name)
        if spark.read.format("delta").load(path).isEmpty():
            build(spark).write.format("delta").mode("append").save(path)


def latency_rows(
    changes: DataFrame, layer: str, table: str, batch_id: int, measured: dt.datetime
) -> DataFrame:
    """End-to-end latency (05 §7): write time in this layer minus the commit time in
    Postgres, over the live changes of a batch (snapshot reads are excluded)."""
    live = changes.filter(F.col("_op").isin("c", "u", "d") & F.col("_source_ts").isNotNull())
    ms = (F.lit(measured).cast("double") - F.col("_source_ts").cast("double")) * 1000.0
    return (
        live.select(ms.alias("ms"))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("ms").alias("latency_ms_min"),
            F.percentile_approx("ms", 0.5).alias("latency_ms_p50"),
            F.percentile_approx("ms", 0.95).alias("latency_ms_p95"),
            F.max("ms").alias("latency_ms_max"),
        )
        .select(
            F.lit(layer).alias("layer"),
            F.lit(table).alias("table_name"),
            F.lit(batch_id).cast("long").alias("batch_id"),
            F.lit(measured).cast("timestamp").alias("measured_at"),
            F.col("rows").cast("long"),
            F.col("latency_ms_min").cast("double"),
            F.col("latency_ms_p50").cast("double"),
            F.col("latency_ms_p95").cast("double"),
            F.col("latency_ms_max").cast("double"),
        )
        .filter("rows > 0")
    )


def freeze(df: DataFrame) -> DataFrame:
    """Materialise an intermediate result and cut its lineage (``localCheckpoint``):
    a persisted DataFrame still carries its whole plan, and the plans of the gold batch
    grew so large that printing them (AQE, UI) ran the driver out of memory."""
    return df.localCheckpoint(eager=True)


def record_latency(summary: DataFrame) -> None:
    from lakehouse.metrics import LATENCY

    LATENCY.add([r.asDict() for r in summary.collect()])


def write_quarantine(spark: SparkSession, lake: Lake, rows: DataFrame) -> None:
    now = F.current_timestamp()
    rows = rows.dropDuplicates(["rule", "source_table", "record_key"]).select(
        "rule",
        "severity",
        "source_table",
        "record_key",
        "payload",
        now.alias("first_seen_at"),
        now.alias("last_seen_at"),
    )
    if rows.isEmpty():
        return
    delta_io.upsert(
        spark,
        lake.ops("quarantine"),
        rows,
        ["rule", "source_table", "record_key"],
        update_cols=["severity", "payload", "last_seen_at"],
    )


class Job:
    name: str

    def __init__(self, spark: SparkSession, settings: Settings, lake: Lake) -> None:
        self.spark, self.settings, self.lake = spark, settings, lake
        self.checkpoint = lake.checkpoint(self.name)
        self.app_id = txn_app_id(spark, self.checkpoint, self.name)

    gold_layer = False

    def _start(self, stream: DataFrame, output_mode: str = "append") -> StreamingQuery:
        s = self.settings
        return (
            stream.writeStream.queryName(self.name)
            .outputMode(output_mode)
            .foreachBatch(self._safe_process)
            .option("checkpointLocation", self.checkpoint)
            .trigger(processingTime=s.gold_trigger if self.gold_layer else s.trigger)
            .start()
        )

    def _safe_process(self, df: DataFrame, batch_id: int) -> None:
        try:
            self.process(df, batch_id)
        except Exception:
            log.exception("%s: batch %d failed", self.name, batch_id)
            raise

    def process(self, df: DataFrame, batch_id: int) -> None:  # pragma: no cover
        raise NotImplementedError


# -------------------------------------------------------------------------- bronze
class BronzeJob(Job):
    name = "kafka_to_bronze"

    def __init__(self, spark, settings, lake, registry: SchemaRegistry) -> None:
        super().__init__(spark, settings, lake)
        self.registry = registry
        self._checked: set[str] = set()

    def start(self) -> StreamingQuery:
        s = self.settings
        src = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", s.kafka_bootstrap)
            .option("subscribePattern", SUBSCRIBE_PATTERN)
            .option("startingOffsets", "earliest")
            # compacted topics have offset gaps and deleted test topics disappear: both
            # are expected here, not data loss of the pipeline (05 "Do weryfikacji")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", str(s.max_offsets_per_trigger))
            # discover topics of tables created at runtime within ~10 s (default 5 min)
            .option("kafka.metadata.max.age.ms", "10000")
            .load()
        )
        return self._start(src)

    def process(self, df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        df = df.persist()
        try:
            decoded = avro.decode(df, self.registry.schema)
            bad = None
            for rows in decoded.values():
                undecodable = bronze.undecodable_rows(rows)
                bad = undecodable if bad is None else bad.unionByName(undecodable)

            def write(item) -> None:
                topic, rows = item
                if topic == HEARTBEAT_TOPIC:
                    delta_io.append_idempotent(
                        bronze.heartbeat_rows(rows).coalesce(1),
                        self.lake.ops("cdc_heartbeat"),
                        self.app_id,
                        batch_id,
                    )
                    return
                table = table_of_topic(topic)
                if table is not None:
                    self._write_bronze(spark, table, topic, bronze.envelope_rows(rows), batch_id)

            # one Delta commit per topic, several at a time (FAIR scheduler, 05 §8)
            with ThreadPoolExecutor(max_workers=4, thread_name_prefix="bronze") as pool:
                list(pool.map(write, decoded.items()))
            if bad is not None:
                write_quarantine(spark, self.lake, bad)
        finally:
            df.unpersist()

    def _write_bronze(self, spark, table, topic, rows: DataFrame, batch_id: int) -> None:
        path = self.lake.bronze(table)
        current = delta_io.table_schema(spark, path)
        if current is not None and table not in self._checked:
            # once per run: bring the properties of an existing table up to date
            delta_io.ensure_table(spark, path, current, properties=BRONZE_PROPERTIES)
            self._checked.add(table)
        if current is None:
            pk = ",".join(self._pk(topic, rows))
            delta_io.ensure_table(
                spark,
                path,
                nullable(rows.schema),
                properties={**BRONZE_PROPERTIES, PK_PROPERTY: pk},
                partition_by=["ingest_date"],
                comment=f"raw Debezium events of {topic}",
            )
            current = delta_io.table_schema(spark, path)
        aligned = conform(rows, merge_structs(current, nullable(rows.schema)))
        delta_io.append_idempotent(
            aligned.coalesce(1), path, self.app_id, batch_id, merge_schema=True
        )

    def _pk(self, topic: str, rows: DataFrame) -> list[str]:
        try:
            return self.registry.key_fields(topic)
        except Exception:  # noqa: BLE001 - fall back to the decoded key struct
            return [f.name for f in rows.schema["key"].dataType.fields]


# -------------------------------------------------------------------------- silver
class SilverJob(Job):
    def __init__(self, spark, settings, lake, table: str, pk: list[str]) -> None:
        self.name = f"silver_{table}"
        self.table, self.pk = table, pk
        super().__init__(spark, settings, lake)

    def start(self) -> StreamingQuery:
        src = self.spark.readStream.format("delta").load(self.lake.bronze(self.table))
        return self._start(src)

    def process(self, df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        latest = silver.latest_per_key(silver.changes(df), self.pk).persist()
        try:
            if latest.isEmpty():
                return
            props = {**SILVER_PROPERTIES, PK_PROPERTY: ",".join(self.pk)}
            delta_io.merge_silver(spark, self.lake.silver(self.table), latest, self.pk, props)
            measured = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            record_latency(latency_rows(latest, "silver", self.table, batch_id, measured))
        finally:
            latest.unpersist()


# ---------------------------------------------------------------------------- gold
CDF_COLUMNS = ("order_id", "customer_id", "product_id", "seller_id", "product_category_name")


def cdf_projection(df: DataFrame, src: str) -> DataFrame:
    cols = [
        (F.col(c) if c in df.columns else F.lit(None)).cast("string").alias(c) for c in CDF_COLUMNS
    ]
    return df.select(
        F.lit(src).alias("src"),
        *cols,
        "_change_type",
        "_commit_version",
        "_op",
        "_source_ts",
    )


SOURCES_MARKER = "_wpdb_gold_sources"


def reset_on_new_sources(spark: SparkSession, checkpoint: str, sources: tuple[str, ...]) -> bool:
    """A streaming query cannot change its set of sources on an existing checkpoint
    (the offset log has one entry per source). When ``GOLD_SOURCES`` changes, the gold
    checkpoint is cleared; the fresh query reads every silver table's CDF from a
    snapshot (all rows as inserts), so every order is recomputed: the new gold tables
    are backfilled and the existing ones get an empty diff (no revenue events).
    Returns True when an existing checkpoint was cleared."""
    marker = checkpoint.rstrip("/") + "/" + SOURCES_MARKER
    want = ",".join(sources)
    have = delta_io.read_text(spark, marker)
    if have is not None and have.strip() == want:
        return False
    started = "offsets" in delta_io.list_dirs(spark, checkpoint)
    if started:
        log.warning("gold sources changed (%s -> %s): clearing %s", have, want, checkpoint)
        delta_io.delete_path(spark, checkpoint)
    delta_io.write_text(spark, marker, want)
    return started


class GoldJob(Job):
    """Star schema from the silver Change Data Feed (05 §5, §6)."""

    gold_layer = True

    name = "silver_to_gold"

    def __init__(self, spark: SparkSession, settings: Settings, lake: Lake) -> None:
        backfill = reset_on_new_sources(spark, lake.checkpoint(self.name), GOLD_SOURCES)
        # after a reset the first batches replay the whole silver state: their source
        # timestamps are not end-to-end latency, so only newer changes are measured
        self.latency_from = (
            dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) if backfill else None
        )
        super().__init__(spark, settings, lake)

    def start(self) -> StreamingQuery:
        streams = [
            cdf_projection(
                self.spark.readStream.format("delta")
                .option("readChangeFeed", "true")
                .load(self.lake.silver(t)),
                t,
            )
            for t in GOLD_SOURCES
        ]
        union = streams[0]
        for s in streams[1:]:
            union = union.unionByName(s)
        return self._start(union)

    def process(self, cdf: DataFrame, batch_id: int) -> None:
        spark = cdf.sparkSession
        lake = self.lake
        cdf = cdf.persist()
        cached: list[DataFrame] = [cdf]
        try:
            if cdf.isEmpty():
                return
            srcs = {r["src"] for r in cdf.select("src").distinct().collect()}
            # silver versions pinned per batch: a re-executed batch reads exactly the
            # same state, so its revenue events (written before the fact) and the fact
            # agree after any crash (exactly-once of the derived events)
            versions = self.pinned_versions(spark, batch_id)
            S = {
                t: spark.read.format("delta")
                .option("versionAsOf", versions[t])
                .load(lake.silver(t))
                for t in GOLD_SOURCES
            }
            for t in ("orders", "order_items", "customers", "order_payments", "order_reviews"):
                S[t] = S[t].persist()
                cached.append(S[t])

            timings: dict[str, float] = {}

            def timed(name: str, fn) -> None:
                t0 = time.perf_counter()
                fn()
                timings[name] = round(time.perf_counter() - t0, 1)

            def dims() -> None:
                if srcs & {"products", "product_category_name_translation"}:
                    desired = gold.dim_product(
                        S["products"], S["product_category_name_translation"]
                    )
                    delta_io.sync_subset(spark, lake.gold("dim_product"), desired, ["product_id"])
                if "sellers" in srcs:
                    delta_io.sync_subset(
                        spark, lake.gold("dim_seller"), gold.dim_seller(S["sellers"]), ["seller_id"]
                    )

            t0 = time.perf_counter()
            orders_a, uniq_a = gold.affected(cdf, S["orders"], S["order_items"], S["customers"])
            orders_a, uniq_a = freeze(orders_a), freeze(uniq_a)
            cached += [orders_a, uniq_a]
            # dim_customer (SCD2) versions of the affected people
            cust_a = S["customers"].join(uniq_a, "customer_unique_id", "left_semi")
            dim_c = freeze(gold.customer_scd2(cust_a, S["orders"]))
            cached.append(dim_c)
            # quality rules (quarantine), then the facts of the affected orders
            orders_rows = S["orders"].join(orders_a, "order_id", "left_semi")
            items_rows = gold.alive(S["order_items"].join(orders_a, "order_id", "left_semi"))
            pay_rows = gold.alive(S["order_payments"].join(orders_a, "order_id", "left_semi"))
            rev_rows = gold.alive(S["order_reviews"].join(orders_a, "order_id", "left_semi"))
            valid_items, v_items = quality.check_items(items_rows, gold.alive(S["products"]))
            valid_items = freeze(valid_items)
            cached.append(valid_items)
            valid_reviews, v_reviews = quality.check_reviews(rev_rows)
            v = (
                v_items.unionByName(quality.check_orders(gold.alive(orders_rows)))
                .unionByName(quality.check_payments(pay_rows))
                .unionByName(v_reviews)
            )
            # the lifecycle rows of the affected orders: their own fact, and the order
            # attributes of the payment and review facts (one definition of is_late)
            desired_lc = freeze(
                gold.fact_orders_lifecycle(
                    orders_rows, S["customers"], dim_c, valid_items, pay_rows
                )
            )
            cached.append(desired_lc)
            timings["prepare"] = round(time.perf_counter() - t0, 1)

            def dim_customer() -> None:
                delta_io.sync_subset(
                    spark,
                    lake.gold("dim_customer"),
                    dim_c,
                    ["customer_sk"],
                    scope=uniq_a,
                    scope_cols=["customer_unique_id"],
                )

            def items_and_events() -> None:
                desired_items = gold.fact_order_items(
                    valid_items, orders_rows, S["customers"], dim_c, S["sellers"]
                )
                keys = ["order_id", "order_item_id"]
                path = lake.gold("fact_order_items")
                changes = delta_io.diff_subset(
                    spark, path, desired_items, keys, scope=orders_a, scope_cols=["order_id"]
                )
                clock = gold.sim_clock(S["orders"])
                events = gold.revenue_events(changes, clock, batch_id)
                # events first, then the fact: a crash in between replays the same diff
                # (pinned silver versions), the events are skipped by txnVersion
                delta_io.append_idempotent(
                    events.coalesce(1), lake.gold("revenue_events"), self.app_id, batch_id
                )
                delta_io.apply_diff(spark, path, changes, keys)

            def lifecycle() -> None:
                delta_io.sync_subset(
                    spark,
                    lake.gold("fact_orders_lifecycle"),
                    desired_lc,
                    ["order_id"],
                    scope=orders_a,
                    scope_cols=["order_id"],
                )

            def payments() -> None:
                delta_io.sync_subset(
                    spark,
                    lake.gold("fact_payments"),
                    gold.fact_payments(pay_rows, desired_lc),
                    ["order_id", "payment_sequential"],
                    scope=orders_a,
                    scope_cols=["order_id"],
                )

            def reviews() -> None:
                delta_io.sync_subset(
                    spark,
                    lake.gold("fact_reviews"),
                    gold.fact_reviews(valid_reviews, desired_lc),
                    ["review_id", "order_id"],
                    scope=orders_a,
                    scope_cols=["order_id"],
                )

            # the gold tables are independent of each other: write them concurrently
            work = {
                "dims": dims,
                "dim_customer": dim_customer,
                "quarantine": lambda: write_quarantine(spark, lake, v),
                "fact_order_items+events": items_and_events,
                "fact_orders_lifecycle": lifecycle,
                "fact_payments": payments,
                "fact_reviews": reviews,
            }
            with ThreadPoolExecutor(max_workers=len(work), thread_name_prefix="gold") as pool:
                futures = [pool.submit(timed, k, fn) for k, fn in work.items()]
                for f in futures:
                    f.result()
            log.info("gold batch %d: %s", batch_id, timings)

            measured = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            live = cdf.filter(F.col("_change_type") != "update_preimage")
            if self.latency_from is not None:
                live = live.filter(F.col("_source_ts") >= F.lit(self.latency_from))
            record_latency(latency_rows(live, "gold", "star", batch_id, measured))
        finally:
            for c in cached:
                c.unpersist()

    def pinned_versions(self, spark: SparkSession, batch_id: int) -> dict[str, int]:
        """The silver versions this batch reads: recorded at its first execution in the
        checkpoint directory (``_wpdb_gold_versions/<batch>.json``), reused on replay."""
        from delta.tables import DeltaTable

        base = self.checkpoint.rstrip("/") + "/_wpdb_gold_versions"
        marker = f"{base}/{batch_id}.json"
        text = delta_io.read_text(self.spark, marker)
        if text:
            return json.loads(text)
        versions = {
            t: int(DeltaTable.forPath(spark, self.lake.silver(t)).history(1).first()["version"])
            for t in GOLD_SOURCES
        }
        delta_io.write_text(self.spark, marker, json.dumps(versions))
        delta_io.delete_path(self.spark, f"{base}/{batch_id - 20}.json")
        return versions


class TransitionsJob(Job):
    """Status funnel input: every orders event of bronze (before -> after), 05 §5."""

    gold_layer = True

    name = "gold_transitions"

    def start(self) -> StreamingQuery:
        return self._start(self.spark.readStream.format("delta").load(self.lake.bronze("orders")))

    def process(self, df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        df = df.persist()
        try:
            if df.isEmpty():
                return
            batch_max = df.select(F.max(gold.latest_business_ts(df, "after."))).first()[0]
            orders_path = self.lake.silver("orders")
            silver_clock = (
                gold.sim_clock(spark.read.format("delta").load(orders_path))
                if delta_io.is_delta(spark, orders_path)
                else None
            )
            clock = max([t for t in (batch_max, silver_clock) if t is not None], default=None)
            rows = gold.status_transitions(df, clock)
            delta_io.upsert(
                spark,
                self.lake.gold("status_transitions"),
                rows,
                ["order_id", "source_lsn"],
                insert_only=True,
            )
        finally:
            df.unpersist()


class AggJob(Job):
    """Windowed aggregate in event time with a watermark, written in update mode: every
    batch replaces the changed windows by key (idempotent)."""

    gold_layer = True

    def __init__(self, spark, settings, lake, name: str, source: str, target: str, fn, keys):
        self.name = name
        self.source, self.target, self.fn, self.keys = source, target, fn, keys
        super().__init__(spark, settings, lake)

    def start(self) -> StreamingQuery:
        events = self.spark.readStream.format("delta").load(self.lake.gold(self.source))
        agg = self.fn(events, self.settings.agg_watermark, self.settings.agg_window)
        return self._start(agg, output_mode="update")

    def process(self, df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        rows = df.withColumn("_updated_at", F.current_timestamp())
        if rows.isEmpty():
            return
        delta_io.upsert(spark, self.lake.gold(self.target), rows, list(self.keys))


def revenue_agg_job(spark, settings, lake) -> AggJob:
    return AggJob(
        spark,
        settings,
        lake,
        "gold_agg_revenue",
        "revenue_events",
        "agg_revenue_hourly",
        gold.revenue_windows,
        ("window_start", "customer_state"),
    )


def funnel_agg_job(spark, settings, lake) -> AggJob:
    return AggJob(
        spark,
        settings,
        lake,
        "gold_agg_funnel",
        "status_transitions",
        "agg_status_funnel",
        gold.funnel_windows,
        ("window_start", "from_status", "to_status"),
    )
