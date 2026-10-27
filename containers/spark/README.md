# spark

Spark Structured Streaming: Kafka (Debezium AVRO) -> bronze -> silver -> gold in Delta on
MinIO. Design: `docs/design/05-spark.md` (processing) and `06-minio-delta.md` (storage,
reading the data back). Owner: `streaming-lakehouse`.

- Compose service `spark`, default (starts with `make up`), one container, `local[*]`.
- Image: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` + connector JARs resolved at
  build time with `.sha1` checks (versions in `Dockerfile` and 08-operations.md §1).
- Stateless container: tables and every streaming checkpoint live in MinIO
  (`checkpoints/<query>`), so `docker compose up -d --force-recreate spark` resumes.
- Never connects to Postgres: primary keys come from the key subjects in Schema Registry.

## Layout

| Path | Purpose |
|---|---|
| `lakehouse/avro.py` | Confluent wire format: schema id from bytes 1-4, decode per id, union of schema versions |
| `lakehouse/schema_align.py` | merge evolving schemas, conform a frame to a target (new fields, casts) |
| `lakehouse/bronze.py`, `silver.py`, `gold.py`, `quality.py` | pure `DataFrame -> DataFrame` transforms |
| `lakehouse/tables.py` | gold/ops table contract (columns, properties, CHECK, Z-ORDER), silver/bronze properties |
| `lakehouse/delta_io.py` | Delta IO: create tables, idempotent append (`txnAppId`), silver MERGE, gold sync |
| `lakehouse/jobs.py` | the streaming queries and their `foreachBatch` functions |
| `lakehouse/supervisor.py` | driver loop: discovery of new tables, restarts, health file, metrics, maintenance |
| `lakehouse/metrics.py`, `maintenance.py` | `StreamingQueryListener` -> `ops/stream_metrics`; OPTIMIZE/Z-ORDER/VACUUM |
| `main.py`, `run.sh`, `healthcheck.py` | entrypoint (`spark-submit`), Docker healthcheck |

## Queries (each with its own checkpoint `checkpoints/<name>`)

| Query | Source | Sink |
|---|---|---|
| `kafka_to_bronze` | Kafka `olist\.shop\..*` + `__debezium-heartbeat.olist` | `bronze/<table>`, `ops/cdc_heartbeat` |
| `silver_<table>` (one per bronze table, started on discovery) | `bronze/<table>` | `silver/<table>` |
| `silver_to_gold` | CDF of the 8 silver tables in `GOLD_SOURCES` (a changed list clears its checkpoint and backfills gold from a silver snapshot) | gold dims and facts (incl. `fact_payments`, `fact_reviews`), `gold/revenue_events`, `ops/quarantine` |
| `gold_transitions` | `bronze/orders` | `gold/status_transitions` |
| `gold_agg_revenue`, `gold_agg_funnel` | `gold/revenue_events`, `gold/status_transitions` | `gold/agg_revenue_hourly`, `gold/agg_status_funnel` |

## Operations

```bash
docker compose logs -f spark                       # driver log ("gold batch N: {...}" timings)
docker compose exec spark python3 /app/healthcheck.py   # status of every query
docker compose exec spark cat /tmp/spark/health.json
docker compose exec spark touch /tmp/spark/run-maintenance   # OPTIMIZE/Z-ORDER/VACUUM now
open http://127.0.0.1:4040                          # Spark UI (Structured Streaming tab)
```

Reset of one sink: stop the stack, delete the table path **and** its checkpoint
(`checkpoints/<query>`) together, start again (stack-ops skill). `make reset` wipes all.

Settings (environment of the service): `SPARK_DRIVER_MEMORY` (1536m), `SPARK_TRIGGER`
(10 s, bronze and silver), `SPARK_GOLD_TRIGGER` (20 s), `SPARK_SHUFFLE_PARTITIONS` (4),
`AGG_WATERMARK` (24 hours of simulated time), `MAINTENANCE_INTERVAL_S` (1800).

## Tests

- Unit (local SparkSession, Delta on the local filesystem, Java 17 on the host):
  `tests/unit/test_spark_*.py`; fixtures `tests/helpers/debezium_fixtures.py` (Confluent
  AVRO messages) and `tests/helpers/silver_fixtures.py` (small silver tables).
- Integration (live stack): `tests/integration/test_spark.py`, `tests/integration/test_delta.py`.
