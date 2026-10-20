# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

Semester project (WPDB, master's studies). Design is done; implementation is in progress. Stage 0 (platform skeleton: `docker-compose.yml` with the infrastructure services, init containers, pytest harness, `Makefile`, CI) is done; component stages 1-5 follow the plan in `docs/design/00-overview.md`. The git repo is public at https://github.com/oszypczy/wpdb-data-platform, default branch `main`. The dataset CSVs are not committed. `make data` downloads them into `data/olist/`, skipping `olist_geolocation_dataset.csv` because it is unused. Layout:
- `docs/README.md`: index of all docs, with a status per component.
- `docs/design/`: our design docs (Polish), numbered in data-flow order: `00-overview.md`, then `NN-<component>.md`. New component docs go here and get a row in `docs/README.md` and `00-overview.md`.
- `docs/requirements/`: source material. `ProjectDescription.docx` is the official spec, `notes.txt` holds the author's own requirements, and there is an architecture diagram.
- `docs/tutorials/`: lab tutorial PDFs, kept locally only (gitignored). Reference only: they contain deliberate bugs.
- `data/olist/`: raw Olist CSVs; source and license are in its README.

Read PDFs with `pdftotext -layout <file> -` and the docx with `textutil -convert txt -stdout <file>`.

## Goal: mini data platform in Docker

```
Olist CSV → Business Simulator (Python) → PostgreSQL → Debezium (Kafka Connect) → Kafka + Schema Registry → Spark (local[*]) → MinIO (Delta) → Streamlit dashboard
```

**The agreed design and work plan live in `docs/design/00-overview.md`; read it first, then the relevant component doc.** For example, `01-business-system.md` covers the simulator's event model, time-normalization rules and the data quirks it must handle. The project is handed in as one integrated, working system, not as 6 separate tasks. The base grade depends on the system working end to end. Bonus points come from extras *inside* existing components, not from new components. Decisions already made:
- **Data:** Olist Brazilian E-Commerce, replayed with time compression so that the order lifecycle produces INSERT, UPDATE and DELETE.
- **Serialization:** AVRO with Confluent Schema Registry.
- **Spark:** medallion layout, bronze → silver (Delta `MERGE`) → gold (star schema + windowed aggregates).
- **Visualization:** Streamlit dashboard.

Required deliverables by task (from `ProjectDescription.docx`), as the checklist the system must cover:
1. **Business system**: a Python script reads **three CSV files**, creates PostgreSQL tables dynamically (schema inferred from the CSVs) and inserts rows, simulating an ongoing business process.
2. **Debezium**: Postgres with `wal_level=logical`; a Debezium Postgres connector registered in Kafka Connect, using JSON or AVRO serialization.
3. **Kafka**: single-node KRaft cluster plus a Schema Registry (for AVRO/JSON schemas) and a Python consumer script that decodes the CDC messages.
4. **Spark**: a Structured Streaming job reads the Debezium topics, deserializes them and applies basic transformations.
5. **MinIO**: Spark writes processed data to MinIO as **Delta** format (not plain Parquet as in the tutorials), with docs on how to read the data back.
6. **Automation**: one `docker-compose.yml` that brings up the whole system from scratch, with volumes, networks, failure handling and data consistency. This means connector registration and MinIO bucket creation must be automated (for example with one-shot init containers), not done by hand as in the tutorials.

## Agents

The system is built by a team of six subagents defined in `.claude/agents/` (one owner per component plus `platform-devops` and an independent `qa-engineer`), with shared procedures in `.claude/skills/` and MCP servers in `.mcp.json`. The team, its contracts and the orchestration order are described in `docs/design/09-agents.md`: devops skeleton first, then stages 1–5 sequentially (owner builds, QA verifies, fix rounds; see `.claude/workflows/build-stage.js`), with pure-logic work in parallel worktrees. Service names and ports are the contract in the `stack-ops` skill.

## Git

- Never add `Co-Authored-By: Claude` or any other Claude/AI attribution to commit messages or PR descriptions.

## Hard requirements from `docs/requirements/notes.txt`

- **Each component runs in its own container.**
- **The tutorial scripts contain deliberate bugs.** Use them for context only and never copy them 1:1. Check every config value and version.
- **Check library and image version compatibility** across the stack before pinning versions.
- **Keep the data generation rate low** so the business simulator doesn't overload the system.
- **Build order:** business system → Postgres/Kafka → Debezium → Spark → MinIO. Get each stage working and verified before starting the next.
- The author wants to use multiple agents (subagents) while building the system.
- **Every behavioural claim in the design is verified by an automated test** at the right level: unit (no Docker), integration (a subset of containers) or e2e (full stack, including fault injection such as killing a container). Covered claims include failure recovery, schema evolution, no loss and no duplicates. These are not manual demo scenarios. Each design doc has a "Testy" section listing them.
- No Kafka UI.
- **Downstream components never connect to Postgres.** Only the simulator and Debezium talk to the database. Spark and the dashboard depend only on Kafka, Schema Registry and Delta. Kafka message keys are always the table PK, so Spark gets PK columns from the key schema in the Schema Registry. Test code may read Postgres for reconciliation.

## Known pitfalls in the reference tutorials (verify, don't trust)

- **Kafka listeners:** `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092` cannot be reached from other containers, while `kafka:9092` cannot be reached from the host. Use two listeners (internal `kafka:29092`-style plus external `localhost:9092`).
- **Kafka images:** the tutorials mix `apache/kafka:latest` and `confluentinc/cp-kafka:7.7.0`. Pick one and pin it, and keep it compatible with the Schema Registry and Debezium Connect images. With `apache/kafka`, the CLI tools are in `/opt/kafka/bin/`, not on `PATH`.
- **Debezium connector JSON:** the tutorial's `postgres-connector.json` contains `//` comments, so it is not valid JSON. Topic names follow `<topic.prefix>.<schema>.<table>`. With `schemas.enable=false` the event is flat (`before`/`after`/`op`/`source`/`ts_ms`). With schemas enabled, it is wrapped in `{"schema":…, "payload":…}`, and the Spark schema must match whichever you choose. `op` is one of `c`/`u`/`d`/`r` (r = snapshot read).
- **Confluent AVRO in Spark:** messages use the Confluent wire format (1 magic byte + 4-byte schema ID + Avro payload). Spark's built-in `from_avro` (`spark-avro`) does not understand that header, so strip the first 5 bytes and supply the schema fetched from Schema Registry. The Debezium Connect image also needs the Confluent Avro converter available, and `key/value.converter.schema.registry.url` must be set.
- **Spark/Kafka/Delta/S3 versions:** these must line up. The Spark Scala version (`_2.12` for Spark 3.5) must match `spark-sql-kafka-0-10` and `delta-spark`, and `delta-spark` must be a version built for that Spark line. `hadoop-aws` must match the Hadoop version bundled with Spark (3.3.4 for Spark 3.5.x, not 3.3.6), along with its matching `aws-java-sdk-bundle`.
- **Delta on S3A:** this needs `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`, `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`, plus `fs.s3a.path.style.access=true` and `fs.s3a.connection.ssl.enabled=false` for MinIO.
- **Streaming queries:** with multiple of them, `q1.awaitTermination(); q2.awaitTermination()` blocks on the first one. Use `spark.streams.awaitAnyTermination()`. Each sink needs its own checkpoint location. When you change the schema or output mode, clear the sink path and the checkpoint together.
- **Compose `${VAR}` substitution:** inside `command:`, compose resolves `${VAR}` from the **host/.env** at parse time, not from the container's `environment:`. Use `$$VAR` to defer substitution to the container shell.
- **`kafka-python==2.0.2`:** this release fails on Python ≥ 3.12. Use a newer release, `kafka-python-ng`, or `confluent-kafka` (which is needed anyway for Schema Registry/AVRO).
- **Startup ordering:** use `depends_on` with `condition: service_healthy` plus healthchecks (`pg_isready`, `kafka-topics.sh --list`, `curl :8083/connectors`, MinIO `/minio/health/live`) rather than a `sleep`.

## Reference commands (from the tutorials)

```bash
docker compose up -d --build            # start the stack
docker compose ps                       # wait for healthy
docker compose logs -f <service>
docker compose restart <service>        # e.g. after changing a Spark job
docker compose down -v                  # full reset (wipes volumes: data, offsets, checkpoints)

# Debezium / Kafka Connect REST
curl -X POST localhost:8083/connectors -H "Content-Type: application/json" --data @<connector>.json
curl -s localhost:8083/connectors/<name>/status | jq

# Inspect a topic
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic> --from-beginning

# Postgres shell
docker exec -it postgres psql -U <user> -d <db>
```

Ports in this project (contract in the `stack-ops` skill; every host port is bound to 127.0.0.1): Postgres 5432, Kafka `localhost:9092` (containers: `kafka:29092`), Schema Registry 8081, Kafka Connect 8083, MinIO API `127.0.0.1:9000` / console `127.0.0.1:9091` (9001 inside the network; 9091 on the host because 9001 is taken locally), Spark UI 4040, dashboard 8501. The tutorials use Kafka UI 8080, Spark 8081/8082 and MinIO console 9001; do not copy those.
