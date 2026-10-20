---
name: version-matrix
description: Procedure for verifying image and library compatibility across Postgres, Debezium, Kafka/Confluent Platform, Schema Registry, Spark, Delta Lake, hadoop-aws, MinIO, delta-rs, DuckDB and Streamlit before pinning a version. Use before adding or changing any version in a Dockerfile, compose file or dependency list.
---
# Version matrix: verify, then pin

The tutorials mix incompatible versions on purpose. Nothing is pinned until it is
checked against the official compatibility source, and the evidence is recorded.

## Procedure

1. Identify the **anchor** of the group and its line:
   - Kafka group: Confluent Platform release (`cp-kafka`, `cp-schema-registry`,
     `kafka-connect-avro-converter`) → one CP version for all three.
   - Debezium group: Debezium release ↔ supported PostgreSQL major ↔ Kafka client
     version (Debezium release notes, "Supported configurations").
   - Spark group: Spark `3.5.x` ↔ Scala `2.12` ↔ `spark-sql-kafka-0-10_2.12`,
     `spark-avro_2.12` (same Spark version) ↔ `delta-spark_2.12` (Delta release
     built for that Spark line) ↔ `hadoop-aws` = Hadoop bundled with Spark
     (3.3.4 for Spark 3.5.x) ↔ matching `aws-java-sdk-bundle`.
   - Reader group: Delta table protocol features ↔ `deltalake` (delta-rs) ↔ DuckDB
     `delta` extension ↔ Streamlit (needs `st.fragment`).
   - MinIO: pinned community image tag that still exists on the registry.
2. For each pair, find the official statement (release notes, Maven Central POM
   dependencies, compatibility table). Use `context7` MCP or WebFetch; never rely on
   memory for a version number.
3. Pin **exact** versions (image tags and library versions). No `latest`.
4. Record a row in the version table of `docs/design/08-operations.md`:
   component | version | compatible with | evidence (link).
5. Prove it: the stack starts healthy and the relevant integration test passes.

## Known constraints (from CLAUDE.md; still verify the exact numbers)

- `quay.io/debezium/connect` ≥ 2.0 ships without the Confluent AVRO converter.
- `kafka-python==2.0.2` breaks on Python ≥ 3.12; use `confluent-kafka`.
- Spark's `from_avro` does not understand the Confluent wire format.
- MinIO changed distribution in 2025; confirm the image before building on it.
