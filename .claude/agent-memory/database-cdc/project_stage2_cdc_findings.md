---
name: stage2-cdc-findings
description: Non-obvious Debezium/Kafka/registry behaviours (dropped-table hazard, txId:lsn ids, registry id reuse, cryptography SIGILL, idle-stack slot-ahead-of-offset fix, tests write only probe_shop tables, compose run -T cancel orphans the consumer)
metadata:
  type: project
---

Stage 2 (Debezium + Kafka + Schema Registry + consumer) was implemented 2026-10-20. Findings
that are not derivable from the code and bit me at least once:

- **Dropping a captured table before the connector committed its offset kills the task after
  any restart** (Debezium reads the PK from the catalog at decode time → keyless record →
  compacted topic rejects it, `Compacted topic cannot accept message without key`). Hence
  `table.exclude.list=shop\._.*` (underscore tables = test probes) and
  `tests/helpers/cdc.py::wait_committed_past` before dropping `zz_stage2_*` probes.
  **Why:** offsets flush every 60 s (worker default); re-delivery after restart.
  **How to apply:** never DROP a `shop.*` table in a test without that wait; new probe tables
  used by stage 0/1 tests must start with `_`.
- Debezium Postgres transaction ids are `txId:lsn` *per record*: BEGIN, END and data events of
  one transaction differ; group by the `txId` prefix.
- Schema Registry returns the existing id for a schema that already exists under the subject
  (no new version): after `DROP COLUMN` messages go back to the v1 id while `latest` stays v2.
  Spark (stage 3) must decode per message schema id.
- `cryptography >= 47` SIGILLs on import inside the Docker Desktop arm64 VM (linuxkit 6.10);
  the consumer pins `cryptography==46.0.7`. Any future Python image that pulls `authlib`
  (confluent-kafka's registry client) needs the same pin.
- confluent-kafka 2.16 `SchemaRegistryClient` shared with an `AvroDeserializer` answers
  `get_version()` with `schema_id=None` from its cache: use a separate client for metadata.
- The heartbeat table must be in the publication (Debezium docs) and the slot only advances
  on offset commits (60 s), so slot lag of ~1–2 MB is normal while running.
- The `kafka` MCP `describe_topic` errored on 3-partition topics (CLI works); `consume` works.

Added 2026-10-26 (fix round after stage 4):
- **Idle stack + connect restart → task FAILED "…no longer available on the server"**: Debezium
  flushes the slot to the last *Kafka-acked* record, Connect persists an older snapshot of
  offsets; on an idle DB restart_lsn == confirmed_flush_lsn, so one heartbeat of skew trips
  `validateLogPosition` (lsn_proc < restart_lsn). Fixed with `offset.mismatch.strategy=
  trust_greater_lsn` + explicit `lsn.flush.mode=connector`. Reproducible in ~10 s: `PUT
  /connectors/X/stop`, heartbeat upsert as `debezium`, `pg_replication_slot_advance`
  (loop until a running-xacts record moves restart_lsn), resume.
- **Tests never write business tables** (Spark gold keeps status history even after DELETE).
  Probe orders go to session tables `shop.zz_probe_<hex>_<table>` (fixture `probe_shop`);
  the consumer maps them to their role via `rules.table_role`. Exceptions that remain:
  evolution test (DDL + no-op UPDATE on shop.orders), compaction test (no-op UPDATE items).

Added 2026-10-27 (stage 5 QA finding "consumer prints nothing without a TTY"):
- Not reproducible: `emit` flushed every line since stage 2. The real defects/pitfalls were
  (a) `| head` → BrokenPipeError traceback (fixed in `Runner.emit`), (b) cancelling the
  `docker compose run -T` *client* (timeout/kill/broken host pipe) does NOT signal the
  container: the final SUMMARY is lost and `wpdb-consumer-run-<id>` keeps consuming in the
  background. **How to apply:** reproduce a QA claim before "fixing" it; after ad-hoc
  `compose run -T` experiments, list `label=com.docker.compose.service=consumer` and stop
  orphans; for tests use `--no-deps` (plain `compose run consumer` re-runs `kafka-init`) and
  bounded runs (`--until-end`/`--max-messages`).
