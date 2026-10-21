# connect

Kafka Connect (Debezium) image and connector registration
(`docs/design/03-debezium.md`). Owner: `database-cdc` for the connector config,
`platform-devops` for the image and `connect-init`.

| File | Purpose |
|---|---|
| `Dockerfile` | `quay.io/debezium/connect:3.6.3.Final` + Confluent AVRO converter 8.3.2 from Confluent Hub (same CP version as the broker) |
| `init.sh` | `connect-init` one-shot: `PUT /connectors/<name>/config` for every `connectors/*.json`, waits for RUNNING; runs after `simulator` is healthy (preload done), so the initial snapshot covers the history before `SIM_START` |
| `connectors/olist-postgres.json` | the stage-2 connector: pure JSON, secret as `${env:DEBEZIUM_DB_PASSWORD}` resolved by the worker's `EnvVarConfigProvider` |

## `olist-postgres.json` in one table

| Setting | Value | Why (design §) |
|---|---|---|
| `plugin.name`, `slot.name`, `publication.name`, `publication.autocreate.mode` | `pgoutput`, `debezium_slot`, `dbz_publication`, `disabled` | publication from initdb (02 §3) |
| `schema.include.list` / `table.exclude.list` | `shop` / `shop\._.*` | tables named `_…` are test probes, never captured (03 §13) |
| `snapshot.mode` | `initial` | preload rows as `op=r`, then stream from the slot's consistent point (03 §2) |
| `decimal.handling.mode`, `time.precision.mode` | `precise`, `connect` | `decimal(10,2)` and `timestamp-millis` logical types (03 §6) |
| `tombstones.on.delete` | `true` | compaction removes deleted keys (04 §2) |
| `heartbeat.interval.ms`, `heartbeat.action.query` | 10 s, upsert of `dbz.heartbeat` | slot advances while `shop` is idle; the table is in the publication (03 §7, 02 §5) |
| `provide.transaction.metadata` | `true` | `olist.transaction` BEGIN/END, `transaction` block in events (03 §7) |
| `producer.override.*` | `acks=all`, `enable.idempotence=true`, `compression.type=zstd` | 04 §7 (needs the worker's override policy `All`) |
| `topic.creation.*` | groups `large` (3 partitions, compact), `transaction` (delete, 1 d), `heartbeat` (delete, 1 h), default (1 partition, compact); `segment.ms=1h` on compacted topics | topics as code (04 §2, §3) |

Worker settings that matter for connector authors (set in `docker-compose.yml`):
`config.providers=env`, `connector.client.config.override.policy=All`, converters
`io.confluent.connect.avro.AvroConverter` with `schema.registry.url=http://schema-registry:8081`,
internal topics `connect-configs/offsets/status` pre-created by `kafka-init`,
`offset.flush.interval.ms=5000` (`OFFSET_FLUSH_INTERVAL_MS`; image default 60000): a task
restart re-delivers at most ~5 s of events, 03 §9.

REST API from the host only (port bound to 127.0.0.1): `http://localhost:8083`. Operations
and the recovery runbooks (FAILED after an incompatible schema change, dropped table, lost
slot) are in `.claude/skills/debezium-connector/SKILL.md` and 03 §5/§13.
