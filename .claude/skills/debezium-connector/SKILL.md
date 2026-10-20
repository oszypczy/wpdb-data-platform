---
name: debezium-connector
description: Register, inspect, restart and debug the Debezium Postgres connector through the Kafka Connect REST API, and check the replication slot and publication on the Postgres side. Use when the connector is missing, FAILED, lagging, or after changing its config.
argument-hint: "[status|register|restart|slot]"
---
# Debezium connector operations

Connector name: `olist-postgres`. Config file: `containers/connect/connectors/olist-postgres.json`
(every `containers/connect/connectors/*.json` is registered by `connect-init`)
(pure JSON, no comments; secrets as `${env:DEBEZIUM_DB_PASSWORD}` resolved by the
Connect `EnvVarConfigProvider`).

```bash
# idempotent registration (create or update) - this is what connect-init does
curl -s -X PUT localhost:8083/connectors/olist-postgres/config \
  -H 'Content-Type: application/json' --data @containers/connect/connectors/olist-postgres.json | jq

curl -s localhost:8083/connectors/olist-postgres/status | jq        # RUNNING / FAILED + trace
curl -s localhost:8083/connectors/olist-postgres/config | jq       # password must stay masked
curl -s -X POST localhost:8083/connectors/olist-postgres/restart?includeTasks=true
curl -s 'localhost:8083/connector-plugins?connectorsOnly=false' | jq '.[].class'  # converters are hidden without connectorsOnly=false
docker compose logs --tail=200 connect | grep -iE 'error|exception'
```

Postgres side (read-only; use the `postgres` MCP or `psql` as `reader`):

```sql
SELECT slot_name, active, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots;
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
SELECT relname, relreplident FROM pg_class WHERE relnamespace = 'shop'::regnamespace AND relkind = 'r';  -- 'f' = FULL
SHOW wal_level; SHOW max_slot_wal_keep_size;
```

## Typical failures

| Symptom | Likely cause |
|---|---|
| `FAILED`, trace mentions `AvroConverter` class not found | Connect image lacks the Confluent converter JARs |
| `publication ... does not exist` | init SQL did not create `dbz_publication`, or `autocreate.mode` mismatch |
| events with `before = null` on UPDATE | table not `REPLICA IDENTITY FULL` (event trigger missing) |
| no events, slot inactive | connector task stopped; check status and restart |
| `could not access file "pgoutput"` / wal_level | Postgres not started with `wal_level=logical` |
| schema registered but `is_compatible: false` | incompatible type change; expected for the negative test |

Snapshot vs stream: rows present before registration arrive with `op=r`; the
stream continues from the slot LSN without a gap or a duplicate at the boundary.
