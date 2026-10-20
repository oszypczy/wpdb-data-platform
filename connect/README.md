# connect

Kafka Connect (Debezium) image and connector registration
(`docs/design/03-debezium.md`). Owner: `database-cdc` for the connector config,
`platform-devops` for the image and `connect-init`.

| File | Purpose |
|---|---|
| `Dockerfile` | `quay.io/debezium/connect` + Confluent AVRO converter from Confluent Hub (same CP version as the broker) |
| `init.sh` | `connect-init` one-shot: `PUT /connectors/<name>/config` for every `connectors/*.json`, waits for RUNNING; no files = no-op |
| `connectors/*.json` | connector configs (stage 2 adds `olist-postgres.json`); pure JSON, secrets as `${env:DEBEZIUM_DB_PASSWORD}` resolved by the `EnvVarConfigProvider` configured on the worker |

Worker settings that matter for connector authors (set in `docker-compose.yml`):
`config.providers=env`, `connector.client.config.override.policy=All`
(needed for the `producer.override.acks/enable.idempotence/compression.type` settings of
04 §7; `Principal` would only allow security overrides. Safe because 8083 is bound to
127.0.0.1), internal topics
`connect-configs/offsets/status` pre-created by `kafka-init`.

REST API from the host only (port bound to 127.0.0.1): `http://localhost:8083` (`/connectors`,
`/connector-plugins?connectorsOnly=false` to see converters too).
