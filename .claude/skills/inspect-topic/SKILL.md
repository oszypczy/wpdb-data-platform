---
name: inspect-topic
description: Inspect Kafka topics and Schema Registry subjects in the running stack - list topics and configs, consume AVRO messages (with keys and tombstones), check subject versions and compatibility. Use to verify what Debezium produced or what Spark will read.
argument-hint: "[topic-name]"
---
# Inspect a topic

Prefer the `kafka` MCP server (`list-topics`, `consume-messages`, `list-schemas`)
for quick looks. For exact wire-level checks use the CLI inside the containers.

```bash
# topics and their config
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker exec kafka kafka-configs --bootstrap-server kafka:29092 --describe --topic olist.shop.orders

# decoded AVRO with keys; tombstones show as null values
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --property schema.registry.url=http://schema-registry:8081 \
  --topic olist.shop.orders --from-beginning --max-messages 5 \
  --property print.key=true --property print.timestamp=true

# raw bytes: the first byte must be 0x00 (magic), next 4 = schema id
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic olist.shop.orders --from-beginning --max-messages 1 | head -c 5 | xxd

# Schema Registry
curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/olist.shop.orders-value/versions | jq
curl -s localhost:8081/subjects/olist.shop.orders-key/versions/latest | jq -r .schema | jq
curl -s localhost:8081/config | jq            # global compatibility level
curl -s -X POST localhost:8081/compatibility/subjects/olist.shop.orders-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @new-schema.json
```

From the host, bootstrap is `localhost:9092`; from a container, `kafka:29092`.
The project's own consumer (`containers/kafka/consumer.py`) prints decoded events with
validation results; use it for the "consumer decodes CDC messages" deliverable.

Checklist when verifying a Debezium topic: key = PK fields only, value envelope
has `before`, `after`, `op`, `source.lsn`, `ts_ms`; `u` and `d` carry full `before`;
DELETE is followed by a tombstone; subjects `<topic>-key` and `<topic>-value` exist.
