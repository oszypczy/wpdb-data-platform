# kafka

Topics as code and (from stage 2) the validating AVRO consumer
(`docs/design/04-kafka.md`). Owner: `database-cdc`.

| File | Purpose |
|---|---|
| `topics.txt` | internal topics (`connect-*`, `_schemas`) with partitions and configs |
| `create-topics.sh` | `kafka-init` entrypoint: `kafka-topics --create --if-not-exists` per line, then `--describe` |

Broker: `confluentinc/cp-kafka` in KRaft combined mode, `auto.create.topics.enable=false`.
Listeners: `kafka:29092` inside the compose network, `localhost:9092` from the host.
Business topics `olist.shop.<table>` are created by Debezium (`topic.creation.*`).
