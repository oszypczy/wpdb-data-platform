# kafka

Topics as code (`docs/design/04-kafka.md` §3). Owner: `database-cdc`. The validating
AVRO consumer lives in `containers/consumer/` (its own compose service).

| File | Purpose |
|---|---|
| `topics.txt` | internal topics (`connect-*`, `_schemas`) with partitions and configs |
| `create-topics.sh` | `kafka-init` entrypoint: `kafka-topics --create --if-not-exists` per line, then `--describe` |

Broker: `confluentinc/cp-kafka` in KRaft combined mode, `auto.create.topics.enable=false`.
Listeners: `kafka:29092` inside the compose network, `localhost:9092` from the host.
Business topics `olist.shop.<table>`, `olist.transaction` and `__debezium-heartbeat.olist`
are created by Debezium (`topic.creation.*` groups in
`containers/connect/connectors/olist-postgres.json`): 3 partitions + compact for the five
big tables, 1 partition + compact for dictionaries and new tables, delete with 1 day / 1 h
retention for transaction / heartbeat, `segment.ms=1h` on compacted topics so compaction
actually runs in a lab. Tables named `shop._*` are excluded from capture.
