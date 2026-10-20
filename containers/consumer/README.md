# consumer

Python AVRO consumer for the CDC topics (`docs/design/04-kafka.md` §6): decodes keys and
values through Schema Registry, applies validation rules and tracks single orders.
Owner: `database-cdc`. Built in stage 2 as the `consumer` service (compose profile `tools`).
