# kafka-mcp

Small read-only MCP server (Python, `mcp` SDK + `confluent-kafka`) used by the
agents as the `kafka` MCP server in `.mcp.json`. It replaces
`@confluentinc/mcp-confluent`, whose native dependency does not build on
Node 26 / arm64.

Tools: `list_topics`, `describe_topic`, `consume`, `list_subjects`, `get_schema`.
Messages are decoded with the Confluent wire format through Schema Registry
(key and value); tombstones come back as `null` values.

Configuration (environment): `KAFKA_BOOTSTRAP` (default `localhost:9092`),
`SCHEMA_REGISTRY_URL` (default `http://localhost:8081`).

Run by hand: `uv run --project tools/kafka-mcp kafka-mcp` (stdio transport).
