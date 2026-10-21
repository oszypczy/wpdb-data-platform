# consumer

Python AVRO consumer for the CDC topics (`docs/design/04-kafka.md` §6): decodes keys and
values through Schema Registry, prints or aggregates per topic and op, applies the
validation rules and follows single orders. Owner: `database-cdc`. Stage 2.

| File | Purpose |
|---|---|
| `consumer/rules.py` | validation rules as pure functions (`before_present`, `status_transition`, `amount_non_negative`, `date_not_in_future`, `schema_id_known`), wire-header parsing; unit tests in `tests/unit/test_consumer_rules.py` |
| `consumer/model.py` | `Message`, one-line `describe()`, `Stats` (per-topic counters, `--stats` lines), `OrderTracker` (`--follow-order`) |
| `consumer/cli.py` | argparse + the Kafka loop (`confluent-kafka` `Consumer` + `AvroDeserializer`), `--json` output |
| `Dockerfile`, `pyproject.toml`, `uv.lock` | `python:3.12.15-slim-bookworm`, `confluent-kafka[avro]==2.16.0`, `cryptography==46.0.7` (>= 47 crashes with SIGILL in the Docker Desktop arm64 VM) |

## Running

```bash
docker compose run --rm consumer                                   # live preview of olist.shop.*
docker compose run --rm consumer --stats                           # rates, c/u/d/r/t, lag, schema ids every 5 s
docker compose run --rm consumer --validate --from-beginning --until-end
docker compose run --rm consumer --follow-order e481f5…            # (alias --trace) lifecycle of one order
docker compose run --rm consumer --from-beginning --max-messages 50 --json --topics olist.shop.orders

cd containers/consumer && uv run python -m consumer --stats        # from the host (localhost:9092, :8081)
```

Options: `--topics` (regex starting with `^`, default `^olist\.shop\..*`, or a comma-separated
list), `--from-beginning` (random group id, no commits), `--max-messages N`, `--until-end`
(stop when every assigned partition reported EOF; the live stream never
goes idle, so prefer this over `--idle-timeout S`), `--stats-interval`, `--group-id`, `--verbose`, `--json`,
`--bootstrap`/`--schema-registry` (defaults from `KAFKA_BOOTSTRAP`, `SCHEMA_REGISTRY_URL`).
Exit code 0; the last line is `SUMMARY events=… violations=… decode_errors=… clock=…`
(or `{"summary": …}` with `--json`).

The consumer never connects to Postgres: the simulation clock used by the date rule is
estimated from the stream (latest event date seen, 30-day tolerance).
