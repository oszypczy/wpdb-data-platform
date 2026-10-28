---
name: stack-ops
description: Operate the Docker Compose stack - service names and ports (shared contract), bring up, wait for healthy, logs, restart one service, full reset, and when a reset of checkpoints is required.
argument-hint: "[up|down|reset|logs <service>|wait]"
---
# Stack operations

## Service names and ports (contract for all agents)

| Service | Container/host | Internal | Host port | Healthcheck |
|---|---|---|---|---|
| `postgres` | `postgres` | 5432 | 127.0.0.1:5432 | `pg_isready -U postgres -d shop` |
| `kafka` | `kafka` | `kafka:29092` | `127.0.0.1:9092` (advertised as `localhost:9092`) | `python3 /usr/local/bin/kafka-healthcheck.py`: one Kafka-protocol `ApiVersions` request to `kafka:29092`, no JVM (~0.06 s); `init: true` reaps orphans (08 §3) |
| `schema-registry` | `schema-registry` | 8081 | 127.0.0.1:8081 | `GET :8081/subjects` (python3 `urllib`; the image has no curl) |
| `connect` | `connect` | 8083 | 127.0.0.1:8083 | `curl -f :8083/connectors` |
| `minio` | `minio` | 9000 (API), 9001 (console) | 127.0.0.1:9000 / 127.0.0.1:9091 (console) | `curl -f :9000/minio/health/live` |
| `spark` | `spark` | 4040 (UI) | 127.0.0.1:4040 | `python3 /app/healthcheck.py`: supervisor health file < 60 s old and every started query active; runs by default since stage 3 |
| `dashboard` | `dashboard` | 8501 | 127.0.0.1:8501 | `python /app/healthcheck.py` = `GET :8501/_stcore/health` (python:slim has no curl); healthy also with an empty gold; runs by default since stage 4, after `minio-init` only |
| `simulator` | `simulator` | - | - | `python /app/healthcheck.py`: heartbeat file fresh and phase `replay`/`done` (healthy = preload finished); runs by default, needs `data/olist` |
| init (one-shot) | `kafka-init`, `connect-init`, `minio-init` | - | - | exit code 0 |

Every host port is published on `127.0.0.1` only (single-machine lab; the Connect REST
API with `config.providers=env` must not be reachable from the LAN, and the last MinIO
community release has an unpatched auth bypass, CVE-2026-40344); a unit test enforces
it. The MinIO console sits on host port 9091 because 9001 is taken on
the author's machine; inside the network it is still `minio:9001`.
Database `shop`, schemas `shop` (CDC) and `sim` (state, truth views). Topic prefix
`olist` → `olist.shop.<table>`. Buckets `bronze`, `silver`, `gold`, `ops`,
`checkpoints`. If you must change any of these, change this table, the design doc
and `docs/design/08-operations.md` in the same commit.

## Commands

```bash
docker compose up -d --build                 # start / rebuild
${CLAUDE_SKILL_DIR}/scripts/wait-healthy.sh  # block until every service is healthy (timeout 300 s)
docker compose ps                            # state and health
docker compose logs -f --tail=200 <service>
docker compose restart <service>             # e.g. spark after a job change
docker compose up -d --force-recreate --no-deps <service>   # re-run one init container
docker compose down                          # stop, keep volumes (offsets, checkpoints, data)
docker compose down -v                       # FULL RESET: wipes Postgres, Kafka, MinIO, checkpoints
```

## Rules

- Prefer `down` + `up` to keep state; use `down -v` only when the design doc says a
  reset is needed (schema change of a sink, output-mode change, corrupted checkpoint).
  When you reset a Spark sink, clear its checkpoint **and** its table path together.
- Never fix ordering with `sleep`. If a service starts before a dependency is ready,
  the healthcheck or `depends_on` is wrong: fix that.
- Ports above are host ports; inside the network use the internal address
  (`kafka:29092`, `schema-registry:8081`, `minio:9000`, `postgres:5432`).
- Only one stack per machine (fixed ports and container names). Agents working in
  parallel git worktrees must not each run `docker compose up`; integration work
  is serialized by the orchestrator.
