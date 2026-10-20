# spark

Spark Structured Streaming jobs: bronze -> silver -> gold in Delta on MinIO.

- Owner: `streaming-lakehouse` (stage 3 of `docs/design/00-overview.md`).
- Compose service: `spark`, profile `pipeline` (not started by a plain `docker compose up` until the component exists).
- The Dockerfile here is a placeholder from stage 0 so that `docker compose --profile pipeline config` is valid.
