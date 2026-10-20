# simulator

business simulator (Python): replays Olist CSVs into PostgreSQL.

- Owner: `business-simulator` (stage 1 of `docs/design/00-overview.md`).
- Compose service: `simulator`, profile `pipeline` (not started by a plain `docker compose up` until the component exists).
- The Dockerfile here is a placeholder from stage 0 so that `docker compose --profile pipeline config` is valid.
