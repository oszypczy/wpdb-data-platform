# dashboard

Streamlit dashboard reading the gold layer (delta-rs / DuckDB).

- Owner: `analytics-dashboard` (stage 5 of `docs/design/00-overview.md`).
- Compose service: `dashboard`, profile `pipeline` (not started by a plain `docker compose up` until the component exists).
- The Dockerfile here is a placeholder from stage 0 so that `docker compose --profile pipeline config` is valid.
