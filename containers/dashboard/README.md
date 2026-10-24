# dashboard

Streamlit dashboard: the platform's end user (docs/design/07-dashboard.md). Reads only
the gold and ops Delta tables in MinIO with the read-only `dashboard` account, through
delta-rs (to Arrow) and DuckDB (SQL on Arrow). Never talks to Spark, Kafka or Postgres.

- Owner: `analytics-dashboard` (stage 4 of `docs/design/00-overview.md`).
- Compose service `dashboard`, default (starts with `make up`), after `minio-init` only:
  healthy with an empty gold, the pages show "czekam na dane" until Spark writes it.
- Open <http://localhost:8501> (bound to 127.0.0.1).

## Layout

| Path | What |
|---|---|
| `app.py` | entry point: `st.navigation` with the 9 pages of 07 §2 |
| `views/*.py` | one file per page (layout and charts only); data in an `st.fragment(run_every=10 s)` |
| `dashboard/contract.py` | Arrow schemas of the gold/ops columns read (subset of `containers/spark/lakehouse/tables.py`) |
| `dashboard/lake.py` | `Lake` (delta-rs: version, read at version N, history) and `VersionedReader` (re-read only when the Delta version changed, 07 §3) |
| `dashboard/queries.py` | query layer: pure functions `table(s) -> DataFrame`, KPI definitions in its docstring |
| `dashboard/charts.py`, `dashboard/ui.py` | one Plotly style (light + dark), shared page plumbing |
| `assets/brazil-states.geojson` | 27 states, Natural Earth (public domain); provenance in `assets/README.md` |
| `healthcheck.py` | `GET :8501/_stcore/health` (the image has no curl) |

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `S3_ENDPOINT` | `http://minio:9000` | MinIO API |
| `MINIO_DASHBOARD_ACCESS_KEY` / `_SECRET_KEY` | – | read-only account (gold, ops), from `.env` |
| `DASHBOARD_REFRESH_S` | `10` | refresh period of the data fragments |
| `DASHBOARD_LAKE_ROOT` | `s3://` | lake root; a local directory with `gold/` and `ops/` runs the app on local Delta tables (tests, fixtures) |

## Run without Docker

```bash
uv sync                                   # root venv has the same pins
S3_ENDPOINT=http://127.0.0.1:9000 MINIO_DASHBOARD_ACCESS_KEY=dashboard \
MINIO_DASHBOARD_SECRET_KEY=... uv run streamlit run containers/dashboard/app.py

# on the small fixture tables of the tests (no stack needed)
PYTHONPATH=containers/dashboard uv run python -c "from tests.helpers.dashboard_fixtures import write_small_lake as w; w('/tmp/wpdb-lake')"
DASHBOARD_LAKE_ROOT=/tmp/wpdb-lake uv run streamlit run containers/dashboard/app.py
```

## Tests

`tests/unit/test_dashboard_*.py` (query layer, cache, version N, contract, every page with
`AppTest` on fixtures and on an empty lake) and `tests/integration/test_dashboard.py`
(every page on live MinIO data, from the host and inside the image; `dashboard` account
permissions; container healthy, also with an empty gold). Table of 07 §7.
