---
name: read-delta
description: Read Delta tables stored in MinIO without Spark - via DuckDB (delta_scan with an S3 secret pointing at MinIO) or Python delta-rs (DeltaTable, time travel, history). Use to verify what Spark wrote, to compare layers, or to build dashboard/test fixtures.
argument-hint: "[s3://bucket/table]"
---
# Read Delta from MinIO

Credentials come from `.env` (`MINIO_*`). Endpoint from the host: `localhost:9000`,
path-style, no SSL. Use the read-only `agent` account for inspection (all buckets), `dashboard` to prove
the dashboard's view, and `spark` only when a test must write fixtures.

## DuckDB (CLI or `duckdb` MCP)

```sql
INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE PERSISTENT SECRET minio (
  TYPE s3, KEY_ID 'agent', SECRET '<MINIO_AGENT_SECRET_KEY from .env>', ENDPOINT 'localhost:9000',
  URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
SELECT count(*) FROM delta_scan('s3://silver/orders');
SELECT order_status, count(*) FROM delta_scan('s3://silver/orders') WHERE NOT _is_deleted GROUP BY 1;
```

The `duckdb` MCP server creates its own secret at startup (`.claude/mcp/duckdb-init.sql`)
from the read-only `agent` account, so it can read every bucket but never write; use
it for inspection, and the `spark`/`dashboard` accounts only inside the components.
If `delta_scan` errors with an unsupported protocol feature, the table enabled a
feature the readers cannot handle: that is a defect in the writer (06 §9).

## Python delta-rs

```python
from deltalake import DeltaTable
opts = {"AWS_ACCESS_KEY_ID": "dashboard", "AWS_SECRET_ACCESS_KEY": "…",
        "AWS_ENDPOINT_URL": "http://localhost:9000", "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
dt = DeltaTable("s3://gold/fact_order_items", storage_options=opts)
dt.version(); dt.history(5); dt.schema()
df = dt.to_pandas()
old = DeltaTable("s3://gold/fact_order_items", version=120, storage_options=opts).to_pandas()
cdf = DeltaTable("s3://silver/orders", storage_options=opts).load_cdf(starting_version=10)
```

Inside a container the endpoint is `http://minio:9000`. Writing fixtures for tests:
`deltalake.write_deltalake(path, df, mode="overwrite")` on a local path is enough
for unit tests; integration tests write to a throwaway prefix in MinIO and delete it.
