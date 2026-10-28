---
name: project-ops-gotchas
description: Live-stack gotchas when rebuilding spark (dependency recreation, delta-rs hang on exit, CHECK vs NULL, idle queries without progress look stuck)
metadata:
  type: project
---

Observed 2026-10-27 while adding gold/fact_payments + fact_reviews:

- `docker compose up -d --build spark` also rebuilt and **recreated connect, minio and the
  simulator**, because other agents' uncommitted working-tree changes touched their build
  contexts. Connector came back RUNNING, simulator stayed `done`, but report it.
  **How to apply:** prefer `docker compose up -d --build --no-deps spark` when only Spark changed.
- A host `uv run python -` script using `deltalake` printed its output and then hung on
  interpreter exit (~20 min) — blocks chained commands. **How to apply:** run delta-rs
  checks as a separate command, or via the duckdb MCP.
- Delta CHECK constraints reject rows where the expression is NULL. Resolved 2026-10-27:
  rules with `guards=` use the CHECK expression and treat NULL as a violation; a unit test
  fails if a new gold CHECK has neither a guarding rule nor an entry in COMPUTED_CHECKS.
  **How to apply:** a new CHECK on gold needs a guarding reject rule plus a probe.
- After a Spark restart while the simulator is in phase `done`, silver/gold queries show
  `last_progress: None` and UI rates NaN for many minutes. That is idle (the stream thread is
  sleeping in ProcessingTimeExecutor, no input means no progress event), not a hang.
  **How to apply:** check simulator phase and jstack before you call it stuck.

Related: [[project-stage3-status]].

Observed 2026-10-27 (disk-full fix round):

- S3A retries are **silent** and two-layered: SDK 1.12.262 retries only IOException and
  500/502/503/504; anything else (MinIO 507 XMinioStorageFull) goes to S3A's exponential
  `retry.limit` x `interval*2^(n+1)`, logged nowhere. **How to apply:** before touching
  `fs.s3a.retry.*`, compute the budget (unit test in test_spark_session.py does it); never
  do blocking Delta writes in the supervisor's main loop (they hang the health file).
- The host is a laptop: clamshell sleep froze the whole Docker VM for 22 min mid-e2e
  (simulator logs stop too; `pmset -g log` shows "Clamshell Sleep", host clock UTC+1).
  **How to apply:** a stack-wide gap in all logs = host sleep, not a Spark hang; rerun.
- `pytest tests/e2e/test_pipeline.py -k minio` alone fails in ~1 s (`max()` of empty in
  `Pipeline.checkpoint`: no committed bronze batch yet on a fresh stack). **How to
  apply:** select `-k "freshness or minio"` so the warm-up test runs first.
