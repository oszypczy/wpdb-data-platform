---
name: project-ops-gotchas
description: Live-stack gotchas hit while rebuilding spark (dependency recreation, delta-rs script hang on exit, Delta CHECK vs NULL)
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
- Delta CHECK constraints reject rows where the expression is NULL, while the quality
  rules in `lakehouse/quality.py` treat NULL as valid; a reject rule paired with a CHECK
  must treat NULL as a violation (done for review_score_range; price/freight rules
  still treat NULL as valid — latent).

Related: [[project-stage3-status]].
