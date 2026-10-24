---
name: project-stage3-status
description: Stage 3 (Spark + MinIO/Delta) built 2026-10-23, not yet QA-verified; e2e rows deferred to the final stage; open items handed to other owners
metadata:
  type: project
---

Stage 3 (Spark + MinIO/Delta) was implemented on 2026-10-23 by streaming-lakehouse and
handed back to the orchestrator for qa-engineer verification (not committed by me).

**Why:** the user decided on 2026-10-21 that e2e tests (kill Spark, MinIO restart,
Postgres ↔ silver reconciliation, gold = silver sums, latency threshold) are written only
in the final stage, on the target platform version.

**How to apply:** when asked about stage 3 follow-ups, check docs/design/05-spark.md §12
and 08-operations.md §10/§14 for the current state first. Known open items at hand-off:
Connect fails after being recreated on a stack idle for > 1 day (offset LSN older than the
slot's restart_lsn; owner database-cdc); CPU bursts of 400–600 % per micro-batch with 13
queries in one local[*] JVM. Related: [[user-progress-updates]].
