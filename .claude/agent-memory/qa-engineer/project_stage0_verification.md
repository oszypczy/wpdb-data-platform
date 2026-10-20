---
name: project-stage0-verification
description: Stage 0 (platform skeleton) QA verdict 2026-10-20, measured timings, host quirks (port 9001 taken by unimot-caddy), and known weak assertions to tighten
metadata:
  type: project
---

Stage 0 verified PASS on 2026-10-20: unit 4/4, integration 30 passed + 1 skipped (twice, no reset),
`WPDB_FRESH=1` 31/31 in 64 s. `make reset && make up` reached "all services healthy" in 42 s
(owner measured 47 s).

**Why:** the orchestrator asked for an independent reproduction of the owner's report; these numbers
are the baseline for later "no regression in startup" claims.

**How to apply:**
- Host port 9001 is held by an unrelated container `unimot-caddy`; MinIO console is therefore
  on 127.0.0.1:9091. Do not "fix" that.
- `test_stack.py::test_fresh_stack_healthy_within_budget` only runs with `WPDB_FRESH=1`; neither
  `make test-integration` nor CI sets it, so the 180 s budget claim is unverified by default runs.
- Weak assertions I flagged (tighten when touching these files): `test_stack.py` idempotency log
  check has an `or "ignore-existing" in logs` escape hatch and the minio snapshot ignores policies
  / policy attachments; `test_schema_registry.py::test_subjects_endpoint` has an `or True` tautology;
  reader/debezium privilege tests skip DELETE and the `sim` schema.
- `mc mb --ignore-existing` and `mc admin user add` print "created/added successfully" on reruns;
  only before/after state (bucket timestamps, user list) proves idempotency, not the logs.
- `make up` always rebuilds (`--build`) but the pytest `stack` fixture never does: a Dockerfile
  change without `make up` leaves tests running on a stale image.
