---
name: project-stage1-verification
description: Stage 1 (simulator + Postgres extensions) QA verdict 2026-10-20 - measured timings, how the suites behave with the simulator writing, claims still without a test, and verification pitfalls
metadata:
  type: project
---

Stage 1 verified PASS on 2026-10-20: unit 40/40 (3.8 s), integration 44 passed + 1 skipped
twice on a writing simulator (60 s, 66 s), `make test-integration-fresh` 45/45 in 106 s,
`make reset && make up` healthy in 43 s (owner: 48 s). Hand kill/restart of the simulator
reconciled exactly with the pure fold (`tests.integration.test_simulator.assert_matches_uninterrupted_run`).

**Why:** baseline for stage 2+ regressions; the orchestrator wants independent numbers, not the
owner's report.

**How to apply:**
- The integration suite now *kills and restarts the simulator* (slow kill test). Never run it
  while hand-sampling `sim.sim_state` or `v_truth_row_counts`; sample first, then run suites.
- At the default window (Jan 2017) the rate is ~0.5 rows/s, far below `MAX_EVENTS_PER_SEC=20`;
  the limiter is only visible as `throttled=NN%` log lines on midnight review bursts and in the
  unit test. The cap cannot be saturated on the shared DB; a bounded-window experiment needs
  the e2e stage (own stack, `SIM_END` short).
- `sim_clock` only moves on events: 30 s plateaus at ×1440 in January are normal, not a hang.
- Every number in 01 §4 (preload/replay events, rows, purchases, minutes, rows/s, Black Friday
  109/s) reproduces with `simulator.events.summarize` on `data/olist` (~10 s). Recompute before
  disputing a doc number.
- Still unverified by any test (claims in 01 §2/§4/§5): refuse-to-start when `shop.*` exist
  without a `sim_state` row; later `SIM_END` resumes / changed `SIM_START` ignored; phase
  `done` idle+healthy; reconnect after `docker restart postgres`; SIGTERM stops after the
  current transaction; stale heartbeat -> unhealthy. 02 §8 "exact values on the known fixture"
  branch of `test_truth_views_apply_and_reconcile` is unreachable now that the simulator always
  owns `shop.*` (only the internal-consistency branch runs).
- Host psql is absent: use the `postgres` MCP (reader) or `docker exec -i -e PGPASSWORD=...
  postgres psql -h 127.0.0.1 -U reader` (`-i` is required for heredocs). In zsh, `$P -c ...`
  with a multi-word `$P` is not split; write helper scripts in bash.
- `docker kill` leaves the container `exited (137)` and `restart: unless-stopped` does not
  bring it back; `docker compose up -d simulator` does, healthy ~21 s later.
