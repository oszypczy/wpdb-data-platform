---
name: project-stage5-e2e
description: Stage 5 (2026-10-27) e2e suite design, timings, pitfalls found while writing it, and the open disk-full log-symptom mismatch
metadata:
  type: project
---

Stage 5 e2e suite written 2026-10-27 (tests/e2e: test_minio_disk_full.py, test_pipeline.py,
test_simulator_window.py; helpers e2e.py, reconcile.py, simfold.py, kpis.py). `make test-e2e`
≈ 58 min: 3 own fresh stacks (disk full ~9 min, pipeline with 9 faults ~33 min, simulator
window ~10 min) + default stack restored at session end (`WPDB_E2E_KEEP=1` skips that).

**Why:** the orchestrator wants every e2e row of 01-07 automated; future runs/regressions need
the baseline and the traps below.

**How to apply:**
- Exact reconciliation only works with the simulator PAUSED (`compose stop` = SIGTERM): gold
  trails Postgres ~45 s, so "poll until equal while it writes" never converges. Same trap hit
  two integration tests (payments/reviews) -> fixture `paused_simulator` in tests/conftest.py.
- E2E stacks need the same env/compose files on EVERY compose call (ComposeStack.env/files);
  a plain `docker compose up` (e.g. the integration `stack` fixture) recreates the simulator
  with the .env window. Never mix e2e with integration fixtures.
- Read the simulator state AFTER `kill`, not before (race: events keep landing).
- Debezium value schema: the row record is defined in `before`, `after` refers to it by name.
- MinIO disk full is simulated with tests/e2e/compose/minio-small-disk.yml (768 MiB tmpfs,
  filler file next to the data dir, minio mem limit 2g). MinIO answers XMinioStorageFull, but
  Spark logs NOTHING for >5-12 min (S3A Invoker.retry loops, jstack evidence); healthcheck goes
  unhealthy after ~200 s (stale health file); Spark self-recovers ~3.5 min after freeing space
  even without the documented restart. Test for the log line in 08 §12.8 fails -> reported.
- Freshness samples range 3-22 s against the 30 s threshold: thin margin.
- Main window 2017-01-05 → 02-01: 9 faults consume ~17 sim days; finishing at SIM_SPEED 11520
  takes ~4 min (rate cap 20/s).
- Integration suite 2026-10-27: four order/race test bugs fixed (probe tables `zz_*` leaking into
  test_simulator/test_kafka expectations, slot-ahead insert vs the type-change test on the
  shared captured probe, payments/reviews and KPI gross+corrections compared with a writing
  simulator). After fixes: fresh run 115/116 + the fixed test 15/15 in its module.
- One integration-fresh run died at stack start (kafka/minio never healthy, simulator CSV parse
  3 min instead of 3 s): Docker VM starvation right after the e2e session, not reproducible.
  If it happens again, rerun before blaming code.

**QA round 2 (2026-10-27, targeted):**
- NEVER SIGINT a running e2e pytest session: failure reports print only at session end, the
  interrupt lost both failure reasons and also broke `restore_default` mid-way. Use
  `--maxfail=1` or let it finish.
- Before a long run check the host: `uptime` load (was 22-92), `top` kernel_task (thermal
  throttling) and other heavy apps (video call). Starved VM => `kafka-topics` CLI 6-28 s vs
  the 5 s healthcheck timeout, kafka `unhealthy` for 15+ min while the broker works, zombie
  healthcheck JVMs pile up (no `init`, PID 1 = broker JVM; 125 zombies), `make up` fails,
  e2e stack healthy after 443 s. Stopping the simulator (Spark idles) let kafka recover.
- Spark self-restart test: `pgrep -f '^python3 /app/main.py'` + `kill -9` inside the
  container -> spark-submit JVM exits -> Docker restarts the same container (~30 s).
- Supervisor: crash-loop exit needs failing attempts < ~17 s each; S3A budget 30-90 s means a
  full MinIO never reaches MAX_FAILURES (unit tests assert 9 / 6 failures per 600 s).
- Dashboard pages that read tables for filter widgets outside `ui.live` (sales,
  payments_reviews: dim_geography) crash when MinIO is down; the in-container AppTest over
  all pages is the test that shows it.
