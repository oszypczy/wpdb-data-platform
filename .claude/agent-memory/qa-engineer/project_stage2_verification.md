---
name: project-stage2-verification
description: Stage 2 (Debezium + Kafka + SR + consumer) QA verdict 2026-10-21 - timings, tests I tightened, weak assertions still open, untested claims, tooling quirks
metadata:
  type: project
---

Stage 2 verified PASS on 2026-10-21: lint clean, unit 69/69 (4.4 s), `make test-integration`
82 passed + 1 skipped (WPDB_FRESH) in 8:59 on a 2-h-old stack (topics already compacted).

**Why:** independent second green run for the orchestrator; baseline for stage 3 regressions.

**How to apply:**
- I tightened two tests (QA-owned): `test_heartbeat_advances_the_slot_while_shop_is_idle` now
  requires confirmed_flush_lsn > pg_current_wal_lsn() taken after the simulator stopped (old
  version could pass just from draining the backlog); `test_register_twice...` requires PUT == 200.
- Open weak spots (owner/QA follow-up): preload "exactly one op=r" is not asserted (only
  reads ⊆ preload; compaction makes it unprovable post hoc - assert reads == preload when the
  orders partitions were never cleaned); zstd test's acks/idempotence log checks match ANY
  producer (defaults are true) - assert the `connector-producer-olist-postgres-0` block
  (acks = -1); consumer --validate excludes order_reviews/products/sellers and uses
  --idle-timeout instead of --until-end; compaction mirror check skips PG rows whose latest
  Kafka record is a tombstone; CapturedProbe.teardown DROPs even if wait_committed_past times out.
- 08 §10 still lists `test_connect.py::test_no_connectors_registered_in_stage0` (removed test).
- kafka MCP `describe_topic` errors on 3-partition topics (works for 1-partition); `consume`
  works. Topic compression.type = broker default `producer`, so zstd codec in the log proves
  the connector override.
- `docker compose run --rm consumer` re-runs kafka-init (depends_on), harmless.
