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
- 2026-10-21 round 2 (after commit 6ebdf0f): I fixed the five weak spots below myself
  (preload exact check on never-cleaned partitions via cleaner-offset-checkpoint; connector
  ProducerConfig block parsed from `logs --tail all`; compaction tombstone keys must be gone
  from PG; --validate on all topics with --until-end; probe teardown never drops after a
  commit-wait timeout, leftovers swept by the next session). Renaming a captured table to
  `_…` does NOT avoid the 03 §13 trap (pgoutput Relation messages carry the historic name).
- Heartbeat moved to schema `dbz` (owned by postgres); debezium has no USAGE on `sim`.
- Slow marks (per-test > ~20 s on the live stack): lsn-order 62 s, validate 25-29 s,
  heartbeat-topic 24 s, new-table (captured probe teardown waits for 60 s offset flush).
  Not-slow set without test_simulator.py measured ~110 s.
- Historical (fixed in round 2) weak spots: preload "exactly one op=r" is not asserted (only
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
