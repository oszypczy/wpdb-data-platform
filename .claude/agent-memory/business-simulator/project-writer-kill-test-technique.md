---
name: writer-kill-test-technique
description: How the interrupted-purchase test kills the real simulator writer deterministically (row lock on sim.sim_state) without test hooks in production code
metadata:
  type: project
---

To freeze the simulator's real writer (`Database.apply`) exactly on the last statement
of a purchase transaction, the test holds `SELECT id FROM sim.sim_state WHERE id = 1
FOR UPDATE` on its own connection; the writer's cursor UPDATE blocks there and the child
process is SIGKILLed while `pg_stat_activity` (queried as the same role `shop_app`,
by `application_name`) shows `state='active', wait_event_type='Lock'`.

**Why:** QA (W4, 2026-10-20) rejected a test that killed a test-owned transaction as
proving only Postgres rollback semantics. A pause hook in production code was the
alternative; the lock keeps production code untouched and runs in ~3 s.

**How to apply:** the lock also stalls the live simulator, so release it in `finally`
and keep the window short; reuse the same trick for any "kill the writer mid-transaction"
scenario in stage 6 e2e tests.
