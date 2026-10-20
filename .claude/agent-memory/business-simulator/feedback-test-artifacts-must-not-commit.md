---
name: test-artifacts-must-not-commit
description: Integration tests that open transactions in child processes must never be able to COMMIT synthetic rows into the live shop schema
metadata:
  type: feedback
---

A test child process holding an open transaction must raise/abort on every exit path;
never let `with conn.transaction()` exit normally after a `stdin.readline()` wait.

**Why:** on 2026-10-20 an aborted run of
`test_purchase_killed_mid_transaction_leaves_no_half_order` left the child alive; when
pytest exited, stdin closed, `readline()` returned and the block COMMITTED a
`test-half-kill-*` order into the live `shop` tables, breaking the fold-equality test
until cleaned by hand. With Debezium running this would have reached Kafka.

**How to apply:** use ids with a recognisable prefix (`test-half-*`), add a
`_remove_test_rows` cleanup before and after, always kill the child in `finally`, and
prefer `raise SystemExit` inside the transaction block over returning from it.
