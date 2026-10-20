---
name: fault-injection
description: Pattern for e2e tests that kill or restart a container (connect, kafka, spark, minio, simulator) while the stream is running and then prove no loss and no duplicates by reconciling Postgres truth views with Delta silver/gold. Use when implementing or reviewing the "e2e (awaria)" rows of the design docs.
argument-hint: "[service]"
---
# Fault injection and reconciliation

Every "e2e (awaria)" row has the same shape:

1. **Arrange**: fresh stack (`make reset && make up`), simulator running at a low
   rate (`SIM_SPEED` small, `MAX_EVENTS_PER_SEC` ≈ 5) with a short window so the
   run finishes in minutes. Record a run id.
2. **Act**: wait until the stream is flowing (silver row count rising), then
   `docker kill <service>` (SIGKILL, not graceful) and after a few seconds
   `docker compose up -d <service>` (or `docker compose restart`). For MinIO also
   test `docker compose restart minio` during a Spark write.
3. **Wait**: until the simulator reports the window finished (`sim.sim_state`)
   **and** end-to-end lag is zero (slot lag 0, Spark metrics show no pending
   offsets, silver version stable for N seconds). Poll with a timeout; no sleeps.
4. **Assert**: reconciliation must be exactly equal:
   - `sim.v_truth_orders` (status → orders, revenue) == same aggregate over
     silver `orders` ⋈ `order_items` where `NOT _is_deleted`;
   - per-table row counts Postgres == silver (non-deleted);
   - bronze has unique `(topic, partition, offset)`;
   - gold sums == silver sums.
   Also assert the component itself recovered: connector `RUNNING`, Spark query
   active, dashboard healthy.

```python
@pytest.mark.e2e
def test_connect_kill_no_loss_no_dups(stack, simulator_run, kill):
    """03 §11: kill connect mid-stream, restart → resume from offset, reconciliation equal."""
    wait_until(lambda: silver_count("orders") > 100, timeout=120)
    kill("connect")                      # docker kill + compose up -d
    simulator_run.wait_finished(timeout=600)
    wait_until(lambda: pipeline_idle(), timeout=300)
    assert truth_by_status() == silver_by_status()
    assert bronze_duplicates() == 0
```

Fixtures live in `tests/e2e/conftest.py`: `stack` (ensures healthy, tears down),
`kill(service)` (records the time of the kill for the report), `truth_*` readers
(Postgres via the read-only `reader` role), `silver_*`/`gold_*` readers (DuckDB or
delta-rs against MinIO). Only test code reads Postgres.

Report for each run: service killed, seconds down, events during outage, final
counts on both sides, duplicates found (must be 0).
