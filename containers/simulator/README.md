# simulator

Business simulator (Python 3.12): replays the Olist data set into PostgreSQL as a
running shop. Contract: `docs/design/01-business-system.md`; owner `business-simulator`.
Compose service `simulator`, started by default (`make up`), healthy once its preload
is done.

## What it does

1. **Load** `data/olist/*.csv` (bind-mounted read-only at `/data/olist`), infer a
   schema from the content (types, primary keys, foreign keys; 01 §3) and create the
   tables in schema `shop` as `shop_app` in **one transaction** together with the
   catalog rows (`products`, `sellers`, `product_category_name_translation`, via
   `COPY`), the reconciliation views (`containers/postgres/sql/truth-views.sql`,
   bind-mounted, single source) and the control row in `sim.sim_state`.
2. **Preload**: every order-lifecycle event before `SIM_START` in batches (pipeline
   mode), no pacing. Debezium (stage 2) sees this history as its snapshot.
3. **Replay** `[SIM_START, SIM_END]`: one transaction per event (a purchase = customer +
   order + items + payments), paced by the simulated clock (`SIM_SPEED`) and capped at
   `MAX_EVENTS_PER_SEC` row changes per second with backpressure (the simulated clock
   slows down instead of dropping anything).
4. **Done**: the window is finished; the container idles (healthy) until stopped.
   `make reset` starts over; a later `SIM_END` in `.env` + restart continues.

Every transaction also moves the cursor `(sim_clock, cursor_order_id, cursor_step)` in
`sim.sim_state`, so after `docker kill` the next start resumes with the exact next
event: no duplicates, no loss. A lost database connection is retried and resumed the
same way. `SIGTERM` stops after the current transaction.

## Layout

| Path | Role |
|---|---|
| `simulator/csvio.py` | CSV → table name, header, rows (`utf-8-sig` for the BOM) |
| `simulator/schema.py` | type / PK / FK detection, DDL and FK-index policy (pure) |
| `simulator/events.py` | order → events, time normalization, final statuses, cursor, expected-state fold (pure) |
| `simulator/render.py` | event → SQL statements (pure) |
| `simulator/clock.py` | `SimClock`, `RateLimiter`, `Pacer` (pure, injectable time) |
| `simulator/config.py` | environment → `Config` with the low defaults |
| `simulator/db.py` | psycopg writer: retry, init transaction, per-event transaction, pipeline preload |
| `simulator/engine.py` | phases init → preload → replay → done, progress log, heartbeat |
| `healthcheck.py` | healthy = heartbeat file < 30 s old **and** phase in (replay, done) |

Unit tests (`tests/unit/test_simulator_*.py`) import the package through
`pythonpath = ["containers/simulator"]` in the root `pyproject.toml`; integration tests
are in `tests/integration/test_simulator.py`.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` | `postgres`, `5432`, `shop`, `shop_app`, from `.env` | connection |
| `SIM_SPEED` | `1440` | simulated seconds per wall second (1 day per minute) |
| `MAX_EVENTS_PER_SEC` | `20` | cap on row changes per second (backpressure) |
| `SIM_START`, `SIM_END` | `2017-01-05`, `2017-03-31` | replay window; a date means 00:00:00 / 23:59:59 |
| `SIM_DATA_DIR` | `/data/olist` | CSV directory |
| `SIM_TRUTH_VIEWS` | `/app/sql/truth-views.sql` | reconciliation views applied after the tables |
| `SIM_HEARTBEAT_FILE` | `/tmp/simulator/heartbeat` | liveness file (`-` disables) |
| `SIM_LOG_EVERY`, `SIM_PRELOAD_BATCH` | `10`, `1000` | progress log period (s), preload batch size |
| `SIM_SCHEMA_EVOLUTION_AT` | empty (off) | simulated moment of the one-off `ALTER TABLE shop.orders ADD COLUMN delivery_channel TEXT NULL`; later purchases fill it (01 §7). Stored in `sim.sim_state.schema_evolution_at`; keep it off while `test_debezium.py` runs |

Progress log, every `SIM_LOG_EVERY` seconds:
`[sim 2017-01-11 14:03:12] phase=replay orders=412 rows=3105 (c=2011 u=1082 d=12) rate=6.8/s throttled`.

## Operating it

```bash
docker compose logs -f simulator
docker compose exec simulator python /app/healthcheck.py; echo $?
psql postgres://reader:<READER_PASSWORD>@localhost:5432/shop -c 'SELECT * FROM sim.sim_state'
docker compose kill simulator && docker compose up -d simulator   # resume test by hand
```

`docker kill` does **not** trigger `restart: unless-stopped` (Docker marks the container
as manually stopped); `docker compose up -d simulator` brings it back and it resumes.
