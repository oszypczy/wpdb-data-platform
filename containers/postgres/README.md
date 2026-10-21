# postgres

PostgreSQL 16 image configuration for the `postgres` service
(`docs/design/02-postgresql.md`). Owner: `database-cdc`; the compose wiring is
owned by `platform-devops`.

| File | Purpose |
|---|---|
| `postgresql.conf` | server config as code (`wal_level=logical`, slot limits, laptop tuning); mounted read-only and selected with `-c config_file=` |
| `initdb/00-roles.sh` | roles `shop_app`, `debezium` (REPLICATION), `reader`; passwords from `.env` |
| `initdb/01-schemas.sql` | schemas `shop` and `sim` owned by `shop_app`; USAGE/SELECT grants and default privileges |
| `initdb/02-publication.sql` | `CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA shop` |
| `initdb/10-event-trigger.sql` | event trigger `shop_replica_identity_full`: every table created in `shop` gets `REPLICA IDENTITY FULL` (02 §2) |
| `initdb/20-heartbeat.sql` | `dbz.heartbeat` (one row) for Debezium's `heartbeat.action.query`; added to `dbz_publication` (required by Debezium for the slot to advance while `shop` is idle); grants `USAGE` on `sim` + `SELECT/INSERT/UPDATE` on this table to `debezium`, `SELECT` to `reader` (02 §5, 03 §7) |
| `sql/truth-views.sql` | reconciliation views `sim.v_truth_*` (02 §8); **not** initdb: applied by the simulator as `shop_app` after it creates the tables, `CREATE OR REPLACE` only, re-runnable |

The scripts in `initdb/` run only when the data volume is empty
(`docker compose down -v` to re-run them) and must stay idempotent in the sense
"two fresh runs give the same catalog" (test
`tests/integration/test_postgres.py::test_initdb_twice_gives_identical_state`).
Numbering reserved for the `database-cdc` owner:

- `1x-*`: event trigger (done, `10-event-trigger.sql`)
- `2x-*`: heartbeat table for Debezium (done, `20-heartbeat.sql`)
- `3x-*`: free. The truth views were moved out of initdb (`sql/truth-views.sql`)
  because they reference tables that do not exist at initdb time.

## What the simulator (`shop_app`) gets for free

- Any table it creates in `shop` is `REPLICA IDENTITY FULL` (event trigger),
  published to Debezium (`FOR TABLES IN SCHEMA`), and readable by `debezium`
  and `reader` (`ALTER DEFAULT PRIVILEGES` in `01-schemas.sql`). Views it creates
  in `sim` are readable by `reader` the same way (`ON TABLES` covers views).
- Nothing else is granted automatically: sequences and functions keep the
  PostgreSQL defaults (functions are executable by PUBLIC, which the
  `sim.truth_row_counts()` helper relies on).

## What the simulator must do (contract)

- Apply `sql/truth-views.sql` as `shop_app` right after creating the tables, as
  one SQL batch (`psql -f` or `conn.execute(path.read_text())`). It needs
  `shop.customers`, `shop.orders`, `shop.order_items`, `shop.order_payments`,
  `shop.order_reviews` with the Olist CSV column names. The view names and
  columns are listed in the file header and in 02 §8.
- **Index policy (02 §7):** the columns of every FK constraint must be the
  *leading* columns of some index on the child table. A PK or unique index
  counts if it starts with them. For the FKs detected on the data (01 §3):

  | FK | Covered by | Extra index needed |
  |---|---|---|
  | `order_items.order_id → orders` | PK `(order_id, order_item_id)` | no |
  | `order_payments.order_id → orders` | PK `(order_id, payment_sequential)` | no |
  | `order_reviews.order_id → orders` | PK `(review_id, order_id)` does **not** cover (2nd column) | `order_reviews (order_id)` |
  | `orders.customer_id → customers` | - | `orders (customer_id)` |
  | `order_items.product_id → products` | - | `order_items (product_id)` |
  | `order_items.seller_id → sellers` | - | `order_items (seller_id)` |

  Tests: `test_every_fk_column_has_a_leading_index` (generic rule over
  `pg_constraint`) and `test_delete_order_items_by_order_id_uses_index`
  (`EXPLAIN DELETE FROM shop.order_items WHERE order_id = …` with
  `enable_seqscan=off` must use an index whose first column is `order_id`).
  Both pass since stage 1 (the simulator creates the tables and the indexes).
- Keep `sim.sim_state` (and everything else that must not reach Kafka) in `sim`,
  never in `shop`. Tables in `shop` whose name starts with `_` are published but not
  captured by the connector (test probes, 03 §13); do not use that prefix for real tables.
- Never `DROP` a captured table before the connector committed its offset past the
  table's last change (03 §13): Debezium resolves the PK from the catalog when it decodes
  the event, and a keyless event is rejected by the compacted topic.

## Test fixtures

`tests/helpers/postgres.py`: `apply_sql_file`, `create_olist_fixture_tables` /
`drop_olist_fixture_tables` (minimal Olist tables with a known data set and the
expected view results), `catalog_snapshot`, `InitdbProbe` (throw-away container
running the same initdb). Connections as `shop_app`/`reader`/`debezium` are the
`pg_app`/`pg_reader`/`pg_debezium` fixtures in `tests/conftest.py`.

Connect as `reader` from the host: `postgres://reader:<READER_PASSWORD>@localhost:5432/shop`.
