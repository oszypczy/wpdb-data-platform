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

The scripts in `initdb/` run only when the data volume is empty
(`docker compose down -v` to re-run them). Keep the numbering free for the
`database-cdc` owner:

- `1x-*`: event trigger `REPLICA IDENTITY FULL` for new tables in `shop` (02 §2)
- `2x-*`: heartbeat table for Debezium (02 §5, 03 §7)
- `3x-*`: reconciliation views `sim.v_truth_*` (02 §8) - note they reference
  simulator tables that do not exist at initdb time, so they must be created
  `CREATE OR REPLACE` from the simulator or an event trigger (open item, stage 1).

Connect as `reader` from the host: `postgres://reader:<READER_PASSWORD>@localhost:5432/shop`.
