#!/bin/bash
# Creates the application roles with passwords taken from the environment
# (docs/design/02-postgresql.md §4). Runs once, on an empty data volume, as the
# superuser; the image's entrypoint executes /docker-entrypoint-initdb.d/* in
# lexical order. Passwords are passed as psql variables, never interpolated into
# the SQL text.
set -euo pipefail

: "${SHOP_APP_PASSWORD:?SHOP_APP_PASSWORD must be set (see .env.example)}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD must be set (see .env.example)}"
: "${READER_PASSWORD:?READER_PASSWORD must be set (see .env.example)}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v shop_app_pw="$SHOP_APP_PASSWORD" \
     -v debezium_pw="$DEBEZIUM_DB_PASSWORD" \
     -v reader_pw="$READER_PASSWORD" <<'SQL'
-- simulator: owns the business schema (DDL + DML in shop, writes to sim)
CREATE ROLE shop_app LOGIN PASSWORD :'shop_app_pw';
-- CDC: replication slot + read-only snapshot of shop
CREATE ROLE debezium LOGIN REPLICATION PASSWORD :'debezium_pw';
-- tests and agents: read-only on shop and sim
CREATE ROLE reader LOGIN PASSWORD :'reader_pw';

-- allow the roles to connect; no CREATE on the database for anyone but postgres
REVOKE CREATE ON DATABASE shop FROM PUBLIC;
GRANT CONNECT ON DATABASE shop TO shop_app, debezium, reader;
SQL
