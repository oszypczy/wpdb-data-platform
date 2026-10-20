-- Publication for Debezium (docs/design/02-postgresql.md §3): every table that
-- exists or will exist in schema shop is published (PostgreSQL 15+ syntax).
-- Debezium uses it with publication.autocreate.mode=disabled.
-- Executed once on an empty volume, as the superuser (required for
-- FOR TABLES IN SCHEMA).
CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA shop;
