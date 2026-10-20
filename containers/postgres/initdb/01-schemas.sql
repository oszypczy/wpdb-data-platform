-- Schemas and privileges (docs/design/02-postgresql.md §4).
-- shop: business tables created dynamically by the simulator (captured by CDC).
-- sim:  simulator state, heartbeat and reconciliation ("truth") views (not captured).
-- Executed once on an empty volume, after 00-roles.sh.

CREATE SCHEMA shop AUTHORIZATION shop_app;
CREATE SCHEMA sim  AUTHORIZATION shop_app;

-- nobody else may create objects in these schemas; readers may only look
REVOKE ALL ON SCHEMA shop, sim FROM PUBLIC;
GRANT USAGE ON SCHEMA shop TO debezium, reader;
GRANT USAGE ON SCHEMA sim  TO reader;

-- tables are created later by shop_app, so grant SELECT on them in advance
ALTER DEFAULT PRIVILEGES FOR ROLE shop_app IN SCHEMA shop GRANT SELECT ON TABLES TO debezium, reader;
ALTER DEFAULT PRIVILEGES FOR ROLE shop_app IN SCHEMA sim  GRANT SELECT ON TABLES TO reader;

-- the reader role never writes anything, not even temp tables
REVOKE TEMP ON DATABASE shop FROM reader;
