-- Heartbeat table for Debezium (docs/design/02-postgresql.md §5, 03-debezium.md §7).
--
-- Every heartbeat.interval.ms the connector runs heartbeat.action.query (an upsert
-- of the single row below) over its JDBC connection. The change lands in the WAL
-- and, because the table is part of dbz_publication, pgoutput sends it to the
-- connector. The connector filters it out (schema.include.list=shop), but a
-- filtered event still forces a heartbeat record with the current LSN, which
-- commits the offset and lets the slot advance (confirmed_flush_lsn) while shop.*
-- is idle. The Debezium 3.6 docs require exactly this: "you must explicitly add
-- the heartbeat table to the publication" (WAL disk space consumption).
--
-- Own schema dbz, owned by postgres: whoever owns the table or its schema can attach
-- a trigger (or drop the table and recreate it with one), and a trigger fired by
-- the connector's upsert runs with the privileges of the debezium role (LOGIN
-- REPLICATION). sim belongs to shop_app, so the table must not live there. In dbz
-- only postgres can create, alter or drop anything; debezium and reader get USAGE.
-- Executed once on an empty volume, as the superuser (ALTER PUBLICATION needs the
-- publication owner).

CREATE SCHEMA dbz AUTHORIZATION postgres;
REVOKE ALL ON SCHEMA dbz FROM PUBLIC;
GRANT USAGE ON SCHEMA dbz TO debezium, reader;

CREATE TABLE dbz.heartbeat (
    id smallint PRIMARY KEY CHECK (id = 1),
    ts timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON TABLE dbz.heartbeat FROM PUBLIC;
INSERT INTO dbz.heartbeat (id, ts) VALUES (1, now());

-- The least the debezium role needs: upsert the one row. It has no USAGE on sim,
-- so sim.sim_state and the truth views stay invisible to the connector.
GRANT SELECT, INSERT, UPDATE ON TABLE dbz.heartbeat TO debezium;
GRANT SELECT ON TABLE dbz.heartbeat TO reader;

ALTER PUBLICATION dbz_publication ADD TABLE dbz.heartbeat;
