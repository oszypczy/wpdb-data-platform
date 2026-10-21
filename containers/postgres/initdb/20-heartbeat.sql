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
-- The schema sim itself stays out of the publication: only this one table is
-- added (pg_publication_rel), sim.sim_state and the truth views are never sent.
-- Executed once on an empty volume, as the superuser (ALTER PUBLICATION needs
-- the publication owner).

CREATE TABLE sim.heartbeat (
    id smallint PRIMARY KEY CHECK (id = 1),
    ts timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE sim.heartbeat OWNER TO shop_app;
INSERT INTO sim.heartbeat (id, ts) VALUES (1, now());

-- The least the debezium role needs: reach the schema and upsert the one row.
-- No SELECT on anything else in sim (the default privileges grant SELECT to
-- reader only), so sim.sim_state stays invisible to the connector.
GRANT USAGE ON SCHEMA sim TO debezium;
GRANT SELECT, INSERT, UPDATE ON TABLE sim.heartbeat TO debezium;
-- the table is created by postgres, so the default privileges of shop_app do not apply
GRANT SELECT ON TABLE sim.heartbeat TO reader;

ALTER PUBLICATION dbz_publication ADD TABLE sim.heartbeat;
