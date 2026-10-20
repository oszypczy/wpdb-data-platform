-- Event trigger: REPLICA IDENTITY FULL for every table created in schema shop
-- (docs/design/02-postgresql.md §2). With the default identity, UPDATE/DELETE
-- events from pgoutput carry only the old primary key; with FULL, Debezium gets
-- the whole old row in `before` (status transitions, revenue corrections on
-- DELETE). Executed once on an empty volume, as the superuser (CREATE EVENT
-- TRIGGER needs it); the simulator creates its tables later as shop_app and
-- never has to know about this.
--
-- The function runs with the privileges of the role that issued the DDL
-- (shop_app owns everything it creates in shop), so it is deliberately not
-- SECURITY DEFINER. Tables in other schemas (sim.sim_state, the heartbeat
-- table) keep the default identity: they are not published.

CREATE OR REPLACE FUNCTION public.shop_replica_identity_full()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    cmd record;
BEGIN
    FOR cmd IN
        SELECT object_identity
        FROM pg_event_trigger_ddl_commands()
        WHERE schema_name = 'shop'
          AND object_type = 'table'
          AND command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
    LOOP
        -- object_identity is already schema-qualified and quoted
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', cmd.object_identity);
    END LOOP;
END;
$$;

COMMENT ON FUNCTION public.shop_replica_identity_full() IS
    'ddl_command_end handler: ALTER TABLE ... REPLICA IDENTITY FULL for new tables in schema shop (02 §2)';

-- the nested ALTER TABLE fires ddl_command_end again, but its tag is filtered out above
CREATE EVENT TRIGGER shop_replica_identity_full
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
    EXECUTE FUNCTION public.shop_replica_identity_full();
