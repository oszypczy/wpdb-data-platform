"""PostgreSQL configuration and roles (docs/design/02-postgresql.md §1, §3, §4, §10)."""

from __future__ import annotations

import pytest
from psycopg import errors

pytestmark = pytest.mark.integration

PROBE = "shop._stage0_probe"


def test_wal_level_logical_and_slot_limits(pg_reader):
    """02 §10: wal_level=logical, max_slot_wal_keep_size and slot/sender limits set."""
    show = {
        k: pg_reader.execute(f"SHOW {k}").fetchone()[0]
        for k in (
            "wal_level",
            "max_replication_slots",
            "max_wal_senders",
            "max_slot_wal_keep_size",
            "wal_compression",
        )
    }
    assert show["wal_level"] == "logical", show
    assert int(show["max_replication_slots"]) == 4, show
    assert int(show["max_wal_senders"]) == 4, show
    assert show["max_slot_wal_keep_size"] == "2GB", show
    assert show["wal_compression"] in ("on", "pglz"), show


def test_schemas_and_roles_exist(pg_reader):
    """02 §4: schemas shop/sim owned by shop_app; roles shop_app, debezium (REPLICATION), reader."""
    schemas = dict(
        pg_reader.execute(
            "SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace "
            "WHERE nspname IN ('shop','sim')"
        ).fetchall()
    )
    assert schemas == {"shop": "shop_app", "sim": "shop_app"}
    roles = {
        r[0]: (r[1], r[2], r[3])
        for r in pg_reader.execute(
            "SELECT rolname, rolcanlogin, rolreplication, rolsuper FROM pg_roles "
            "WHERE rolname IN ('shop_app','debezium','reader')"
        ).fetchall()
    }
    assert roles == {
        "shop_app": (True, False, False),
        "debezium": (True, True, False),
        "reader": (True, False, False),
    }


def test_publication_covers_schema_shop(pg_reader, pg_app):
    """02 §3: dbz_publication is FOR TABLES IN SCHEMA shop: a new table is published at once."""
    pub = pg_reader.execute(
        "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate "
        "FROM pg_publication WHERE pubname='dbz_publication'"
    ).fetchone()
    assert pub is not None, "publication dbz_publication missing"
    assert pub[0] is False and pub[1:4] == (True, True, True), pub
    schemas = [
        r[0]
        for r in pg_reader.execute(
            "SELECT n.nspname FROM pg_publication_namespace pn "
            "JOIN pg_publication p ON p.oid = pn.pnpubid "
            "JOIN pg_namespace n ON n.oid = pn.pnnspid WHERE p.pubname='dbz_publication'"
        ).fetchall()
    ]
    assert schemas == ["shop"]
    pg_app.execute(f"DROP TABLE IF EXISTS {PROBE}")
    pg_app.execute(f"CREATE TABLE {PROBE} (id int PRIMARY KEY, v text)")
    try:
        published = [
            r[0]
            for r in pg_reader.execute(
                "SELECT tablename FROM pg_publication_tables "
                "WHERE pubname='dbz_publication' AND schemaname='shop'"
            ).fetchall()
        ]
        assert "_stage0_probe" in published, published
    finally:
        pg_app.execute(f"DROP TABLE {PROBE}")


def test_reader_is_read_only(pg_reader, pg_app):
    """02 §4: reader sees shop_app tables (default privileges) but cannot write or create."""
    pg_app.execute(f"DROP TABLE IF EXISTS {PROBE}")
    pg_app.execute(f"CREATE TABLE {PROBE} (id int PRIMARY KEY, v text)")
    pg_app.execute(f"INSERT INTO {PROBE} VALUES (1, 'a')")
    try:
        assert pg_reader.execute(f"SELECT count(*) FROM {PROBE}").fetchone()[0] == 1
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"INSERT INTO {PROBE} VALUES (2, 'b')")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"UPDATE {PROBE} SET v = 'z'")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"DELETE FROM {PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"TRUNCATE {PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute("CREATE TABLE shop._reader_table (id int)")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute("CREATE TABLE sim._reader_table (id int)")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute("CREATE TABLE public._reader_table (id int)")
    finally:
        pg_app.execute(f"DROP TABLE {PROBE}")


def test_debezium_cannot_write(pg_debezium, pg_app):
    """02 §10: debezium reads shop and may replicate; INSERT/UPDATE/DDL in shop are rejected."""
    pg_app.execute(f"DROP TABLE IF EXISTS {PROBE}")
    pg_app.execute(f"CREATE TABLE {PROBE} (id int PRIMARY KEY, v text)")
    pg_app.execute(f"INSERT INTO {PROBE} VALUES (1, 'a')")
    try:
        assert pg_debezium.execute(f"SELECT v FROM {PROBE}").fetchone() == ("a",)
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"INSERT INTO {PROBE} VALUES (2, 'b')")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"UPDATE {PROBE} SET v = 'z'")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"DELETE FROM {PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"TRUNCATE {PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"ALTER TABLE {PROBE} ADD COLUMN extra text")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute("CREATE TABLE shop._dbz_table (id int)")
        # replication is allowed: a temporary slot can be created and dropped
        slot = pg_debezium.execute(
            "SELECT slot_name FROM pg_create_logical_replication_slot("
            "'_stage0_probe_slot', 'pgoutput', true)"
        ).fetchone()
        assert slot == ("_stage0_probe_slot",)
    finally:
        pg_app.execute(f"DROP TABLE {PROBE}")


SIM_PROBE = "sim._stage0_probe"


def test_sim_schema_privileges(pg_reader, pg_debezium, pg_app):
    """02 §4: reader may SELECT in sim (not write); debezium may enter sim only for the
    heartbeat table (02 §5, stage 2) and cannot read or write anything else there."""
    pg_app.execute(f"DROP TABLE IF EXISTS {SIM_PROBE}")
    pg_app.execute(f"CREATE TABLE {SIM_PROBE} (id int PRIMARY KEY, v text)")
    pg_app.execute(f"INSERT INTO {SIM_PROBE} VALUES (1, 'a')")
    try:
        assert pg_reader.execute(f"SELECT v FROM {SIM_PROBE}").fetchone() == ("a",)
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"INSERT INTO {SIM_PROBE} VALUES (2, 'b')")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_reader.execute(f"DELETE FROM {SIM_PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"SELECT v FROM {SIM_PROBE}")
        with pytest.raises(errors.InsufficientPrivilege):
            pg_debezium.execute(f"INSERT INTO {SIM_PROBE} VALUES (3, 'c')")
        assert pg_reader.execute(
            "SELECT has_schema_privilege('debezium', 'sim', 'USAGE'), "
            "has_schema_privilege('debezium', 'sim', 'CREATE')"
        ).fetchone() == (True, False)
        assert pg_reader.execute(
            "SELECT has_table_privilege('debezium', 'sim.sim_state', 'SELECT')"
        ).fetchone() == (False,)
        assert pg_reader.execute(
            "SELECT has_schema_privilege('reader', 'sim', 'USAGE')"
        ).fetchone() == (True,)
        assert pg_reader.execute(
            "SELECT has_schema_privilege('reader', 'sim', 'CREATE')"
        ).fetchone() == (False,)
    finally:
        pg_app.execute(f"DROP TABLE {SIM_PROBE}")


# --- stage 1 (02 §2, §7, §8, §9, §10) -------------------------------------------
def test_new_table_in_shop_gets_replica_identity_full(pg_reader, pg_app):
    """02 §10: new table in shop → REPLICA IDENTITY FULL (event trigger) and in dbz_publication."""
    trigger = pg_reader.execute(
        "SELECT evtevent, evtenabled, evttags FROM pg_event_trigger "
        "WHERE evtname = 'shop_replica_identity_full'"
    ).fetchone()
    assert trigger is not None, "event trigger shop_replica_identity_full missing"
    assert trigger[0] == "ddl_command_end" and trigger[1] == "O", trigger
    assert set(trigger[2]) == {"CREATE TABLE", "CREATE TABLE AS", "SELECT INTO"}, trigger

    def identity(table: str) -> str:
        return pg_reader.execute(
            "SELECT relreplident FROM pg_class WHERE oid = %s::regclass", (table,)
        ).fetchone()[0]

    ctas = "shop._stage1_probe_ctas"
    pg_app.execute(f"DROP TABLE IF EXISTS {PROBE}")
    pg_app.execute(f"DROP TABLE IF EXISTS {ctas}")
    pg_app.execute(f"DROP TABLE IF EXISTS {SIM_PROBE}")
    pg_app.execute(f"CREATE TABLE {PROBE} (id int PRIMARY KEY, v text)")
    pg_app.execute(f"CREATE TABLE {ctas} AS SELECT 1 AS id, 'a' AS v")
    pg_app.execute(f"CREATE TABLE {SIM_PROBE} (id int PRIMARY KEY, v text)")
    try:
        assert identity(PROBE) == "f", "CREATE TABLE in shop did not get REPLICA IDENTITY FULL"
        assert identity(ctas) == "f", "CREATE TABLE AS in shop did not get REPLICA IDENTITY FULL"
        assert identity(SIM_PROBE) == "d", "tables outside shop must keep the default identity"
        published = {
            r[0]
            for r in pg_reader.execute(
                "SELECT tablename FROM pg_publication_tables "
                "WHERE pubname = 'dbz_publication' AND schemaname = 'shop'"
            ).fetchall()
        }
        assert {"_stage0_probe", "_stage1_probe_ctas"} <= published, published
        # the identity survives DDL that the simulator also issues (schema evolution, 03 §5)
        pg_app.execute(f"ALTER TABLE {PROBE} ADD COLUMN extra text")
        assert identity(PROBE) == "f"
    finally:
        pg_app.execute(f"DROP TABLE {PROBE}")
        pg_app.execute(f"DROP TABLE {ctas}")
        pg_app.execute(f"DROP TABLE {SIM_PROBE}")


def test_initdb_twice_gives_identical_state(stack, settings, pg_reader):
    """02 §10: initdb scripts on an empty volume, twice → identical database state.

    Two throw-away containers from the same image, initdb/ and postgresql.conf are
    compared with each other and with the running stack (initdb objects only, so
    simulator tables do not matter).
    """
    from tests.helpers.postgres import InitdbProbe, catalog_snapshot

    snapshots = []
    for _ in range(2):
        with InitdbProbe(root=stack.root, env=settings) as probe:
            snapshots.append(probe.catalog_snapshot())
    assert snapshots[0] == snapshots[1], "two initdb runs differ:\n" + "\n".join(
        sorted(set(snapshots[0]) ^ set(snapshots[1]))
    )
    live = catalog_snapshot(pg_reader)
    assert live == snapshots[0], "running stack differs from a fresh initdb:\n" + "\n".join(
        sorted(set(live) ^ set(snapshots[0]))
    )
    kinds = {line.split()[0] for line in live}
    assert kinds == {
        "role",
        "database",
        "schema",
        "defacl",
        "publication",
        "pubschema",
        "pubtable",
        "acl",
        "eventtrigger",
        "setting",
    }, kinds


def _shop_tables(conn) -> set[str]:
    return {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='shop'")}


def test_truth_views_exact_on_known_data(stack, settings, root):
    """02 §8: truth-views.sql applied twice as shop_app on a known Olist-shaped data set
    gives exactly the expected values, and reader can SELECT every view.

    Runs in a throw-away container (same image + initdb as the stack) because the
    live stack already holds the simulator's tables.
    """
    from tests.helpers import postgres as pgh

    sql = (root / pgh.TRUTH_VIEWS_SQL).read_text()
    exp = pgh.OLIST_FIXTURE_EXPECTED
    with pgh.InitdbProbe(root=stack.root, env=settings) as probe:
        probe.psql(pgh.OLIST_FIXTURE_DDL + pgh.OLIST_FIXTURE_DATA, user="shop_app")
        probe.psql(sql, user="shop_app")
        probe.psql(sql, user="shop_app")  # re-runnable: CREATE OR REPLACE only
        # the event trigger of a fresh initdb covered the fixture tables too
        idents = probe.psql(
            "SELECT relname, relreplident FROM pg_class "
            "WHERE relnamespace = 'shop'::regnamespace AND relkind = 'r' ORDER BY 1"
        )
        assert idents == [[t, "f"] for t in sorted(pgh.OLIST_FIXTURE_TABLES)], idents
        for view, cols in pgh.TRUTH_VIEW_COLUMNS.items():
            got = [
                r[0]
                for r in probe.psql(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = 'sim' AND table_name = '{view}' "
                    "ORDER BY ordinal_position",
                    user="reader",
                )
            ]
            assert got == cols, (view, got)

        def rows(view: str) -> list[list[str]]:
            return probe.psql(f"SELECT * FROM sim.{view}", user="reader")

        counts = {r[0]: int(r[1]) for r in rows("v_truth_row_counts")}
        assert counts == exp["v_truth_row_counts"], counts
        by_status = {r[0]: (int(r[1]), int(r[2]), r[3], r[4]) for r in rows("v_truth_orders")}
        assert by_status == exp["v_truth_orders"], by_status
        (totals,) = rows("v_truth_totals")
        assert tuple(int(v) for v in totals[:5]) + tuple(totals[5:]) == exp["v_truth_totals"]
        by_state = {r[0]: (int(r[1]), int(r[2]), r[3]) for r in rows("v_truth_revenue_by_state")}
        assert by_state == exp["v_truth_revenue_by_state"], by_state
        payments = {r[0]: (int(r[1]), r[2]) for r in rows("v_truth_payments")}
        assert payments == exp["v_truth_payments"], payments
        (lifecycle,) = rows("v_truth_order_lifecycle")
        assert tuple(int(v) for v in lifecycle) == exp["v_truth_order_lifecycle"], lifecycle


def test_truth_views_apply_and_reconcile(pg_reader):
    """02 §8: on the live stack the simulator has applied truth-views.sql: every
    sim.v_truth_* view exists with the agreed columns, is readable by reader, and
    agrees with direct count(*) of the tables inside one REPEATABLE READ snapshot
    (exact even while the simulator keeps writing)."""
    from tests.helpers import postgres as pgh
    from tests.helpers.wait import wait_until

    def views_ready() -> bool:
        views = {
            r[0] for r in pg_reader.execute("SELECT viewname FROM pg_views WHERE schemaname='sim'")
        }
        return set(pgh.TRUTH_VIEW_COLUMNS) <= views

    # the simulator (default service) creates tables and views in its init
    # transaction; it may be restarting under another test, so wait, do not assume
    wait_until(views_ready, timeout=120, every=2, desc="simulator applied truth-views.sql")
    for view, cols in pgh.TRUTH_VIEW_COLUMNS.items():
        assert pgh.view_columns(pg_reader, "sim", view) == cols, view
        assert pg_reader.execute(
            "SELECT has_table_privilege('reader', %s, 'SELECT')", (f"sim.{view}",)
        ).fetchone() == (True,), f"reader cannot SELECT sim.{view}"

    with pg_reader.transaction():
        pg_reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        counts = dict(pg_reader.execute("SELECT * FROM sim.v_truth_row_counts").fetchall())
        direct = {
            t: pg_reader.execute(f"SELECT count(*) FROM shop.{t}").fetchone()[0]
            for t in _shop_tables(pg_reader)
        }
        by_status = pg_reader.execute("SELECT orders, items FROM sim.v_truth_orders").fetchall()
        totals = pg_reader.execute("SELECT * FROM sim.v_truth_totals").fetchone()
        by_state = pg_reader.execute("SELECT orders FROM sim.v_truth_revenue_by_state").fetchall()
        payments = pg_reader.execute("SELECT payments FROM sim.v_truth_payments").fetchall()
        lifecycle = pg_reader.execute("SELECT orders FROM sim.v_truth_order_lifecycle").fetchone()
    assert set(pgh.OLIST_FIXTURE_TABLES) <= set(direct), sorted(direct)
    assert counts == direct, (counts, direct)
    assert sum(r[0] for r in by_status) == totals[0] == lifecycle[0] == direct["orders"]
    assert sum(r[1] for r in by_status) == totals[2] == direct["order_items"]
    assert sum(r[0] for r in by_state) == totals[0]
    assert sum(r[0] for r in payments) == totals[3] == direct["order_payments"]


def _leading_index_columns(conn, table: str) -> dict[str, list[str]]:
    """index name -> its key columns in order, for every index of shop.<table>."""
    rows = conn.execute(
        "SELECT i.relname, a.attname, array_position(x.indkey, a.attnum) AS pos "
        "FROM pg_index x "
        "JOIN pg_class t ON t.oid = x.indrelid "
        "JOIN pg_class i ON i.oid = x.indexrelid "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (x.indkey) "
        "WHERE t.oid = %s::regclass AND x.indpred IS NULL "
        "ORDER BY 1, 3",
        (f"shop.{table}",),
    ).fetchall()
    out: dict[str, list[str]] = {}
    for name, col, _ in rows:
        out.setdefault(name, []).append(col)
    return out


def test_every_fk_column_has_a_leading_index(pg_reader):
    """02 §7: every detected FK column is the leading column of an index on the child table."""
    fks = pg_reader.execute(
        "SELECT c.conrelid::regclass::text, c.conname, "
        "       array_agg(a.attname ORDER BY k.ord) "
        "FROM pg_constraint c "
        "JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum, ord) ON true "
        "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum "
        "WHERE c.contype = 'f' AND c.connamespace = 'shop'::regnamespace "
        "GROUP BY 1, 2 ORDER BY 1, 2"
    ).fetchall()
    # since stage 1 the simulator runs by default and `stack` waits until it is healthy
    # (tables created), so missing FKs are a defect, not a reason to skip
    assert fks, "no foreign keys in schema shop: the simulator must create them (01 §3)"
    missing = []
    for table, conname, cols in fks:
        cols = list(cols)
        indexes = _leading_index_columns(pg_reader, table.split(".", 1)[1])
        if not any(keys[: len(cols)] == cols for keys in indexes.values()):
            missing.append((table, conname, cols, indexes))
    assert missing == [], f"FK columns without a leading index: {missing}"


def _plan_nodes(node: dict) -> list[dict]:
    yield_ = [node]
    for child in node.get("Plans", []):
        yield_ += _plan_nodes(child)
    return yield_


def test_delete_order_items_by_order_id_uses_index(pg_reader, pg_app):
    """02 §10: DELETE FROM order_items WHERE order_id=… → the plan uses an index on order_id."""
    assert "order_items" in _shop_tables(pg_reader), (
        "shop.order_items missing: the simulator (default service, stage 1) must create it"
    )
    indexes = _leading_index_columns(pg_reader, "order_items")
    candidates = {name for name, keys in indexes.items() if keys[:1] == ["order_id"]}
    assert candidates, f"no index on shop.order_items leading with order_id: {indexes}"
    # EXPLAIN (without ANALYZE) plans but does not execute; shop_app is needed because
    # planning a DELETE checks the DELETE privilege. enable_seqscan=off removes the
    # size dependency: on a tiny table the planner would otherwise prefer a seq scan
    # even with a perfectly good index, and the claim here is "an index is usable".
    with pg_app.transaction():
        pg_app.execute("SET LOCAL enable_seqscan = off")
        plan = pg_app.execute(
            "EXPLAIN (FORMAT JSON) DELETE FROM shop.order_items WHERE order_id = %s",
            ("e481f51cbdc54678b7cc49136f2d6af7",),
        ).fetchone()[0]
    nodes = _plan_nodes(plan[0]["Plan"])
    scans = [
        n
        for n in nodes
        if n.get("Node Type") in ("Index Scan", "Index Only Scan", "Bitmap Index Scan")
    ]
    assert scans, f"no index scan in the DELETE plan: {plan}"
    assert any(n.get("Index Name") in candidates for n in scans), (scans, candidates)
    assert not any(n.get("Node Type") == "Seq Scan" for n in nodes), plan


# --- stage 2 (02 §5): heartbeat table ---------------------------------------------------
def test_heartbeat_table_grants_and_publication(pg_reader, pg_debezium):
    """02 §5, 03 §7: sim.heartbeat (one row, id = 1) exists from initdb; debezium may
    SELECT/INSERT/UPDATE exactly this table; it is the only sim table in dbz_publication
    (the schema sim itself is not published); reader can read it."""
    assert pg_reader.execute("SELECT id FROM sim.heartbeat").fetchall() == [(1,)]
    privs = pg_reader.execute(
        "SELECT has_table_privilege('debezium', 'sim.heartbeat', 'SELECT'), "
        "has_table_privilege('debezium', 'sim.heartbeat', 'INSERT'), "
        "has_table_privilege('debezium', 'sim.heartbeat', 'UPDATE'), "
        "has_table_privilege('debezium', 'sim.heartbeat', 'DELETE'), "
        "has_table_privilege('debezium', 'sim.heartbeat', 'TRUNCATE')"
    ).fetchone()
    assert privs == (True, True, True, False, False), privs
    published_sim = [
        r[0]
        for r in pg_reader.execute(
            "SELECT tablename FROM pg_publication_tables "
            "WHERE pubname = 'dbz_publication' AND schemaname = 'sim'"
        )
    ]
    assert published_sim == ["heartbeat"]
    rels = [
        r[0]
        for r in pg_reader.execute(
            "SELECT c.relname FROM pg_publication_rel pr "
            "JOIN pg_publication p ON p.oid = pr.prpubid JOIN pg_class c ON c.oid = pr.prrelid "
            "WHERE p.pubname = 'dbz_publication'"
        )
    ]
    assert rels == ["heartbeat"], "only the heartbeat table is added explicitly"
    # the debezium role can run the connector's upsert and nothing more in sim
    pg_debezium.execute(
        "INSERT INTO sim.heartbeat (id, ts) VALUES (1, now()) "
        "ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
    )
    with pytest.raises(errors.InsufficientPrivilege):
        pg_debezium.execute("DELETE FROM sim.heartbeat")
    with pytest.raises(errors.CheckViolation):
        pg_debezium.execute("INSERT INTO sim.heartbeat (id, ts) VALUES (2, now())")
