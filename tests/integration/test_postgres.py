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
    """02 §4: reader may SELECT in sim (not write); debezium has no access to sim at all."""
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
            "SELECT has_schema_privilege('debezium', 'sim', 'USAGE')"
        ).fetchone() == (False,)
        assert pg_reader.execute(
            "SELECT has_schema_privilege('reader', 'sim', 'USAGE')"
        ).fetchone() == (True,)
        assert pg_reader.execute(
            "SELECT has_schema_privilege('reader', 'sim', 'CREATE')"
        ).fetchone() == (False,)
    finally:
        pg_app.execute(f"DROP TABLE {SIM_PROBE}")
