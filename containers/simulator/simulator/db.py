"""PostgreSQL writer: the only module that talks to the database.

Everything it executes is rendered by the pure modules; this one adds the connection
retry (01 §5), the single-transaction initialization (tables, indexes, catalog COPY,
truth views, sim.sim_state), the per-event transaction that also moves the resume
cursor (02 §6) and pipeline mode for the bulk preload.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg

from simulator.csvio import CsvTable
from simulator.events import Cursor, Event
from simulator.render import Renderer, sql_row
from simulator.schema import (
    TableSchema,
    create_index_sql,
    create_table_sql,
    creation_order,
    qualified,
    quote_ident,
)

log = logging.getLogger("simulator.db")

PHASES = ("preload", "replay", "done")

SIM_STATE_DDL = """
CREATE TABLE {state} (
    id                  smallint PRIMARY KEY CHECK (id = 1),
    phase               text NOT NULL CHECK (phase IN ('preload', 'replay', 'done')),
    sim_start           timestamp NOT NULL,
    sim_end             timestamp NOT NULL,
    sim_speed           double precision NOT NULL,
    sim_clock           timestamp,
    cursor_order_id     text,
    cursor_step         smallint,
    events_applied      bigint NOT NULL DEFAULT 0,
    orders_inserted     bigint NOT NULL DEFAULT 0,
    rows_inserted       bigint NOT NULL DEFAULT 0,
    rows_updated        bigint NOT NULL DEFAULT 0,
    rows_deleted        bigint NOT NULL DEFAULT 0,
    started_at          timestamptz NOT NULL DEFAULT now(),
    preload_finished_at timestamptz,
    finished_at         timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    schema_evolution_at timestamp
)
"""
# columns added after stage 1: created on start if a stage-1 sim_state lacks them
STATE_MIGRATIONS = {"schema_evolution_at": "timestamp"}

STATE_COLUMNS = (
    "phase",
    "sim_start",
    "sim_end",
    "sim_speed",
    "sim_clock",
    "cursor_order_id",
    "cursor_step",
    "events_applied",
    "orders_inserted",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "schema_evolution_at",
)


@dataclass(slots=True)
class SimState:
    phase: str
    sim_start: datetime
    sim_end: datetime
    sim_speed: float
    sim_clock: datetime | None
    cursor_order_id: str | None
    cursor_step: int | None
    events_applied: int
    orders_inserted: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    schema_evolution_at: datetime | None = None

    @property
    def cursor(self) -> Cursor | None:
        if self.sim_clock is None or self.cursor_order_id is None or self.cursor_step is None:
            return None
        return (self.sim_clock, self.cursor_order_id, self.cursor_step)


class Stopped(Exception):
    """A stop was requested while waiting."""


class Database:
    def __init__(
        self, conninfo: Mapping[str, object], *, pg_schema: str = "shop", state_schema: str = "sim"
    ):
        self.conninfo = dict(conninfo)
        self.pg_schema = pg_schema
        self.state_table = qualified(state_schema, "sim_state")
        self._conn: psycopg.Connection | None = None
        self._state_update = (
            f"UPDATE {self.state_table} SET sim_clock = %s, cursor_order_id = %s, "
            "cursor_step = %s, "
            "events_applied = events_applied + %s, orders_inserted = orders_inserted + %s, "
            "rows_inserted = rows_inserted + %s, rows_updated = rows_updated + %s, "
            "rows_deleted = rows_deleted + %s, updated_at = now() WHERE id = 1"
        )

    # -- connection ---------------------------------------------------------------
    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            raise psycopg.OperationalError("not connected")
        return self._conn

    def connect(
        self,
        *,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], None] = time.sleep,
        retry_every: float = 2.0,
    ) -> None:
        """Wait for PostgreSQL (01 §5): retry until it accepts connections or a stop is asked."""
        attempt = 0
        while True:
            try:
                self._conn = psycopg.connect(**self.conninfo)  # type: ignore[arg-type]
                self._conn.autocommit = False
                log.info(
                    "connected to %s@%s:%s/%s",
                    self.conninfo.get("user"),
                    self.conninfo.get("host"),
                    self.conninfo.get("port"),
                    self.conninfo.get("dbname"),
                )
                return
            except psycopg.OperationalError as exc:
                attempt += 1
                if attempt == 1 or attempt % 15 == 0:
                    log.warning(
                        "PostgreSQL not ready (%s); retrying every %.0f s",
                        str(exc).strip(),
                        retry_every,
                    )
            if should_stop():
                raise Stopped
            sleep(retry_every)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 - closing a broken connection may itself fail
                pass
            self._conn = None

    # -- state --------------------------------------------------------------------
    def existing_tables(self, names: Iterable[str]) -> set[str]:
        rows = self.conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename = ANY(%s)",
            (self.pg_schema, list(names)),
        ).fetchall()
        self.conn.rollback()
        return {r[0] for r in rows}

    def load_state(self) -> SimState | None:
        """The resume position, or None when the simulator has never initialized this database."""
        cols = ", ".join(STATE_COLUMNS)
        self._migrate_state()
        try:
            row = self.conn.execute(
                f"SELECT {cols} FROM {self.state_table} WHERE id = 1"
            ).fetchone()
        except psycopg.errors.UndefinedTable:
            self.conn.rollback()
            return None
        self.conn.rollback()
        return SimState(*row) if row else None

    def _migrate_state(self) -> None:
        """Add the columns a sim_state created by an older image lacks (no-op otherwise)."""
        schema, table = self.state_table.replace('"', "").split(".")
        present = {
            r[0]
            for r in self.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            ).fetchall()
        }
        self.conn.rollback()
        missing = {c: t for c, t in STATE_MIGRATIONS.items() if present and c not in present}
        if missing:
            with self.conn.transaction():
                for column, sql_type in missing.items():
                    self.conn.execute(
                        f"ALTER TABLE {self.state_table} ADD COLUMN IF NOT EXISTS "
                        f"{quote_ident(column)} {sql_type}"
                    )
            log.info("sim_state migrated: added %s", ", ".join(missing))

    def set_schema_evolution(self, at: datetime | None) -> None:
        with self.conn.transaction():
            self.conn.execute(
                f"UPDATE {self.state_table} SET schema_evolution_at = %s, updated_at = now() "
                "WHERE id = 1",
                (at,),
            )

    def initialize(
        self,
        schemas: Mapping[str, TableSchema],
        catalogs: Mapping[str, CsvTable],
        truth_views_sql: str,
        *,
        sim_start: datetime,
        sim_end: datetime,
        sim_speed: float,
        schema_evolution_at: datetime | None = None,
    ) -> None:
        """Tables, indexes, catalog rows, truth views and the state row: one transaction, so a
        crash in the middle leaves nothing behind and the next start begins again."""
        conn = self.conn
        with conn.transaction():
            for name in creation_order(schemas):
                conn.execute(create_table_sql(schemas[name], self.pg_schema))
                for stmt in create_index_sql(schemas[name], self.pg_schema):
                    conn.execute(stmt)
            for table in catalogs.values():
                self._copy(table)
            conn.execute(truth_views_sql)
            conn.execute(SIM_STATE_DDL.format(state=self.state_table))
            conn.execute(
                f"INSERT INTO {self.state_table} "
                "(id, phase, sim_start, sim_end, sim_speed, schema_evolution_at) "
                "VALUES (1, 'preload', %s, %s, %s, %s)",
                (sim_start, sim_end, sim_speed, schema_evolution_at),
            )

    def _copy(self, table: CsvTable) -> None:
        """Bulk insert of a catalog table (COPY streams the rows in batches)."""
        cols = ", ".join(quote_ident(c) for c in table.columns)
        with (
            self.conn.cursor() as cur,
            cur.copy(f"COPY {qualified(self.pg_schema, table.name)} ({cols}) FROM STDIN") as copy,
        ):
            for row in table.rows:
                copy.write_row(sql_row(row))

    def set_phase(self, phase: str, *, sim_end: datetime | None = None) -> None:
        if phase not in PHASES:
            raise ValueError(phase)
        with self.conn.transaction():
            self.conn.execute(
                f"UPDATE {self.state_table} SET phase = %s, sim_end = coalesce(%s, sim_end), "
                "preload_finished_at = CASE WHEN %s = 'replay' "
                "THEN coalesce(preload_finished_at, now()) ELSE preload_finished_at END, "
                "finished_at = CASE WHEN %s = 'done' THEN now() ELSE NULL END, "
                "updated_at = now() WHERE id = 1",
                (phase, sim_end, phase, phase),
            )

    # -- events -------------------------------------------------------------------
    def apply(self, events: Sequence[Event], renderer: Renderer, *, pipeline: bool = False) -> None:
        """One transaction: the row changes of `events` plus the cursor of the last one (02 §6).
        `pipeline` sends the statements without waiting for each reply (bulk preload)."""
        if not events:
            return
        conn = self.conn
        last = events[-1]
        inserts = updates = deletes = orders = 0
        for e in events:
            inserts += e.inserts
            updates += e.updates
            deletes += e.deletes
            orders += e.kind == "purchase"
        state_params = (
            last.ts,
            last.order_id,
            last.step,
            len(events),
            orders,
            inserts,
            updates,
            deletes,
        )

        def run() -> None:
            with conn.transaction():
                for e in events:
                    for statement, params in renderer.statements(e):
                        conn.execute(statement, params)
                conn.execute(self._state_update, state_params)

        if pipeline:
            with conn.pipeline():
                run()
        else:
            run()
