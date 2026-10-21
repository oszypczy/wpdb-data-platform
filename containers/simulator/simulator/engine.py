"""Phases, pacing and resume (01 §4, §5): init → preload → replay → done.

init     one transaction: tables from the CSV headers, catalog rows, truth views, sim_state
preload  every event before SIM_START, in batches (pipeline mode), no pacing
replay   [SIM_START, SIM_END]: one transaction per event, paced by SimClock + RateLimiter
done     the window is finished; the container stays up (heartbeat) until stopped

Every transaction moves the cursor in sim.sim_state, so after a kill the next start
continues from the exact event after the last committed one. A lost connection is
retried and the run resumes from the stored cursor.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from itertools import batched
from pathlib import Path

import psycopg

from simulator import events as ev
from simulator.clock import Pacer, RateLimiter, SimClock
from simulator.config import Config
from simulator.csvio import load_dir
from simulator.db import Database, SimState, Stopped
from simulator.render import Renderer
from simulator.schema import TableSchema, infer_schemas

log = logging.getLogger("simulator")


class Heartbeat:
    """Liveness file for the container healthcheck: mtime = last beat, content = phase."""

    def __init__(self, path: Path | None):
        self.path = path
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            # a file left by a killed process must not make the restarted container healthy
            path.unlink(missing_ok=True)

    def beat(self, phase: str) -> None:
        if self.path is None:
            return
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(phase)
        tmp.replace(self.path)


@dataclass(slots=True)
class Loaded:
    schemas: dict[str, TableSchema]
    dataset: ev.Dataset
    events: list[ev.Event]
    renderer: Renderer


class Simulator:
    def __init__(
        self,
        cfg: Config,
        db: Database,
        *,
        heartbeat: Heartbeat | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.cfg = cfg
        self.db = db
        self.heartbeat = heartbeat or Heartbeat(None)
        self._sleep = sleep
        self._mono = monotonic
        self.stopping = False
        self.phase = "init"

    def request_stop(self) -> None:
        self.stopping = True

    # -- helpers ------------------------------------------------------------------
    def _pause(self, seconds: float) -> None:
        """Sleep in slices so the heartbeat keeps beating and a stop request is honoured."""
        deadline = self._mono() + seconds
        while not self.stopping:
            remaining = deadline - self._mono()
            if remaining <= 0:
                return
            self.heartbeat.beat(self.phase)
            self._sleep(min(remaining, 1.0))

    def _set_phase(self, phase: str) -> None:
        self.phase = phase
        self.heartbeat.beat(phase)

    # -- data ---------------------------------------------------------------------
    def load(self) -> Loaded:
        cfg = self.cfg
        t0 = self._mono()
        tables = load_dir(cfg.data_dir)
        log.info(
            "loaded %d CSV files from %s: %s",
            len(tables),
            cfg.data_dir,
            ", ".join(f"{n}={len(t)}" for n, t in tables.items()),
        )
        schemas, rejected = infer_schemas(tables, force_nullable={"orders": ev.FUTURE_COLUMNS})
        for name, s in schemas.items():
            fks = ", ".join(f"{fk.column}→{fk.ref_table}" for fk in s.foreign_keys) or "-"
            log.info(
                "schema %s: pk=%s fk=[%s] columns=%s",
                name,
                "+".join(s.primary_key),
                fks,
                ", ".join(
                    f"{c.name} {c.sql_type}{'' if c.nullable else ' NOT NULL'}" for c in s.columns
                ),
            )
        for r in rejected:
            log.info(
                "fk rejected: %s.%s → %s (%d values missing, e.g. %s)",
                r.table,
                r.column,
                r.ref_table,
                r.missing,
                ", ".join(r.examples),
            )
        dataset = ev.Dataset(tables)
        events = ev.build_events(dataset.build_orders(), dataset, until=cfg.sim_end)
        log.info("built %d events up to %s in %.1f s", len(events), cfg.sim_end, self._mono() - t0)
        return Loaded(schemas, dataset, events, Renderer(schemas, dataset))

    # -- phases -------------------------------------------------------------------
    def run(self) -> None:
        loaded = self.load()
        while not self.stopping:
            try:
                self._run_phases(loaded)
                return
            except Stopped:
                return
            except psycopg.OperationalError as exc:
                log.error("database connection lost (%s); reconnecting", str(exc).strip())
                self.db.close()
                self._pause(2.0)
        log.info("stopped")

    def _run_phases(self, loaded: Loaded) -> None:
        cfg = self.cfg
        self.db.connect(should_stop=lambda: self.stopping, sleep=self._pause)
        state = self.db.load_state()
        if state is None:
            existing = self.db.existing_tables(loaded.schemas)
            if existing:
                raise RuntimeError(
                    f"tables {sorted(existing)} exist in shop but sim.sim_state has no row: "
                    "reset the database (make reset)"
                )
            self._set_phase("init")
            self.db.initialize(
                loaded.schemas,
                {t: loaded.dataset.tables[t] for t in ev.CATALOG_TABLES},
                cfg.truth_views.read_text(),
                sim_start=cfg.sim_start,
                sim_end=cfg.sim_end,
                sim_speed=cfg.sim_speed,
                schema_evolution_at=cfg.schema_evolution_at,
            )
            state = self.db.load_state()
            assert state is not None
            log.info(
                "initialized %d tables, catalogs and truth views; window %s → %s",
                len(loaded.schemas),
                cfg.sim_start,
                cfg.sim_end,
            )
        else:
            log.info(
                "resuming: phase=%s cursor=%s events_applied=%d",
                state.phase,
                state.cursor,
                state.events_applied,
            )
            state = self._reconcile_window(state)
            state = self._reconcile_schema_evolution(state)

        if state.schema_evolution_at is not None:
            log.info(
                "schema evolution (01 §7): ADD COLUMN %s at %s",
                ev.DELIVERY_CHANNEL_COLUMN,
                state.schema_evolution_at,
            )
        events = ev.with_schema_evolution(loaded.events, state.schema_evolution_at)
        preload, replay = ev.split_window(events, state.sim_start, cfg.sim_end)
        if state.phase == "preload":
            self._preload(preload, loaded.renderer, state)
            self.db.set_phase("replay")
            state = self.db.load_state()
            assert state is not None
            log.info("preload finished: %d events applied; replay starts", state.events_applied)
        if state.phase == "replay":
            if not self._replay(replay, loaded.renderer, state):
                return
            self.db.set_phase("done")
            state = self.db.load_state()
            assert state is not None
            log.info(
                "window finished at %s: %d events (rows c=%d u=%d d=%d), orders=%d",
                cfg.sim_end,
                state.events_applied,
                state.rows_inserted,
                state.rows_updated,
                state.rows_deleted,
                state.orders_inserted,
            )
        self._set_phase("done")
        while not self.stopping:
            self._pause(5.0)

    def _reconcile_window(self, state: SimState) -> SimState:
        """The stored SIM_START stays (the preload already happened with it); a changed
        SIM_END is adopted, and a finished run with a later SIM_END goes back to replay."""
        cfg = self.cfg
        if state.sim_start != cfg.sim_start:
            log.warning(
                "SIM_START=%s differs from the stored %s: keeping the stored one",
                cfg.sim_start,
                state.sim_start,
            )
        if state.sim_end != cfg.sim_end:
            log.warning("SIM_END changed %s → %s", state.sim_end, cfg.sim_end)
            phase = (
                "replay" if state.phase == "done" and cfg.sim_end > state.sim_end else state.phase
            )
            self.db.set_phase(phase, sim_end=cfg.sim_end)
            state = self.db.load_state()
            assert state is not None
        return state

    def _reconcile_schema_evolution(self, state: SimState) -> SimState:
        """The stored moment is part of the deterministic stream. Once the cursor has passed
        it, it is fixed (a changed or removed setting is ignored with a warning). Before
        that, the setting may be changed or switched on/off, unless the new moment is
        already behind the cursor (ignored with a warning: the stream cannot be rewritten)."""
        wanted, stored = self.cfg.schema_evolution_at, state.schema_evolution_at
        if wanted == stored:
            return state
        cursor = state.cursor

        def passed(at: datetime) -> bool:
            return cursor is not None and ev.schema_event(at).key <= cursor

        if stored is not None and passed(stored):
            log.warning(
                "SIM_SCHEMA_EVOLUTION_AT=%s differs from the stored %s, which is already "
                "applied: keeping the stored one",
                wanted,
                stored,
            )
            return state
        if wanted is not None and passed(wanted):
            log.warning(
                "SIM_SCHEMA_EVOLUTION_AT=%s is behind the resume position %s: ignored",
                wanted,
                cursor,
            )
            wanted = None
            if stored is None:
                return state
        log.info("schema evolution moment changed %s → %s", stored, wanted)
        self.db.set_schema_evolution(wanted)
        new_state = self.db.load_state()
        assert new_state is not None
        return new_state

    def _preload(self, events: list[ev.Event], renderer: Renderer, state: SimState) -> None:
        self._set_phase("preload")
        pending = list(ev.after_cursor(events, state.cursor))
        log.info(
            "preload: %d events before %s (%d already applied)",
            len(pending),
            state.sim_start,
            len(events) - len(pending),
        )
        applied = 0
        last_log = self._mono()
        for chunk in batched(pending, self.cfg.preload_batch):
            if self.stopping:
                raise Stopped
            self.db.apply(chunk, renderer, pipeline=True)
            applied += len(chunk)
            self.heartbeat.beat("preload")
            if self._mono() - last_log >= self.cfg.log_every:
                log.info("[sim %s] phase=preload %d/%d events", chunk[-1].ts, applied, len(pending))
                last_log = self._mono()

    def _replay(self, events: list[ev.Event], renderer: Renderer, state: SimState) -> bool:
        """Returns False when stopped before the window was finished."""
        cfg = self.cfg
        self._set_phase("replay")
        start = (
            state.sim_clock
            if state.sim_clock is not None and state.sim_clock >= state.sim_start
            else state.sim_start
        )
        clock = SimClock(start, cfg.sim_speed, self._mono)
        pacer = Pacer(clock, RateLimiter(cfg.max_events_per_sec, self._mono), self._pause)
        log.info(
            "replay: clock at %s, speed ×%g, limit %g rows/s, %d events in window",
            start,
            cfg.sim_speed,
            cfg.max_events_per_sec,
            len(events),
        )
        orders, c, u, d = (
            state.orders_inserted,
            state.rows_inserted,
            state.rows_updated,
            state.rows_deleted,
        )
        last_log = last_beat = self._mono()
        rows_since_log = 0
        throttle_wait = 0.0
        sim_clock = state.sim_clock
        for e in ev.after_cursor(events, state.cursor):
            result = pacer.wait_for(e.ts, e.rows)
            if self.stopping:
                return False
            self.db.apply([e], renderer)
            sim_clock = e.ts
            orders += e.kind == "purchase"
            c += e.inserts
            u += e.updates
            d += e.deletes
            rows_since_log += e.rows
            throttle_wait += result.throttle_wait
            now = self._mono()
            if now - last_beat >= 1.0:
                self.heartbeat.beat("replay")
                last_beat = now
            if now - last_log >= cfg.log_every:
                share = throttle_wait / (now - last_log)
                log.info(
                    "[sim %s] phase=replay orders=%d rows=%d (c=%d u=%d d=%d) rate=%.1f/s%s",
                    sim_clock,
                    orders,
                    c + u + d,
                    c,
                    u,
                    d,
                    rows_since_log / (now - last_log),
                    f" throttled={share:.0%}" if share >= 0.01 else "",
                )
                last_log, rows_since_log, throttle_wait = now, 0, 0.0
        return not self.stopping
