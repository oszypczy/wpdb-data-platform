"""Simulation clock, rate limiter and pacing (01 §4), pure: time sources are injected.

SimClock: simulated time runs SIM_SPEED times faster than wall time from an anchor.
RateLimiter: at most MAX_EVENTS_PER_SEC row changes per second; an event may start when
the previous one has "paid" for its rows (rows / rate seconds), so no window of one
second contains more than the limit plus the overshoot of a single transaction.
Pacer: waits for the clock, then for the limiter; when the limiter had to wait, the
simulation clock is held back to the event time (backpressure: the simulation slows
down instead of building a backlog, and nothing is dropped).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta


class SimClock:
    def __init__(
        self, start: datetime, speed: float, monotonic: Callable[[], float] = time.monotonic
    ):
        if speed <= 0:
            raise ValueError("speed must be positive")
        self.speed = speed
        self._mono = monotonic
        self._anchor_sim = start
        self._anchor_wall = monotonic()

    def now(self) -> datetime:
        return self._anchor_sim + timedelta(seconds=(self._mono() - self._anchor_wall) * self.speed)

    def seconds_until(self, ts: datetime) -> float:
        """Wall seconds to wait until the simulated clock reaches `ts` (0 if already past)."""
        return max(0.0, (ts - self.now()).total_seconds() / self.speed)

    def hold_back_to(self, ts: datetime) -> None:
        """Backpressure: make `ts` the current simulated time if the clock ran ahead of it."""
        if ts < self.now():
            self._anchor_sim = ts
            self._anchor_wall = self._mono()


class RateLimiter:
    def __init__(self, max_rows_per_sec: float, monotonic: Callable[[], float] = time.monotonic):
        if max_rows_per_sec <= 0:
            raise ValueError("max_rows_per_sec must be positive")
        self.rate = max_rows_per_sec
        self._mono = monotonic
        self._next_allowed: float | None = None

    def seconds_until_allowed(self) -> float:
        if self._next_allowed is None:
            return 0.0
        return max(0.0, self._next_allowed - self._mono())

    def record(self, rows: int) -> None:
        """Account for an event of `rows` row changes starting now."""
        now = self._mono()
        start = now if self._next_allowed is None else max(now, self._next_allowed)
        self._next_allowed = start + rows / self.rate


@dataclass(frozen=True, slots=True)
class PaceResult:
    slept: float
    throttled: bool
    throttle_wait: float = 0.0


class Pacer:
    def __init__(self, clock: SimClock, limiter: RateLimiter, sleep: Callable[[float], None]):
        self.clock = clock
        self.limiter = limiter
        self._sleep = sleep

    def wait_for(self, ts: datetime, rows: int) -> PaceResult:
        slept = 0.0
        delay = self.clock.seconds_until(ts)
        if delay > 0:
            self._sleep(delay)
            slept += delay
        wait = self.limiter.seconds_until_allowed()
        throttled = wait > 0
        if throttled:
            self._sleep(wait)
            slept += wait
            self.clock.hold_back_to(ts)
        self.limiter.record(rows)
        return PaceResult(slept, throttled, wait)
