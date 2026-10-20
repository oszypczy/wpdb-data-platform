"""Business simulator, pacing (01 §4, §8 unit row "limit tempa") and configuration:
simulated clock, rate limiter with backpressure, env parsing. No Docker, no sleeping:
time is a fake."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta

import pytest

from simulator.clock import Pacer, RateLimiter, SimClock
from simulator.config import DEFAULTS, Config, ConfigError, parse_bound

pytestmark = pytest.mark.unit

T = datetime.fromisoformat
START = T("2017-11-24 00:00:00")


class FakeTime:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_sim_clock_runs_speed_times_faster_than_wall_time():
    ft = FakeTime()
    clock = SimClock(START, 1440, ft.monotonic)
    assert clock.now() == START
    ft.sleep(60)
    assert clock.now() == START + timedelta(days=1)
    assert clock.seconds_until(START + timedelta(days=2)) == pytest.approx(60)
    assert clock.seconds_until(START) == 0.0
    with pytest.raises(ValueError):
        SimClock(START, 0, ft.monotonic)


def test_rate_limiter_paces_row_changes():
    ft = FakeTime()
    limiter = RateLimiter(20, ft.monotonic)
    assert limiter.seconds_until_allowed() == 0
    limiter.record(5)  # a purchase with 5 rows costs 0.25 s
    assert limiter.seconds_until_allowed() == pytest.approx(0.25)
    ft.sleep(0.1)
    assert limiter.seconds_until_allowed() == pytest.approx(0.15)
    ft.sleep(0.2)
    assert limiter.seconds_until_allowed() == 0
    limiter.record(1)
    limiter.record(1)  # queued behind the previous one, not from `now`
    assert limiter.seconds_until_allowed() == pytest.approx(0.1)


def test_pacer_waits_for_the_simulated_clock_when_ahead_of_schedule():
    ft = FakeTime()
    clock = SimClock(START, 3600, ft.monotonic)  # 1 h of history per second
    pacer = Pacer(clock, RateLimiter(100, ft.monotonic), ft.sleep)
    result = pacer.wait_for(START + timedelta(hours=2), rows=3)
    assert result.slept == pytest.approx(2.0) and not result.throttled
    assert ft.sleeps == [pytest.approx(2.0)]
    assert clock.now() == START + timedelta(hours=2)


def test_backpressure_caps_the_rate_holds_the_clock_back_and_loses_nothing():
    """01 §8: at a peak the simulation clock slows down, events/s ≤ MAX_EVENTS_PER_SEC and
    every event is applied. Black Friday shape: 300 events due at once, limit 50 rows/s."""
    ft = FakeTime()
    speed, limit = 1440, 50
    clock = SimClock(START, speed, ft.monotonic)
    pacer = Pacer(clock, RateLimiter(limit, ft.monotonic), ft.sleep)
    burst_ts = START + timedelta(minutes=1)  # due 1/24 s after the start at ×1440
    events = [(burst_ts + timedelta(seconds=i // 10), 1 if i % 7 else 4) for i in range(300)]
    applied: list[tuple[float, int, int]] = []  # (wall time, rows, index)
    throttled = 0
    for i, (ts, rows) in enumerate(events):
        result = pacer.wait_for(ts, rows)
        throttled += result.throttled
        applied.append((ft.now, rows, i))
    assert [a[2] for a in applied] == list(range(300))  # all of them, in order
    assert throttled >= 290
    total_rows = sum(r for _, r in events)
    wall = applied[-1][0] - applied[0][0]
    assert wall == pytest.approx((total_rows - events[-1][1]) / limit, abs=0.05)
    window = lambda t: int(t - applied[0][0] + 1e-6)  # noqa: E731 - float-safe 1 s buckets
    per_second = Counter(window(t) for t, _, _ in applied)
    rows_per_second: Counter[int] = Counter()
    for t, rows, _ in applied:
        rows_per_second[window(t)] += rows
    assert max(per_second.values()) <= limit
    # a transaction is atomic: a window may overshoot by less than one event's rows
    assert max(rows_per_second.values()) < limit + max(r for _, r in events)
    # the clock was held back to the event times instead of running ahead by wall × speed
    last_ts = events[-1][0]
    assert clock.now() == last_ts
    assert clock.now() < START + timedelta(seconds=(ft.now - 1000.0) * speed) - timedelta(hours=1)


# --- configuration --------------------------------------------------------------------
def test_config_defaults_are_a_short_window_at_a_low_rate():
    cfg = Config.from_env({"PG_PASSWORD": "x"})
    assert (cfg.sim_speed, cfg.max_events_per_sec) == (1440, 20)
    assert cfg.sim_start == T("2017-01-05 00:00:00")
    assert cfg.sim_end == T("2017-03-31 23:59:59")
    assert DEFAULTS["SIM_START"] == "2017-01-05" and DEFAULTS["SIM_END"] == "2017-03-31"
    assert cfg.conninfo["application_name"] == "simulator" and cfg.pg_password == "x"
    assert cfg.heartbeat_file is not None and cfg.preload_batch == 1000


def test_config_parses_bounds_and_rejects_bad_values():
    assert parse_bound("2017-01-01", end=False) == T("2017-01-01 00:00:00")
    assert parse_bound("2017-01-01", end=True) == T("2017-01-01 23:59:59")
    assert parse_bound("2017-11-24 14:00:00", end=True) == T("2017-11-24 14:00:00")
    cfg = Config.from_env(
        {"SIM_SPEED": "720", "MAX_EVENTS_PER_SEC": "5", "SIM_START": "2017-11-20",
         "SIM_END": "2017-12-10", "SIM_HEARTBEAT_FILE": "-", "SIM_SPEED ": "ignored"}
    )  # fmt: skip
    assert (cfg.sim_speed, cfg.max_events_per_sec, cfg.heartbeat_file) == (720, 5, None)
    assert cfg.sim_end == T("2017-12-10 23:59:59")
    assert Config.from_env({"SIM_SPEED": "  "}).sim_speed == 1440  # blank → default
    with pytest.raises(ConfigError, match="not a date"):
        Config.from_env({"SIM_START": "yesterday"})
    with pytest.raises(ConfigError, match="after SIM_START"):
        Config.from_env({"SIM_START": "2017-03-31", "SIM_END": "2017-01-01"})
    with pytest.raises(ConfigError, match="> 0"):
        Config.from_env({"MAX_EVENTS_PER_SEC": "0"})
    with pytest.raises(ConfigError, match="invalid numeric"):
        Config.from_env({"SIM_SPEED": "fast"})
