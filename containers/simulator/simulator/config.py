"""Configuration from the environment (01 §4, .env.example)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path

# Development defaults (01 §4): a short window at a low rate; the presentation
# settings are documented there and in .env.example.
DEFAULTS = {
    "SIM_SPEED": "1440",
    "MAX_EVENTS_PER_SEC": "20",
    "SIM_START": "2017-01-05",
    "SIM_END": "2017-03-31",
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_DB": "shop",
    "PG_USER": "shop_app",
    "PG_PASSWORD": "",
    "SIM_DATA_DIR": "/data/olist",
    "SIM_TRUTH_VIEWS": "/app/sql/truth-views.sql",
    "SIM_HEARTBEAT_FILE": "/tmp/simulator/heartbeat",
    "SIM_LOG_EVERY": "10",
    "SIM_PRELOAD_BATCH": "1000",
    "SIM_SCHEMA_EVOLUTION_AT": "",  # off: the default run stays deterministic (01 §7)
}


class ConfigError(ValueError):
    pass


def parse_bound(value: str, *, end: bool) -> datetime:
    """`YYYY-MM-DD` means the start of that day, or its last second for the end bound;
    `YYYY-MM-DD HH:MM:SS` is taken as is."""
    value = value.strip()
    try:
        if len(value) == 10:
            d = datetime.fromisoformat(value).date()
            return datetime.combine(d, time(23, 59, 59) if end else time.min)
        return datetime.fromisoformat(value)
    except ValueError:
        raise ConfigError(f"not a date or timestamp: {value!r}") from None


@dataclass(frozen=True, slots=True)
class Config:
    sim_speed: float
    max_events_per_sec: float
    sim_start: datetime
    sim_end: datetime
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    data_dir: Path
    truth_views: Path
    heartbeat_file: Path | None
    log_every: float
    preload_batch: int
    schema_evolution_at: datetime | None = None

    @property
    def conninfo(self) -> dict[str, object]:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_db,
            "user": self.pg_user,
            "password": self.pg_password,
            "application_name": "simulator",
            "connect_timeout": 5,
        }

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        def get(key: str) -> str:
            value = env.get(key, "")
            return value if value.strip() else DEFAULTS[key]

        try:
            speed = float(get("SIM_SPEED"))
            rate = float(get("MAX_EVENTS_PER_SEC"))
            log_every = float(get("SIM_LOG_EVERY"))
            batch = int(get("SIM_PRELOAD_BATCH"))
            port = int(get("PG_PORT"))
        except ValueError as exc:
            raise ConfigError(f"invalid numeric setting: {exc}") from None
        if speed <= 0 or rate <= 0 or log_every <= 0 or batch <= 0:
            raise ConfigError(
                "SIM_SPEED, MAX_EVENTS_PER_SEC, SIM_LOG_EVERY, SIM_PRELOAD_BATCH must be > 0"
            )
        start = parse_bound(get("SIM_START"), end=False)
        end = parse_bound(get("SIM_END"), end=True)
        if end <= start:
            raise ConfigError(f"SIM_END ({end}) must be after SIM_START ({start})")
        heartbeat = get("SIM_HEARTBEAT_FILE")
        evolution = get("SIM_SCHEMA_EVOLUTION_AT").strip()
        return cls(
            sim_speed=speed,
            max_events_per_sec=rate,
            sim_start=start,
            sim_end=end,
            pg_host=get("PG_HOST"),
            pg_port=port,
            pg_db=get("PG_DB"),
            pg_user=get("PG_USER"),
            pg_password=env.get("PG_PASSWORD", ""),
            data_dir=Path(get("SIM_DATA_DIR")),
            truth_views=Path(get("SIM_TRUTH_VIEWS")),
            heartbeat_file=Path(heartbeat) if heartbeat != "-" else None,
            log_every=log_every,
            preload_batch=batch,
            schema_evolution_at=parse_bound(evolution, end=False) if evolution else None,
        )
