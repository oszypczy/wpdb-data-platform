"""Reading the lakehouse from the host without Spark (read-delta skill, 06 §9).

- ``storage_options(settings, account)``: delta-rs options for MinIO on localhost:9000.
- ``duck(settings, account)``: a DuckDB connection with the ``delta`` and ``httpfs``
  extensions and an S3 secret for that account (read-only ``agent`` by default).
- ``Lake``: small helpers on top of both (query, wait for a row, table properties).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb
from deltalake import DeltaTable

ENDPOINT = "localhost:9000"
ACCOUNTS = {
    "spark": ("MINIO_SPARK_ACCESS_KEY", "MINIO_SPARK_SECRET_KEY"),
    "dashboard": ("MINIO_DASHBOARD_ACCESS_KEY", "MINIO_DASHBOARD_SECRET_KEY"),
    "agent": ("MINIO_AGENT_ACCESS_KEY", "MINIO_AGENT_SECRET_KEY"),
}


def keys(settings: dict[str, str], account: str) -> tuple[str, str]:
    k, s = ACCOUNTS[account]
    return settings[k], settings[s]


def storage_options(settings: dict[str, str], account: str = "agent") -> dict[str, str]:
    key, secret = keys(settings, account)
    return {
        "AWS_ACCESS_KEY_ID": key,
        "AWS_SECRET_ACCESS_KEY": secret,
        "AWS_ENDPOINT_URL": f"http://{ENDPOINT}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def duck(settings: dict[str, str], account: str = "agent") -> duckdb.DuckDBPyConnection:
    key, secret = keys(settings, account)
    con = duckdb.connect()
    # Delta `timestamp` is TIMESTAMP WITH TIME ZONE in DuckDB; the lake stores the naive
    # Postgres values as UTC, so show them in UTC (not in the host's zone)
    con.execute("SET TimeZone = 'UTC'")
    for ext in ("httpfs", "delta"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    con.execute(
        f"CREATE SECRET minio (TYPE s3, KEY_ID '{key}', SECRET '{secret}', "
        f"ENDPOINT '{ENDPOINT}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"
    )
    return con


@dataclass
class Lake:
    settings: dict[str, str]
    account: str = "agent"

    def __post_init__(self) -> None:
        self.con = duck(self.settings, self.account)

    def sql(self, query: str, params: list[Any] | None = None) -> list[tuple]:
        return self.con.execute(query, params or []).fetchall()

    def scalar(self, query: str, params: list[Any] | None = None) -> Any:
        return self.con.execute(query, params or []).fetchone()[0]

    def table(self, uri: str, version: int | None = None) -> DeltaTable:
        return DeltaTable(
            uri, version=version, storage_options=storage_options(self.settings, self.account)
        )

    def exists(self, uri: str) -> bool:
        try:
            self.table(uri)
            return True
        except Exception:  # noqa: BLE001 - TableNotFoundError and friends
            return False

    def properties(self, uri: str) -> dict[str, str]:
        return dict(self.table(uri).metadata().configuration)
