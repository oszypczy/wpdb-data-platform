"""Reading gold/ops Delta tables with delta-rs, and the version-aware cache (07 §3).

- ``Lake`` knows where the tables are (``s3://`` in MinIO, or a local directory in unit
  tests) and reads them: the current version, a table at a version (time travel), its
  history. A missing table reads as an empty table with the contract schema.
- ``VersionedReader`` is the cache: every refresh asks each table for its version (a
  cheap ``update_incremental`` = list of new ``_delta_log`` entries) and re-reads the
  data only when the version changed. Derived results (KPIs, aggregates) are cached
  under the versions of their input tables, so they are recomputed only then too.

Nothing here imports Streamlit; the UI keeps one ``VersionedReader`` per process in
``st.cache_resource`` (shared by all sessions).
"""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.dataset as pds
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from dashboard import contract

# Spark writes timestamps as Parquet INT96 (its default outputTimestampType); pyarrow
# reads INT96 as nanoseconds, which overflows for dim_customer.valid_to = 9999-12-31.
# Reading them as microseconds keeps every value (the Delta type is microseconds anyway).
PARQUET_READ = pds.ParquetReadOptions(coerce_int96_timestamp_unit="us")


@dataclass(frozen=True)
class LakeConfig:
    root: str = "s3://"
    endpoint: str = "http://minio:9000"
    access_key: str = ""
    secret_key: str = ""

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> LakeConfig:
        e = os.environ if env is None else env
        return cls(
            root=e.get("DASHBOARD_LAKE_ROOT", "s3://"),
            endpoint=e.get("S3_ENDPOINT", "http://minio:9000"),
            access_key=e.get("MINIO_DASHBOARD_ACCESS_KEY", ""),
            secret_key=e.get("MINIO_DASHBOARD_SECRET_KEY", ""),
        )

    @property
    def is_s3(self) -> bool:
        return self.root.startswith("s3://")

    def storage_options(self) -> dict[str, str] | None:
        if not self.is_s3:
            return None
        return {
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
            "AWS_ENDPOINT_URL": self.endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": "us-east-1",
        }

    def uri(self, table: str) -> str:
        if self.is_s3:
            return f"s3://{table}"
        return os.path.join(self.root, table)


def _not_found(exc: Exception) -> bool:
    return isinstance(exc, TableNotFoundError | FileNotFoundError) or "not a Delta table" in str(
        exc
    )


class Lake:
    """delta-rs access to the tables of one lake root. Thread-safe."""

    def __init__(self, config: LakeConfig):
        self.config = config
        self._tables: dict[str, DeltaTable] = {}
        self._lock = threading.Lock()

    def _open(self, table: str, version: int | None = None) -> DeltaTable:
        return DeltaTable(
            self.config.uri(table),
            version=version,
            storage_options=self.config.storage_options(),
        )

    def version(self, table: str) -> int | None:
        """Current version (cheap: only new log entries are listed); None = no table yet."""
        with self._lock:
            dt = self._tables.get(table)
            try:
                if dt is None:
                    dt = self._open(table)
                    self._tables[table] = dt
                else:
                    dt.update_incremental()
            except Exception as exc:
                if _not_found(exc):
                    self._tables.pop(table, None)
                    return None
                raise
            return dt.version()

    def read(self, table: str, version: int | None = None) -> pa.Table:
        """The table at ``version`` (None = current), projected on the contract schema."""
        if version is not None:
            with self._lock:
                dt = self._tables.get(table)
                if dt is not None and dt.version() == version:
                    # the handle the version check just updated: no second log replay
                    return self._to_arrow(table, dt)
        try:
            dt = self._open(table, version)
        except Exception as exc:
            if _not_found(exc) and version is None:
                return contract.empty(table)
            raise
        return self._to_arrow(table, dt)

    @staticmethod
    def _to_arrow(table: str, dt: DeltaTable) -> pa.Table:
        ds = dt.to_pyarrow_dataset(parquet_read_options=PARQUET_READ)
        wanted = [f.name for f in contract.SCHEMAS[table] if f.name in ds.schema.names]
        return contract.conform(table, ds.to_table(columns=wanted))


@dataclass
class ReadStats:
    """Counters for tests and the pipeline page: how often data was really read."""

    version_checks: int = 0
    reads: dict[str, int] = field(default_factory=dict)
    computes: int = 0


class VersionedReader:
    """Re-read a table only when its Delta version changed (07 §3)."""

    def __init__(self, lake: Lake, max_derived: int = 256):
        self.lake = lake
        self.stats = ReadStats()
        self._data: dict[str, tuple[int | None, pa.Table]] = {}
        self._derived: OrderedDict[Hashable, Any] = OrderedDict()
        self._max_derived = max_derived
        self._lock = threading.RLock()

    def versions(self, tables: Sequence[str]) -> dict[str, int | None]:
        out = {}
        for t in tables:
            self.stats.version_checks += 1
            out[t] = self.lake.version(t)
        return out

    def get(self, table: str, version: int | None = None) -> pa.Table:
        """Current data of ``table``; ``version`` = already checked version (skips a check)."""
        v = self.versions([table])[table] if version is None else version
        with self._lock:
            cached = self._data.get(table)
            if cached is not None and cached[0] == v:
                return cached[1]
        data = contract.empty(table) if v is None else self.lake.read(table, v)
        with self._lock:
            self.stats.reads[table] = self.stats.reads.get(table, 0) + 1
            self._data[table] = (v, data)
        return data

    def derive(
        self,
        name: str,
        tables: Sequence[str],
        fn: Callable[..., Any],
        *args: Hashable,
    ) -> Any:
        """``fn(*table_data, *args)``, recomputed only when a version of ``tables`` (or
        ``args``) changed. Returns the cached object: callers must not mutate it."""
        vs = self.versions(tables)
        key = (name, tuple(vs.items()), args)
        with self._lock:
            if key in self._derived:
                self._derived.move_to_end(key)
                return self._derived[key]
        data = [self.get(t, vs[t]) for t in tables]
        result = fn(*data, *args)
        with self._lock:
            self.stats.computes += 1
            self._derived[key] = result
            while len(self._derived) > self._max_derived:
                self._derived.popitem(last=False)
        return result
