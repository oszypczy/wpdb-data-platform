"""CSV input (01 §1, §3): file name → table name, header → column names, rows as tuples.

Values stay strings and "" means empty (NULL in the database). Everything else
about the data (types, keys, events) is decided by schema.py and events.py.
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path

ENCODING = "utf-8-sig"  # strips the BOM that product_category_name_translation.csv starts with
INTERN_MAX_LEN = 64  # ids, dates and amounts repeat across tables; review comments do not


class CsvError(ValueError):
    pass


def table_name(path: str | Path) -> str:
    """olist_order_items_dataset.csv → order_items; the translation file keeps its name."""
    return Path(path).stem.removeprefix("olist_").removesuffix("_dataset")


@dataclass(slots=True)
class CsvTable:
    name: str
    columns: tuple[str, ...]
    rows: list[tuple[str, ...]]

    def index(self, column: str) -> int:
        try:
            return self.columns.index(column)
        except ValueError:
            raise CsvError(f"{self.name}: no column {column!r}") from None

    def values(self, column: str) -> list[str]:
        i = self.index(column)
        return [r[i] for r in self.rows]

    def __len__(self) -> int:
        return len(self.rows)


def _intern(value: str) -> str:
    return sys.intern(value) if len(value) <= INTERN_MAX_LEN else value


def load_csv(path: str | Path, *, name: str | None = None) -> CsvTable:
    """Read one CSV file; multi-line quoted fields (review comments) are handled by csv."""
    path = Path(path)
    with path.open(newline="", encoding=ENCODING) as f:
        reader = csv.reader(f)
        try:
            header = tuple(next(reader))
        except StopIteration:
            raise CsvError(f"{path.name}: empty file") from None
        if any(not h for h in header) or len(set(header)) != len(header):
            raise CsvError(f"{path.name}: empty or duplicate column names in header {header}")
        width = len(header)
        rows: list[tuple[str, ...]] = []
        for line, row in enumerate(reader, start=2):
            if len(row) != width:
                raise CsvError(f"{path.name}:{line}: {len(row)} fields, header has {width}")
            rows.append(tuple(_intern(v) for v in row))
    return CsvTable(name or table_name(path), header, rows)


def load_dir(
    directory: str | Path, *, exclude: tuple[str, ...] = ("geolocation",)
) -> dict[str, CsvTable]:
    """All *.csv files of a directory as tables keyed by table name (sorted by file name)."""
    tables: dict[str, CsvTable] = {}
    for path in sorted(Path(directory).glob("*.csv")):
        name = table_name(path)
        if name in exclude:
            continue
        tables[name] = load_csv(path, name=name)
    if not tables:
        raise CsvError(f"no CSV files in {directory} (run `make data`)")
    return tables
