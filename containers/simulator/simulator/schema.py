"""Schema inference from the CSV content (01 §3), pure: no database.

Types: the narrowest of INTEGER/BIGINT → NUMERIC(p,s) → TIMESTAMP → TEXT that fits every
non-empty value; any empty value makes the column nullable. A digit string with a
leading zero ("01037") is not a number, so zip codes become TEXT.

Primary key: a unique, never-empty column, preferring `<singular table>_id`, then any
`*_id`, then header order; if there is none, the first unique pair of never-empty
columns in header order. Foreign key: a column named like the single-column PK of
another table **and** every non-empty value exists there (name match alone is not
enough: products.product_category_name has two categories without a translation).
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from typing import NamedTuple

from simulator.csvio import CsvTable

INT_RE = re.compile(r"-?(0|[1-9][0-9]*)")
DEC_RE = re.compile(r"-?(0|[1-9][0-9]*)\.([0-9]+)")
TS_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
INT32_MAX = 2**31 - 1
# NUMERIC(p, s): s = widest observed scale, p = s + max(NUMERIC_MIN_INT_DIGITS, observed
# integer digits); amounts with two decimals and up to 8 integer digits → NUMERIC(10,2).
NUMERIC_MIN_INT_DIGITS = 8


class SchemaError(ValueError):
    pass


class TypeInfo(NamedTuple):
    sql_type: str
    nullable: bool


class Column(NamedTuple):
    name: str
    sql_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True, slots=True)
class RejectedForeignKey:
    """A name match that the data contradicts: `missing` distinct values absent from the parent."""

    table: str
    column: str
    ref_table: str
    ref_column: str
    missing: int
    examples: tuple[str, ...]


@dataclass(slots=True)
class TableSchema:
    name: str
    columns: list[Column]
    primary_key: tuple[str, ...]
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise SchemaError(f"{self.name}: no column {name!r}")

    @property
    def index_columns(self) -> list[str]:
        """FK columns that need their own index: not already the leading PK column (02 §7)."""
        return [fk.column for fk in self.foreign_keys if fk.column != self.primary_key[0]]


# --- types -----------------------------------------------------------------------
def detect_type(values: Iterable[str]) -> TypeInfo:
    """Narrowest SQL type for a column's values ("" = empty → nullable)."""
    nullable = False
    kinds: set[str] = set()
    max_abs = 0
    max_int_digits = 0
    max_scale = 0
    for v in values:
        if v == "":
            nullable = True
            continue
        if m := INT_RE.fullmatch(v):
            kinds.add("int")
            max_int_digits = max(max_int_digits, len(m.group(1)))
            max_abs = max(max_abs, abs(int(v)))
        elif m := DEC_RE.fullmatch(v):
            kinds.add("dec")
            max_int_digits = max(max_int_digits, len(m.group(1)))
            max_scale = max(max_scale, len(m.group(2)))
        elif TS_RE.fullmatch(v):
            kinds.add("ts")
        else:
            kinds.add("text")
    if not kinds:
        return TypeInfo("TEXT", True)
    if kinds == {"int"}:
        return TypeInfo("INTEGER" if max_abs <= INT32_MAX else "BIGINT", nullable)
    if kinds <= {"int", "dec"}:
        precision = max_scale + max(NUMERIC_MIN_INT_DIGITS, max_int_digits)
        return TypeInfo(f"NUMERIC({precision},{max_scale})", nullable)
    if kinds == {"ts"}:
        return TypeInfo("TIMESTAMP", nullable)
    return TypeInfo("TEXT", nullable)


# --- keys ------------------------------------------------------------------------
def singular(table: str) -> str:
    return table[:-1] if table.endswith("s") else table


def detect_primary_key(table: CsvTable) -> tuple[str, ...]:
    n = len(table.rows)
    if n == 0:
        raise SchemaError(f"{table.name}: cannot detect a primary key without rows")
    never_empty = [i for i in range(len(table.columns)) if all(r[i] != "" for r in table.rows)]
    unique = [i for i in never_empty if len({r[i] for r in table.rows}) == n]
    if unique:
        names = [table.columns[i] for i in unique]
        preferred = f"{singular(table.name)}_id"
        if preferred in names:
            return (preferred,)
        ids = [c for c in names if c.endswith("_id")]
        return (ids[0],) if ids else (names[0],)
    for a in never_empty:
        for b in never_empty:
            if b <= a:
                continue
            if len({(r[a], r[b]) for r in table.rows}) == n:
                return (table.columns[a], table.columns[b])
    raise SchemaError(f"{table.name}: no unique column or pair of columns")


def detect_foreign_keys(
    tables: Mapping[str, CsvTable], primary_keys: Mapping[str, tuple[str, ...]]
) -> tuple[dict[str, list[ForeignKey]], list[RejectedForeignKey]]:
    """Name match against single-column PKs, confirmed by the data (every value exists)."""
    parents: dict[str, str] = {}
    for name in tables:  # deterministic: insertion order of `tables`
        pk = primary_keys[name]
        if len(pk) == 1 and pk[0] not in parents:
            parents[pk[0]] = name
    accepted: dict[str, list[ForeignKey]] = {name: [] for name in tables}
    rejected: list[RejectedForeignKey] = []
    for name, child in tables.items():
        for column in child.columns:
            parent = parents.get(column)
            if parent is None or parent == name:
                continue
            parent_values = set(tables[parent].values(column))
            missing = sorted({v for v in child.values(column) if v and v not in parent_values})
            if missing:
                rejected.append(
                    RejectedForeignKey(
                        name, column, parent, column, len(missing), tuple(missing[:5])
                    )
                )
            else:
                accepted[name].append(ForeignKey(column, parent, column))
    return accepted, rejected


def infer_schemas(
    tables: Mapping[str, CsvTable], *, force_nullable: Mapping[str, Collection[str]] | None = None
) -> tuple[dict[str, TableSchema], list[RejectedForeignKey]]:
    """Types, PKs and FKs for every table. `force_nullable` lists columns that the
    simulator fills in later (future dates of an order), whatever the data says."""
    force_nullable = force_nullable or {}
    primary_keys = {name: detect_primary_key(t) for name, t in tables.items()}
    foreign_keys, rejected = detect_foreign_keys(tables, primary_keys)
    schemas: dict[str, TableSchema] = {}
    for name, t in tables.items():
        columns = []
        for i, col in enumerate(t.columns):
            info = detect_type(r[i] for r in t.rows)
            nullable = info.nullable or col in force_nullable.get(name, ())
            if col in primary_keys[name]:
                nullable = False
            columns.append(Column(col, info.sql_type, nullable))
        schemas[name] = TableSchema(name, columns, primary_keys[name], foreign_keys[name])
    return schemas, rejected


# --- DDL -------------------------------------------------------------------------
def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualified(pg_schema: str, table: str) -> str:
    return f"{quote_ident(pg_schema)}.{quote_ident(table)}"


def create_table_sql(schema: TableSchema, pg_schema: str = "shop") -> str:
    parts = [
        f"{quote_ident(c.name)} {c.sql_type}{'' if c.nullable else ' NOT NULL'}"
        for c in schema.columns
    ]
    pk_cols = ", ".join(quote_ident(c) for c in schema.primary_key)
    parts.append(f"CONSTRAINT {quote_ident(schema.name + '_pkey')} PRIMARY KEY ({pk_cols})")
    for fk in schema.foreign_keys:
        parts.append(
            f"CONSTRAINT {quote_ident(f'{schema.name}_{fk.column}_fkey')} "
            f"FOREIGN KEY ({quote_ident(fk.column)}) "
            f"REFERENCES {qualified(pg_schema, fk.ref_table)} ({quote_ident(fk.ref_column)})"
        )
    return (
        f"CREATE TABLE {qualified(pg_schema, schema.name)} (\n    " + ",\n    ".join(parts) + "\n)"
    )


def create_index_sql(schema: TableSchema, pg_schema: str = "shop") -> list[str]:
    """One index per FK column that is not already the leading column of the PK (02 §7)."""
    return [
        f"CREATE INDEX {quote_ident(f'{schema.name}_{col}_idx')} "
        f"ON {qualified(pg_schema, schema.name)} ({quote_ident(col)})"
        for col in schema.index_columns
    ]


def creation_order(schemas: Mapping[str, TableSchema]) -> list[str]:
    """Parents before children (FKs), alphabetical among the ready ones."""
    remaining = dict(schemas)
    order: list[str] = []
    while remaining:
        ready = sorted(
            name
            for name, s in remaining.items()
            if all(fk.ref_table in order or fk.ref_table not in schemas for fk in s.foreign_keys)
        )
        if not ready:
            raise SchemaError(f"circular foreign keys among {sorted(remaining)}")
        order.extend(ready)
        for name in ready:
            del remaining[name]
    return order
