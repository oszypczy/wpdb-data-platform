"""Aligning DataFrames with different writer schemas (pure functions).

Schema evolution inside one topic (05 §2): after ``ADD COLUMN`` a batch holds rows of
schema v1 and v2; after a type change that the operator forced through the registry
(``compatibility=NONE``, 03 §5) it may hold ``v: string`` and ``v: int``. Rows of every
schema are conformed to one *merged* schema, in which the type seen first wins (the
existing Delta table's type, then the older schema id) and new fields are appended at
the end, which is exactly what Delta ``mergeSchema`` accepts.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructField, StructType


def merge_types(first: DataType, other: DataType) -> DataType:
    if isinstance(first, StructType) and isinstance(other, StructType):
        return merge_structs(first, other)
    return first  # first wins: existing column types are never changed


def merge_structs(first: StructType, other: StructType) -> StructType:
    fields = []
    seen = {}
    for f in first.fields:
        seen[f.name] = f
    for f in first.fields:
        o = next((x for x in other.fields if x.name == f.name), None)
        if o is None:
            fields.append(StructField(f.name, f.dataType, True))
        else:
            fields.append(StructField(f.name, merge_types(f.dataType, o.dataType), True))
    for o in other.fields:
        if o.name not in seen:
            fields.append(StructField(o.name, o.dataType, True))
    return StructType(fields)


def merge_schemas(schemas: list[StructType]) -> StructType:
    if not schemas:
        return StructType([])
    merged = schemas[0]
    for s in schemas[1:]:
        merged = merge_structs(merged, s)
    return merged


def _conform_col(col: Column, source: DataType | None, target: DataType) -> Column:
    if source is None:
        return F.lit(None).cast(target)
    if isinstance(target, StructType):
        if not isinstance(source, StructType):
            return F.lit(None).cast(target)
        src_fields = {f.name: f.dataType for f in source.fields}
        parts = [
            _conform_col(
                col.getField(f.name) if f.name in src_fields else F.lit(None),
                src_fields.get(f.name),
                f.dataType,
            ).alias(f.name)
            for f in target.fields
        ]
        return F.when(col.isNull(), F.lit(None).cast(target)).otherwise(F.struct(*parts))
    if source == target:
        return col
    return col.cast(target)


def same_shape(a: StructType, b: StructType) -> bool:
    """Equal names, order and types, ignoring nullability and metadata."""
    return nullable(a).simpleString() == nullable(b).simpleString()


def conform(df: DataFrame, target: StructType) -> DataFrame:
    """Select exactly ``target``'s columns (missing -> null, other types -> cast);
    columns of ``df`` that are not in ``target`` are dropped. A frame that already has
    the target shape is returned unchanged (the common case: no plan rewriting)."""
    if same_shape(df.schema, target):
        return df
    src = {f.name: f.dataType for f in df.schema.fields}
    cols = [
        _conform_col(
            F.col(f"`{f.name}`") if f.name in src else F.lit(None),
            src.get(f.name),
            f.dataType,
        ).alias(f.name)
        for f in target.fields
    ]
    return df.select(*cols)


def union_conformed(frames: list[DataFrame], first: StructType | None = None) -> DataFrame:
    """Union of frames with different (evolving) schemas, conformed to the merged one.

    ``first`` is the schema of the existing table, whose types win."""
    if len(frames) == 1 and (first is None or same_shape(frames[0].schema, first)):
        return frames[0]
    schemas = ([first] if first is not None else []) + [f.schema for f in frames]
    merged = merge_schemas(schemas)
    out = conform(frames[0], merged)
    for f in frames[1:]:
        out = out.unionByName(conform(f, merged))
    return out


def nullable(schema: StructType) -> StructType:
    """The same schema with every field (recursively) nullable: Delta tables of this
    project never carry NOT NULL invariants from AVRO (a later schema may drop them)."""

    def fix(dt: DataType) -> DataType:
        if isinstance(dt, StructType):
            return StructType([StructField(f.name, fix(f.dataType), True) for f in dt.fields])
        return dt

    return fix(schema)  # type: ignore[return-value]
