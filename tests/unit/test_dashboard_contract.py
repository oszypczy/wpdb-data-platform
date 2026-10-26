"""07 §2 / 06 §9: the dashboard reads only gold and ops tables, and every column it
reads exists in the writer's contract (containers/spark/lakehouse/tables.py) with the
matching type. No JVM: only the PySpark type objects are imported."""

from __future__ import annotations

import ast
from pathlib import Path

import pyarrow as pa
import pytest
from pyspark.sql import types as T

from dashboard import contract
from lakehouse import tables

pytestmark = pytest.mark.unit

DASHBOARD = Path(__file__).resolve().parents[2] / "containers" / "dashboard"


def _arrow(dt: T.DataType) -> pa.DataType:
    if isinstance(dt, T.DecimalType):
        return pa.decimal128(dt.precision, dt.scale)
    return {
        T.StringType: pa.string(),
        T.IntegerType: pa.int32(),
        T.LongType: pa.int64(),
        T.DoubleType: pa.float64(),
        T.BooleanType: pa.bool_(),
        T.TimestampType: pa.timestamp("us", tz="UTC"),
        T.DateType: pa.date32(),
    }[type(dt)]


def test_every_dashboard_column_matches_the_writer_contract():
    for name, schema in contract.SCHEMAS.items():
        layer, table = name.split("/")
        assert layer in ("gold", "ops"), name  # 07: only gold and ops, never bronze/silver
        spec = tables.ALL[name]
        writer = {f.name: _arrow(f.dataType) for f in spec.schema.fields}
        for field in schema:
            assert field.name in writer, f"{name}.{field.name} is not written by Spark"
            assert field.type == writer[field.name], f"{name}.{field.name}"


def test_dashboard_code_never_imports_spark_kafka_or_postgres_clients():
    """07: the dashboard depends only on Delta (delta-rs, DuckDB on Arrow)."""
    banned = {"pyspark", "delta", "confluent_kafka", "kafka", "psycopg", "psycopg2", "boto3"}
    files = [*DASHBOARD.glob("dashboard/*.py"), *DASHBOARD.glob("views/*.py"),
             DASHBOARD / "app.py"]  # fmt: skip
    assert len(files) >= 15
    for f in files:
        for node in ast.walk(ast.parse(f.read_text())):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for n in names:
                assert n.split(".")[0] not in banned, f"{f.name} imports {n}"
