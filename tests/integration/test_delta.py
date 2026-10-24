"""06 §11 integration rows: Delta on MinIO (time travel, RESTORE, CDF, CHECK, schema
enforcement, OPTIMIZE, VACUUM) and the readers' view of the live gold/ops tables.

Destructive scenarios run on throw-away tables under ``s3a://gold/_it_<hex>/`` written by
a host SparkSession (``tests/helpers/spark.py``, the `spark` account, the same Delta and
S3A versions as the image) and are removed afterwards; read-back is done with delta-rs
and DuckDB, never only with Spark.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import pyarrow as pa
import pytest

from lakehouse import delta_io
from lakehouse.tables import GOLD, OPS, SILVER_PROPERTIES
from tests.helpers.lake import Lake
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(stack, settings):
    from tests.helpers.spark import session

    return session(settings)


@pytest.fixture(scope="module")
def lake(stack, settings) -> Lake:
    return Lake(settings, "agent")


@pytest.fixture
def scratch(minio):
    """A throw-away prefix in the gold bucket: (s3a uri for Spark, s3 uri for readers)."""
    prefix = f"_it_{uuid.uuid4().hex[:10]}"
    yield f"s3a://gold/{prefix}", f"s3://gold/{prefix}"
    s3 = minio("spark")
    keys = [
        o["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket="gold", Prefix=prefix)
        for o in page.get("Contents", [])
    ]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(
            Bucket="gold", Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]}
        )


def _items(spark, rows):
    spec = GOLD["fact_order_items"]
    cols = [f.name for f in spec.schema]
    base = {c: None for c in cols}
    data = [tuple({**base, **r}[c] for c in cols) for r in rows]
    return spark.createDataFrame(data, spec.schema)


def _item(i: int, price: str, state: str = "SP") -> dict:
    return {
        "order_id": f"o{i}",
        "order_item_id": 1,
        "customer_state": state,
        "price": Decimal(price),
        "freight_value": Decimal("1.00"),
        "purchased_at": dt.datetime(2017, 1, 5, 10, i % 60),
        "purchase_date": dt.date(2017, 1, 5),
    }


def _fact_table(spark, uri: str):
    spec = GOLD["fact_order_items"]
    delta_io.ensure_table(
        spark, uri, spec.schema, properties=spec.properties, constraints=spec.constraints
    )


# ------------------------------------------------------------------- readers agree
def test_every_gold_and_ops_table_is_readable_by_delta_rs_and_duckdb(lake, settings):
    """06 §9, §12: only features delta-rs and DuckDB read: every gold/ops table opens in
    both readers (the `dashboard` account for delta-rs, as the dashboard will), with the
    columns of the frozen contract, reader protocol 1, no deletion vectors, no column
    mapping."""
    dash = Lake(settings, "dashboard")
    for layer, specs in (("gold", GOLD), ("ops", OPS)):
        for name, spec in specs.items():
            uri = f"s3://{layer}/{name}"
            dt_ = dash.table(uri)
            assert [f.name for f in dt_.schema().fields] == [f.name for f in spec.schema], uri
            proto = dt_.protocol()
            assert proto.min_reader_version == 1, (uri, proto)
            conf = dict(dt_.metadata().configuration)
            assert conf.get("delta.enableDeletionVectors") == "false", uri
            assert conf.get("delta.columnMapping.mode", "none") == "none", uri
            assert (
                "delta.enableChangeDataFeed" not in conf
                or conf["delta.enableChangeDataFeed"] == "false"
            ), uri
            n = lake.scalar(f"SELECT count(*) FROM delta_scan('{uri}')")
            assert n >= 0


def test_spark_write_reads_identically_in_delta_rs_and_duckdb(spark, lake, scratch):
    """06 §11: "zapis przez Sparka, odczyt przez delta-rs i DuckDB" → identyczne wyniki
    (decimal, timestamp, date, CHECK-constrained gold table)."""
    s3a, s3 = scratch
    uri = f"{s3a}/fact"
    _fact_table(spark, uri)
    rows = [_item(i, f"{i}.{i:02d}", "SP" if i % 2 else "RJ") for i in range(1, 41)]
    _items(spark, rows).write.format("delta").mode("append").save(uri)
    spark_rows = sorted(
        (r["order_id"], r["price"], r["customer_state"], r["purchased_at"])
        for r in spark.read.format("delta").load(uri).collect()
    )
    t = lake.table(f"{s3}/fact").to_pyarrow_table().to_pylist()
    rs = sorted(
        (r["order_id"], r["price"], r["customer_state"], r["purchased_at"].replace(tzinfo=None))
        for r in t
    )
    duck = sorted(
        (a, b, c, d.replace(tzinfo=None))
        for a, b, c, d in lake.sql(
            f"SELECT order_id, price, customer_state, purchased_at FROM delta_scan('{s3}/fact')"
        )
    )
    assert spark_rows == rs == duck
    assert len(spark_rows) == 40


# ----------------------------------------------------------- time travel, RESTORE
def test_time_travel_returns_the_state_before_a_later_merge(spark, lake, scratch):
    """06 §11: "time travel" → `VERSION AS OF n` zwraca stan sprzed późniejszego MERGE."""
    from delta.tables import DeltaTable

    s3a, s3 = scratch
    uri = f"{s3a}/tt"
    _fact_table(spark, uri)
    _items(spark, [_item(1, "10.00"), _item(2, "20.00")]).write.format("delta").mode("append").save(
        uri
    )
    v1 = DeltaTable.forPath(spark, uri).history(1).first()["version"]
    upd = _items(spark, [_item(1, "11.00"), _item(3, "30.00")])
    (
        DeltaTable.forPath(spark, uri)
        .alias("t")
        .merge(upd.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    old = spark.read.format("delta").option("versionAsOf", v1).load(uri)
    assert sorted((r["order_id"], r["price"]) for r in old.collect()) == [
        ("o1", Decimal("10.00")),
        ("o2", Decimal("20.00")),
    ]
    rs_old = lake.table(f"{s3}/tt", version=v1).to_pyarrow_table().to_pylist()
    assert sorted((r["order_id"], r["price"]) for r in rs_old) == [
        ("o1", Decimal("10.00")),
        ("o2", Decimal("20.00")),
    ]
    now = lake.sql(f"SELECT order_id, price FROM delta_scan('{s3}/tt') ORDER BY 1")
    assert now == [("o1", Decimal("11.00")), ("o2", Decimal("20.00")), ("o3", Decimal("30.00"))]
    ops = [h["operation"] for h in lake.table(f"{s3}/tt").history()]
    assert ops[0] == "MERGE"


def test_restore_returns_the_table_to_a_version(spark, lake, scratch):
    """06 §11: "RESTORE" → tabela wraca do wskazanej wersji."""
    from delta.tables import DeltaTable

    s3a, s3 = scratch
    uri = f"{s3a}/restore"
    _fact_table(spark, uri)
    _items(spark, [_item(1, "10.00")]).write.format("delta").mode("append").save(uri)
    good = DeltaTable.forPath(spark, uri).history(1).first()["version"]
    _items(spark, [_item(2, "99.00")]).write.format("delta").mode("append").save(uri)
    DeltaTable.forPath(spark, uri).delete("order_id = 'o1'")  # the "bad deployment"
    spark.sql(f"RESTORE TABLE delta.`{uri}` TO VERSION AS OF {good}")
    assert lake.sql(f"SELECT order_id, price FROM delta_scan('{s3}/restore')") == [
        ("o1", Decimal("10.00"))
    ]
    assert lake.table(f"{s3}/restore").history(1)[0]["operation"] == "RESTORE"


# ------------------------------------------------------------------------- CDF
def test_change_data_feed_after_merge_has_pre_post_images_and_deletes(spark, lake, scratch):
    """06 §11: "CDF po MERGE" → zmiany z typami `update_preimage`/`update_postimage`/
    `delete` (a silver-like table, CDF read through delta-rs as the readers do)."""
    from delta.tables import DeltaTable

    s3a, s3 = scratch
    uri = f"{s3a}/cdf"
    df = spark.createDataFrame([("a", 1), ("b", 2)], "k string, v int")
    delta_io.ensure_table(spark, uri, df.schema, properties=SILVER_PROPERTIES)
    df.write.format("delta").mode("append").save(uri)
    start = DeltaTable.forPath(spark, uri).history(1).first()["version"]
    upd = spark.createDataFrame([("a", 10), ("c", 3)], "k string, v int")
    (
        DeltaTable.forPath(spark, uri)
        .alias("t")
        .merge(upd.alias("s"), "t.k = s.k")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    DeltaTable.forPath(spark, uri).delete("k = 'b'")
    reader = lake.table(f"{s3}/cdf").load_cdf(starting_version=start + 1)
    cdf = pa.RecordBatchReader.from_stream(reader).read_all().to_pylist()
    got = sorted((r["k"], r["v"], r["_change_type"]) for r in cdf)
    assert got == [
        ("a", 1, "update_preimage"),
        ("a", 10, "update_postimage"),
        ("b", 2, "delete"),
        ("c", 3, "insert"),
    ]
    live = lake.properties("s3://silver/orders")
    assert live["delta.enableChangeDataFeed"] == "true"


# ----------------------------------------------------------- constraints, schema
def test_write_breaking_a_check_constraint_is_rejected(spark, lake, scratch):
    """06 §11: "zapis łamiący CHECK" → odrzucony (the live fact table carries the same
    constraint, so this is its last line of defence behind the quarantine)."""
    s3a, s3 = scratch
    uri = f"{s3a}/check"
    _fact_table(spark, uri)
    _items(spark, [_item(1, "1.00")]).write.format("delta").mode("append").save(uri)
    with pytest.raises(Exception, match="(?i)price_nonneg|CHECK constraint"):
        _items(spark, [_item(2, "-1.00")]).write.format("delta").mode("append").save(uri)
    assert lake.scalar(f"SELECT count(*) FROM delta_scan('{s3}/check')") == 1
    live = lake.properties("s3://gold/fact_order_items")
    assert live["delta.constraints.price_nonneg"] == "price >= 0"
    assert live["delta.constraints.freight_nonneg"] == "freight_value >= 0"


def test_type_mismatch_is_rejected_and_a_new_column_needs_merge_schema(spark, lake, scratch):
    """06 §11: "niezgodny typ kolumny / nowa kolumna z `mergeSchema`" → odrzucony /
    przyjęta (a new column without mergeSchema is rejected too)."""
    s3a, s3 = scratch
    uri = f"{s3a}/schema"
    base = spark.createDataFrame([("a", 1)], "k string, v int")
    base.write.format("delta").save(uri)
    bad_type = spark.createDataFrame([("b", "not-a-number")], "k string, v string")
    with pytest.raises(Exception, match="(?i)merge|incompatible|schema"):
        bad_type.write.format("delta").mode("append").save(uri)
    new_col = spark.createDataFrame([("c", 3, "x")], "k string, v int, w string")
    with pytest.raises(Exception, match="(?i)schema mismatch|mergeSchema"):
        new_col.write.format("delta").mode("append").save(uri)
    new_col.write.format("delta").mode("append").option("mergeSchema", "true").save(uri)
    assert lake.sql(f"SELECT k, v, w FROM delta_scan('{s3}/schema') ORDER BY k") == [
        ("a", 1, None),
        ("c", 3, "x"),
    ]


# ---------------------------------------------------------------- maintenance
def _checksum(lake: Lake, uri: str) -> tuple:
    return lake.sql(
        f"SELECT count(*), sum(price), sum(hash(order_id, price)) FROM delta_scan('{uri}')"
    )[0]


def test_optimize_compacts_files_and_keeps_the_data(spark, lake, scratch):
    """06 §11: "OPTIMIZE" → mniej plików, dane identyczne (suma kontrolna); with the same
    Z-ORDER the maintenance job uses on fact_order_items."""
    from lakehouse import maintenance

    s3a, s3 = scratch
    uri = f"{s3a}/opt"
    _fact_table(spark, uri)
    for b in range(8):
        rows = [_item(b * 10 + i, f"{b}.{i:02d}", "SP" if i % 3 else "MG") for i in range(10)]
        _items(spark, rows).coalesce(1).write.format("delta").mode("append").save(uri)
    before = _checksum(lake, f"{s3}/opt")
    files_before = len(lake.table(f"{s3}/opt").file_uris())
    res = maintenance.optimize(spark, uri, GOLD["fact_order_items"].zorder)
    files_after = len(lake.table(f"{s3}/opt").file_uris())
    assert files_before == 8 and files_after == 1
    assert (res["files_before"], res["files_after"]) == (8, 1)
    assert _checksum(lake, f"{s3}/opt") == before


def test_vacuum_removes_only_files_older_than_the_retention(spark, lake, scratch):
    """06 §11: "VACUUM" → usuwa tylko pliki starsze niż retencja; time travel w oknie
    retencji działa. Delta rounds RETAIN to whole hours, so the two sides are shown with
    1 h (every removed file is younger: nothing goes, time travel works) and 0 h (every
    removed file is older: they go, time travel to them fails, the current state and the
    _delta_log stay intact). The 0 h run needs the retention check off, which only this
    test session does; the container keeps 168 h / 720 h."""
    from delta.tables import DeltaTable

    s3a, s3 = scratch
    uri = f"{s3a}/vac"
    _fact_table(spark, uri)
    _items(spark, [_item(1, "1.00")]).coalesce(1).write.format("delta").mode("append").save(uri)
    v_first = DeltaTable.forPath(spark, uri).history(1).first()["version"]
    _items(spark, [_item(2, "2.00")]).coalesce(1).write.format("delta").mode("overwrite").save(uri)
    files = len(lake.table(f"{s3}/vac").file_uris())
    DeltaTable.forPath(spark, uri).vacuum(1)
    old = spark.read.format("delta").option("versionAsOf", v_first).load(uri).collect()
    assert [r["order_id"] for r in old] == ["o1"]  # inside the retention window
    DeltaTable.forPath(spark, uri).vacuum(0)
    with pytest.raises(Exception, match="(?i)not found|FileNotFound|does not exist|No such"):
        spark.read.format("delta").option("versionAsOf", v_first).load(uri).collect()
    assert lake.sql(f"SELECT order_id FROM delta_scan('{s3}/vac')") == [("o2",)]
    assert len(lake.table(f"{s3}/vac").file_uris()) == files == 1
    # the commits themselves are untouched: VACUUM never removes _delta_log files
    assert lake.table(f"{s3}/vac").version() == v_first + 1


@pytest.mark.slow
def test_maintenance_job_runs_in_the_container_and_logs_before_after(stack, lake):
    """06 §7: the periodic maintenance job of the Spark container (triggered on demand
    by /tmp/spark/run-maintenance) compacts gold with Z-ORDER, vacuums every layer and
    records files/bytes/query time before and after in ops/maintenance_log."""
    t0 = dt.datetime.now(dt.UTC)
    stack.exec("spark", "touch", "/tmp/spark/run-maintenance")

    def logged():
        rows = lake.sql(
            "SELECT table_path, operation, files_before, files_after, query_ms_before, "
            "query_ms_after FROM delta_scan('s3://ops/maintenance_log') WHERE started_at >= ?",
            [t0.replace(tzinfo=None) - dt.timedelta(seconds=5)],
        )
        return rows if any("fact_order_items" in r[0] and "ZORDER" in r[1] for r in rows) else None

    rows = wait_until(logged, 900, every=10, desc="maintenance run in ops/maintenance_log")
    z = next(r for r in rows if r[0].endswith("/fact_order_items") and "ZORDER" in r[1])
    assert z[3] <= z[2] and z[4] is not None and z[5] is not None
    assert any(r[1].startswith("VACUUM RETAIN 720") and "/bronze/" in r[0] for r in rows) or any(
        r[1].startswith("VACUUM") for r in rows
    )
