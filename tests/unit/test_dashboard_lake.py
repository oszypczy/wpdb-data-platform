"""07 §7 unit rows on the reader: reading a given version and the version-aware cache
(07 §3: data is re-read only when the Delta version changed). Local Delta tables
written with ``deltalake``; the same code reads MinIO in the container."""

from __future__ import annotations

from decimal import Decimal

import pytest

from dashboard import contract
from dashboard import queries as q
from dashboard.lake import Lake, LakeConfig, VersionedReader
from tests.helpers import dashboard_fixtures as fx

pytestmark = pytest.mark.unit

ITEMS = "gold/fact_order_items"
LC = "gold/fact_orders_lifecycle"
AGG = "gold/agg_revenue_hourly"


class CountingLake(Lake):
    """A Lake that counts real data reads per table."""

    def __init__(self, config):
        super().__init__(config)
        self.data_reads: list[tuple[str, int | None]] = []

    def read(self, table, version=None):
        self.data_reads.append((table, version))
        return super().read(table, version)


def _item(order_id: str, price: str) -> dict:
    row = dict(fx.ROWS[ITEMS][0])
    row.update(order_id=order_id, price=Decimal(price))
    return row


def test_read_of_a_version_returns_that_state(tmp_path):
    """The reader reads the version it just checked: version N gives that state."""
    fx.write(tmp_path, ITEMS, [_item("a", "10.00")])  # version 0
    fx.write(tmp_path, ITEMS, [_item("b", "20.00")])  # version 1
    fx.write(tmp_path, ITEMS, [_item("c", "30.00")], mode="overwrite")  # version 2
    lake = Lake(LakeConfig(root=str(tmp_path)))

    assert lake.version(ITEMS) == 2
    by_version = {v: sorted(lake.read(ITEMS, v).column("order_id").to_pylist()) for v in (0, 1, 2)}
    assert by_version == {0: ["a"], 1: ["a", "b"], 2: ["c"]}
    with pytest.raises(Exception):  # noqa: B017 - delta-rs raises its own error type
        lake.read(ITEMS, 7)


def test_no_reread_when_the_version_did_not_change(tmp_path):
    """07 §7: "pamięć podręczna" → brak ponownego odczytu, gdy wersja tabeli się nie
    zmieniła; nowa wersja = jeden nowy odczyt i nowy wynik."""
    fx.write_small_lake(tmp_path, [ITEMS, LC, AGG])
    lake = CountingLake(LakeConfig(root=str(tmp_path)))
    reader = VersionedReader(lake)

    for _ in range(5):  # five refreshes of the overview page
        k = reader.derive("kpis", [ITEMS, LC, AGG], q.kpis)
        assert k.revenue == Decimal("180.00")
        assert reader.get(ITEMS).num_rows == 3
    assert sorted(lake.data_reads) == [(AGG, 0), (ITEMS, 0), (LC, 0)]
    assert reader.stats.computes == 1
    assert reader.stats.version_checks == 5 * 3 + 5  # every refresh checks the versions

    fx.write(tmp_path, ITEMS, [_item("o9", "20.00")])  # Spark commits version 1
    k = reader.derive("kpis", [ITEMS, LC, AGG], q.kpis)
    assert k.revenue == Decimal("200.00") and k.items == 4
    assert lake.data_reads[3:] == [(ITEMS, 1)]  # only the changed table is read again
    assert reader.stats.computes == 2
    reader.derive("kpis", [ITEMS, LC, AGG], q.kpis)
    assert len(lake.data_reads) == 4 and reader.stats.computes == 2

    # parameters are part of the key: another grain is computed once, then cached too
    reader.derive("ts", [AGG], q.revenue_timeseries, "day", None)
    reader.derive("ts", [AGG], q.revenue_timeseries, "hour", None)
    reader.derive("ts", [AGG], q.revenue_timeseries, "day", None)
    assert reader.stats.computes == 4 and len(lake.data_reads) == 4


def test_missing_table_reads_as_empty_and_appears_later(tmp_path):
    """07 §3 "puste stany": before Spark created a table the reader returns an empty
    table with the contract schema; once it exists it is read."""
    lake = CountingLake(LakeConfig(root=str(tmp_path)))
    reader = VersionedReader(lake)
    assert lake.version(ITEMS) is None
    empty = reader.get(ITEMS)
    assert empty.num_rows == 0 and empty.schema == contract.SCHEMAS[ITEMS]
    assert lake.data_reads == []  # nothing to read
    assert lake.read(ITEMS).num_rows == 0

    fx.write_small_lake(tmp_path, [ITEMS])
    assert reader.get(ITEMS).num_rows == 3


def test_contract_projection_tolerates_extra_and_missing_columns(tmp_path):
    """A writer-side ``ADD COLUMN`` (extra) or an older version without a column (missing)
    does not break the pages: the contract schema is projected."""
    import pyarrow as pa
    from deltalake import write_deltalake

    data = fx.arrow(ITEMS, fx.ROWS[ITEMS]).drop_columns(["seller_state"])
    data = data.append_column("delivery_channel", pa.array(["x"] * data.num_rows))
    write_deltalake(str(tmp_path / ITEMS), data)
    t = Lake(LakeConfig(root=str(tmp_path))).read(ITEMS)
    assert t.schema == contract.SCHEMAS[ITEMS]
    assert t.column("seller_state").null_count == 3


def test_config_from_env_builds_s3_options():
    c = LakeConfig.from_env(
        {
            "S3_ENDPOINT": "http://minio:9000",
            "MINIO_DASHBOARD_ACCESS_KEY": "dashboard",
            "MINIO_DASHBOARD_SECRET_KEY": "s3cret",
        }
    )
    assert c.is_s3 and c.uri("gold/dim_date") == "s3://gold/dim_date"
    assert c.storage_options() == {
        "AWS_ACCESS_KEY_ID": "dashboard",
        "AWS_SECRET_ACCESS_KEY": "s3cret",
        "AWS_ENDPOINT_URL": "http://minio:9000",
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
    }
    local = LakeConfig.from_env({"DASHBOARD_LAKE_ROOT": "/tmp/lake"})
    assert local.storage_options() is None and local.uri("ops/quarantine") == (
        "/tmp/lake/ops/quarantine"
    )
