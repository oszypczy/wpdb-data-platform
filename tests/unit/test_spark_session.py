"""05 §8 / 06 §7 / 06 §9: guard rails of the SparkSession configuration (no JVM needed)."""

from __future__ import annotations

import pytest

from lakehouse import session

pytestmark = pytest.mark.unit


def test_fast_s3a_list_from_stays_off():
    """Delta 3.3.3 + S3A: `delta.enableFastS3AListFrom=true` made VACUUM delete the
    `_delta_log/*.json` commits of a table on MinIO (reproduced while building stage 3):
    the setting must never be switched on."""
    conf = {**session.DELTA_CONF, **session.s3a_conf("http://minio:9000", "k", "s")}
    assert not any("enableFastS3AListFrom" in k for k in conf)


def test_readers_compatible_defaults_and_s3a_for_minio():
    """06 §9, §12 and CLAUDE.md "Delta on S3A": deletion vectors off for new tables, the
    Delta extension and catalog, path-style access without SSL."""
    assert (
        session.DELTA_CONF["spark.databricks.delta.properties.defaults.enableDeletionVectors"]
        == "false"
    )
    assert session.DELTA_CONF["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    s3 = session.s3a_conf("http://minio:9000", "k", "s")
    assert s3["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert s3["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
