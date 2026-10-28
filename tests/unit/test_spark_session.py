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


def _s3a_storage_error_budget_s(conf: dict[str, str]) -> tuple[float, float]:
    """(min, max) seconds S3A retries one call that fails with a non-retryable-by-SDK
    status such as MinIO's 507 XMinioStorageFull: hadoop-aws 3.3.4 S3ARetryPolicy ->
    retryIdempotentCalls = exponentialBackoffRetry(retry.limit, retry.interval), sleep n
    = interval * 2^(n+1) * U[0.5, 1.5) (RetryPolicies.calculateExponentialTime)."""
    limit = int(conf["spark.hadoop.fs.s3a.retry.limit"])
    interval = conf["spark.hadoop.fs.s3a.retry.interval"]
    assert interval.endswith("s") and not interval.endswith("ms"), interval
    base = sum(float(interval[:-1]) * 2 ** (n + 1) for n in range(limit))
    return 0.5 * base, 1.5 * base


def test_s3a_retry_budget_surfaces_a_full_minio_within_two_minutes_but_rides_out_restarts():
    """08 §12.8 / 06 §11: a write MinIO rejects (507 XMinioStorageFull, retried by S3A
    only and silently) fails the batch within 2 min, so the supervisor logs it; it was
    ~34 min with retry.limit=10/interval=1s. A MinIO restart (connection refused: retried
    by the SDK `attempts.maximum` times inside each of the S3A attempts) is still ridden
    out: >= 30 s of S3A sleeps alone, plus >= 5 SDK attempts per S3A attempt."""
    conf = session.s3a_conf("http://minio:9000", "k", "s")
    low, high = _s3a_storage_error_budget_s(conf)
    assert high <= 120, (low, high)
    assert low >= 15, (low, high)
    assert int(conf["spark.hadoop.fs.s3a.attempts.maximum"]) >= 5
