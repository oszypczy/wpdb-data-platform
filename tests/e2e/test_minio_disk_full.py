"""E2E: MinIO's disk full while the stream flows, then space freed (08 §12.8, 00-overview
"Pozostało do zrobienia": `XMinioStorageFull`, Spark's queries stopped, recovery without
loss or duplicates after freeing space).

Own fresh stack with MinIO on a 768 MiB tmpfs (``tests/e2e/compose/minio-small-disk.yml``):
the test fills it with a filler file next to MinIO's data directory, so only that
tmpfs is ever full, never the Docker VM disk. Default rate, short window.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from tests.helpers.clients import minio_client
from tests.helpers.e2e import Pipeline, StackSpec, now_utc, report
from tests.helpers.wait import wait_until

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

DISK = StackSpec(
    "minio-small-disk",
    env={
        "SIM_START": "2017-01-05",
        "SIM_END": "2017-02-28",
        "SIM_SPEED": "1440",
        "MAX_EVENTS_PER_SEC": "20",
        "SIM_SCHEMA_EVOLUTION_AT": "",
    },
    files=("tests/e2e/compose/minio-small-disk.yml",),
)
MOUNT, FILLER = "/mnt/minio", "/mnt/minio/filler"
PROBE_BUCKET, PROBE_KEY = "ops", "_e2e_disk_full_probe"
FULL_MARKERS = ("XMinioStorageFull", "minimum free drive threshold")
# 08 §12.8: the S3A retry budget (4 x 2 s exponential, 30..90 s, 05 §8) runs out, the
# batch fails and the supervisor logs "query ... stopped ... [S3 507 XMinioStorageFull"
LOG_SIGNAL_S = 150


@pytest.fixture(scope="module")
def pipe(stacks, settings):
    p = Pipeline(stacks.start(DISK), settings)
    yield p
    p.close()


def _df_kb(pipe: Pipeline) -> tuple[int, int]:
    """(size, available) of MinIO's tmpfs in KiB."""
    line = pipe.stack.exec("minio", "df", "-k", MOUNT).stdout.splitlines()[-1].split()
    return int(line[1]), int(line[3])


def _probe_write(s3) -> str:
    """'ok' when MinIO accepts a small write (the probe is removed again), else the S3
    error code."""
    try:
        s3.put_object(Bucket=PROBE_BUCKET, Key=PROBE_KEY, Body=b"probe")
    except ClientError as exc:
        return exc.response["Error"]["Code"]
    s3.delete_object(Bucket=PROBE_BUCKET, Key=PROBE_KEY)
    return "ok"


OBSERVED: dict = {}  # what the scenario saw, for the separate log-line test below


def _full_lines(pipe: Pipeline, since: str) -> list[str]:
    logs = pipe.logs_since("spark", since)
    return [ln for ln in logs.splitlines() if any(m in ln for m in FULL_MARKERS)]


def _gold_orders(pipe: Pipeline) -> int:
    return pipe.lake.scalar("SELECT count(*) FROM delta_scan('s3://gold/fact_orders_lifecycle')")


@pytest.mark.timeout(2400)
def test_minio_disk_full_stops_spark_and_after_freeing_space_it_recovers_without_loss(
    pipe, settings
):
    """08 §12.8: przy pełnym dysku MinIO zapytania Sparka nic nie zatwierdzają, w logu
    `spark` jest `XMinioStorageFull`, `spark` `unhealthy`; po zwolnieniu miejsca Spark
    wraca sam (bez restartu kontenera), bez strat i duplikatów. While MinIO's disk is
    full it rejects writes with XMinioStorageFull, Spark commits nothing (silver frozen
    while the simulator keeps writing), logs the S3 error (observed for the separate
    test below) and turns unhealthy without restarting its container; after the filler
    is removed, the same container becomes healthy by itself, the pipeline catches up
    and Postgres ↔ silver ↔ gold reconcile exactly (no loss, no duplicates), and the
    kafka_to_bronze checkpoint continues (same txnAppId, later micro-batches)."""
    s = settings
    s3 = minio_client(s["MINIO_SPARK_ACCESS_KEY"], s["MINIO_SPARK_SECRET_KEY"])
    size_kb, _ = _df_kb(pipe)
    assert size_kb <= 768 * 1024, f"MinIO is not on the small tmpfs ({size_kb} KiB)"

    pipe.ensure_replaying()
    wait_until(lambda: _gold_orders(pipe) > 0, 600, every=5, desc="gold has orders")
    pipe.wait_flowing()
    token, last = pipe.checkpoint()
    spark0 = pipe.stack.inspect("spark")["State"]["StartedAt"]

    # --- fill the disk --------------------------------------------------------------
    since = now_utc().isoformat()
    t0 = time.monotonic()
    pipe.stack.exec("minio", "sh", "-c", f"dd if=/dev/zero of={FILLER} bs=1M 2>/dev/null; true")
    _, avail_kb = _df_kb(pipe)
    assert avail_kb < 1024, f"tmpfs not full: {avail_kb} KiB available"
    code = wait_until(
        lambda: (c := _probe_write(s3)) != "ok" and c, 60, every=2, desc="MinIO rejects writes"
    )
    assert code == "XMinioStorageFull", code
    rejected = time.monotonic()
    # the operator's signal (separate assertion below): the first matching log line
    try:
        first = wait_until(lambda: _full_lines(pipe, since), LOG_SIGNAL_S + 120, every=5,
                           desc="XMinioStorageFull in the spark log")  # fmt: skip
    except TimeoutError:
        first = None  # asserted by test_spark_logs_xminiostoragefull_while_minio_is_full
    OBSERVED["log_signal_after_s"] = round(time.monotonic() - rejected) if first else None
    # healthcheck: the driver loop stops refreshing its health file (> 60 s old), then
    # Docker needs 12 failed checks 10 s apart
    wait_until(lambda: pipe.stack.health("spark") == "unhealthy", 420, every=5,
               desc="spark unhealthy while the disk is full")  # fmt: skip
    unhealthy_after_s = round(time.monotonic() - t0)
    frozen = pipe.silver_lsn()
    events = pipe.sim()["events_applied"]
    wait_until(lambda: pipe.sim()["events_applied"] >= events + 60, 300, every=5,
               desc="60 more events written while the disk is full")  # fmt: skip
    assert pipe.silver_lsn() == frozen, "silver changed although MinIO rejects every write"
    full_s = round(time.monotonic() - t0)
    OBSERVED["storage_full_lines"] = _full_lines(pipe, since)
    OBSERVED["full_for_s"] = full_s
    assert pipe.stack.inspect("spark")["State"]["StartedAt"] == spark0, (
        "spark container restarted while the disk was full (crash loop)",
        pipe.spark_queries(),
    )

    # --- free the space: nothing else to do (08 §12.8) ------------------------------
    pipe.stack.exec("minio", "rm", "-f", FILLER)
    wait_until(lambda: _probe_write(s3) == "ok", 120, every=2, desc="MinIO accepts writes")
    freed = time.monotonic()
    wait_until(lambda: pipe.stack.health("spark") == "healthy", 420, every=3,
               desc="spark healthy by itself after freeing space")  # fmt: skip
    healthy_after_free_s = round(time.monotonic() - freed)
    assert pipe.stack.inspect("spark")["State"]["StartedAt"] == spark0, "spark was restarted"
    pipe.wait_services_healthy()
    pipe.wait_connector_running()
    pipe.wait_flowing()
    lsn = pipe.pause_simulator()
    pipe.wait_slot_caught_up(lsn)
    recon = pipe.wait_reconciled()
    token2, last2 = pipe.checkpoint()
    report(
        "minio_disk_full",
        full_for_s=full_s,
        spark_unhealthy_after_s=unhealthy_after_s,
        log_signal_after_s=OBSERVED["log_signal_after_s"],
        storage_full_log_lines=len(OBSERVED["storage_full_lines"]),
        sample_log_line=(OBSERVED["storage_full_lines"] or [""])[0][:400],
        spark_healthy_after_free_s=healthy_after_free_s,
        checkpoint=(token, last, token2, last2),
        reconciliation=recon,
    )
    assert token2 == token and last2 > last, (token, last, token2, last2)


def test_spark_logs_xminiostoragefull_while_minio_is_full():
    """08 §12.8 (objaw): „w logu `spark` w ciągu ok. 1–2 min linia `query … stopped …
    [S3 507 XMinioStorageFull: MinIO's disk is full …]`” (observed during the scenario
    above): the first such line appears at most LOG_SIGNAL_S after MinIO started
    rejecting writes, and the supervisor's query-failure line names the S3 status and
    error code (not only a stack trace)."""
    if "storage_full_lines" not in OBSERVED:
        pytest.skip("the disk-full scenario did not reach its observation point")
    lines = OBSERVED["storage_full_lines"]
    assert lines, (
        f"no line with {FULL_MARKERS} in the spark log during {OBSERVED['full_for_s']} s "
        "of a full MinIO disk"
    )
    after = OBSERVED["log_signal_after_s"]
    assert after is not None and after <= LOG_SIGNAL_S, (after, lines[:3])
    assert any("stopped" in ln and "[S3 507 XMinioStorageFull" in ln for ln in lines), lines[:5]
