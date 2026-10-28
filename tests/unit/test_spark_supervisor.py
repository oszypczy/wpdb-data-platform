"""05 §8: the supervisor retires only the silver query of a table whose topic is gone
(no JVM needed: the queries are fakes)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from lakehouse import supervisor as sv

pytestmark = pytest.mark.unit


def test_retire_dropped_stops_only_idle_silver_queries_without_topic(monkeypatch):
    stopped = []

    def managed(name):
        q = SimpleNamespace(isActive=True, stop=lambda: stopped.append(name))
        return sv.Managed(name, starter=lambda: q, query=q, started_at=0.0)

    names = ["silver_orders", "silver_dropped_test", "silver_to_gold", "gold_agg_revenue"]
    sup = sv.Supervisor.__new__(sv.Supervisor)
    sup.managed = {n: managed(n) for n in names}
    sup.listener = SimpleNamespace(last_input={})
    monkeypatch.setattr(sv.time, "time", lambda: sv.RETIRE_IDLE_S + 1.0)

    sup.retire_dropped(topics={"olist.shop.orders"})

    assert stopped == ["silver_dropped_test"]
    assert set(sup.managed) == {"silver_orders", "silver_to_gold", "gold_agg_revenue"}


# A failure of a query while MinIO's disk is full, shaped like Spark's message (the S3A
# exception is nested deep inside "Job aborted ... Lost task ..."), 08 §12.8.
STORAGE_FULL = (
    "[STREAM_FAILED] Query [id = 1, runId = 2] terminated with exception: Job aborted due "
    "to stage failure: Task 0 in stage 692.0 failed 1 times, most recent failure: Lost task "
    "0.0 in stage 692.0 (TID 1233) (spark executor driver): "
    "org.apache.hadoop.fs.s3a.AWSS3IOException: Writing Object on bronze/orders/part-0.parquet: "
    "com.amazonaws.services.s3.model.AmazonS3Exception: Storage backend has reached its "
    "minimum free drive threshold. Please delete a few objects to proceed. (Service: Amazon "
    "S3; Status Code: 507; Error Code: XMinioStorageFull; Request ID: 18A; S3 Extended "
    "Request ID: dd9; Proxy: null), S3 Extended Request ID: dd9:XMinioStorageFull"
)


def test_s3_error_names_status_code_and_minio_storage_full():
    """08 §12.8: the log line of a failed query names the S3 status and error code (the
    operator's signal), with a hint for a full MinIO disk; other errors stay unannotated."""
    assert sv.s3_error(STORAGE_FULL).startswith("S3 507 XMinioStorageFull: MinIO's disk is full")
    assert sv.s3_error("x (Service: Amazon S3; Status Code: 500; Error Code: InternalError)") == (
        "S3 500 InternalError"
    )
    assert sv.s3_error("... :XMinioStorageFull").startswith("S3 XMinioStorageFull")
    assert sv.s3_error("DeltaSourceSchemaEvolution: the schema of the table changed") == ""
    assert sv.s3_error("") == ""


def test_restart_delay_first_restart_immediate_then_growing_and_capped():
    """05 §5 schema change: first failure restarts at once; a lasting store error backs
    off to at most one restart per minute (so it never reaches MAX_FAILURES in the
    window together with the S3A retry budget, test_spark_session)."""
    delays = [sv.restart_delay(n) for n in range(1, 8)]
    assert delays == [0.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0]


class _Q:
    def __init__(self, qid="q-1", active=True, exc=None):
        self.id, self.isActive, self._exc = qid, active, exc

    def exception(self):
        return self._exc


def _sup(monkeypatch, clock, last_alive):
    sup = sv.Supervisor.__new__(sv.Supervisor)
    sup.managed = {}
    sup._status = None
    sup.listener = SimpleNamespace(last_alive=last_alive, last_progress={})
    sup.settings = SimpleNamespace(health_file=str(clock["dir"] / "health.json"))
    monkeypatch.setattr(sv.time, "time", lambda: clock["now"])
    return sup


def test_failing_query_is_restarted_with_backoff_logged_with_s3_cause_and_unhealthy(
    monkeypatch, tmp_path, caplog
):
    """08 §12.8: a query failing on a full MinIO is logged with `XMinioStorageFull`,
    restarted after restart_delay, and the health status stays `degraded` (inactive or
    recovering) until it completes a trigger again; then `ok` and the change is logged."""
    import json

    clock = {"now": 1000.0, "dir": tmp_path}
    alive: dict[str, float] = {"q-1": 990.0}
    sup = _sup(monkeypatch, clock, alive)
    starts = []

    def starter():
        starts.append(clock["now"])
        return _Q()

    m = sv.Managed("kafka_to_bronze", starter=starter, query=_Q(), query_id="q-1")
    sup.managed[m.name] = m
    sup.write_health()
    assert json.load(open(sup.settings.health_file))["status"] == "ok"

    caplog.set_level("INFO", logger=sv.__name__)
    for i in range(3):  # three failures in a row, each after its restart
        m.query = _Q(active=False, exc=Exception(STORAGE_FULL))
        sup.check_queries()
        sup.write_health()
        doc = json.load(open(sup.settings.health_file))
        assert doc["status"] == "degraded"
        clock["now"] += sv.restart_delay(i + 1)
        sup.check_queries()  # restarted now (not earlier)
        assert starts[-1] == clock["now"]
        sup.write_health()
        doc = json.load(open(sup.settings.health_file))
        assert doc["queries"]["kafka_to_bronze"]["recovering"] is True
        assert doc["status"] == "degraded"
        clock["now"] += 45  # the next S3A retry budget runs out
    assert len(starts) == 3
    stopped = [r.getMessage() for r in caplog.records if "stopped" in r.getMessage()]
    assert len(stopped) == 3 and all("[S3 507 XMinioStorageFull" in s for s in stopped)
    degraded = [r.getMessage() for r in caplog.records if "health degraded" in r.getMessage()]
    assert len(degraded) == 1 and "XMinioStorageFull" in degraded[0]

    # space freed: the restarted query completes a trigger -> ok again, logged
    alive["q-1"] = clock["now"]
    clock["now"] += 1
    sup.write_health()
    assert json.load(open(sup.settings.health_file))["status"] == "ok"
    assert any("health ok" in r.getMessage() for r in caplog.records)


def test_one_restart_after_a_schema_change_does_not_count_as_recovering(monkeypatch, tmp_path):
    """05 §5: a Delta source stops on a schema change and is restarted at once; it is
    `recovering` only until its next trigger, and an old failure (> RECOVERING_S) never
    keeps the container unhealthy even without listener events."""
    clock = {"now": 5000.0, "dir": tmp_path}
    alive: dict[str, float] = {}
    sup = _sup(monkeypatch, clock, alive)
    m = sv.Managed("silver_orders", starter=lambda: _Q("q-2"), query=_Q("q-2", active=False,
                   exc=Exception("schema changed")), query_id="q-2")  # fmt: skip
    sup.managed[m.name] = m
    sup.check_queries()
    assert m.query is not None and m.query.isActive  # restarted at once
    assert sup.recovering(m, clock["now"]) is True
    alive["q-2"] = clock["now"] + 3
    assert sup.recovering(m, clock["now"] + 4) is False
    del alive["q-2"]
    assert sup.recovering(m, clock["now"] + sv.RECOVERING_S) is False


def _failing_run(monkeypatch, tmp_path, attempt_s: float, horizon_s: float, failures: list):
    """Drive ``check_queries`` on a fake clock for a query that fails every time it runs:
    each attempt lives ``attempt_s`` seconds (0 = fails at once; the S3A retry budget
    when MinIO rejects writes), then terminates with the full-disk error, and the
    supervisor restarts it after ``restart_delay``. Appends the failure times (seconds
    from the first start) to ``failures`` until the supervisor raises SystemExit (it
    propagates) or ``horizon_s`` passes."""
    clock = {"now": 10_000.0, "dir": tmp_path}
    sup = _sup(monkeypatch, clock, last_alive={})
    t0 = clock["now"]
    started = [t0]

    def starter():
        started.append(clock["now"])
        return _Q(active=True)

    m = sv.Managed("kafka_to_bronze", starter=starter, query=_Q(active=True), query_id="q-1")
    sup.managed[m.name] = m
    while clock["now"] - t0 < horizon_s:
        clock["now"] = started[-1] + attempt_s  # the running attempt ends with the error
        m.query = _Q(active=False, exc=Exception(STORAGE_FULL))
        failures.append(clock["now"] - t0)
        sup.check_queries()  # records the failure (may raise), restarts if no delay
        if m.query is None:  # inactive until restart_delay is over (the loop polls)
            assert m.start_at > clock["now"]
            clock["now"] = m.start_at
            sup.check_queries()
        assert m.query is not None and m.query.isActive


def test_query_failing_more_than_max_failures_in_the_window_exits_the_process(
    monkeypatch, tmp_path
):
    """05 §12: "Zapytanie, które pada > 10 razy w 10 min, kończy proces kodem 1 (Docker
    restartuje kontener)". A query failing at once on every start (restarted after
    0, 0, 10, 20, 40, 60 ... s) fails for the 11th time 430 s after the first, inside the
    600 s window: the supervisor raises SystemExit naming the query and the S3 cause, not
    before. The process part: a SystemExit raised by ``check_queries`` ends the Python
    driver with status 1 (subprocess below). The container part (the driver exits,
    Docker restarts `spark` by itself) is the integration test
    `test_stack.py::test_crashed_long_running_container_is_restarted_by_docker`."""
    import subprocess
    import sys

    assert (sv.MAX_FAILURES, sv.FAILURE_WINDOW_S) == (10, 600)
    failures: list[float] = []
    with pytest.raises(SystemExit) as exc:
        _failing_run(monkeypatch, tmp_path, attempt_s=0.0, horizon_s=3600, failures=failures)
    assert failures == [0.0, 0.0, 10.0, 30.0, 70.0, 130.0, 190.0, 250.0, 310.0, 370.0, 430.0]
    msg = exc.value.code
    assert isinstance(msg, str) and msg.startswith(
        "query kafka_to_bronze keeps failing [S3 507 XMinioStorageFull"
    ), msg

    spark_dir = Path(sv.__file__).resolve().parents[1]
    code = (
        f"import sys; sys.path.insert(0, {str(spark_dir)!r})\n"
        "from lakehouse import supervisor as sv\n"
        "class Q:\n"
        "    id, isActive = 'q', False\n"
        "    def exception(self): return Exception('boom')\n"
        "sv.restart_delay = lambda n: 0.0\n"
        "s = sv.Supervisor.__new__(sv.Supervisor)\n"
        "s.managed = {'q': sv.Managed('q', starter=Q, query=Q())}\n"
        "while True: s.check_queries()\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          timeout=120)  # fmt: skip
    assert proc.returncode == 1, proc.stderr[-2000:]
    assert "query q keeps failing: boom" in proc.stderr, proc.stderr[-2000:]


@pytest.mark.parametrize(("attempt_s", "most_in_window"), [(30.0, 9), (90.0, 6)])
def test_lasting_store_error_never_reaches_the_failure_limit(
    monkeypatch, tmp_path, attempt_s, most_in_window
):
    """08 §12.8: while MinIO rejects writes, Spark retries each write 30-90 s before the
    batch fails; with restart_delay the query then fails at most 9 (30 s attempts) or 6
    (90 s) times in any 600 s, so the supervisor never exits and the container is not
    restarted (it recovers by itself once space is freed). One hour of failures, no
    SystemExit; the limit is only reached by attempts shorter than ~17 s."""
    from lakehouse import session

    # the simulated attempt lengths are the S3A budget of the real session config:
    # retry.limit x retry.interval, sleeps interval * 2^(n+1) * U[0.5, 1.5)
    conf = session.s3a_conf("http://minio:9000", "k", "s")
    limit = int(conf["spark.hadoop.fs.s3a.retry.limit"])
    interval = float(conf["spark.hadoop.fs.s3a.retry.interval"].removesuffix("s"))
    base = sum(interval * 2 ** (n + 1) for n in range(limit))
    assert (0.5 * base, 1.5 * base) == (30.0, 90.0)
    failures: list[float] = []
    _failing_run(monkeypatch, tmp_path, attempt_s=attempt_s, horizon_s=3600, failures=failures)
    most = max(sum(1 for t in failures if f <= t < f + sv.FAILURE_WINDOW_S) for f in failures)
    assert len(failures) > sv.MAX_FAILURES and most == most_in_window, (most, failures)
