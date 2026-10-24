"""A local SparkSession for the tests (unit: Delta on the local filesystem; integration:
the same session also reaches MinIO through S3A on localhost:9000).

Spark 3.5 runs on Java 8/11/17; the host may default to a newer JDK, so JAVA_HOME is
pointed at a 17 when the current one is not usable (macOS ``java_home -v 17``, the
``JAVA_HOME_17_*`` variables of GitHub runners). JARs come from Maven through Ivy
(cached in ~/.ivy2), the same versions as the image (08 §1).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from lakehouse.session import DELTA_PACKAGE, S3A_PACKAGE, build_session, s3a_conf

AVRO_PACKAGE = "org.apache.spark:spark-avro_2.12:3.5.9"
PACKAGES = [DELTA_PACKAGE, AVRO_PACKAGE, S3A_PACKAGE]
SUPPORTED_JAVA = ("17", "11")


def _java_major(home: str) -> str | None:
    java = Path(home) / "bin" / "java"
    if not java.exists():
        return None
    out = subprocess.run([str(java), "-version"], capture_output=True, text=True).stderr
    m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
    if not m:
        return None
    return m.group(2) if m.group(1) == "1" else m.group(1)


def ensure_java() -> str:
    current = os.environ.get("JAVA_HOME")
    if current and _java_major(current) in SUPPORTED_JAVA:
        return current
    candidates = [os.environ.get(f"JAVA_HOME_17_{a}") for a in ("X64", "ARM64", "AARCH64")]
    if shutil.which("/usr/libexec/java_home"):
        r = subprocess.run(["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True)
        if r.returncode == 0:
            candidates.append(r.stdout.strip())
    candidates += ["/usr/lib/jvm/java-17-openjdk-amd64", "/usr/lib/jvm/java-17-openjdk-arm64"]
    for c in candidates:
        if c and _java_major(c) in SUPPORTED_JAVA:
            os.environ["JAVA_HOME"] = c
            return c
    raise RuntimeError("Spark 3.5 needs Java 17 (or 11); set JAVA_HOME")


def session(settings: dict[str, str] | None = None, warehouse: str | None = None):
    ensure_java()
    # PySpark converts timestamps to/from the *Python* process time zone on collect() and
    # lit(); UTC everywhere (like the container, ENV TZ=UTC) keeps the naive values
    os.environ["TZ"] = "UTC"
    time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", shutil.which("python") or "python3")
    settings = settings or {}
    extra = {
        "spark.driver.memory": "1g",
        "spark.sql.warehouse.dir": warehouse or "/tmp/wpdb-spark-warehouse",
        "spark.ui.enabled": "false",
        "spark.sql.streaming.stateStore.providerClass": (
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
        ),
        # unit tests write and VACUUM short-lived tables
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
    }
    s3 = s3a_conf(
        "http://localhost:9000",
        settings.get("MINIO_SPARK_ACCESS_KEY", "spark"),
        settings.get("MINIO_SPARK_SECRET_KEY", "spark-secret-dev"),
    )
    return build_session(
        app_name="wpdb-tests",
        master="local[2]",
        shuffle_partitions=2,
        s3=s3,
        packages=PACKAGES,
        extra=extra,
    )
