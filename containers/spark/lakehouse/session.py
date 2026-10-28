"""SparkSession factory shared by the container, the unit tests and the integration tests.

Inside the image every JAR is already in /opt/spark/jars (Dockerfile), so ``packages``
stays empty. On the host the tests pass Maven coordinates and Spark resolves them
through Ivy once (cached in ~/.ivy2).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# versions pinned in docs/design/08-operations.md §1 (same as the image)
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.3.3"
S3A_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"  # pulls aws-java-sdk-bundle 1.12.262

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # never create tables the readers cannot open (06 §9, §12): deletion vectors off
    # by default; column mapping is never set
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false",
    # CDF is enabled per table (silver), not globally
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "false",
}


def s3a_conf(endpoint: str, access_key: str, secret_key: str) -> dict[str, str]:
    """S3A on MinIO: path-style, plain HTTP (CLAUDE.md "Delta on S3A")."""
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        # Two retry layers (hadoop-aws 3.3.4 / aws-java-sdk 1.12.262, 05 §8):
        # - SDK (`attempts.maximum`): retries IOExceptions (MinIO restarting: connection
        #   refused/reset) and HTTP 500/502/503/504, jittered backoff of 0.1 s .. 20 s;
        # - S3A (`retry.limit`/`retry.interval`): around each call, exponential sleeps of
        #   interval * 2^(n+1) * U[0.5, 1.5), WITHOUT any log line. Every other status
        #   lands here, e.g. MinIO's 507 XMinioStorageFull (AWSS3IOException ->
        #   retryIdempotentCalls). 10 x 1 s meant ~34 min of silent retries per write
        #   (08 §12.8); 4 x 2 s = 4+8+16+32 s * U, i.e. 30 .. 90 s, then the batch fails,
        #   the supervisor logs the S3 error code and restarts the query (backoff).
        #   Connectivity still gets 5 S3A attempts x 11 SDK attempts: minutes, far longer
        #   than a MinIO restart or kill (06 §11 e2e).
        "spark.hadoop.fs.s3a.attempts.maximum": "10",
        "spark.hadoop.fs.s3a.retry.limit": "4",
        "spark.hadoop.fs.s3a.retry.interval": "2s",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
        "spark.hadoop.fs.s3a.connection.timeout": "20000",
        # upload buffers on local disk (the default): "bytebuffer"/"array" reserve a
        # whole multipart block (64 MB) per open stream and exhausted direct memory
        "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
        "spark.hadoop.fs.s3a.buffer.dir": "/tmp/spark/s3a",
        "spark.hadoop.fs.s3a.multipart.size": "16M",
        # NOT "spark.hadoop.delta.enableFastS3AListFrom": with Delta 3.3.3 on S3A it makes
        # VACUUM delete the _delta_log/*.json commits (reproduced on MinIO, 06 §7);
        # tests/unit/test_spark_session.py guards against turning it on
    }


def build_session(
    app_name: str = "wpdb-lakehouse",
    master: str = "local[*]",
    shuffle_partitions: int = 4,
    s3: dict[str, str] | None = None,
    packages: list[str] | None = None,
    extra: dict[str, str] | None = None,
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master(master)
    conf: dict[str, str] = {
        **DELTA_CONF,
        # 05 §8: 4 instead of 200 shuffle partitions for small micro-batches
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.default.parallelism": str(shuffle_partitions),
        # Debezium `timestamp-millis` (time.precision.mode=connect) carries the naive
        # Postgres TIMESTAMP as if it were UTC; a UTC session shows the original values
        "spark.sql.session.timeZone": "UTC",
        # tens of rows per batch: adaptive re-planning costs more than it saves
        "spark.sql.adaptive.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.streaming.metricsEnabled": "true",
        # small tables: Delta rebuilds a snapshot's state with 50 tasks by default, which
        # dominated every micro-batch (05 §8, measured); 2 is plenty for < 1 GB tables
        "spark.databricks.delta.snapshotPartitions": "2",
        # many small concurrent queries: share the 8 cores instead of FIFO queueing
        "spark.scheduler.mode": "FAIR",
        # micro-batches of tens of rows: compiling whole-stage Java code for every new
        # plan cost ~30 % of the driver CPU (JFR profile, 05 §8); interpreted is cheaper
        "spark.sql.codegen.wholeStage": "false",
        # ... and the expression projections of Delta's log replay were recompiled all the
        # time (100-entry codegen cache shared by 13 queries): interpret expressions too
        "spark.sql.codegen.factoryMode": "NO_CODEGEN",
        "spark.sql.codegen.cache.maxEntries": "1000",
        # VACUUM lists files with this many tasks (default 10 000: one maintenance run
        # took > 15 min of 10 006-task jobs next to the streams); small tables need few
        "spark.sql.sources.parallelPartitionDiscovery.parallelism": "8",
        # guard: plan strings are built for the UI/AQE on every query; cap them
        "spark.sql.maxPlanStringLength": "100000",
        # release blocks of materialised intermediates (localCheckpoint) regularly
        "spark.cleaner.periodicGC.interval": "5min",
    }
    if s3:
        conf.update(s3)
    if packages:
        conf["spark.jars.packages"] = ",".join(packages)
    if extra:
        conf.update(extra)
    for k, v in conf.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
