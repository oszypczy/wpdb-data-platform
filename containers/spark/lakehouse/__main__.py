"""Container entrypoint: ``spark-submit ... -m lakehouse`` is not possible, so the
Dockerfile runs ``spark-submit /app/main.py``, which calls :func:`main`.

``python -m lakehouse maintenance`` runs one maintenance pass and exits (used by hand
and documented in containers/spark/README.md)."""

from __future__ import annotations

import logging
import sys

from lakehouse.config import Lake, Settings
from lakehouse.session import build_session, s3a_conf


def session(settings: Settings, app: str = "wpdb-lakehouse"):
    return build_session(
        app_name=app,
        shuffle_partitions=settings.shuffle_partitions,
        s3=s3a_conf(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key),
    )


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for noisy in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    settings = Settings.from_env()
    lake = Lake(settings.base_uri)
    spark = session(settings)
    spark.sparkContext.setLogLevel("WARN")

    from lakehouse import jobs, maintenance

    jobs.ensure_gold_and_ops(spark, lake)
    if argv and argv[0] == "maintenance":
        for row in maintenance.run(spark, lake):
            print(row)
        return 0

    from lakehouse.metrics import MetricsListener
    from lakehouse.registry import SchemaRegistry
    from lakehouse.supervisor import Supervisor

    listener = MetricsListener()
    spark.streams.addListener(listener)
    Supervisor(spark, settings, lake, SchemaRegistry(settings.schema_registry_url), listener).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
