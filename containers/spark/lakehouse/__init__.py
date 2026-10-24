"""WPDB lakehouse: Kafka (Debezium AVRO) -> bronze -> silver -> gold in Delta on MinIO.

Layout (docs/design/05-spark.md §9): pure ``DataFrame -> DataFrame`` transforms live in
``avro``, ``schema_align``, ``bronze``, ``silver``, ``gold`` and ``quality``; reading and
writing Delta is in ``delta_io``; the streaming queries are wired in ``jobs`` and run
by ``supervisor`` (the container entrypoint, ``python -m lakehouse``).

The container image runs Python 3.10 (Ubuntu 22.04 in apache/spark), so this package
must not use 3.11+ syntax; ``containers/spark/ruff.toml`` targets py310.
"""

__version__ = "0.3.0"
