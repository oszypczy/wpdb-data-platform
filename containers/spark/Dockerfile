# Spark Structured Streaming: Kafka (Debezium AVRO) -> bronze -> silver -> gold in Delta
# on MinIO (docs/design/05-spark.md, 06-minio-delta.md). Version matrix: 08-operations.md §1.
#
# Spark 3.5.9 / Scala 2.12 / Java 17 / Hadoop 3.3.4 (bundled). Every connector JAR is
# resolved at build time from Maven Central with its .sha1 checked, instead of
# `--packages` downloading them at every start:
#   spark-sql-kafka-0-10_2.12 3.5.9 (+ spark-token-provider-kafka-0-10 3.5.9,
#     kafka-clients 3.4.1, commons-pool2 2.11.1 = its POM dependencies)
#   spark-avro_2.12 3.5.9 (avro 1.11.5 and xz 1.9 already in /opt/spark/jars)
#   delta-spark_2.12 3.3.3 + delta-storage 3.3.3 (Delta 3.3 = the Spark 3.5 line)
#   hadoop-aws 3.3.4 (= bundled Hadoop) + aws-java-sdk-bundle 1.12.262 (its POM)
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/spark/jars; \
    for a in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/spark/spark-avro_2.12/3.5.9/spark-avro_2.12-3.5.9.jar \
      io/delta/delta-spark_2.12/3.3.3/delta-spark_2.12-3.3.3.jar \
      io/delta/delta-storage/3.3.3/delta-storage-3.3.3.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    ; do \
      f="$(basename "$a")"; \
      curl -fsSL --retry 3 -o "$f" "$MAVEN/$a"; \
      echo "$(curl -fsSL --retry 3 "$MAVEN/$a.sha1" | cut -c1-40)  $f" | sha1sum -c -; \
    done

# PySpark comes from the image (/opt/spark/python = exactly Spark 3.5.9); only the
# delta-spark Python API is added, without its pyspark dependency (it would install a
# second, 300 MB copy of Spark from PyPI). The build fails if the versions drift.
ARG DELTA_SPARK_PY=3.3.3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC
RUN pip install --no-cache-dir --no-deps "delta-spark==${DELTA_SPARK_PY}" \
 && python3 -c "import pyspark, delta, importlib.metadata as m; \
assert pyspark.__version__ == '3.5.9', pyspark.__version__; \
assert m.version('delta-spark') == '${DELTA_SPARK_PY}'; print('pyspark', pyspark.__version__)"

WORKDIR /app
COPY lakehouse /app/lakehouse
COPY main.py healthcheck.py run.sh /app/
RUN chmod 0755 /app/run.sh && mkdir -p /tmp/spark && chown -R spark:spark /tmp/spark /app

USER spark
EXPOSE 4040
ENTRYPOINT ["/app/run.sh"]
