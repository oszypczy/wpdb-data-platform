#!/bin/sh
# Start the driver in local mode (05 §1: one container, local[*]). Memory follows the
# RAM budget of 08 §7: the whole application (driver = executor in local mode) lives
# in this JVM, plus the Python process of foreachBatch.
set -eu
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1536m}" \
  --conf spark.ui.port=4040 \
  --conf spark.local.dir=/tmp/spark/local \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:MaxMetaspaceSize=384m -XX:MaxDirectMemorySize=256m -Dlog4j2.formatMsgNoLookups=true" \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider \
  --conf spark.sql.streaming.minBatchesToRetain=20 \
  --conf spark.ui.retainedJobs=200 \
  --conf spark.ui.retainedStages=200 \
  --conf spark.sql.ui.retainedExecutions=50 \
  --conf spark.streaming.ui.retainedBatches=50 \
  /app/main.py "$@"
