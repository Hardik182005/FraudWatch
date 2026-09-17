#!/usr/bin/env bash
# Launches the FraudWatch Spark Structured Streaming job in local[2] mode.
# All job settings come from environment variables (see job.py). Extra spark-submit flags can be
# passed as container arguments, e.g. `--conf spark.ui.enabled=false`.
set -euo pipefail

DRIVER_MEM="${SPARK_DRIVER_MEMORY:-1400m}"
exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --name "fraudwatch-spark${SPARK_RUN_LABEL:+-$SPARK_RUN_LABEL}" \
  --driver-memory "$DRIVER_MEM" \
  --conf spark.ui.port="${SPARK_UI_PORT:-4040}" \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.streaming.stateStore.stateSchemaCheck=true \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.python.worker.reuse=true \
  --conf spark.driver.extraJavaOptions="-Divy.home=/tmp/.ivy2" \
  "$@" \
  /opt/fraudwatch/job.py
