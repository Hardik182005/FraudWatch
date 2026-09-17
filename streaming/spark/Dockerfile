# syntax=docker/dockerfile:1.7
# FraudWatch Spark image: Apache Spark 3.5.9 (Scala 2.12, Java 17, Python 3.10) + Kafka source jars
# baked into /opt/spark/jars (no --packages download at runtime) + fwcore + job dependencies.
# Build context: repository root.
#   docker build -f streaming/spark/Dockerfile -t fraudwatch/spark:1.0.0 .
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

# Kafka integration for Structured Streaming (versions match spark-sql-kafka-0-10_2.12:3.5.9's POM).
# SHA-1 values pinned from Maven Central.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    fetch() { curl -fsSL --retry 3 -o "$(basename "$1")" "$MAVEN/$1"; echo "$2  $(basename "$1")" | sha1sum -c -; }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar 17ef44bb094642e6cbe4c31283b49b0e524225a3; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar a925bf703b98506dd130cb56584eb2e8bd325380; \
    fetch org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar a2f55251deb3d31a21903a0cfbd4d1fa6b7463ca; \
    fetch org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar 8970fd110c965f285ed4c6e40be7630c62db6f68; \
    chmod 0644 ./*.jar

# Python dependencies. numpy < 2 is required by PySpark 3.5's pandas/Arrow integration.
COPY streaming/spark/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY packages/fwcore /tmp/fwcore
# Ubuntu 22.04's pip 22.0 does not read PEP 621 metadata (installs "UNKNOWN-0.0.0"): upgrade pip first,
# build without a stale egg-info, and fail the build unless fwcore actually imports.
RUN python3 -m pip install --no-cache-dir "pip==24.2" \
    && rm -rf /tmp/fwcore/*.egg-info \
    && python3 -m pip install --no-cache-dir /tmp/fwcore && rm -rf /tmp/fwcore \
    && cd / && python3 -c "import fwcore.features, fwcore.schema; from importlib.metadata import version; print('fwcore', version('fwcore'))"

RUN mkdir -p /opt/fraudwatch /opt/spark-checkpoints /tmp/spark-events \
    && chown -R spark:spark /opt/spark-checkpoints /tmp/spark-events
COPY streaming/spark/job.py /opt/fraudwatch/job.py
COPY streaming/spark/run.sh /opt/fraudwatch/run.sh
RUN sed -i 's/\r$//' /opt/fraudwatch/run.sh && chmod 0755 /opt/fraudwatch/run.sh

ENV PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    SPARK_CHECKPOINT_ROOT=/opt/spark-checkpoints

USER spark
WORKDIR /opt/fraudwatch
LABEL org.opencontainers.image.title="fraudwatch/spark" \
      org.opencontainers.image.version="1.0.0" \
      org.fraudwatch.spark.version="3.5.9"
ENTRYPOINT ["/opt/fraudwatch/run.sh"]
