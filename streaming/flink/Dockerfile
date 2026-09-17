# syntax=docker/dockerfile:1.7
# FraudWatch Flink image: Flink 1.20.5 (Java 17) + shaded job jar + submitter helper.
# Build context: repository root (the parity test reads tests/streaming/fixtures).
#   docker build -f streaming/flink/Dockerfile -t fraudwatch/flink:1.0.0 .

FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /src/streaming/flink
COPY streaming/flink/pom.xml ./pom.xml
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline -DexcludeReactor=true || true
COPY streaming/flink/src ./src
COPY tests/streaming/fixtures /src/tests/streaming/fixtures
# Unit + parity tests run here; the build fails if the Java features diverge from fwcore.
RUN --mount=type=cache,target=/root/.m2 mvn -B package -Dfixture.dir=/src/tests/streaming/fixtures \
    && mkdir -p /out/reports && cp target/fraudwatch-flink.jar /out/ \
    && cp target/surefire-reports/*.txt /out/reports/ 2>/dev/null || true
RUN mkdir -p /out/reports
RUN test -f /out/fraudwatch-flink.jar

FROM flink:1.20.5-java17
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints /opt/fraudwatch \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints
COPY --from=build --chown=flink:flink /out/fraudwatch-flink.jar /opt/flink/usrlib/fraudwatch-flink.jar
COPY --from=build /out/reports/ /opt/fraudwatch/build-reports/
COPY --chmod=0755 streaming/flink/submit.sh /opt/fraudwatch/submit.sh
RUN sed -i 's/\r$//' /opt/fraudwatch/submit.sh
LABEL org.opencontainers.image.title="fraudwatch/flink" \
      org.opencontainers.image.version="1.0.0" \
      org.fraudwatch.flink.version="1.20.5" \
      org.fraudwatch.kafka-connector="3.4.0-1.20"
# Entrypoint/CMD inherited from the official image (jobmanager | taskmanager).
