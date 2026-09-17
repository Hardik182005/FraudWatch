"""Environment settings. Names match the master build prompt; secrets are never logged."""
from __future__ import annotations

import os


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"environment variable {name} is required")
    return value or ""


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


KAFKA_BOOTSTRAP_SERVERS = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TRANSACTIONS = env("KAFKA_TRANSACTIONS_TOPIC", "fraudwatch.transactions")
TOPIC_RESULTS = {
    "flink": env("KAFKA_FLINK_RESULTS_TOPIC", "fraudwatch.results.flink"),
    "spark": env("KAFKA_SPARK_RESULTS_TOPIC", "fraudwatch.results.spark"),
}
TOPIC_WINDOWS = {
    "flink": env("KAFKA_FLINK_WINDOWS_TOPIC", "fraudwatch.windows.flink"),
    "spark": env("KAFKA_SPARK_WINDOWS_TOPIC", "fraudwatch.windows.spark"),
}
TOPIC_DLQ = env("KAFKA_DLQ_TOPIC", "fraudwatch.dlq")
TOPIC_SPARK_METRICS = env("KAFKA_SPARK_METRICS_TOPIC", "fraudwatch.metrics.spark")
DATABASE_URL = env("DATABASE_URL", "postgresql://fraudwatch:fraudwatch@postgres:5432/fraudwatch")
