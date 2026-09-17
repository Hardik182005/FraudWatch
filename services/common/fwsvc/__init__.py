"""Helpers shared by the FraudWatch Python services (not by the engines)."""
import time


def now_ms() -> int:
    return time.time_ns() // 1_000_000
