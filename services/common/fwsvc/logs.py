"""Structured log sink shared by the Python services (system_logs table, migration 003).

Writes are buffered in a bounded in-memory queue and flushed by one background thread, so a slow or
unavailable database never blocks request handling or event production. When the queue is full new
entries are dropped and counted (``LogSink.dropped``) rather than blocking.

Every message and payload is redacted before it is queued: credentials, tokens, cookies, connection
strings and long opaque secrets never reach the table.
"""
from __future__ import annotations

import json
import logging
import queue
import re
import threading
import time
import traceback

from . import now_ms
from .settings import env, env_int

SEVERITIES = ("debug", "info", "warning", "error", "critical")
FIELDS = ("timestamp_utc", "observed_at", "severity", "service", "environment", "session_id", "run_id", "event_id",
          "trace_id", "engine", "job_id", "stage", "status", "duration_ms", "message", "error_code", "retry_attempt",
          "payload")
ENVIRONMENT = env("FRAUDWATCH_ENV", "local")
RETENTION_MS = env_int("LOG_RETENTION_MS", 7 * 24 * 3_600_000)
MAX_ROWS = env_int("LOG_MAX_ROWS", 200_000)

_log = logging.getLogger("fwsvc.logs")

_SENSITIVE_KEY = re.compile(r"(pass(word|phrase)?|secret|token|api[_-]?key|authorization|cookie|credential|"
                            r"private[_-]?key|database_url|dsn|x-internal-token|x-operator-token)", re.I)
_PATTERNS = [
    (re.compile(r"\b([a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@", re.I), r"\1[redacted]@"),          # user:pass@ in URLs
    (re.compile(r"\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}", re.I), r"\1 [redacted]"),
    (re.compile(r"((?:pass(?:word|phrase)?|secret|token|api[_-]?key|authorization|cookie)[\"']?\s*[:=]\s*[\"']?)"
                r"[^\s\"',;&]{4,}", re.I), r"\1[redacted]"),
    (re.compile(r"\b(sk|gsk|AIza|ghp|xox[abp])[-_A-Za-z0-9]{16,}"), "[redacted-key]"),
    (re.compile(r"\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{16,}(\.[A-Za-z0-9_-]{16,})?\b"), "[redacted-token]"),      # signed tokens / JWT
    (re.compile(r"\b[A-Fa-f0-9]{48,}\b"), "[redacted-hex]"),
]
MAX_MESSAGE = 2000
MAX_PAYLOAD_BYTES = 8000


def redact_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = str(s)
    for rx, repl in _PATTERNS:
        s = rx.sub(repl, s)
    return s


def redact(value, depth: int = 0):
    if depth > 6:
        return "[truncated]"
    if isinstance(value, dict):
        out = {}
        for k, v in list(value.items())[:100]:
            out[str(k)] = "[redacted]" if _SENSITIVE_KEY.search(str(k)) else redact(v, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        return [redact(v, depth + 1) for v in list(value)[:100]]
    if isinstance(value, str):
        return redact_text(value[:MAX_MESSAGE])
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return redact_text(str(value)[:MAX_MESSAGE])


def build_entry(service: str, severity: str, message: str, **fields) -> dict:
    sev = severity.lower()
    if sev == "warn":
        sev = "warning"
    if sev not in SEVERITIES:
        sev = "info"
    now = now_ms()
    payload = fields.pop("payload", None)
    if payload is not None:
        payload = redact(payload)
        raw = json.dumps(payload, default=str)
        if len(raw) > MAX_PAYLOAD_BYTES:
            payload = {"truncated": True, "excerpt": raw[:MAX_PAYLOAD_BYTES]}
    entry = {k: None for k in FIELDS}
    entry.update({k: v for k, v in fields.items() if k in entry})
    entry.update({"service": service, "severity": sev, "message": redact_text(str(message)[:MAX_MESSAGE]),
                  "payload": payload, "observed_at": now,
                  "timestamp_utc": int(fields.get("timestamp_utc") or now),
                  "environment": fields.get("environment") or ENVIRONMENT})
    for k in ("error_code", "stage", "status"):
        if entry[k] is not None:
            entry[k] = redact_text(str(entry[k])[:200])
    if entry["duration_ms"] is not None:
        entry["duration_ms"] = float(entry["duration_ms"])
    return entry


def dlq_reason(error: str | None) -> str:
    """Classify a dead-letter error string (contracts section 3 validation messages) for data-quality counts."""
    e = (error or "").lower()
    if "json" in e or "unparseable" in e or "decode" in e or "empty record" in e:
        return "malformed"
    if "missing field" in e or "required" in e:
        return "missing_fields"
    if any(t in e for t in ("schema_version", "wrong type", "must be", "invalid ", "out of range", "too long",
                            "kind must")):
        return "schema_mismatch"
    return "processing_failure"


INSERT_SQL = (f"INSERT INTO system_logs ({', '.join(FIELDS)}) VALUES ({', '.join(['%s'] * len(FIELDS))})")


class LogSink:
    """Bounded, non-blocking, batched writer. ``trim=True`` also enforces retention (run it in one service)."""

    def __init__(self, database_url: str, service: str, *, trim: bool = False, maxsize: int = 5000):
        self.url, self.service, self.trim = database_url, service, trim
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self.dropped = 0
        self.written = 0
        self._conn = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"logsink-{service}", daemon=True)
        self._thread.start()

    def emit(self, severity: str, message: str, **fields) -> None:
        try:
            self.q.put_nowait(build_entry(fields.pop("service", None) or self.service, severity, message, **fields))
        except queue.Full:
            self.dropped += 1
        except Exception:  # a log call must never break the caller
            self.dropped += 1

    def info(self, message: str, **f) -> None:
        self.emit("info", message, **f)

    def warning(self, message: str, **f) -> None:
        self.emit("warning", message, **f)

    def error(self, message: str, **f) -> None:
        self.emit("error", message, **f)

    def close(self, timeout: float = 3.0) -> None:
        self._stop.set()
        self._thread.join(timeout)

    # -- writer thread ---------------------------------------------------------------------------
    def _connection(self):
        import psycopg

        if self._conn is None or self._conn.closed or self._conn.broken:
            self._conn = psycopg.connect(self.url, autocommit=True, connect_timeout=5)
        return self._conn

    def _loop(self) -> None:
        from psycopg.types.json import Jsonb

        last_trim = 0.0
        while not (self._stop.is_set() and self.q.empty()):
            batch = []
            try:
                batch.append(self.q.get(timeout=1.0))
                while len(batch) < 500:
                    batch.append(self.q.get_nowait())
            except queue.Empty:
                pass
            if batch:
                rows = [tuple(Jsonb(e[k]) if k == "payload" and e[k] is not None else e[k] for k in FIELDS)
                        for e in batch]
                for attempt in range(3):
                    try:
                        with self._connection().cursor() as cur:
                            cur.executemany(INSERT_SQL, rows)
                        self.written += len(rows)
                        break
                    except Exception as exc:
                        self._conn = None
                        if attempt == 2:
                            self.dropped += len(rows)
                            _log.warning("dropping %d log rows: %s", len(rows), type(exc).__name__)
                        else:
                            time.sleep(0.5 * (attempt + 1))
            if self.trim and time.monotonic() - last_trim > 300:
                last_trim = time.monotonic()
                try:
                    c = self._connection()
                    c.execute("DELETE FROM system_logs WHERE observed_at < %s", (now_ms() - RETENTION_MS,))
                    c.execute("DELETE FROM system_logs WHERE id < (SELECT max(id) FROM system_logs) - %s", (MAX_ROWS,))
                    c.execute("DELETE FROM client_receipts WHERE recorded_at < %s", (now_ms() - RETENTION_MS,))
                except Exception as exc:
                    self._conn = None
                    _log.warning("log retention trim failed: %s", type(exc).__name__)


class SinkHandler(logging.Handler):
    """Forwards Python logging records (default WARNING+) into the sink, with a stack trace payload."""

    def __init__(self, sink: LogSink, level: int = logging.WARNING):
        super().__init__(level)
        self.sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith("fwsvc.logs"):
            return
        try:
            payload = {"logger": record.name}
            if record.exc_info:
                payload["stack"] = "".join(traceback.format_exception(*record.exc_info))[-4000:]
            self.sink.emit(record.levelname.lower(), record.getMessage(), stage="process", payload=payload,
                           timestamp_utc=int(record.created * 1000),
                           error_code=record.exc_info[0].__name__ if record.exc_info and record.exc_info[0] else None)
        except Exception:
            pass


def install(database_url: str, service: str, *, trim: bool = False, level: int = logging.WARNING) -> LogSink:
    sink = LogSink(database_url, service, trim=trim)
    logging.getLogger().addHandler(SinkHandler(sink, level))
    return sink
