"""Postgres helpers: connection pools and forward-only migrations under an advisory lock."""
from __future__ import annotations

import logging
import time
from importlib import resources

import psycopg
from psycopg.rows import dict_row

from . import now_ms

log = logging.getLogger("fwsvc.db")
MIGRATION_LOCK_ID = 470_2026


def connect(url: str, *, attempts: int = 60, autocommit: bool = False) -> psycopg.Connection:
    """Connect with retries so services can start before Postgres is ready."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            return psycopg.connect(url, autocommit=autocommit, row_factory=dict_row, connect_timeout=5)
        except psycopg.OperationalError as exc:
            last = exc
            if i == 0 or i % 10 == 9:
                log.warning("postgres not ready (%s); retrying", type(exc).__name__)
            time.sleep(1)
    raise RuntimeError(f"could not connect to Postgres: {last}")


def migrate(url: str) -> list[str]:
    """Apply pending migrations in filename order. Safe to call from several services at once."""
    applied: list[str] = []
    with connect(url, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK_ID,))
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at bigint NOT NULL)")
            done = {r["name"] for r in conn.execute("SELECT name FROM schema_migrations")}
            files = sorted(p for p in resources.files("fwsvc.migrations").iterdir() if p.name.endswith(".sql"))
            for f in files:
                if f.name in done:
                    continue
                with conn.transaction():
                    conn.execute(f.read_text(encoding="utf-8"))
                    conn.execute("INSERT INTO schema_migrations VALUES (%s, %s)", (f.name, now_ms()))
                applied.append(f.name)
                log.info("applied migration %s", f.name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK_ID,))
    return applied


def heartbeat(conn: psycopg.Connection, component: str, status: str, detail: str, metrics: dict) -> None:
    from psycopg.types.json import Jsonb

    conn.execute(
        """INSERT INTO component_heartbeats (id, status, detail, metrics, checked_at) VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, detail = EXCLUDED.detail,
                                          metrics = EXCLUDED.metrics, checked_at = EXCLUDED.checked_at""",
        (component, status, detail, Jsonb(metrics), now_ms()),
    )
