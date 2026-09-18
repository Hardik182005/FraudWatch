"""Postgres access for the experiment runner.

Every statement runs on one autocommit connection guarded by a lock and is dispatched from the event
loop with ``asyncio.to_thread``, matching the generator's store: psycopg's sync driver is used
everywhere in this repository except the API, and an experiment does far too little database work to
justify a second driver.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import threading

from psycopg.types.json import Jsonb

from fwsvc import now_ms
from fwsvc.db import connect

log = logging.getLogger("ops-agent.store")

EXPERIMENT_SESSION_TTL_MS = 6 * 3_600_000


class Store:
    def __init__(self, url: str) -> None:
        self.url = url
        self.conn = connect(url, autocommit=True)
        self.lock = threading.Lock()

    def _exec(self, fn):
        with self.lock:
            try:
                return fn(self.conn)
            except Exception:
                if self.conn.closed or self.conn.broken:
                    log.warning("postgres connection lost; reconnecting")
                    self.conn = connect(self.url, autocommit=True)
                    return fn(self.conn)
                raise

    async def run(self, fn):
        return await asyncio.to_thread(self._exec, fn)

    async def one(self, sql: str, params=()) -> dict | None:
        return await self.run(lambda c: c.execute(sql, params).fetchone())

    async def all(self, sql: str, params=()) -> list[dict]:
        return await self.run(lambda c: c.execute(sql, params).fetchall())

    # -- experiment queue -------------------------------------------------------------------------
    async def claim(self) -> dict | None:
        """Claim the oldest queued experiment. SKIP LOCKED keeps two agents off the same row."""
        return await self.one(
            """UPDATE experiments SET status = 'running', started_at = %s
               WHERE id = (SELECT id FROM experiments WHERE status = 'queued'
                           ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED)
               RETURNING *""", (now_ms(),))

    async def progress(self, exp_id: str, **fields) -> None:
        """Merge fields into experiments.progress, preserving cancel_requested written by the API."""
        await self.one("UPDATE experiments SET progress = progress || %s::jsonb WHERE id = %s RETURNING id",
                       (Jsonb(fields), exp_id))

    async def cancelled(self, exp_id: str) -> bool:
        row = await self.one("SELECT progress, status FROM experiments WHERE id = %s", (exp_id,))
        if row is None:
            return True
        return bool((row["progress"] or {}).get("cancel_requested")) or row["status"] == "cancelled"

    async def attach_run(self, exp_id: str, run_id: str) -> None:
        await self.one("UPDATE experiments SET run_ids = array_append(run_ids, %s) WHERE id = %s RETURNING id",
                       (run_id, exp_id))

    async def finish(self, exp_id: str, status: str, *, results=None, error: str | None = None,
                     environment: dict | None = None) -> None:
        await self.one(
            """UPDATE experiments SET status = %s, finished_at = %s, results = %s, error = %s,
                                      environment = coalesce(%s, environment)
               WHERE id = %s RETURNING id""",
            (status, now_ms(), Jsonb(results) if results is not None else None, error,
             Jsonb(environment) if environment is not None else None, exp_id))

    async def requeue_stale(self, older_than_ms: int) -> list[str]:
        """Fail experiments left 'running' by an agent that died, so the queue cannot wedge."""
        rows = await self.all(
            """UPDATE experiments SET status = 'failed', finished_at = %s,
                                      error = 'the experiment runner stopped before this finished'
               WHERE status = 'running' AND coalesce(started_at, created_at) < %s RETURNING id""",
            (now_ms(), now_ms() - older_than_ms))
        return [r["id"] for r in rows]

    # -- runs -------------------------------------------------------------------------------------
    async def experiment_session(self, exp_id: str) -> str:
        """One dedicated session per experiment: kind 'experiment', no SSE fan-out, generous caps.

        Experiment runs are not visitor demos, so they must neither consume a visitor session's event
        cap nor push thousands of rows through the live stream.
        """
        sid = f"s_exp_{exp_id[-12:]}_{secrets.token_hex(3)}"
        now = now_ms()
        await self.one(
            """INSERT INTO demo_sessions (id, label, kind, created_at, expires_at, event_cap, rate_cap_eps,
                                          stream_updates)
               VALUES (%s, %s, 'experiment', %s, %s, %s, %s, false) RETURNING id""",
            (sid, f"experiment {exp_id}", now, now + EXPERIMENT_SESSION_TTL_MS, 10_000_000, 100_000.0))
        return sid

    async def create_run(self, *, session_id: str, exp_id: str, scenario: str, seed: int, rate_eps: float,
                         max_events: int, engines: list[str], options: dict | None = None) -> str:
        run_id = "r_" + secrets.token_hex(8)
        now = now_ms()
        await self.one(
            """INSERT INTO runs (id, session_id, scenario, seed, rate_eps, initial_rate_eps, engines, status,
                                 max_events, created_at, control_log, created_by, experiment_id, options, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'starting', %s, %s, %s, 'operator', %s, %s, %s) RETURNING id""",
            (run_id, session_id, scenario, seed, rate_eps, rate_eps, engines, max_events, now, Jsonb([]),
             exp_id, Jsonb(options or {}), now))
        await self.attach_run(exp_id, run_id)
        return run_id

    async def fail_run(self, run_id: str, error: str) -> None:
        await self.one(
            "UPDATE runs SET status = 'failed', error = %s, ended_at = %s, updated_at = %s WHERE id = %s RETURNING id",
            (error[:500], now_ms(), now_ms(), run_id))

    async def run_row(self, run_id: str) -> dict | None:
        return await self.one("SELECT * FROM runs WHERE id = %s", (run_id,))

    # -- measurements -----------------------------------------------------------------------------
    async def persisted_count(self, run_id: str) -> int:
        row = await self.one("SELECT count(*) AS n FROM scored_events WHERE run_id = %s", (run_id,))
        return int(row["n"]) if row else 0

    async def engine_stats(self, run_id: str) -> dict:
        """Per-engine counts and latency percentiles.

        This is the same aggregation the API's ``/runs/{id}/comparison`` view uses, so a figure
        computed here and one read from the API cannot disagree.
        """
        rows = await self.all(
            """SELECT engine, count(*) AS persisted, count(*) FILTER (WHERE late) AS late,
                      count(*) FILTER (WHERE scoring_status <> 'ok') AS failed_scoring,
                      coalesce(sum(duplicates), 0) AS duplicates,
                      percentile_cont(0.5)  WITHIN GROUP (ORDER BY e2e_ms) AS p50,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_ms) AS p95,
                      percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_ms) AS p99,
                      avg(e2e_ms) AS mean, count(e2e_ms) AS n,
                      min(persisted_at) AS first_persisted_at, max(persisted_at) AS last_persisted_at
               FROM scored_events WHERE run_id = %s GROUP BY engine""", (run_id,))
        out: dict[str, dict] = {}
        for r in rows:
            span = None
            if r["first_persisted_at"] is not None and r["last_persisted_at"] is not None:
                span = int(r["last_persisted_at"]) - int(r["first_persisted_at"])
            out[r["engine"]] = {
                "persisted": int(r["persisted"]), "late": int(r["late"]),
                "failed_scoring": int(r["failed_scoring"]), "duplicates": int(r["duplicates"]),
                "latency_ms": {k: None if r[k] is None else round(float(r[k]), 1)
                               for k in ("p50", "p95", "p99", "mean")} | {"n": int(r["n"]), "interval_ms": span},
                "first_persisted_at": r["first_persisted_at"], "last_persisted_at": r["last_persisted_at"],
            }
        return out

    async def parity(self, run_id: str) -> dict:
        row = await self.one(
            """WITH f AS (SELECT event_id, action, late, detail->'raw_features' AS raw FROM scored_events
                          WHERE run_id = %(r)s AND engine = 'flink'),
                    s AS (SELECT event_id, action, late, detail->'raw_features' AS raw FROM scored_events
                          WHERE run_id = %(r)s AND engine = 'spark')
               SELECT count(*) FILTER (WHERE f.event_id IS NOT NULL AND s.event_id IS NOT NULL) AS both,
                      count(*) FILTER (WHERE s.event_id IS NULL) AS only_flink,
                      count(*) FILTER (WHERE f.event_id IS NULL) AS only_spark,
                      count(*) FILTER (WHERE f.action = s.action) AS action_agree,
                      count(*) FILTER (WHERE f.action <> s.action) AS action_disagree,
                      count(*) FILTER (WHERE f.raw IS DISTINCT FROM s.raw
                                         AND f.event_id IS NOT NULL AND s.event_id IS NOT NULL)
                        AS raw_feature_mismatch,
                      count(*) FILTER (WHERE f.late <> s.late) AS late_disagree
               FROM f FULL OUTER JOIN s ON f.event_id = s.event_id""", {"r": run_id})
        return {k: int(v) for k, v in (row or {}).items()}

    async def late_examples(self, run_id: str, engine: str, limit: int = 25) -> list[dict]:
        return await self.all(
            """SELECT event_id, event_time, watermark_ms, watermark_ms - event_time AS lag_ms, pattern
               FROM scored_events WHERE run_id = %s AND engine = %s AND late ORDER BY seq LIMIT %s""",
            (run_id, engine, limit))

    async def late_breakdown(self, run_id: str) -> dict:
        """What the generator planned as late (pattern 'late_delivery') against what each engine flagged."""
        rows = await self.all(
            """SELECT engine,
                      count(*) AS persisted,
                      count(*) FILTER (WHERE late) AS late,
                      count(*) FILTER (WHERE pattern = 'late_delivery') AS planned,
                      count(*) FILTER (WHERE pattern = 'late_delivery' AND late) AS late_planned,
                      count(*) FILTER (WHERE pattern IS DISTINCT FROM 'late_delivery' AND late) AS late_unplanned,
                      count(*) FILTER (WHERE pattern = 'late_delivery' AND NOT late) AS planned_not_late
               FROM scored_events WHERE run_id = %s GROUP BY engine""", (run_id,))
        return {r["engine"]: {k: int(r[k]) for k in ("persisted", "late", "planned", "late_planned",
                                                     "late_unplanned", "planned_not_late")} for r in rows}

    async def spark_dropped(self, since: int, until: int) -> int | None:
        row = await self.one(
            """SELECT sum((op->>'numRowsDroppedByWatermark')::bigint) AS n
               FROM spark_progress sp, jsonb_array_elements(sp.progress->'stateOperators') op
               WHERE sp.at BETWEEN %s AND %s""", (since, until))
        return int(row["n"]) if row and row["n"] is not None else None

    # -- evaluation labels ------------------------------------------------------------------------
    async def label_count(self, pack_id: str) -> int:
        row = await self.one("SELECT count(*) AS n FROM replay_labels WHERE pack_id = %s", (pack_id,))
        return int(row["n"]) if row else 0

    async def load_labels(self, pack_id: str, labels: dict[str, bool]) -> int:
        """Load the replay pack's evaluation-only labels.

        These are ground truth for the offline audit in the Lab. They are never joined into the live
        feed, the Kafka events or the scoring request, so a label can never influence a score.
        """
        items = list(labels.items())

        def go(conn):
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO replay_labels (source_ref, is_fraud, pack_id) VALUES (%s, %s, %s) "
                    "ON CONFLICT (source_ref) DO NOTHING",
                    [(ref, bool(v), pack_id) for ref, v in items])
            return len(items)

        return await self.run(go)
