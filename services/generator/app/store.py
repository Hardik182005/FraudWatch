"""Postgres writes for run lifecycle. Run in a thread so the event loop keeps its timing."""
from __future__ import annotations

import asyncio
import logging
import threading

from psycopg.types.json import Jsonb

from fwsvc import now_ms
from fwsvc.db import connect

log = logging.getLogger("generator.store")

RUN_COLUMNS = ("id, session_id, scenario, seed, rate_eps, engines, status, max_events, events_sent, created_at, "
               "started_at, ended_at, replay_of, control_log, created_by, experiment_id, error, preset, options, events_acked, "
               "delivery_errors, duplicates_sent")

LIFECYCLE = {"starting": "starting", "running": "running", "paused": "paused", "completed": "completed",
             "stopped": "cancelled", "failed": "failed", "queued": "queued"}


def run_row_to_api(r: dict) -> dict:
    return {k: r[k] for k in ("id", "session_id", "scenario", "seed", "rate_eps", "engines", "status", "max_events",
                              "events_sent", "started_at", "ended_at", "replay_of", "control_log", "created_by",
                              "error", "preset")} | {"rate_eps": round(float(r["rate_eps"]), 3),
                                                     "options": r.get("options") or {},
                                                     "lifecycle": LIFECYCLE.get(r["status"], r["status"]),
                                                     "counters": None}


class Store:
    def __init__(self, url: str):
        self.url = url
        self.conn = connect(url, autocommit=True)
        self.lock = threading.Lock()
        self.reported: dict[str, int] = {}
        self.logs = None  # fwsvc.logs.LogSink, set by main

    def _exec(self, fn):
        with self.lock:
            try:
                return fn(self.conn)
            except Exception:
                if self.conn.closed or self.conn.broken:
                    self.conn = connect(self.url, autocommit=True)
                    return fn(self.conn)
                raise

    async def _run(self, fn):
        return await asyncio.get_running_loop().run_in_executor(None, self._exec, fn)

    # -- lifecycle ----------------------------------------------------------------------
    def fail_orphans(self) -> int:
        """Runs left active by a previous generator process cannot be resumed faithfully."""
        def q(c):
            rows = c.execute(
                """UPDATE runs SET status = 'failed', error = 'generator restarted while run was active',
                          ended_at = %s, updated_at = %s
                   WHERE status IN ('starting', 'running', 'paused') RETURNING """ + RUN_COLUMNS,
                (now_ms(), now_ms())).fetchall()
            for r in rows:
                self._publish(c, r)
            return len(rows)
        return self._exec(q)

    async def run_started(self, run) -> None:
        def q(c):
            r = c.execute(f"UPDATE runs SET status = %s, started_at = %s, updated_at = %s WHERE id = %s RETURNING {RUN_COLUMNS}",
                          (run.status, run.start_epoch_ms, now_ms(), run.spec.run_id)).fetchone()
            self._publish(c, r)
        await self._run(q)
        if self.logs:
            sp = run.spec
            self.logs.info(f"run started: {sp.scenario} seed={sp.seed} rate={sp.rate_eps:g}/s max={sp.max_events}",
                           session_id=sp.session_id, run_id=sp.run_id, stage="generated", status="running",
                           timestamp_utc=run.start_epoch_ms,
                           payload={"scenario": sp.scenario, "seed": sp.seed, "rate_eps": sp.rate_eps,
                                    "max_events": sp.max_events, "engines": sp.engines, "options": sp.options})

    async def progress(self, run, publish: bool = False) -> None:
        run_id = run.spec.run_id
        sent = run.events_sent
        delta = sent - self.reported.get(run_id, 0)
        self.reported[run_id] = sent

        def q(c):
            with c.transaction():
                r = c.execute(
                    f"""UPDATE runs SET events_sent = %s, status = %s, rate_eps = %s, control_log = %s, updated_at = %s,
                               events_acked = %s, delivery_errors = %s, duplicates_sent = %s
                        WHERE id = %s RETURNING {RUN_COLUMNS}""",
                    (sent, run.status, run.stream.rate_eps, Jsonb(run.stream.control_log), now_ms(), run.acked,
                     run.delivery_errors, run.duplicates_sent, run_id)).fetchone()
                if delta:
                    c.execute("UPDATE demo_sessions SET events_used = events_used + %s WHERE id = %s",
                              (delta, run.spec.session_id))
                if publish:
                    self._publish(c, r)
        await self._run(q)

    async def run_finished(self, run) -> None:
        await self.progress(run)

        def q(c):
            with c.transaction():
                r = c.execute(
                    f"""UPDATE runs SET status = %s, ended_at = %s, error = %s, updated_at = %s WHERE id = %s
                        RETURNING {RUN_COLUMNS}""",
                    (run.status, now_ms(), run.error, now_ms(), run.spec.run_id)).fetchone()
                c.execute("UPDATE demo_sessions SET active_run_id = NULL WHERE id = %s AND active_run_id = %s",
                          (run.spec.session_id, run.spec.run_id))
                self._publish(c, r)
        await self._run(q)
        self.reported.pop(run.spec.run_id, None)
        if self.logs:
            sp = run.spec
            sev = "error" if run.status == "failed" else ("warning" if run.delivery_errors else "info")
            self.logs.emit(sev, f"run {run.status}: {run.events_sent} generated, {run.acked} broker acks, "
                                f"{run.duplicates_sent} redeliveries, {run.delivery_errors} delivery errors"
                                + (f" ({run.error})" if run.error else ""),
                           session_id=sp.session_id, run_id=sp.run_id, stage="kafka_acked", status=run.status,
                           duration_ms=max(0, now_ms() - run.start_epoch_ms) if run.start_epoch_ms else None,
                           error_code="delivery_error" if run.delivery_errors else None,
                           payload={"generated": run.events_sent, "acked": run.acked, "delivery_errors": run.delivery_errors,
                                    "duplicates_sent": run.duplicates_sent, "error": run.error})

    def _publish(self, c, r: dict | None) -> None:
        if r is None:
            return
        c.execute(
            """INSERT INTO stream_log (session_id, type, payload, at)
               SELECT %s, 'run', %s, %s FROM demo_sessions WHERE id = %s AND stream_updates""",
            (r["session_id"], Jsonb(run_row_to_api(r)), now_ms(), r["session_id"]))

    async def heartbeat(self, status: str, detail: str, metrics: dict) -> None:
        from fwsvc.db import heartbeat

        await self._run(lambda c: heartbeat(c, "generator", status, detail, metrics))
