"""Driving one measured run: start it, wait for the generator to finish, wait for the sinks to drain.

The distinction that matters for every latency figure in this file: the generator finishing is not
the run finishing. The last event is still in Kafka, then in the engine, then in scoring, then on its
way to Postgres. Measuring at generator-completion would silently drop the slowest records, which is
exactly the tail a comparison is supposed to show. So every timed run waits for the persisted count
to stop moving, and records *why* it stopped waiting (``complete``, ``stable`` or ``timeout``).
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from fwsvc import now_ms

log = logging.getLogger("ops-agent.runs")

TERMINAL = ("completed", "stopped", "failed", "cancelled")


class RunFailed(RuntimeError):
    pass


class Cancelled(RuntimeError):
    """The operator cancelled the experiment; unwind and mark it cancelled, not failed."""


class Runner:
    def __init__(self, store, generator_url: str, internal_token: str, *, poll_s: float = 1.0) -> None:
        self.store = store
        self.generator_url = generator_url.rstrip("/")
        self.poll_s = poll_s
        headers = {"x-internal-token": internal_token} if internal_token else {}
        self.http = httpx.AsyncClient(timeout=30.0, headers=headers)

    async def aclose(self) -> None:
        await self.http.aclose()

    async def _post(self, path: str, **kw) -> httpx.Response:
        return await self.http.post(f"{self.generator_url}{path}", **kw)

    async def start(self, run_id: str) -> None:
        try:
            r = await self._post(f"/runs/{run_id}/start")
        except httpx.HTTPError as exc:
            await self.store.fail_run(run_id, f"generator unreachable: {type(exc).__name__}")
            raise RunFailed(f"generator unreachable: {type(exc).__name__}") from exc
        if r.status_code >= 300:
            message = _error_message(r)
            await self.store.fail_run(run_id, f"generator refused start: {message}")
            raise RunFailed(f"generator refused start for {run_id}: {message}")

    async def stop(self, run_id: str) -> None:
        """Best-effort stop. A run that already ended returns 409 from the generator; that is not an error."""
        try:
            await self._post(f"/runs/{run_id}/stop")
        except httpx.HTTPError as exc:
            log.warning("could not stop %s: %s", run_id, type(exc).__name__)

    async def await_generator(self, run_id: str, exp_id: str, timeout_s: float) -> dict:
        """Wait until the run row reaches a terminal status. Returns the final row."""
        deadline = now_ms() + int(timeout_s * 1000)
        while True:
            if await self.store.cancelled(exp_id):
                await self.stop(run_id)
                raise Cancelled()
            row = await self.store.run_row(run_id)
            if row is None:
                raise RunFailed(f"run {run_id} disappeared")
            if row["status"] in TERMINAL:
                if row["status"] == "failed":
                    raise RunFailed(f"run {run_id} failed: {row.get('error') or 'no error recorded'}")
                return row
            if now_ms() > deadline:
                await self.stop(run_id)
                raise RunFailed(f"run {run_id} did not finish within {timeout_s:g} s "
                                f"(status {row['status']}, {row['events_sent']} events sent)")
            await asyncio.sleep(self.poll_s)

    async def await_drain(self, run_id: str, exp_id: str, *, expected: int, engines: int,
                          timeout_s: float, stable_s: float = 6.0) -> str:
        """Wait for persisted results to reach the expected total, or to stop growing.

        Returns the reason the wait ended: ``complete`` (every event persisted by every engine),
        ``stable`` (results arrived and then stopped growing) or ``timeout``. The caller records this
        verbatim — a run that drained by timeout is not the same measurement as one that drained
        completely, and the results must say which it was.

        The stability rule deliberately does **not** apply while the persisted count is still zero.
        A cold engine, a Spark micro-batch that has not reached its first trigger, or a result
        consumer that has not yet committed its first batch all look identical to "growth has
        stopped" if you start the clock at zero, and the wait then ends before a single record has
        landed — reporting ``stable`` with ``persisted = 0`` for a run whose records arrive moments
        later. Zero therefore falls through to the deadline, so the recorded reason is ``timeout``:
        nothing persisted within the budget, which is what actually happened.
        """
        target = expected * max(engines, 1)
        deadline = now_ms() + int(timeout_s * 1000)
        last_count, last_change = -1, now_ms()
        while True:
            if await self.store.cancelled(exp_id):
                raise Cancelled()
            count = await self.store.persisted_count(run_id)
            if count >= target:
                return "complete"
            if count != last_count:
                last_count, last_change = count, now_ms()
            elif count > 0 and now_ms() - last_change >= stable_s * 1000:
                return "stable"
            if now_ms() > deadline:
                return "timeout"
            await asyncio.sleep(self.poll_s)

    async def measured_run(self, *, exp_id: str, session_id: str, scenario: str, seed: int, rate_eps: float,
                           events: int, engines: list[str], options: dict | None = None,
                           drain_timeout_s: float = 120.0, on_start=None) -> dict:
        """Create, start and fully drain one run. Returns the run row plus drain bookkeeping."""
        run_id = await self.store.create_run(session_id=session_id, exp_id=exp_id, scenario=scenario, seed=seed,
                                             rate_eps=rate_eps, max_events=events, engines=engines,
                                             options=options)
        started = now_ms()
        await self.start(run_id)
        if on_start is not None:
            await on_start(run_id)
        # Allow the generator the time the rate implies, plus headroom for startup and jitter.
        budget = events / max(rate_eps, 0.1) + 90.0
        row = await self.await_generator(run_id, exp_id, timeout_s=budget)
        drain = await self.await_drain(run_id, exp_id, expected=int(row["events_sent"]), engines=len(engines),
                                       timeout_s=drain_timeout_s)
        return {"run_id": run_id, "row": row, "drain": drain, "started_at": started, "ended_at": now_ms(),
                "sent": int(row["events_sent"])}


def _error_message(r: httpx.Response) -> str:
    try:
        return str(r.json()["error"]["message"])
    except Exception:
        return f"HTTP {r.status_code}"
