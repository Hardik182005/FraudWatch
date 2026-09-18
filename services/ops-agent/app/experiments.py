"""The three experiment kinds, producing the ``results`` shapes in docs/contracts.md section 7.7.

Three rules apply to every experiment here, because the alternative is numbers that look convincing
and mean nothing:

1. **Nothing is invented.** A value that could not be measured is ``null`` with a note saying so, not
   a plausible-looking default. ``winner`` is always ``null``: choosing a winner is the reader's job,
   and a single-host comparison does not support that claim anyway.
2. **The first run of any engine is a warm-up and is excluded.** JIT, Kafka consumer group joins and
   Spark's first micro-batch all make run one unrepresentative.
3. **Engine order alternates across repeats.** Running all of Flink then all of Spark would give one
   of them a cold page cache and the other a warm one, and that difference would be reported as an
   engine difference.
"""
from __future__ import annotations

import asyncio
import logging
import statistics
import time

import httpx

from fwsvc import now_ms

from .dockerapi import ContainerNotFound, DockerUnavailable
from .runs import Cancelled, RunFailed

log = logging.getLogger("ops-agent.experiments")

KINDS = ("comparison", "late_events", "recovery")
# Below this the injected 15-30 s delays never come due inside the run (see late_events).
MIN_LATE_RUN_SECONDS = 60.0
DEFAULTS = {
    "comparison": {"events_per_run": 300, "rate_eps": 10.0, "repeats": 3, "seed": 20260917},
    "late_events": {"events": 600, "rate_eps": 5.0, "seed": 20260917},
    "recovery": {"events": 600, "rate_eps": 10.0, "seed": 20260917, "kill_after_events": 150},
}


def _int(params: dict, key: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(params.get(key, default))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer")
    if not lo <= v <= hi:
        raise ValueError(f"{key} must be between {lo} and {hi}")
    return v


def _float(params: dict, key: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(params.get(key, default))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number")
    if not lo <= v <= hi:
        raise ValueError(f"{key} must be between {lo:g} and {hi:g}")
    return v


class Experiments:
    def __init__(self, store, runner, docker, *, flink_rest: str, spark_trigger: str,
                 engine_containers: dict[str, str]) -> None:
        self.store = store
        self.runner = runner
        self.docker = docker
        self.flink_rest = flink_rest.rstrip("/")
        self.spark_trigger = spark_trigger
        self.containers = engine_containers
        self.http = httpx.AsyncClient(timeout=10.0)

    async def aclose(self) -> None:
        await self.http.aclose()

    # -- shared helpers ---------------------------------------------------------------------------
    async def _step(self, exp_id: str, phase: str, message: str, step: int, total: int, run_id: str | None = None):
        fields = {"phase": phase, "message": message, "step": step, "total_steps": total}
        if run_id:
            fields["run_id"] = run_id
        await self.store.progress(exp_id, **fields)

    async def _flink_job(self) -> dict:
        """Current Flink job state, checkpoints and restart count. Every field is nullable on failure."""
        out = {"job_id": None, "state": None, "restarts": None, "last_checkpoint": None, "counts": None}
        try:
            jobs = (await self.http.get(f"{self.flink_rest}/jobs/overview")).json().get("jobs", [])
        except Exception as exc:
            out["error"] = f"JobManager REST unreachable: {type(exc).__name__}"
            return out
        running = [j for j in jobs if j.get("state") == "RUNNING"]
        job = (running or sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True) or [None])[0]
        if job is None:
            return out
        out["job_id"], out["state"] = job["jid"], job.get("state")
        try:
            cp = (await self.http.get(f"{self.flink_rest}/jobs/{job['jid']}/checkpoints")).json()
            done = (cp.get("latest") or {}).get("completed")
            out["counts"] = cp.get("counts")
            if done:
                out["last_checkpoint"] = {"id": done.get("id"),
                                          "completed_at": done.get("latest_ack_timestamp"),
                                          "duration_ms": done.get("end_to_end_duration")}
            restored = (cp.get("latest") or {}).get("restored")
            if restored:
                out["restored_from"] = restored
        except Exception:
            pass
        try:
            m = (await self.http.get(f"{self.flink_rest}/jobs/{job['jid']}/metrics",
                                     params={"get": "numRestarts"})).json()
            if m:
                out["restarts"] = int(float(m[0]["value"]))
        except Exception:
            pass
        return out

    async def _sample_resources(self, engines: list[str], out: list[dict]) -> None:
        """Append one CPU/memory sample per engine container. Silent on any Docker problem: resource
        sampling is a nice-to-have, and losing it must never fail an otherwise valid measurement."""
        for engine in engines:
            service = self.containers.get(engine)
            if not service:
                continue
            try:
                s = await self.docker.sample(service)
            except (DockerUnavailable, ContainerNotFound, Exception):
                s = None
            if s:
                out.append({"at": now_ms(), **s})

    async def environment(self, engines: list[str]) -> dict:
        versions: dict = {"spark_trigger_interval": self.spark_trigger}
        notes: list[str] = []
        try:
            cfg = (await self.http.get(f"{self.flink_rest}/config")).json()
            versions["flink"] = cfg.get("flink-version")
        except Exception:
            versions["flink"] = None
            notes.append("Flink version not read: the JobManager REST endpoint did not answer.")
        resources: dict = {}
        for engine in engines:
            service = self.containers.get(engine)
            if not service:
                continue
            try:
                c = await self.docker.find(service)
                resources[engine] = {"container": service, "image": c.get("Image"), "state": c.get("State")}
            except (DockerUnavailable, ContainerNotFound):
                resources[engine] = {"container": service, "image": None, "state": None}
                notes.append(f"No container found for {engine}; resource details are unavailable.")
        notes.append("All engines share one host, one Kafka cluster and one scoring service. "
                     "These figures compare engines under that shared load, not engines in isolation.")
        return {"versions": versions, "resources": resources, "trigger_interval": self.spark_trigger,
                "host": "single-host docker compose", "notes": notes}

    # -- comparison -------------------------------------------------------------------------------
    async def comparison(self, exp: dict, session_id: str) -> dict:
        p = exp["params"] or {}
        d = DEFAULTS["comparison"]
        events = _int(p, "events_per_run", d["events_per_run"], 50, 20_000)
        rate = _float(p, "rate_eps", d["rate_eps"], 1.0, 200.0)
        repeats = _int(p, "repeats", d["repeats"], 1, 10)
        seed = _int(p, "seed", d["seed"], 0, 2**62 - 1)
        profile = str(p.get("profile") or "balanced")[:40]
        engines = ["flink", "spark"]

        # Spark runs on demand; a comparison that silently measured a stopped engine would be a lie.
        await self._require_running("spark")

        total_steps = 2 + repeats * len(engines)
        resources: list[dict] = []
        notes: list[str] = []
        runs: list[dict] = []

        await self._step(exp["id"], "warmup", "warm-up run (excluded from the results)", 1, total_steps)
        warm = await self.runner.measured_run(exp_id=exp["id"], session_id=session_id, scenario="replay", seed=seed,
                                              rate_eps=rate, events=min(events, 100), engines=engines)
        warmup = {"run_id": warm["run_id"], "events": warm["sent"], "excluded": True}

        await self._step(exp["id"], "parity", "parity run on both engines", 2, total_steps, warm["run_id"])
        par = await self.runner.measured_run(exp_id=exp["id"], session_id=session_id, scenario="replay",
                                             seed=seed, rate_eps=rate, events=events, engines=engines)
        parity = {"run_id": par["run_id"]} | await self.store.parity(par["run_id"])
        if par["drain"] != "complete":
            notes.append(f"The parity run drained by '{par['drain']}', not 'complete', so "
                         f"only_flink / only_spark may count records that were still in flight.")

        step = 2
        for repeat in range(1, repeats + 1):
            # Alternate which engine goes first so warm-cache effects do not land on one engine.
            order = engines if repeat % 2 else list(reversed(engines))
            for position, engine in enumerate(order):
                step += 1
                await self._step(exp["id"], "measure", f"{engine} repeat {repeat} of {repeats}", step, total_steps)
                sampler = asyncio.create_task(self._sample_loop([engine], resources))
                try:
                    r = await self.runner.measured_run(exp_id=exp["id"], session_id=session_id, scenario="replay",
                                                       seed=seed + repeat, rate_eps=rate, events=events,
                                                       engines=[engine])
                finally:
                    sampler.cancel()
                    await asyncio.gather(sampler, return_exceptions=True)
                stats = (await self.store.engine_stats(r["run_id"])).get(engine, {})
                duration = max((r["ended_at"] - r["started_at"]) / 1000.0, 0.001)
                persisted = int(stats.get("persisted", 0))
                runs.append({
                    "run_id": r["run_id"], "engine": engine, "repeat": repeat, "order": position,
                    "sent": r["sent"], "persisted": persisted, "missing": max(r["sent"] - persisted, 0),
                    "duplicates": int(stats.get("duplicates", 0)), "late": int(stats.get("late", 0)),
                    "failed_scoring": int(stats.get("failed_scoring", 0)),
                    "latency_ms": stats.get("latency_ms"),
                    "offered_eps": round(rate, 3),
                    "processed_eps": round(persisted / duration, 3) if persisted else 0.0,
                    "duration_ms": r["ended_at"] - r["started_at"], "drain": r["drain"],
                })
                if r["drain"] == "timeout":
                    notes.append(f"{engine} repeat {repeat} drained by timeout; its missing count is an "
                                 f"upper bound, not a confirmed loss.")

        pooled = {engine: _pool([r for r in runs if r["engine"] == engine], engine, resources) for engine in engines}
        notes.append("Latency is end to end: generator produce timestamp to Postgres persist timestamp, "
                     "measured per record in the database, not sampled in the engine.")
        notes.append("The offered rate is fixed, so these runs measure latency at that rate. They do not "
                     "measure maximum throughput, which needs a saturating load.")
        return {
            "profile": profile, "rate_eps": rate, "events_per_run": events, "repeats": repeats, "seed": seed,
            "trigger_interval": self.spark_trigger, "warmup": warmup, "parity": parity,
            "engines": pooled, "runs": runs, "resources": {"samples": resources},
            "delay_boundary": None, "notes": notes, "winner": None,
        }

    async def _sample_loop(self, engines: list[str], out: list[dict], every_s: float = 5.0) -> None:
        while True:
            await self._sample_resources(engines, out)
            await asyncio.sleep(every_s)

    async def _require_running(self, engine: str) -> None:
        service = self.containers.get(engine)
        if not service:
            return
        try:
            state = await self.docker.state(service)
        except DockerUnavailable as exc:
            raise RunFailed(f"cannot check whether {engine} is running: {exc}") from exc
        if state != "running":
            raise RunFailed(
                f"{engine} is not running (container state: {state or 'no container'}). "
                f"Start it first — POST /api/v1/ops/spark/start, or "
                f"`docker compose --profile spark up -d {service}`.")

    # -- late events ------------------------------------------------------------------------------
    async def late_events(self, exp: dict, session_id: str) -> dict:
        p = exp["params"] or {}
        d = DEFAULTS["late_events"]
        events = _int(p, "events", d["events"], 50, 20_000)
        rate = _float(p, "rate_eps", d["rate_eps"], 1.0, 200.0)
        seed = _int(p, "seed", d["seed"], 0, 2**62 - 1)
        engines = _engines(p, self.containers)
        # The generator delays a planned-late record by 15-30 s. A run shorter than that finishes its
        # event budget before those records are due, so it delivers none of them and reports zero late
        # events - which reads like "the engine saw nothing late" when in fact nothing late was sent.
        if events / max(rate, 0.1) < MIN_LATE_RUN_SECONDS:
            raise ValueError(
                f"events / rate_eps is {events / max(rate, 0.1):.0f} s; a late-event experiment needs at "
                f"least {MIN_LATE_RUN_SECONDS:g} s so the 15-30 s injected delays are actually delivered. "
                f"Raise events or lower rate_eps.")
        for engine in engines:
            if engine != "flink":
                await self._require_running(engine)

        await self._step(exp["id"], "measure", "replaying with planned late deliveries", 1, 2)
        r = await self.runner.measured_run(exp_id=exp["id"], session_id=session_id, scenario="late_events",
                                           seed=seed, rate_eps=rate, events=events, engines=engines,
                                           drain_timeout_s=240.0)
        await self._step(exp["id"], "collect", "reading late-event counters", 2, 2, r["run_id"])

        breakdown = await self.store.late_breakdown(r["run_id"])
        out_engines: dict = {}
        for engine in engines:
            b = breakdown.get(engine, {})
            dropped = None
            if engine == "spark":
                dropped = await self.store.spark_dropped(r["started_at"], r["ended_at"] + 120_000)
            out_engines[engine] = {
                "persisted": b.get("persisted", 0), "late": b.get("late", 0),
                "late_planned": b.get("late_planned", 0), "late_unplanned": b.get("late_unplanned", 0),
                "planned_not_late": b.get("planned_not_late", 0),
                "out_of_order": b.get("out_of_order", 0),
                "out_of_order_late": b.get("out_of_order_late", 0),
                "windows_late_dropped": dropped,
                "examples": await self.store.late_examples(r["run_id"], engine),
            }
        planned = max((b.get("planned", 0) for b in breakdown.values()), default=0)
        return {
            "run_id": r["run_id"], "rate_eps": rate, "events": r["sent"],
            "watermark_bound_ms": _watermark_bound(),
            "planned_late": planned, "drain": r["drain"], "engines": out_engines,
            "notes": ["The generator injects two delay classes: 'late_delivery' (15-30 s, beyond the "
                      "watermark bound) and 'out_of_order' (1-4 s, inside it). The second class should "
                      "arrive out of order but NOT late; that is what separates lateness from disorder.",
                      "'late' is the engine's own flag: the record arrived after the watermark had "
                      "passed its event time. 'planned' is what the generator deliberately delayed. "
                      "They are not required to match — a planned-late record that still beat the "
                      "watermark is correctly not late."],
        }

    # -- recovery ---------------------------------------------------------------------------------
    async def recovery(self, exp: dict, session_id: str) -> dict:
        p = exp["params"] or {}
        d = DEFAULTS["recovery"]
        events = _int(p, "events", d["events"], 100, 20_000)
        rate = _float(p, "rate_eps", d["rate_eps"], 1.0, 200.0)
        seed = _int(p, "seed", d["seed"], 0, 2**62 - 1)
        kill_after = _int(p, "kill_after_events", d["kill_after_events"], 10, events - 10)
        service = self.containers.get("flink_taskmanager", "flink-taskmanager")

        before = await self._flink_job()
        if before.get("state") != "RUNNING":
            raise RunFailed(f"the Flink job is {before.get('state') or 'not submitted'}; "
                            f"a recovery experiment needs a running job to interrupt")

        killed: dict = {}

        async def kill_when_ready(run_id: str) -> None:
            """Wait until the run has produced enough events, then kill the TaskManager."""
            deadline = now_ms() + int((kill_after / max(rate, 0.1) + 120) * 1000)
            while now_ms() < deadline:
                row = await self.store.run_row(run_id)
                if row is None or row["status"] in ("completed", "failed", "stopped", "cancelled"):
                    break
                if int(row["events_sent"] or 0) >= kill_after:
                    break
                await asyncio.sleep(0.5)
            try:
                await self.docker.kill(service, "SIGKILL")
                killed.update({"at": now_ms(), "container": service, "signal": "SIGKILL"})
            except (DockerUnavailable, ContainerNotFound, RuntimeError) as exc:
                killed.update({"at": now_ms(), "container": service, "signal": "SIGKILL",
                               "error": f"{type(exc).__name__}: {exc}"})

        await self._step(exp["id"], "measure", "replaying, then killing the TaskManager mid-stream", 1, 3)
        killer: asyncio.Task | None = None

        async def on_start(run_id: str) -> None:
            nonlocal killer
            killer = asyncio.create_task(kill_when_ready(run_id))

        try:
            r = await self.runner.measured_run(exp_id=exp["id"], session_id=session_id, scenario="replay", seed=seed,
                                               rate_eps=rate, events=events, engines=["flink"],
                                               drain_timeout_s=420.0, on_start=on_start)
        finally:
            if killer is not None:
                killer.cancel()
                await asyncio.gather(killer, return_exceptions=True)

        if killed.get("error"):
            raise RunFailed(f"could not kill {service}: {killed['error']}")

        await self._step(exp["id"], "recover", "waiting for the job to return to RUNNING", 2, 3, r["run_id"])
        after, running_at = await self._await_running(after_ms=killed.get("at") or now_ms())

        await self._step(exp["id"], "collect", "reading integrity counters", 3, 3, r["run_id"])
        stats = (await self.store.engine_stats(r["run_id"])).get("flink", {})
        persisted = int(stats.get("persisted", 0))
        recovery_ms = (running_at - killed["at"]) if (running_at and killed.get("at")) else None
        return {
            "run_id": r["run_id"], "rate_eps": rate, "events": r["sent"],
            "before": {"job_id": before.get("job_id"), "state": before.get("state"),
                       "restarts": before.get("restarts"), "last_checkpoint": before.get("last_checkpoint")},
            "kill": killed,
            "restart": {"restarted_by": "docker_restart_policy", "at": running_at},
            "after": {"state": after.get("state"), "restarts": after.get("restarts"), "running_at": running_at,
                      "first_new_checkpoint": after.get("last_checkpoint"),
                      "restored_from": after.get("restored_from"), "recovery_ms": recovery_ms},
            "integrity": {"sent": r["sent"], "persisted": persisted, "missing": max(r["sent"] - persisted, 0),
                          "duplicate_deliveries": int(stats.get("duplicates", 0)), "drain": r["drain"]},
            "notes": ["The TaskManager is killed with SIGKILL, so nothing is flushed on the way out; "
                      "recovery is from the last completed checkpoint, not from a clean shutdown.",
                      "recovery_ms is measured from the kill to the job reporting RUNNING again. It "
                      "includes the JobManager's heartbeat timeout, which dominates it."],
        }

    async def _await_running(self, after_ms: int, timeout_s: float = 300.0) -> tuple[dict, int | None]:
        """Wait for the Flink job to report RUNNING again after the kill."""
        deadline = time.monotonic() + timeout_s
        saw_interruption = False
        last: dict = {}
        while time.monotonic() < deadline:
            last = await self._flink_job()
            state = last.get("state")
            if state and state != "RUNNING":
                saw_interruption = True
            if state == "RUNNING" and (saw_interruption or now_ms() - after_ms > 15_000):
                return last, now_ms()
            await asyncio.sleep(2.0)
        return last, None


def _engines(params: dict, containers: dict) -> list[str]:
    requested = params.get("engines") or ["flink"]
    if not isinstance(requested, list) or not requested:
        raise ValueError("engines must be a non-empty list")
    out = []
    for e in requested:
        if e not in ("flink", "spark"):
            raise ValueError("engines may contain only 'flink' and 'spark'")
        out.append(e)
    return out


def _watermark_bound() -> int | None:
    """The out-of-orderness the engines allow, from the environment. None when it is not configured
    here rather than a guessed default: reporting a bound the job does not use would be worse than
    reporting none."""
    import os

    raw = os.getenv("WATERMARK_BOUND_MS") or os.getenv("FLINK_WATERMARK_BOUND_MS")
    try:
        return int(raw) if raw else None
    except ValueError:
        return None


def _pool(runs: list[dict], engine: str, samples: list[dict]) -> dict:
    """Pool the timed repeats for one engine into the section 6 comparison shape."""
    if not runs:
        return {"repeats": 0, "note": "no timed runs completed for this engine"}
    lat = [r["latency_ms"] for r in runs if r.get("latency_ms") and r["latency_ms"].get("n")]
    service_samples = [s for s in samples if s.get("container", "").find(engine) >= 0]

    def med(key: str):
        vals = [l[key] for l in lat if l.get(key) is not None]
        return round(statistics.median(vals), 1) if vals else None

    return {
        "offered_eps": round(statistics.mean([r["offered_eps"] for r in runs]), 3),
        "processed_eps": round(statistics.mean([r["processed_eps"] for r in runs]), 3),
        "sent": sum(r["sent"] for r in runs),
        "persisted": sum(r["persisted"] for r in runs),
        "missing": sum(r["missing"] for r in runs),
        "duplicates": sum(r["duplicates"] for r in runs),
        "failed_scoring": sum(r["failed_scoring"] for r in runs),
        "late": sum(r["late"] for r in runs),
        # Percentiles are medians *of the per-run percentiles*, not percentiles of pooled records:
        # pooling records across runs of different lengths would weight the longest run most.
        "latency_ms": {"p50": med("p50"), "p95": med("p95"), "p99": med("p99"), "mean": med("mean"),
                       "n": sum(l["n"] for l in lat), "interval_ms": None,
                       "basis": "median of the per-run percentiles"},
        "backlog_max": None,
        "cpu_pct": round(statistics.mean([s["cpu_pct"] for s in service_samples if s.get("cpu_pct") is not None]), 1)
                   if any(s.get("cpu_pct") is not None for s in service_samples) else None,
        "mem_mb": round(statistics.mean([s["mem_mb"] for s in service_samples if s.get("mem_mb") is not None]), 1)
                  if any(s.get("mem_mb") is not None for s in service_samples) else None,
        "repeats": len(runs),
    }
