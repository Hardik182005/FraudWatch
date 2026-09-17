"""Run execution: turns a ScenarioStream into Kafka records at a controlled wall-clock rate.

Timing model: each run has a *virtual clock* (ms since start, pauses excluded). A planned event
with arrival offset A is produced when the virtual clock reaches A; its event_time is
run_epoch + event offset, where run_epoch = wall-clock start minus accumulated pause time,
so event_time is always <= produced_at and late events carry an old event_time.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field

from confluent_kafka import Producer

from fwcore import SCHEMA_VERSION
from fwcore.schema import make_event_id, state_key, validate_event
from fwcore.simulation import ScenarioConfig, ScenarioStream
from fwsvc import now_ms

log = logging.getLogger("generator.runner")


@dataclass
class RunSpec:
    run_id: str
    session_id: str
    scenario: str
    seed: int
    rate_eps: float
    max_events: int
    engines: list[str]
    control_log: list[dict] = field(default_factory=list)   # replay: apply at the same base index


class Run:
    def __init__(self, spec: RunSpec, profile: dict, producer: Producer, topic: str, store, max_rate: float):
        self.spec = spec
        self.stream = ScenarioStream(ScenarioConfig.for_scenario(spec.scenario, spec.seed, spec.rate_eps), profile,
                                     spec.session_id)
        self.producer, self.topic, self.store, self.max_rate = producer, topic, store, max_rate
        self.status = "starting"
        self.events_sent = 0
        self.error: str | None = None
        self.started_wall = 0.0
        self.start_epoch_ms = 0
        self.paused_total = 0.0
        self.paused_since: float | None = None
        self.task: asyncio.Task | None = None
        self.lock = threading.Lock()
        self.delivery_errors = 0
        self.pending_replay = sorted(spec.control_log, key=lambda c: c["at_base_index"])

    # -- virtual clock ------------------------------------------------------------------
    def virtual_ms(self) -> float:
        now = time.monotonic()
        paused = self.paused_total + ((now - self.paused_since) if self.paused_since else 0.0)
        return (now - self.started_wall - paused) * 1000.0

    def epoch_ms(self) -> int:
        """Wall-clock epoch ms corresponding to virtual offset 0 (start time shifted by pauses)."""
        return self.start_epoch_ms + int(self.paused_total * 1000)

    # -- controls -------------------------------------------------------------------------
    def pause(self) -> None:
        if self.status == "running":
            self.paused_since = time.monotonic()
            self.status = "paused"

    def resume(self) -> None:
        if self.status == "paused" and self.paused_since is not None:
            self.paused_total += time.monotonic() - self.paused_since
            self.paused_since = None
            self.status = "running"

    def set_rate(self, rate: float) -> None:
        with self.lock:
            self.stream.set_rate(min(rate, self.max_rate))

    def inject(self, pattern: str) -> dict:
        # Patterns start at the next base event's offset (not the wall clock) so a replay that
        # applies the same control at the same base index schedules identical events.
        with self.lock:
            return self.stream.inject(pattern, self.stream.next_base_offset)

    # -- main loop --------------------------------------------------------------------------
    async def run(self) -> None:
        self.started_wall = time.monotonic()
        self.start_epoch_ms = now_ms()
        self.status = "running"
        await self.store.run_started(self)
        try:
            while self.events_sent < self.spec.max_events and self.status in ("running", "paused"):
                if self.status == "paused":
                    await asyncio.sleep(0.1)
                    continue
                with self.lock:
                    self._apply_replay_controls()
                    planned = self.stream.next()
                wait = (planned.arrival_offset_ms - self.virtual_ms()) / 1000.0
                while wait > 0 and self.status in ("running", "paused"):
                    await asyncio.sleep(min(wait, 0.05))
                    wait = (planned.arrival_offset_ms - self.virtual_ms()) / 1000.0 if self.status == "running" else 0.05
                if self.status not in ("running", "paused"):
                    break
                self._produce(planned)
                if self.events_sent % 50 == 0:
                    await self.store.progress(self)
            if self.status in ("running", "paused"):
                self.status = "completed"
        except asyncio.CancelledError:
            if self.status in ("running", "paused", "starting"):
                self.status = "stopped"
        except Exception as exc:  # recorded on the run, visible in both websites
            log.exception("run %s failed", self.spec.run_id)
            self.status, self.error = "failed", f"{type(exc).__name__}: {exc}"
        finally:
            await asyncio.get_running_loop().run_in_executor(None, self.producer.flush, 10)
            await self.store.run_finished(self)

    def _apply_replay_controls(self) -> None:
        while self.pending_replay and self.pending_replay[0]["at_base_index"] <= self.stream.base_index:
            c = self.pending_replay.pop(0)
            if c["action"] == "rate":
                self.stream.set_rate(float(c["value"]))
            elif c["action"] == "inject":
                self.stream.inject(c["value"], self.stream.next_base_offset)

    def _produce(self, planned) -> None:
        seq = self.events_sent
        epoch = self.epoch_ms()
        event = {
            "schema_version": SCHEMA_VERSION, "kind": "txn",
            "event_id": make_event_id(self.spec.run_id, seq),
            "demo_session_id": self.spec.session_id, "run_id": self.spec.run_id, "seq": seq,
            "event_time": epoch + int(planned.event_offset_ms),
            "produced_at": 0,
            "target_engines": list(self.spec.engines),
            **{k: v for k, v in planned.body.items() if v is not None},
        }
        event["produced_at"] = now_ms()  # immediately before produce(): the ingestion boundary
        validate_event(event)
        self.producer.produce(self.topic, key=state_key(event).encode(), value=json.dumps(event, separators=(",", ":")),
                              on_delivery=self._on_delivery)
        self.producer.poll(0)
        self.events_sent += 1

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.delivery_errors += 1
            log.error("delivery failed for run %s: %s", self.spec.run_id, err)
