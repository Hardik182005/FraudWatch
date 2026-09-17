"""Replay of held-out IEEE-CIS records as the live input stream (replaces the retired synthetic generator).

Input is a *replay pack* built by ``ml/training/build_replay_pack.py`` from the chronological TEST split:
``records`` (raw fs-1.1 input fields + ``source_ref`` + ``source_dt_s`` + pseudo-account ``account_id``) and
``selections`` (index lists). Fraud labels live in a separate file that this module never reads.

Time mapping (documented in docs/apache-streaming.md):
* ``event_time`` = replay time = run epoch + event offset. Base records are spaced uniformly at the
  configured rate (1000 / rate_eps ms apart), in source TransactionDT order. Source time is carried unchanged
  in ``source_dt_s`` and is never used for watermarks, because watermarks are operator-wide and replaying
  months of source time into one run would make every later run late.
* ``arrival_delay_ms`` shifts *arrival* (produce time) only; delayed records keep their earlier event_time and
  are therefore late (beyond the 5 s bound) or out of order (within it). Feature values are unchanged.
* Window features (counts / sums per pseudo-account per hour) are computed on replay time, so they differ from
  the offline fs-1.1 features computed on source time.

``modified`` marks events whose model inputs differ from the unaltered replay (pseudo-account bursts compress
a pseudo-account's records in time, changing its window features). Modified events are excluded from any
label-based evaluation.

Determinism: for a given pack, seed and control log (rate changes / injections keyed by base index) the
sequence of records, delivery tags and relative timings is identical, so Flink-only and Spark-only runs with the
same seed replay the same records.
"""
from __future__ import annotations

import hashlib
import heapq
import random
from dataclasses import dataclass, field

from .schema import SCENARIOS

PATTERNS = ("selected_fraud_example", "late_delivery", "duplicate_delivery", "pseudo_account_burst")
MODIFYING_PATTERNS = ("pseudo_account_burst",)
LATE_DELAY_RANGE_MS = (15_000, 30_000)      # well beyond the 5 s bounded out-of-orderness
OUT_OF_ORDER_RANGE_MS = (1_000, 4_000)      # within the bound: out of order but not late
RECORD_FIELDS = ("source_ref", "source_dt_s", "account_id", "amount", "product_cd", "card_network", "card_type",
                 "email_domain", "device_type", "has_identity")
REPLAY_LABEL = "Replay of historical IEEE-CIS records (not a live bank feed)"

SCENARIO_TEXT = {
    "replay": "Held-out IEEE-CIS test-split records replayed in source order at the chosen rate",
    "selected_examples": "Selected examples: records chosen with verified source labels (fraud and ordinary), "
                         "not a representative sample",
    "late_events": "Unaltered replay; 10% of records delivered 15-30 s late and 10% out of order within the 5 s bound",
    "duplicates": "Unaltered replay; some records delivered twice (at-least-once redelivery)",
    "account_burst": "Modified IEEE-CIS demo scenario: a pseudo-account's records replayed close together every ~15 s",
}
PATTERN_TEXT = {
    "selected_fraud_example": "Replay one record labelled fraud in the source data (selected example; the score is "
                              "whatever the model produces)",
    "late_delivery": "Deliver the next 5 records 15-30 s after their replay event time (transport only)",
    "duplicate_delivery": "Deliver the next 3 records twice (transport only; results are de-duplicated)",
    "pseudo_account_burst": "Modified IEEE-CIS demo scenario: replay one pseudo-account's records seconds apart",
}


def _rng(*parts: object) -> random.Random:
    digest = hashlib.sha256(":".join(str(p) for p in parts).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


@dataclass
class ScenarioConfig:
    scenario: str = "replay"
    seed: int = 20260917
    rate_eps: float = 5.0
    late_fraction: float = 0.0
    out_of_order_fraction: float = 0.0
    burst_every_s: float = 0.0

    @classmethod
    def for_scenario(cls, scenario: str, seed: int, rate_eps: float) -> "ScenarioConfig":
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario}")
        cfg = cls(scenario=scenario, seed=seed, rate_eps=rate_eps)
        if scenario == "late_events":
            cfg.late_fraction, cfg.out_of_order_fraction = 0.10, 0.10
        elif scenario == "account_burst":
            cfg.burst_every_s = 15.0
        return cfg


@dataclass(order=True)
class Planned:
    arrival_offset_ms: float              # when the generator should produce it (heap key)
    order: int
    event_offset_ms: float = field(compare=False)  # replay event time relative to run start
    body: dict = field(compare=False)


class ScenarioStream:
    """Produces planned replay events ordered by virtual arrival offset (ms since run start, pauses excluded)."""

    def __init__(self, cfg: ScenarioConfig, pack: dict, demo_session_id: str):
        self.cfg = cfg
        self.session = demo_session_id
        self.rate_eps = cfg.rate_eps
        self.records: list[dict] = pack["records"]
        sel = pack["selections"]
        rng = _rng(cfg.seed, "order")
        if cfg.scenario == "selected_examples":
            order = list(sel["selected_fraud"]) + list(sel["selected_ordinary"])
            rng.shuffle(order)
        else:
            contiguous = list(sel["contiguous"])
            start = rng.randrange(len(contiguous))       # seed picks the start position; source order is kept
            order = contiguous[start:] + contiguous[:start]
        self.order_idx = order
        self.fraud_examples = list(sel["selected_fraud"])
        self.bursts = [list(b) for b in sel["pseudo_account_bursts"]]
        self.base_rng = _rng(cfg.seed, "base")
        self.heap: list[Planned] = []
        self.order = 0
        self.base_index = 0
        self.next_base_offset = 0.0
        self.injections = 0
        self.last_burst_ms = 0.0
        self.control_log: list[dict] = []
        self.late_pending = 0          # next N base records delivered late (late_delivery injection)
        self.redeliver_pending = 0     # next N produced records redelivered (duplicate_delivery injection)

    # -- controls -------------------------------------------------------------------
    def set_rate(self, rate_eps: float) -> None:
        self.rate_eps = rate_eps
        self.control_log.append({"at_base_index": self.base_index, "action": "rate", "value": rate_eps})

    def inject(self, pattern: str, now_offset_ms: float) -> dict:
        if pattern not in PATTERNS:
            raise ValueError(f"unknown pattern {pattern}")
        self.control_log.append({"at_base_index": self.base_index, "action": "inject", "value": pattern})
        return self._schedule_pattern(pattern, now_offset_ms)

    # -- generation -------------------------------------------------------------------
    def next(self) -> Planned:
        """Next event in arrival order (every later plan has arrival offset >= next_base_offset)."""
        while not self.heap or self.heap[0].arrival_offset_ms > self.next_base_offset:
            self._plan_base()
        return heapq.heappop(self.heap)

    def _push(self, offset_ms: float, body: dict) -> None:
        self.order += 1
        heapq.heappush(self.heap, Planned(offset_ms + body["arrival_delay_ms"], self.order, offset_ms, body))

    def _plan_base(self) -> None:
        cfg, rng = self.cfg, self.base_rng
        off = self.next_base_offset
        rec = self.records[self.order_idx[self.base_index % len(self.order_idx)]]
        delay, tag = 0, None
        r = rng.random()
        if self.late_pending > 0:
            self.late_pending -= 1
            delay, tag = rng.randint(*LATE_DELAY_RANGE_MS), "late_delivery"
        elif r < cfg.late_fraction:
            delay, tag = rng.randint(*LATE_DELAY_RANGE_MS), "late_delivery"
        elif r < cfg.late_fraction + cfg.out_of_order_fraction:
            delay, tag = rng.randint(*OUT_OF_ORDER_RANGE_MS), "out_of_order"
        pattern = "selected_example" if cfg.scenario == "selected_examples" and tag is None else tag
        self._push(off, self._body(rec, pattern, injected=tag is not None, delay=delay))
        self.base_index += 1
        self.next_base_offset = off + 1000.0 / max(self.rate_eps, 0.1)
        if cfg.burst_every_s and off - self.last_burst_ms >= cfg.burst_every_s * 1000:
            self.last_burst_ms = off
            self._schedule_pattern("pseudo_account_burst", off)

    def _schedule_pattern(self, pattern: str, start: float) -> dict:
        self.injections += 1
        rng = _rng(self.cfg.seed, "pattern", self.injections)
        planned, account = 0, None
        if pattern == "selected_fraud_example":
            rec = self.records[rng.choice(self.fraud_examples)]
            self._push(start + 500, self._body(rec, pattern, injected=True))
            planned, account = 1, rec["account_id"]
        elif pattern == "late_delivery":
            self.late_pending += 5
            planned = 5
        elif pattern == "duplicate_delivery":
            self.redeliver_pending += 3
            planned = 3
        elif pattern == "pseudo_account_burst":
            burst = self.bursts[rng.randrange(len(self.bursts))]
            t = start + 500
            for i in burst:
                rec = self.records[i]
                self._push(t, self._body(rec, pattern, injected=True, modified=True))
                t += rng.randint(3_000, 7_000)
                planned += 1
                account = rec["account_id"]
        return {"pattern": pattern, "account_id": account, "events": planned, "injection": self.injections,
                "modified": pattern in MODIFYING_PATTERNS}

    def take_redelivery(self) -> bool:
        if self.redeliver_pending > 0:
            self.redeliver_pending -= 1
            return True
        return False

    def _body(self, rec: dict, pattern: str | None, injected: bool, delay: int = 0, modified: bool = False) -> dict:
        return {**{k: rec[k] for k in RECORD_FIELDS}, "scenario": self.cfg.scenario, "pattern": pattern,
                "injected": injected, "modified": modified, "arrival_delay_ms": delay}
