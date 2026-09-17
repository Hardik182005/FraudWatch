"""Seeded, reproducible demo transaction stream (synthetic, for the live demo only).

The stream is *illustrative*: account attributes and amount scales are sampled from
marginals of the IEEE-CIS training split (``generator_profile.json``), while merchants,
cities and account identifiers are synthetic enrichment and are never model inputs.
Held-out model evaluation uses the IEEE-CIS test split, never this generator.

Determinism: for a given ``seed`` and control log (rate changes / injections keyed by
the base-event index at which they were applied) the sequence of accounts, amounts,
patterns and relative timings is identical. Absolute timestamps are wall-clock.
"""
from __future__ import annotations

import hashlib
import heapq
import math
import random
from dataclasses import dataclass, field

from .schema import SCENARIOS

MERCHANTS = [  # fictional sample merchants (synthetic enrichment)
    ("m_001", "Aurora Grocers (sample)", "grocery"),
    ("m_002", "Blue Lotus Pharmacy (sample)", "pharmacy"),
    ("m_003", "Cedar Fuel Stop (sample)", "fuel"),
    ("m_004", "Deccan Electronics (sample)", "electronics"),
    ("m_005", "Everest Travel Desk (sample)", "travel"),
    ("m_006", "Fern Cafe (sample)", "dining"),
    ("m_007", "Granite Hardware (sample)", "home"),
    ("m_008", "Harbor Apparel (sample)", "apparel"),
    ("m_009", "Indigo Mobile Recharge (sample)", "digital"),
    ("m_010", "Jade Streaming Plus (sample)", "digital"),
    ("m_011", "Kestrel Gift Cards (sample)", "gift_cards"),
    ("m_012", "Lumen Online Mart (sample)", "marketplace"),
]
CITIES = ["Mumbai", "Pune", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad", "Jaipur", "Kochi"]
PATTERNS = ("burst", "profile_deviation", "card_testing", "late_burst")
LATE_DELAY_RANGE_MS = (15_000, 30_000)      # well beyond the 5 s bounded out-of-orderness
OUT_OF_ORDER_RANGE_MS = (1_000, 4_000)      # within the bound: out of order but not late


def _rng(*parts: object) -> random.Random:
    digest = hashlib.sha256(":".join(str(p) for p in parts).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


@dataclass
class ScenarioConfig:
    scenario: str = "normal"
    seed: int = 20260917
    rate_eps: float = 5.0
    n_accounts: int = 4000
    # 4000 accounts with a flat-ish Zipf(0.4) keep ordinary traffic sparse per account at
    # 5 eps, so base-traffic flag rates stay close to the validation flag rate.
    activity_exponent: float = 0.4
    late_fraction: float = 0.0            # share of base events delivered late (late_events / mixed)
    out_of_order_fraction: float = 0.0    # share delivered out of order within the bound
    pattern_every_s: float = 0.0          # automatic pattern injection period (0 = off)

    @classmethod
    def for_scenario(cls, scenario: str, seed: int, rate_eps: float) -> "ScenarioConfig":
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario}")
        cfg = cls(scenario=scenario, seed=seed, rate_eps=rate_eps)
        if scenario == "late_events":
            cfg.late_fraction, cfg.out_of_order_fraction = 0.10, 0.10
        elif scenario == "mixed":
            cfg.late_fraction, cfg.out_of_order_fraction, cfg.pattern_every_s = 0.03, 0.05, 20.0
        elif scenario in ("burst", "profile_deviation", "card_testing"):
            cfg.pattern_every_s = 15.0
        return cfg


@dataclass(order=True)
class Planned:
    arrival_offset_ms: float              # when the generator should produce it (heap key)
    order: int
    event_offset_ms: float = field(compare=False)  # event time relative to run start
    body: dict = field(compare=False)


@dataclass
class Account:
    account_id: str
    attrs: dict
    median_amount: float
    home_city: str
    favourite_merchants: list


class ScenarioStream:
    """Produces planned events ordered by virtual offset (ms since run start, pauses excluded)."""

    def __init__(self, cfg: ScenarioConfig, profile: dict, demo_session_id: str):
        self.cfg = cfg
        self.session = demo_session_id
        self.rate_eps = cfg.rate_eps
        rng = _rng(cfg.seed, "accounts")
        combos = profile["attribute_combos"]
        weights = [c["weight"] for c in combos]
        q = profile["account_median_amount_inr_quantiles"]
        qs = sorted((float(k), v) for k, v in q.items())
        self.accounts: list[Account] = []
        for i in range(cfg.n_accounts):
            c = rng.choices(combos, weights=weights)[0]
            attrs = {k: c[k] for k in ("product_cd", "card_network", "card_type", "email_domain", "device_type", "has_identity")}
            u = rng.uniform(qs[0][0], qs[-1][0])
            median = _interp(qs, u)
            aid = "acc_" + hashlib.sha256(f"{cfg.seed}:{i}".encode()).hexdigest()[:8]
            favs = rng.sample(MERCHANTS[:10], 3)
            self.accounts.append(Account(aid, attrs, median, rng.choice(CITIES), favs))
        # Zipf-like activity weights: a few busy accounts, many quiet ones.
        self.activity = [1.0 / (i + 1) ** cfg.activity_exponent for i in range(cfg.n_accounts)]
        self.base_rng = _rng(cfg.seed, "base")
        self.heap: list[Planned] = []
        self.order = 0
        self.base_index = 0
        self.next_base_offset = 0.0
        self.injections = 0
        self.last_auto_pattern_ms = 0.0
        self.control_log: list[dict] = []
        self.history_len: dict[str, int] = {}

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
        """Next event in *arrival* order. Every event planned later has event offset (and
        therefore arrival offset) >= next_base_offset, so popping below it is safe."""
        while not self.heap or self.heap[0].arrival_offset_ms > self.next_base_offset:
            self._plan_base()
        return heapq.heappop(self.heap)

    def _push(self, offset_ms: float, body: dict) -> None:
        self.order += 1
        arrival = offset_ms + body["arrival_delay_ms"]
        heapq.heappush(self.heap, Planned(arrival, self.order, offset_ms, body))

    def _plan_base(self) -> None:
        cfg, rng = self.cfg, self.base_rng
        off = self.next_base_offset
        acct = rng.choices(self.accounts, weights=self.activity)[0]
        amount = _amount(rng, acct.median_amount)
        delay, tag = 0, None
        r = rng.random()
        if r < cfg.late_fraction:
            delay, tag = rng.randint(*LATE_DELAY_RANGE_MS), "late_delayed"
        elif r < cfg.late_fraction + cfg.out_of_order_fraction:
            delay, tag = rng.randint(*OUT_OF_ORDER_RANGE_MS), "out_of_order"
        self._push(off, self._body(acct, amount, rng, pattern=tag, injected=tag is not None, delay=delay))
        self.base_index += 1
        self.next_base_offset = off + 1000.0 / max(self.rate_eps, 0.1)
        if cfg.pattern_every_s and off - self.last_auto_pattern_ms >= cfg.pattern_every_s * 1000:
            self.last_auto_pattern_ms = off
            pattern = cfg.scenario if cfg.scenario in PATTERNS else PATTERNS[self.injections % 3]
            self._schedule_pattern(pattern, off)

    def _schedule_pattern(self, pattern: str, start: float) -> dict:
        self.injections += 1
        rng = _rng(self.cfg.seed, "pattern", self.injections)
        acct = rng.choice(self.accounts)
        planned = 0
        if pattern == "burst":
            t = start + 500
            for _ in range(rng.randint(6, 8)):
                self._push(t, self._body(acct, _amount(rng, acct.median_amount), rng, pattern, True))
                t += rng.randint(3_000, 7_000)
                planned += 1
        elif pattern == "profile_deviation":
            t = start + 500
            for _ in range(6):  # establish history (spaced to avoid the burst rule)
                self._push(t, self._body(acct, _amount(rng, acct.median_amount), rng, "profile_history", True))
                t += rng.randint(16_000, 20_000)
                planned += 1
            big = max(acct.median_amount * rng.uniform(12, 25), rng.uniform(6_000, 15_000))
            self._push(t, self._body(acct, round(big, 2), rng, pattern, True, merchant=MERCHANTS[3]))
            planned += 1
        elif pattern == "card_testing":
            t = start + 500
            for _ in range(rng.randint(4, 5)):
                self._push(t, self._body(acct, round(rng.uniform(15, 150), 2), rng, "card_testing_probe", True,
                                         merchant=MERCHANTS[9]))
                t += rng.randint(18_000, 35_000)
                planned += 1
            self._push(t + rng.randint(10_000, 40_000),
                       self._body(acct, round(rng.uniform(4_000, 25_000), 2), rng, pattern, True, merchant=MERCHANTS[10]))
            planned += 1
        elif pattern == "late_burst":
            t = start + 500
            for _ in range(5):
                self._push(t, self._body(acct, _amount(rng, acct.median_amount), rng, "late_delayed", True,
                                         delay=rng.randint(*LATE_DELAY_RANGE_MS)))
                t += 1_000
                planned += 1
        return {"pattern": pattern, "account_id": acct.account_id, "events": planned, "injection": self.injections}

    def _body(self, acct: Account, amount: float, rng: random.Random, pattern: str | None, injected: bool,
              delay: int = 0, merchant: tuple | None = None) -> dict:
        m = merchant or (rng.choice(acct.favourite_merchants) if rng.random() < 0.7 else rng.choice(MERCHANTS[:10]))
        return {
            "account_id": acct.account_id,
            "amount_inr": amount,
            **acct.attrs,
            "merchant_id": m[0],
            "merchant_name": m[1],
            "merchant_category": m[2],
            "city": acct.home_city,
            "scenario": self.cfg.scenario,
            "pattern": pattern,
            "injected": injected,
            "arrival_delay_ms": delay,
        }


def _interp(qs: list[tuple[float, float]], u: float) -> float:
    for (q0, v0), (q1, v1) in zip(qs, qs[1:]):
        if q0 <= u <= q1:
            # interpolate in log space: amounts are right-skewed
            w = (u - q0) / (q1 - q0) if q1 > q0 else 0
            return math.exp(math.log(v0) + w * (math.log(v1) - math.log(v0)))
    return qs[-1][1]


def _amount(rng: random.Random, median: float) -> float:
    return round(max(10.0, median * math.exp(rng.gauss(0, 0.5))), 2)
