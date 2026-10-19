import { Card, Segmented, StatusPill, TabPanel, Tabs, formatMs } from '@fraudwatch/ui';
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { useLocation } from 'react-router-dom';
import { HEADLINE } from './measured';

/**
 * Beyond-the-syllabus stream-processing labs. Every figure here is computed in the browser from the
 * controls; they are labelled as simulations and never presented as FraudWatch measurements.
 */

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function hash32(n: number) {
  let h = Math.imul(n ^ 0x9e3779b9, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

function Slider({ label, value, min, max, step = 1, onChange, fmt = String }: { label: string; value: number; min: number; max: number; step?: number; onChange: (v: number) => void; fmt?: (v: number) => string }) {
  return (
    <label className="fw-slider">
      <span>{label}<b>{fmt(value)}</b></span>
      <input type="range" min={min} max={max} step={step} value={value} onChange={(e) => onChange(Number(e.target.value))} />
    </label>
  );
}

function Fact({ v, l, tone }: { v: ReactNode; l: string; tone?: 'ok' | 'warn' | 'bad' }) {
  return <div className="fw-mfact" data-tone={tone}><strong>{v}</strong><span>{l}</span></div>;
}

function Legend({ items }: { items: Array<{ c: string; l: string; ring?: boolean }> }) {
  return (
    <div className="fw-legend">
      {items.map((i) => <span key={i.l}><i style={i.ring ? { border: `2px solid ${i.c}` } : { background: i.c }} />{i.l}</span>)}
    </div>
  );
}

const s = (v: number) => `${v} s`;

/* ---------- 1. Watermarks and allowed lateness ---------- */

type Late = 'ontime' | 'late' | 'dropped';
const LATE_COLOR: Record<Late, string> = { ontime: 'var(--chart-1)', late: 'var(--degraded)', dropped: 'var(--down)' };

function WatermarkLab() {
  const [bound, setBound] = useState(3);
  const [lateness, setLateness] = useState(0);
  const [stragglers, setStragglers] = useState(15);
  const [seed, setSeed] = useState(7);
  const WIN = 10;

  const sim = useMemo(() => {
    const r = mulberry32(seed);
    const evs = Array.from({ length: 40 }, (_, i) => {
      const t = r() * 60;
      const straggle = r() * 100 < stragglers;
      const delay = straggle ? 5 + r() * 20 : -0.8 * Math.log(1 - r());
      return { i, t, a: t + delay, cls: 'ontime' as Late };
    }).sort((x, y) => x.a - y.a);
    let maxT = -Infinity;
    const steps: Array<[number, number]> = [];
    const emitted = new Map<number, number>();
    for (const e of evs) {
      const wm = maxT - bound;
      const end = (Math.floor(e.t / WIN) + 1) * WIN;
      e.cls = end > wm ? 'ontime' : end + lateness > wm ? 'late' : 'dropped';
      maxT = Math.max(maxT, e.t);
      const wmAfter = maxT - bound;
      steps.push([e.a, wmAfter]);
      for (let k = 0; k < 6; k++) if (!emitted.has(k) && wmAfter >= (k + 1) * WIN) emitted.set(k, e.a - (k + 1) * WIN);
    }
    const delays = [...emitted.values()];
    return {
      evs,
      steps,
      count: (c: Late) => evs.filter((e) => e.cls === c).length,
      emitDelay: delays.length ? delays.reduce((a, b) => a + b, 0) / delays.length : null,
      pending: 6 - emitted.size,
    };
  }, [bound, lateness, stragglers, seed]);

  const W = 640, H = 260, P = 34;
  const maxA = Math.max(65, ...sim.evs.map((e) => e.a));
  const x = (a: number) => P + (a / maxA) * (W - P - 10);
  const y = (t: number) => H - P - (t / 60) * (H - P - 10);
  const wmPath = sim.steps.map(([a, wm], i) => `${i ? 'L' : 'M'}${x(a).toFixed(1)},${y(Math.max(0, Math.min(60, wm))).toFixed(1)}`).join(' ');

  return (
    <div className="fw-stack" style={{ gap: 14 }}>
      <div className="fw-beyond-controls">
        <Slider label="Out-of-orderness bound" value={bound} min={0} max={12} onChange={setBound} fmt={s} />
        <Slider label="Allowed lateness" value={lateness} min={0} max={20} onChange={setLateness} fmt={s} />
        <Slider label="Stragglers (5–25 s delay)" value={stragglers} min={0} max={50} onChange={setStragglers} fmt={(v) => `${v}%`} />
        <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={() => setSeed((v) => v + 1)}>New stream</button>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="fw-sim-svg" role="img" aria-label="Event time against processing time with the watermark">
        {[0, 1, 2, 3, 4, 5].map((k) => <rect key={k} x={P} width={W - P - 10} y={y((k + 1) * WIN)} height={y(k * WIN) - y((k + 1) * WIN)} fill={k % 2 ? 'var(--surface-2)' : 'transparent'} />)}
        <line x1={x(0)} y1={y(0)} x2={x(60)} y2={y(60)} stroke="var(--text-subtle)" strokeDasharray="4 4" />
        <path d={wmPath} fill="none" stroke="var(--chart-4)" strokeWidth={2} />
        {sim.evs.map((e) => <circle key={e.i} cx={x(e.a)} cy={y(e.t)} r={4.5} fill={LATE_COLOR[e.cls]} />)}
        <text x={P} y={H - 8} className="fw-sim-axis">processing time →</text>
        <text x={6} y={14} className="fw-sim-axis">event time ↑ (10 s windows)</text>
      </svg>
      <Legend items={[{ c: LATE_COLOR.ontime, l: 'On time' }, { c: LATE_COLOR.late, l: 'Late, window re-fires' }, { c: LATE_COLOR.dropped, l: 'Too late → side output' }, { c: 'var(--chart-4)', l: 'Watermark' }]} />
      <div className="fw-beyond-stats">
        <Fact v={sim.count('ontime')} l="on time" tone="ok" />
        <Fact v={sim.count('late')} l="late, accepted" tone="warn" />
        <Fact v={sim.count('dropped')} l="side output" tone={sim.count('dropped') ? 'bad' : 'ok'} />
        <Fact v={sim.emitDelay == null ? '—' : `${sim.emitDelay.toFixed(1)} s`} l={`avg result delay${sim.pending ? ` · ${sim.pending} windows waiting` : ''}`} />
      </div>
      <p className="fw-small fw-muted">Bigger bound → fewer late events, slower results. FraudWatch runs Flink with a 5 s bound.</p>
    </div>
  );
}

/* ---------- 2. Window types ---------- */

const ACCOUNT_EVENTS = [3, 9, 21, 24, 25, 26, 27, 41, 58, 61, 62, 79, 96, 111];
type WinKind = 'tumbling' | 'sliding' | 'session';

function WindowLab() {
  const [kind, setKind] = useState<WinKind>('sliding');
  const [size, setSize] = useState(20);
  const [slide, setSlide] = useState(10);
  const [gap, setGap] = useState(10);
  const [n, setN] = useState(4);
  const span = 120;
  const effSlide = Math.max(slide, Math.ceil(size / 6));

  const wins = useMemo(() => {
    const out: Array<{ start: number; end: number; row: number }> = [];
    if (kind === 'tumbling') for (let st = 0; st < span; st += size) out.push({ start: st, end: st + size, row: 0 });
    else if (kind === 'sliding') {
      const rows = Math.ceil(size / effSlide);
      for (let j = -(rows - 1); j * effSlide < span; j++) out.push({ start: j * effSlide, end: j * effSlide + size, row: ((j % rows) + rows) % rows });
    } else {
      let st = ACCOUNT_EVENTS[0]!, last = st;
      for (const t of ACCOUNT_EVENTS.slice(1)) {
        if (t - last >= gap) { out.push({ start: st, end: last + gap, row: 0 }); st = t; }
        last = t;
      }
      out.push({ start: st, end: last + gap, row: 0 });
    }
    return out.map((w) => ({ ...w, count: ACCOUNT_EVENTS.filter((t) => t >= w.start && t < w.end).length }));
  }, [kind, size, effSlide, gap]);

  const rows = Math.max(1, ...wins.map((w) => w.row + 1));
  const W = 640, P = 10, top = 34, RH = 24, H = top + rows * RH + 22;
  const x = (t: number) => P + (Math.max(0, Math.min(span, t)) / span) * (W - 2 * P);
  const alerts = wins.filter((w) => w.count >= n).length;
  const stateCopies = wins.reduce((a, w) => a + w.count, 0);

  return (
    <div className="fw-stack" style={{ gap: 14 }}>
      <div className="fw-beyond-controls">
        <Segmented<WinKind> label="Window type" value={kind} onChange={setKind} options={[{ value: 'tumbling', label: 'Tumbling' }, { value: 'sliding', label: 'Sliding' }, { value: 'session', label: 'Session' }]} />
        {kind !== 'session' && <Slider label="Size" value={size} min={10} max={60} step={5} onChange={setSize} fmt={s} />}
        {kind === 'sliding' && <Slider label="Slide" value={effSlide} min={5} max={size} step={5} onChange={setSlide} fmt={s} />}
        {kind === 'session' && <Slider label="Inactivity gap" value={gap} min={3} max={30} onChange={setGap} fmt={s} />}
        <Slider label="Burst rule: alert at ≥" value={n} min={2} max={6} onChange={setN} fmt={(v) => `${v} payments`} />
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="fw-sim-svg" role="img" aria-label={`${kind} windows over one account's payments`}>
        <line x1={x(0)} x2={x(span)} y1={18} y2={18} stroke="var(--border-strong)" />
        {ACCOUNT_EVENTS.map((t) => <circle key={t} cx={x(t)} cy={18} r={5} fill="var(--chart-1)" />)}
        {wins.map((w, i) => {
          const hot = w.count >= n;
          const x1 = x(w.start), x2 = x(w.end);
          if (x2 - x1 < 1) return null;
          return (
            <g key={i}>
              <rect x={x1 + 1} y={top + w.row * RH} width={x2 - x1 - 2} height={RH - 5} rx={4} fill={hot ? 'var(--alert-bg)' : 'var(--surface-2)'} stroke={hot ? 'var(--down)' : 'var(--border)'} />
              {x2 - x1 > 18 && <text x={(x1 + x2) / 2} y={top + w.row * RH + 13} textAnchor="middle" className="fw-sim-label">{w.count}</text>}
            </g>
          );
        })}
        <text x={P} y={H - 4} className="fw-sim-axis">0 s</text>
        <text x={W - P} y={H - 4} textAnchor="end" className="fw-sim-axis">{span} s</text>
      </svg>
      <div className="fw-beyond-stats">
        <Fact v={wins.length} l="windows" />
        <Fact v={alerts} l="burst alerts" tone={alerts ? 'bad' : 'ok'} />
        <Fact v={`${(stateCopies / ACCOUNT_EVENTS.length).toFixed(1)}×`} l="state copies per event" tone={stateCopies / ACCOUNT_EVENTS.length > 2 ? 'warn' : undefined} />
      </div>
      <p className="fw-small fw-muted">Same 14 payments, different windows, different alerts. Sliding windows catch bursts that tumbling edges split, but each event is stored once per overlapping window.</p>
    </div>
  );
}

/* ---------- 3. Micro-batch latency and backpressure ---------- */

function model(rate: number, trigger: number, overhead: number, cost: number, base: number) {
  const rhoF = (rate * cost) / 1000;
  const waitF = rhoF < 1 ? (rhoF * cost) / (2 * (1 - rhoF)) : Infinity;
  const proc = overhead + (cost * rate * trigger) / 1000;
  const rhoS = proc / trigger;
  return {
    flink: { p50: base + cost + waitF, p95: base + cost + 3 * waitF, rho: rhoF },
    spark: { p50: rhoS < 1 ? base + trigger / 2 + proc : Infinity, p95: rhoS < 1 ? base + 0.95 * trigger + proc : Infinity, rho: rhoS, proc },
  };
}

function MicroBatchLab() {
  const [rate, setRate] = useState(10);
  const [trigger, setTrigger] = useState(1000);
  const [overhead, setOverhead] = useState(300);
  const [cost, setCost] = useState(2);
  const base = 90;
  const m = model(rate, trigger, overhead, cost, base);
  const k = 1 - (cost * rate) / 1000;
  const minTrigger = k > 0 ? overhead / k : Infinity;

  const W = 640, H = 220, P = 44, TMAX = 5000;
  const curve = useMemo(() => {
    const pts: string[] = [];
    let yMax = 1;
    const vals: Array<[number, number]> = [];
    for (let t = 100; t <= TMAX; t += 50) {
      const v = model(rate, t, overhead, cost, base).spark.p50;
      if (Number.isFinite(v)) { vals.push([t, v]); yMax = Math.max(yMax, v); }
    }
    return { vals, yMax, pts };
  }, [rate, overhead, cost]);
  const yMax = Math.max(curve.yMax, Number.isFinite(m.flink.p50) ? m.flink.p50 : 0) * 1.1;
  const x = (t: number) => P + (t / TMAX) * (W - P - 10);
  const y = (v: number) => H - 26 - (Math.min(v, yMax) / yMax) * (H - 40);
  const sparkPath = curve.vals.map(([t, v], i) => `${i ? 'L' : 'M'}${x(t).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
  const fmt = (v: number) => (Number.isFinite(v) ? formatMs(v) : 'falls behind');
  const shade = Math.min(TMAX, minTrigger);

  return (
    <div className="fw-stack" style={{ gap: 14 }}>
      <div className="fw-beyond-controls">
        <Slider label="Arrival rate" value={rate} min={1} max={400} onChange={setRate} fmt={(v) => `${v} ev/s`} />
        <Slider label="Spark trigger" value={trigger} min={100} max={TMAX} step={50} onChange={setTrigger} fmt={(v) => formatMs(v)} />
        <Slider label="Per-batch overhead" value={overhead} min={0} max={2000} step={25} onChange={setOverhead} fmt={(v) => formatMs(v)} />
        <Slider label="Per-record cost" value={cost} min={0.1} max={10} step={0.1} onChange={setCost} fmt={(v) => `${v.toFixed(1)} ms`} />
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="fw-sim-svg" role="img" aria-label="Modelled p50 latency against Spark trigger interval">
        {shade > 100 && <rect x={x(0)} y={10} width={x(shade) - x(0)} height={H - 36} fill="var(--alert-bg)" />}
        {shade > 300 && <text x={x(shade) - 6} y={24} textAnchor="end" className="fw-sim-axis" fill="var(--down)">unstable: batch takes longer than trigger</text>}
        <line x1={P} x2={W - 10} y1={H - 26} y2={H - 26} stroke="var(--border-strong)" />
        <path d={sparkPath} fill="none" stroke="var(--chart-4)" strokeWidth={2} />
        {Number.isFinite(m.flink.p50) && <line x1={P} x2={W - 10} y1={y(m.flink.p50)} y2={y(m.flink.p50)} stroke="var(--chart-1)" strokeWidth={2} />}
        <line x1={x(trigger)} x2={x(trigger)} y1={10} y2={H - 26} stroke="var(--text-subtle)" strokeDasharray="3 3" />
        <text x={P} y={H - 8} className="fw-sim-axis">trigger 0</text>
        <text x={W - 10} y={H - 8} textAnchor="end" className="fw-sim-axis">5 s</text>
        <text x={4} y={16} className="fw-sim-axis">p50</text>
      </svg>
      <Legend items={[{ c: 'var(--chart-1)', l: 'Flink, per event' }, { c: 'var(--chart-4)', l: 'Spark, by trigger' }, { c: 'var(--down)', l: 'Unstable zone', ring: true }]} />
      <div className="fw-beyond-stats">
        <Fact v={fmt(m.flink.p50)} l={`Flink p50 · p95 ${fmt(m.flink.p95)}`} tone={Number.isFinite(m.flink.p50) ? 'ok' : 'bad'} />
        <Fact v={fmt(m.spark.p50)} l={`Spark p50 · p95 ${fmt(m.spark.p95)}`} tone={Number.isFinite(m.spark.p50) ? undefined : 'bad'} />
        <Fact v={`${Math.round(m.spark.rho * 100)}%`} l="Spark batch busy time" tone={m.spark.rho >= 1 ? 'bad' : m.spark.rho > 0.8 ? 'warn' : 'ok'} />
        <Fact v={Number.isFinite(minTrigger) ? formatMs(minTrigger) : 'none'} l="lowest stable trigger" />
        <Fact v={Number.isFinite(m.spark.p50) ? Math.round((rate * m.spark.p50) / 1000) : '∞'} l="Spark records in flight (Little's law)" />
      </div>
      <p className="fw-small fw-muted">
        Toy queueing model (M/D/1 for Flink, wait + batch for Spark), not a measurement. Measured on GCP at 10 ev/s, 1 s trigger: Flink {formatMs(HEADLINE.flink.p50)}, Spark {formatMs(HEADLINE.spark.p50)}.
      </p>
    </div>
  );
}

/* ---------- 4. Delivery guarantees across a crash ---------- */

type Mode = 'amo' | 'alo' | 'eo';
const N_EVENTS = 12;

function DeliveryLab() {
  const [mode, setMode] = useState<Mode>('eo');
  const [every, setEvery] = useState(4);
  const [crash, setCrash] = useState(7);

  const sim = useMemo(() => {
    const ckpt = Math.floor(crash / every) * every;
    const writes: Array<{ id: number; dup: boolean; replay: boolean }> = [];
    let lost: number[] = [];
    if (mode === 'amo') {
      const committed = Math.min(N_EVENTS, Math.ceil(crash / every) * every);
      for (let i = 1; i < crash; i++) writes.push({ id: i, dup: false, replay: false });
      lost = Array.from({ length: committed - crash + 1 }, (_, j) => crash + j);
      for (let i = committed + 1; i <= N_EVENTS; i++) writes.push({ id: i, dup: false, replay: false });
    } else {
      for (let i = 1; i <= crash; i++) writes.push({ id: i, dup: false, replay: false });
      for (let i = ckpt + 1; i <= N_EVENTS; i++) {
        const seen = writes.some((w) => w.id === i);
        if (mode === 'eo' && seen) continue;
        writes.push({ id: i, dup: seen, replay: i <= crash });
      }
    }
    const replayed = mode === 'amo' ? 0 : crash - ckpt;
    return { ckpt, writes, lost, replayed, dups: writes.filter((w) => w.dup).length, rows: new Set(writes.map((w) => w.id)).size };
  }, [mode, every, crash]);

  return (
    <div className="fw-stack" style={{ gap: 14 }}>
      <div className="fw-beyond-controls">
        <Segmented<Mode> label="Guarantee" value={mode} onChange={setMode} options={[{ value: 'amo', label: 'At-most-once' }, { value: 'alo', label: 'At-least-once' }, { value: 'eo', label: 'Exactly-once*' }]} />
        <Slider label="Checkpoint every" value={every} min={2} max={6} onChange={setEvery} fmt={(v) => `${v} events`} />
        <Slider label="Crash after event" value={crash} min={1} max={N_EVENTS} onChange={setCrash} fmt={(v) => `#${v}`} />
      </div>
      <div>
        <div className="fw-small fw-muted" style={{ marginBottom: 6 }}>Kafka log</div>
        <div className="fw-log">
          {Array.from({ length: N_EVENTS }, (_, j) => j + 1).map((i) => (
            <span key={i} className="fw-log-cell" data-ckpt={i % every === 0 && (mode !== 'amo') ? '' : undefined} data-crash={i === crash ? '' : undefined}>
              {i}
            </span>
          ))}
        </div>
      </div>
      <div>
        <div className="fw-small fw-muted" style={{ marginBottom: 6 }}>Sink, in write order</div>
        <div className="fw-log" data-testid="delivery-sink">
          {sim.writes.map((w, j) => (
            <span key={j} className="fw-log-cell" data-state={w.dup ? 'dup' : w.replay ? 'replay' : 'ok'}>{w.id}</span>
          ))}
          {sim.lost.map((i) => <span key={`l${i}`} className="fw-log-cell" data-state="lost">{i}</span>)}
        </div>
      </div>
      <Legend items={[{ c: 'var(--chart-1)', l: 'Written once' }, { c: 'var(--degraded)', l: 'Replayed' }, { c: 'var(--down)', l: 'Duplicate row' }, { c: 'var(--down)', l: 'Lost', ring: true }, { c: 'var(--chart-4)', l: 'Checkpoint', ring: true }]} />
      <div className="fw-beyond-stats">
        <Fact v={sim.lost.length} l="lost" tone={sim.lost.length ? 'bad' : 'ok'} />
        <Fact v={sim.dups} l="duplicate rows" tone={sim.dups ? 'bad' : 'ok'} />
        <Fact v={sim.replayed} l="events replayed" />
        <Fact v={`${sim.rows} / ${N_EVENTS}`} l="distinct rows" tone={sim.rows === N_EVENTS && !sim.dups ? 'ok' : 'warn'} />
      </div>
      <p className="fw-small fw-muted">*Replay from the last checkpoint plus an upsert on <code>event_id</code>: effectively once at the sink. FraudWatch measured this on GCP: 0 lost, 0 duplicates after a TaskManager kill.</p>
    </div>
  );
}

/* ---------- 5. Hot keys and partition skew ---------- */

function SkewLab() {
  const [parts, setParts] = useState(8);
  const [skew, setSkew] = useState(1.1);
  const [salt, setSalt] = useState(false);
  const KEYS = 500;

  const sim = useMemo(() => {
    const w = Array.from({ length: KEYS }, (_, k) => 1 / Math.pow(k + 1, skew));
    const total = w.reduce((a, b) => a + b, 0);
    const load = new Array<number>(parts).fill(0);
    w.forEach((wk, k) => {
      const p = hash32(k) % parts;
      if (salt && k < 5) for (let j = 0; j < parts; j++) load[(p + j) % parts]! += wk / parts;
      else load[p]! += wk;
    });
    const shares = load.map((l) => l / total);
    const max = Math.max(...shares);
    return { shares, max, imbalance: max * parts, hot: w[0]! / total };
  }, [parts, skew, salt]);

  return (
    <div className="fw-stack" style={{ gap: 14 }}>
      <div className="fw-beyond-controls">
        <Slider label="Kafka partitions" value={parts} min={2} max={16} onChange={setParts} />
        <Slider label="Key skew (Zipf s)" value={skew} min={0} max={1.6} step={0.1} onChange={setSkew} fmt={(v) => v.toFixed(1)} />
        <Segmented<'off' | 'on'> label="Salting" value={salt ? 'on' : 'off'} onChange={(v) => setSalt(v === 'on')} options={[{ value: 'off', label: 'Key = account' }, { value: 'on', label: 'Salt top 5 keys' }]} />
      </div>
      <div className="fw-skew" role="img" aria-label="Share of traffic per partition">
        {sim.shares.map((sh, i) => (
          <div key={i} className="fw-skew-col">
            <span className="fw-skew-bar" style={{ height: `${Math.max(2, (sh / Math.max(sim.max, 1e-9)) * 100)}%` }} data-hot={sh === sim.max && sim.imbalance > 1.5 ? '' : undefined} />
            <span className="fw-skew-label">p{i}</span>
          </div>
        ))}
      </div>
      <div className="fw-beyond-stats">
        <Fact v={`${(sim.max * 100).toFixed(0)}%`} l="hottest partition" tone={sim.imbalance > 2 ? 'bad' : sim.imbalance > 1.3 ? 'warn' : 'ok'} />
        <Fact v={`${sim.imbalance.toFixed(1)}×`} l="max / mean load" />
        <Fact v={`${(1 / sim.max).toFixed(1)} of ${parts}`} l="effective parallelism" />
        <Fact v={`${(sim.hot * 100).toFixed(1)}%`} l="traffic from the #1 account" />
      </div>
      <p className="fw-small fw-muted">Keying by account keeps per-account state correct but one busy account pins one task. Salting spreads it, at the price of a second merge stage for that account's features.</p>
    </div>
  );
}

/* ---------- Section ---------- */

const LABS = [
  { id: 'watermarks', label: 'Watermarks', el: <WatermarkLab /> },
  { id: 'windows', label: 'Windows', el: <WindowLab /> },
  { id: 'microbatch', label: 'Micro-batch model', el: <MicroBatchLab /> },
  { id: 'delivery', label: 'Exactly-once', el: <DeliveryLab /> },
  { id: 'skew', label: 'Hot keys', el: <SkewLab /> },
];

export function BeyondSyllabus() {
  const [tab, setTab] = useState(LABS[0]!.id);
  const { hash } = useLocation();
  useEffect(() => {
    if (hash === '#beyond') document.getElementById('beyond')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, [hash]);
  const lab = LABS.find((l) => l.id === tab) ?? LABS[0]!;
  return (
    <section id="beyond" data-testid="beyond-syllabus">
      <Card title="Beyond the syllabus: stream labs" description="Drag the controls. Everything recomputes live in your browser." actions={<StatusPill tone="info">Simulation</StatusPill>}>
        <Tabs label="Stream labs" idBase="beyond" items={LABS.map((l) => ({ id: l.id, label: l.label }))} value={tab} onChange={setTab} />
        <TabPanel idBase="beyond" id={lab.id}>
          <div className="fw-space-top">{lab.el}</div>
        </TabPanel>
      </Card>
    </section>
  );
}
