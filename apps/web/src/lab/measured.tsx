import { Card, StatusPill, formatInt, formatMs } from '@fraudwatch/ui';
import { useState } from 'react';

/**
 * Prerecorded measurements, copied from docs/measured-results.md (each read back from Postgres or the Flink
 * REST API on the run named). They are shown labelled as prerecorded so the CA2 evidence is visible even
 * when no visitor run has been compared yet. Nothing here is estimated.
 */
export interface EngineMeasure {
  persisted: string;
  p50: number;
  p95: number;
  p99: number | null;
  eps: number | null;
  cpu: number | null;
  memMb: number | null;
}

export interface ComparisonRecord {
  id: string;
  label: string;
  where: 'GCP' | 'Local';
  date: string;
  protocol: string;
  flink: EngineMeasure;
  spark: EngineMeasure;
  parity: { both: number; same: number; different: number; engineOnly: number };
  parityRun: string;
}

export const COMPARISONS: ComparisonRecord[] = [
  {
    id: 'exp_20260918_115240_64fd11',
    label: 'Experiment runner on GCP',
    where: 'GCP',
    date: '2026-09-18',
    protocol: '100-event warm-up excluded, 3 repeats × 300 IEEE-CIS replay records per engine at 10 ev/s, seed 20260917, engine order alternated. Percentiles are the median of the three per-run percentiles.',
    flink: { persisted: '900 / 900', p50: 110.5, p95: 155.0, p99: 166.0, eps: 9.97, cpu: 17.0, memMb: 866.7 },
    spark: { persisted: '900 / 900', p50: 3553.0, p95: 4999.3, p99: 5391.9, eps: 8.8, cpu: 197.0, memMb: 1277.0 },
    parity: { both: 300, same: 300, different: 0, engineOnly: 0 },
    parityRun: 'r_93ae9398e138b9bc',
  },
  {
    id: 'exp_20260918_102135_6201bb',
    label: 'Experiment runner, local Compose',
    where: 'Local',
    date: '2026-09-18',
    protocol: 'Same protocol as the GCP run, on the local Docker Compose stack.',
    flink: { persisted: '900 / 900', p50: 110, p95: 167, p99: 207, eps: 9.95, cpu: 25.0, memMb: 707 },
    spark: { persisted: '900 / 900', p50: 4027, p95: 6518, p99: 7387, eps: 8.86, cpu: 208.7, memMb: 1518 },
    parity: { both: 300, same: 300, different: 0, engineOnly: 0 },
    parityRun: 'r_626d380d31223a40',
  },
  {
    id: 'r_d48339d70bb9ceb4',
    label: 'Manual parity run on GCP',
    where: 'GCP',
    date: '2026-09-18',
    protocol: 'One generator run, 500 IEEE-CIS replay records, 10 ev/s, seed 20260917, both engines on the same Kafka records. Spark trigger 1000 ms.',
    flink: { persisted: '500 / 500', p50: 92, p95: 155, p99: null, eps: null, cpu: null, memMb: null },
    spark: { persisted: '500 / 500', p50: 3862, p95: 5695, p99: null, eps: null, cpu: null, memMb: null },
    parity: { both: 500, same: 500, different: 0, engineOnly: 0 },
    parityRun: 'r_d48339d70bb9ceb4',
  },
];

export const HEADLINE = COMPARISONS[2]!;

function Bars({ rows, unit }: { rows: Array<{ label: string; flink: number | null; spark: number | null }>; unit: (v: number) => string }) {
  const max = Math.max(...rows.flatMap((r) => [r.flink ?? 0, r.spark ?? 0]), 1);
  return (
    <div className="fw-mbars" role="table" aria-label="Flink and Spark side by side">
      {rows.map((r) => (
        <div className="fw-mbars-row" role="row" key={r.label}>
          <span className="fw-mbars-label" role="rowheader">{r.label}</span>
          {(['flink', 'spark'] as const).map((e) => {
            const v = r[e];
            return (
              <span className="fw-mbars-cell" role="cell" key={e} data-engine={e}>
                <span className="fw-mbars-bar" style={{ width: v == null ? 0 : `${Math.max(1.5, (v / max) * 100)}%` }} />
                <span className="fw-mbars-value">{e === 'flink' ? 'Flink' : 'Spark'} {v == null ? 'not measured' : unit(v)}</span>
              </span>
            );
          })}
        </div>
      ))}
    </div>
  );
}

/** Flink vs Spark, from the recorded runs; the visitor's own run is compared separately in the experiment lab. */
export function MeasuredComparison() {
  const [id, setId] = useState(COMPARISONS[0]!.id);
  const rec = COMPARISONS.find((c) => c.id === id) ?? COMPARISONS[0]!;
  const ratio = rec.spark.p50 / rec.flink.p50;
  return (
    <Card
      title="Flink vs Spark, measured"
      description={`Prerecorded · ${rec.where} · ${rec.date} · ${rec.id}`}
      actions={
        <label className="fw-row fw-small">
          <span className="fw-sr-only">Recorded run</span>
          <select className="fw-input fw-select-sm" value={id} onChange={(e) => setId(e.target.value)} data-testid="measured-run">
            {COMPARISONS.map((c) => (
              <option key={c.id} value={c.id}>{c.label}</option>
            ))}
          </select>
        </label>
      }
    >
      <div className="fw-stack" style={{ gap: 14 }} data-testid="measured-comparison">
        <div className="fw-row" style={{ flexWrap: 'wrap', gap: 8 }}>
          <StatusPill tone="review">Prerecorded</StatusPill>
          <StatusPill tone="allow">{formatInt(rec.parity.same)} / {formatInt(rec.parity.both)} same decision</StatusPill>
          <StatusPill tone="neutral">{formatInt(rec.parity.different)} disagreements · {formatInt(rec.parity.engineOnly)} engine-only</StatusPill>
          <StatusPill tone="info">Spark p50 ≈ {ratio.toFixed(0)}× Flink at 10 ev/s</StatusPill>
        </div>
        <Bars
          unit={(v) => formatMs(v)}
          rows={[
            { label: 'e2e p50', flink: rec.flink.p50, spark: rec.spark.p50 },
            { label: 'e2e p95', flink: rec.flink.p95, spark: rec.spark.p95 },
            { label: 'e2e p99', flink: rec.flink.p99, spark: rec.spark.p99 },
          ]}
        />
        <div className="fw-table-wrap">
          <table className="fw-table">
            <thead>
              <tr><th>Engine</th><th>Persisted</th><th>p50</th><th>p95</th><th>p99</th><th>Processed ev/s</th><th>CPU %</th><th>Memory</th></tr>
            </thead>
            <tbody>
              {(['flink', 'spark'] as const).map((e) => {
                const m = rec[e];
                return (
                  <tr key={e}>
                    <td><strong>{e === 'flink' ? 'Flink 1.20' : 'Spark 3.5'}</strong></td>
                    <td>{m.persisted}</td>
                    <td className="fw-num">{formatMs(m.p50)}</td>
                    <td className="fw-num">{formatMs(m.p95)}</td>
                    <td className="fw-num">{m.p99 == null ? 'Not measured' : formatMs(m.p99)}</td>
                    <td className="fw-num">{m.eps ?? 'Not measured'}</td>
                    <td className="fw-num">{m.cpu ?? 'Not measured'}</td>
                    <td className="fw-num">{m.memMb == null ? 'Not measured' : `${m.memMb} MB`}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="fw-small fw-muted">
          {rec.protocol} Parity over run <code>{rec.parityRun}</code>. The latency gap is architectural: Flink scores each record as it arrives, Spark waits for the next 1 s micro-batch boundary. This is latency at a fixed, low arrival rate on one shared host, not a throughput benchmark, and no winner is declared.
        </p>
      </div>
    </Card>
  );
}

/** Late-event and recovery evidence, also prerecorded. */
export function MeasuredRobustness() {
  return (
    <div className="fw-grid fw-grid-2">
      <Card title="Late events track the watermark bound" description="Prerecorded · Local · exp_20260918_070300_29d804">
        <div className="fw-grid fw-grid-2">
          <div className="fw-mfact"><strong>49 / 49</strong><span>records delayed 15–30 s (beyond the 5 s bound) flagged late</span></div>
          <div className="fw-mfact"><strong>0 / 59</strong><span>records delayed 1–4 s (inside the bound) flagged late</span></div>
        </div>
        <p className="fw-small fw-muted fw-space-top">600 records at 5 ev/s, Flink with <code>forBoundedOutOfOrderness(5 s)</code> and a 60 s tumbling window. Late records go to a side output and are marked, never silently dropped.</p>
      </Card>
      <Card title="Checkpoint recovery after a TaskManager kill" description="Prerecorded · GCP · job 20d1ba34…">
        <div className="fw-grid fw-grid-2">
          <div className="fw-mfact"><strong>chk-80</strong><span>checkpoint restored (EXACTLY_ONCE, every 10 s)</span></div>
          <div className="fw-mfact"><strong>0 lost · 0 dup</strong><span>persisted rows across the failure</span></div>
        </div>
        <p className="fw-small fw-muted fw-space-top">The failure is detected only after the ~60 s heartbeat timeout; 6 checkpoints failed during the outage, as expected with no TaskManager.</p>
      </Card>
    </div>
  );
}
