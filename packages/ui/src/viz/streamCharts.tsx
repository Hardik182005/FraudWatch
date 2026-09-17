import { Area, AreaChart, Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatInt, formatMs, formatNumber, formatTimestamp } from '../format';
import { histogram } from './math';
import { VizEmpty, VizFrame, type SourceKind } from './VizFrame';

const AXIS = { fontSize: 12, fill: 'var(--text-muted)' };
export const ENGINE_COLOR: Record<string, string> = { flink: 'var(--chart-1)', spark: 'var(--chart-4)' };

/* ---------------- Throughput timeline ---------------- */

export interface ThroughputPoint {
  t: number;
  offered_eps?: number | null;
  processed_eps?: number | null;
}

export function ThroughputTimeline({ points, source = 'live', title = 'Throughput', intervalLabel, freshness, offeredLabel = 'Offered (target rate)' }: { points: ThroughputPoint[]; source?: SourceKind; title?: string; intervalLabel?: string; freshness?: string; offeredLabel?: string }) {
  if (points.length < 2) {
    return (
      <VizFrame title={title} source={source} summary="Not enough throughput samples yet.">
        <VizEmpty kind="no_data" title="Waiting for processed events">A line appears after at least two measurement intervals.</VizEmpty>
      </VizFrame>
    );
  }
  const hasOffered = points.some((p) => p.offered_eps != null);
  const last = points[points.length - 1]!;
  return (
    <VizFrame
      title={title}
      source={source}
      testId="throughput-timeline"
      meta={[`events/second`, intervalLabel, `${formatTimestamp(points[0]!.t)}–${formatTimestamp(last.t)}`, freshness].filter(Boolean).join(' · ')}
      summary={`Throughput over ${points.length} intervals; latest processed ${last.processed_eps != null ? formatNumber(last.processed_eps, 1) : 'not recorded'} events per second.`}
      csvName="throughput.csv"
      table={{ columns: [{ key: 't', label: 'Time', format: (v) => formatTimestamp(Number(v), true) }, { key: 'offered_eps', label: 'Offered eps', align: 'right' }, { key: 'processed_eps', label: 'Processed eps', align: 'right' }], rows: points as unknown as Array<Record<string, unknown>> }}
      footer={hasOffered ? 'Offered = configured generator rate; processed = results persisted per interval. Gaps mean no measurement, not zero.' : 'Processed = results persisted per interval. Offered rate is not available for this source.'}
    >
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={points} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--chart-grid)" />
            <XAxis dataKey="t" type="number" domain={['dataMin', 'dataMax']} tickFormatter={(v) => formatTimestamp(Number(v))} tick={AXIS} minTickGap={40} />
            <YAxis tick={AXIS} width={40} domain={[0, 'auto']} allowDecimals />
            <Tooltip labelFormatter={(v) => formatTimestamp(Number(v), true)} formatter={(v) => (v == null ? 'Not recorded' : `${formatNumber(Number(v), 1)} eps`)} />
            <Legend verticalAlign="top" height={24} />
            {hasOffered && <Line dataKey="offered_eps" name={offeredLabel} stroke="var(--text-muted)" strokeDasharray="5 4" dot={false} isAnimationActive={false} connectNulls={false} type="stepAfter" />}
            <Line dataKey="processed_eps" name="Processed (persisted)" stroke="var(--chart-1)" dot={false} strokeWidth={2} isAnimationActive={false} connectNulls={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Decision distribution ---------------- */

export function DecisionDistribution({ counts, source = 'live', meta }: { counts: { allow: number; review: number; alert: number; unscored: number } | null; source?: SourceKind; meta?: string }) {
  if (!counts) {
    return (
      <VizFrame title="Decision distribution" source={source} summary="No decisions recorded.">
        <VizEmpty kind="no_data" title="No scored transactions yet" />
      </VizFrame>
    );
  }
  const rows = [
    { decision: 'Allow', count: counts.allow, key: 'allow' },
    { decision: 'Review', count: counts.review, key: 'review' },
    { decision: 'Alert', count: counts.alert, key: 'alert' },
    { decision: 'Unscored', count: counts.unscored, key: 'unscored' },
  ];
  const total = rows.reduce((a, r) => a + r.count, 0);
  return (
    <VizFrame
      title="Decision distribution"
      source={source}
      testId="decision-distribution"
      meta={[`n = ${formatInt(total)}`, meta].filter(Boolean).join(' · ')}
      summary={rows.map((r) => `${r.decision} ${formatInt(r.count)}`).join(', ')}
      csvName="decisions.csv"
      table={{ columns: [{ key: 'decision', label: 'Decision' }, { key: 'count', label: 'Count', align: 'right' }], rows }}
      footer="Model-and-rule decisions, not fraud truth. Replayed records are not labelled at decision time."
    >
      <div className="fw-stackbar" aria-hidden="true">
        {total === 0 ? (
          <span className="fw-stackbar-empty">0 decisions</span>
        ) : (
          rows.map((r) => r.count > 0 && <span key={r.key} className="fw-stackbar-seg" data-action={r.key} style={{ flexGrow: r.count }} title={`${r.decision}: ${formatInt(r.count)}`} />)
        )}
      </div>
      <ul className="fw-stackbar-legend">
        {rows.map((r) => (
          <li key={r.key}>
            <span className="fw-stackbar-swatch" data-action={r.key} aria-hidden="true" />
            {r.decision} <strong className="fw-num">{formatInt(r.count)}</strong>
            {total > 0 && <span className="fw-muted"> {formatNumber((100 * r.count) / total, 1)}%</span>}
          </li>
        ))}
      </ul>
    </VizFrame>
  );
}

/* ---------------- Score histogram ---------------- */

export function ScoreHistogram({ scores, review, alert, source = 'live', meta }: { scores: Array<number | null>; review?: number | null; alert?: number | null; source?: SourceKind; meta?: string }) {
  const valid = scores.filter((s): s is number => typeof s === 'number' && Number.isFinite(s));
  if (!valid.length) {
    return (
      <VizFrame title="Score distribution" source={source} summary="No scores yet.">
        <VizEmpty kind="no_data" title="No model scores in view" />
      </VizFrame>
    );
  }
  const bins = histogram(valid, 20).map((b) => ({ ...b, mid: (b.x0 + b.x1) / 2, range: `${formatNumber(b.x0, 2)}–${formatNumber(b.x1, 2)}` }));
  return (
    <VizFrame
      title="Score distribution"
      source={source}
      testId="score-histogram"
      meta={[`n = ${formatInt(valid.length)}`, '20 bins of width 0.05', meta].filter(Boolean).join(' · ')}
      summary={`Histogram of ${valid.length} model scores. Review threshold ${review ?? 'not reported'}, alert threshold ${alert ?? 'not reported'}.`}
      csvName="score-histogram.csv"
      table={{ columns: [{ key: 'range', label: 'Score range' }, { key: 'count', label: 'Transactions', align: 'right' }], rows: bins }}
      footer="Model score is a ranking signal from the deployed model, not a calibrated fraud probability for these replayed records."
    >
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={bins} margin={{ top: 16, right: 12, bottom: 0, left: 0 }} barCategoryGap={1}>
            <CartesianGrid stroke="var(--chart-grid)" vertical={false} />
            <XAxis dataKey="mid" type="number" domain={[0, 1]} tick={AXIS} ticks={[0, 0.2, 0.4, 0.6, 0.8, 1]} />
            <YAxis tick={AXIS} width={36} allowDecimals={false} />
            <Tooltip labelFormatter={(v) => `Score around ${formatNumber(Number(v), 3)}`} formatter={(v) => formatInt(Number(v))} />
            <Bar dataKey="count" name="Transactions" fill="var(--chart-4)" isAnimationActive={false} />
            {review != null && <ReferenceLine x={review} stroke="var(--review)" strokeDasharray="4 3" label={{ value: `review ${formatNumber(review, 3)}`, position: 'top', fontSize: 11, fill: 'var(--review)' }} />}
            {alert != null && <ReferenceLine x={alert} stroke="var(--alert)" strokeDasharray="4 3" label={{ value: `alert ${formatNumber(alert, 3)}`, position: 'insideTopRight', fontSize: 11, fill: 'var(--alert)' }} />}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Latency percentiles ---------------- */

export interface LatencyRow {
  label: string;
  p50: number | null;
  p95: number | null;
  p99?: number | null;
  n: number | null;
}

export function LatencyPercentiles({ rows, source = 'live', title = 'End-to-end latency', meta, boundary }: { rows: LatencyRow[]; source?: SourceKind; title?: string; meta?: string; boundary?: string }) {
  const any = rows.some((r) => r.n && r.n > 0);
  return (
    <VizFrame
      title={title}
      source={source}
      testId="latency-percentiles"
      meta={['milliseconds', meta].filter(Boolean).join(' · ')}
      summary={rows.map((r) => `${r.label}: p50 ${formatMs(r.p50)}, p95 ${formatMs(r.p95)}, n ${r.n ?? 'not recorded'}`).join('; ')}
      csvName="latency.csv"
      table={{ columns: [{ key: 'label', label: 'Series' }, { key: 'p50', label: 'p50 ms', align: 'right' }, { key: 'p95', label: 'p95 ms', align: 'right' }, { key: 'p99', label: 'p99 ms', align: 'right' }, { key: 'n', label: 'n', align: 'right' }], rows: rows as unknown as Array<Record<string, unknown>> }}
      footer={boundary ?? 'Measured from generator produce time to result persisted. Percentiles with small n are unstable.'}
    >
      {!any ? (
        <VizEmpty kind="no_data" title="No latency samples yet" />
      ) : (
        <div className="fw-latency">
          {rows.map((r) => {
            const max = Math.max(...rows.map((x) => x.p99 ?? x.p95 ?? x.p50 ?? 0), 1);
            return (
              <div key={r.label} className="fw-latency-row">
                <span className="fw-latency-label">
                  {r.label}
                  <em>n = {r.n == null ? 'not recorded' : formatInt(r.n)}</em>
                </span>
                <span className="fw-latency-track" aria-hidden="true">
                  {r.p50 != null && <span className="fw-latency-bar" data-p="50" style={{ width: `${(100 * r.p50) / max}%` }} />}
                  {r.p95 != null && <span className="fw-latency-mark" data-p="95" style={{ left: `${(100 * r.p95) / max}%` }} />}
                  {r.p99 != null && <span className="fw-latency-mark" data-p="99" style={{ left: `${(100 * r.p99) / max}%` }} />}
                </span>
                <span className="fw-num fw-small">
                  p50 {formatMs(r.p50)} · p95 {formatMs(r.p95)}
                  {r.p99 !== undefined ? ` · p99 ${formatMs(r.p99)}` : ''}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </VizFrame>
  );
}

/* ---------------- Delay distribution per engine ---------------- */

export function DelayDistribution({ samples, source = 'saved', title = 'Delay distribution', meta }: { samples: Record<string, number[]>; source?: SourceKind; title?: string; meta?: string }) {
  const engines = Object.keys(samples).filter((e) => samples[e]!.length > 0);
  if (!engines.length) {
    return (
      <VizFrame title={title} source={source} summary="No delay samples.">
        <VizEmpty kind="no_data" title="No per-event delay samples for this selection">Run a bounded comparison or pick a run where both engines persisted results.</VizEmpty>
      </VizFrame>
    );
  }
  const all = engines.flatMap((e) => samples[e]!);
  const sorted = [...all].sort((a, b) => a - b);
  const hi = Math.max(1, sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.99))]!);
  const nb = 24;
  const hs = Object.fromEntries(engines.map((e) => [e, histogram(samples[e]!.map((v) => Math.min(v, hi)), nb, 0, hi)]));
  const data = Array.from({ length: nb }, (_, i) => ({
    range: `${Math.round(hs[engines[0]!]![i]!.x0)}–${Math.round(hs[engines[0]!]![i]!.x1)}`,
    mid: Math.round((hs[engines[0]!]![i]!.x0 + hs[engines[0]!]![i]!.x1) / 2),
    ...Object.fromEntries(engines.map((e) => [e, hs[e]![i]!.count])),
  }));
  return (
    <VizFrame
      title={title}
      source={source}
      testId="delay-distribution"
      meta={[`ms · top bin includes values ≥ p99 (${formatMs(hi)})`, engines.map((e) => `${e} n=${formatInt(samples[e]!.length)}`).join(', '), meta].filter(Boolean).join(' · ')}
      summary={`Delay histograms for ${engines.join(' and ')}.`}
      csvName="delay-distribution.csv"
      table={{ columns: [{ key: 'range', label: 'Delay ms' }, ...engines.map((e) => ({ key: e, label: `${e} events`, align: 'right' as const }))], rows: data }}
    >
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--chart-grid)" />
            <XAxis dataKey="mid" tick={AXIS} tickFormatter={(v) => `${v}`} minTickGap={24} />
            <YAxis tick={AXIS} width={36} allowDecimals={false} />
            <Tooltip labelFormatter={(v) => `~${v} ms`} />
            <Legend verticalAlign="top" height={24} />
            {engines.map((e) => (
              <Area key={e} dataKey={e} name={e === 'flink' ? 'Flink' : e === 'spark' ? 'Spark' : e} type="stepAfter" stroke={ENGINE_COLOR[e] ?? 'var(--chart-3)'} fill={ENGINE_COLOR[e] ?? 'var(--chart-3)'} fillOpacity={0.18} isAnimationActive={false} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Trace waterfall ---------------- */

export interface WaterfallStage {
  stage: string;
  label: string;
  at_ms: number | null;
  status: 'ok' | 'failed' | 'missing' | 'not_applicable' | string;
  detail?: string | null;
}

export function TraceWaterfall({ lanes, clientReceipt, title = 'Event journey', source = 'live' }: { lanes: Array<{ id: string; label: string; stages: WaterfallStage[] }>; clientReceipt?: { at_ms: number; note?: string | null } | null; title?: string; source?: SourceKind }) {
  const times = lanes.flatMap((l) => l.stages.map((s) => s.at_ms)).filter((t): t is number => t != null);
  if (!times.length) {
    return (
      <VizFrame title={title} source={source} summary="No timestamps.">
        <VizEmpty kind="unavailable" title="No stage timestamps recorded for this event" />
      </VizFrame>
    );
  }
  const t0 = Math.min(...times);
  const span = Math.max(1, Math.max(...times) - t0);
  const rows = lanes.flatMap((l) =>
    l.stages.map((s, i) => {
      const prev = l.stages.slice(0, i).reverse().find((p) => p.at_ms != null);
      return { lane: l.label, stage: s.label, status: s.status, at_ms: s.at_ms, offset_ms: s.at_ms != null ? s.at_ms - t0 : null, since_previous_ms: s.at_ms != null && prev?.at_ms != null ? s.at_ms - prev.at_ms : null, detail: s.detail ?? null };
    }),
  );
  return (
    <VizFrame
      title={title}
      source={source}
      testId="trace-waterfall"
      meta={`server clocks · offsets from first stage · total ${formatMs(span)}`}
      summary={lanes.map((l) => `${l.label}: ${l.stages.map((s) => `${s.label} ${s.at_ms != null ? `+${formatMs(s.at_ms - t0)}` : s.status}`).join(', ')}`).join('. ')}
      csvName="trace.csv"
      table={{ columns: [{ key: 'lane', label: 'Engine' }, { key: 'stage', label: 'Stage' }, { key: 'status', label: 'Status' }, { key: 'offset_ms', label: 'Offset ms', align: 'right' }, { key: 'since_previous_ms', label: 'Since previous ms', align: 'right' }, { key: 'detail', label: 'Detail' }], rows }}
      footer="Missing stages are shown as gaps, never as zero duration."
    >
      <div className="fw-waterfall">
        {lanes.map((l) => (
          <div key={l.id} className="fw-waterfall-lane">
            <div className="fw-waterfall-lanehead">{l.label}</div>
            <ol>
              {l.stages.map((s, i) => {
                const prev = l.stages.slice(0, i).reverse().find((p) => p.at_ms != null);
                const start = prev?.at_ms != null ? prev.at_ms - t0 : s.at_ms != null ? s.at_ms - t0 : 0;
                const end = s.at_ms != null ? s.at_ms - t0 : null;
                return (
                  <li key={s.stage} className="fw-waterfall-row" data-status={s.status}>
                    <span className="fw-waterfall-label">{s.label}</span>
                    <span className="fw-waterfall-track" aria-hidden="true">
                      {end != null ? <span className="fw-waterfall-bar" style={{ left: `${(100 * start) / span}%`, width: `max(3px, ${(100 * (end - start)) / span}%)` }} /> : <span className="fw-waterfall-gap" />}
                    </span>
                    <span className="fw-num fw-small fw-waterfall-val">
                      {s.status === 'not_applicable' ? 'Not applicable' : end == null ? (s.status === 'failed' ? 'Failed' : 'Not recorded') : `+${formatMs(end)}`}
                      {s.status === 'failed' && end != null ? ' · failed' : ''}
                    </span>
                  </li>
                );
              })}
            </ol>
          </div>
        ))}
        <div className="fw-waterfall-lane" data-kind="client">
          <div className="fw-waterfall-lanehead">Browser receipt (client clock)</div>
          <p className="fw-small fw-muted">
            {clientReceipt
              ? `Received by a browser at ${formatTimestamp(clientReceipt.at_ms, true)}. Browser clocks can drift, so this is not added to server-side delays.`
              : 'No browser receipt recorded for this event.'}
          </p>
        </div>
      </div>
    </VizFrame>
  );
}

/* ---------------- Error frequency strip ---------------- */

export function ErrorStrip({ items, buckets = 30, from, to }: { items: Array<{ timestamp_utc: number; severity: string }>; buckets?: number; from?: number; to?: number }) {
  if (!items.length) return null;
  const lo = from ?? Math.min(...items.map((i) => i.timestamp_utc));
  const hi = Math.max(lo + 1, to ?? Math.max(...items.map((i) => i.timestamp_utc)));
  const w = (hi - lo) / buckets;
  const counts = Array.from({ length: buckets }, (_, i) => ({ t: lo + i * w, errors: 0, warnings: 0, total: 0 }));
  for (const it of items) {
    const b = counts[Math.min(buckets - 1, Math.max(0, Math.floor((it.timestamp_utc - lo) / w)))]!;
    b.total++;
    if (it.severity === 'error' || it.severity === 'critical') b.errors++;
    else if (it.severity === 'warning') b.warnings++;
  }
  const max = Math.max(1, ...counts.map((c) => c.total));
  const errs = counts.reduce((a, c) => a + c.errors, 0);
  return (
    <div className="fw-errstrip" role="img" aria-label={`Log frequency over the loaded rows: ${errs} errors across ${items.length} entries between ${formatTimestamp(lo, true)} and ${formatTimestamp(hi, true)}.`} data-testid="error-strip">
      {counts.map((c, i) => (
        <span key={i} className="fw-errstrip-col" title={`${formatTimestamp(c.t, true)}: ${c.total} rows, ${c.errors} errors, ${c.warnings} warnings`}>
          <span style={{ height: `${(100 * (c.total - c.errors - c.warnings)) / max}%` }} data-sev="info" />
          <span style={{ height: `${(100 * c.warnings) / max}%` }} data-sev="warning" />
          <span style={{ height: `${(100 * c.errors) / max}%` }} data-sev="error" />
        </span>
      ))}
      <span className="fw-errstrip-axis fw-small fw-muted">
        <span>{formatTimestamp(lo)}</span>
        <span>loaded rows only</span>
        <span>{formatTimestamp(hi)}</span>
      </span>
    </div>
  );
}

/* ---------------- Event time vs arrival (watermarks) ---------------- */

export interface ArrivalPoint {
  event_id: string;
  event_time: number;
  arrival: number;
  late: boolean;
  watermark_ms?: number | null;
}

export function EventTimeDiagram({ points, boundMs, source = 'saved', title = 'Event time vs arrival' }: { points: ArrivalPoint[]; boundMs?: number | null; source?: SourceKind; title?: string }) {
  if (!points.length) {
    return (
      <VizFrame title={title} source={source} summary="No late-event examples.">
        <VizEmpty kind="no_data" title="No event-time samples for this run">Late-event examples appear after a late-events experiment or a run with delayed arrival.</VizEmpty>
      </VizFrame>
    );
  }
  const W = 640;
  const H = 260;
  const pad = { l: 56, r: 12, t: 12, b: 34 };
  const xs = points.map((p) => p.arrival);
  const ys = points.flatMap((p) => [p.event_time, p.watermark_ms ?? p.event_time]);
  const x0 = Math.min(...xs), x1 = Math.max(x0 + 1, ...xs);
  const y0 = Math.min(...ys), y1 = Math.max(y0 + 1, ...ys);
  const sx = (v: number) => pad.l + ((v - x0) / (x1 - x0)) * (W - pad.l - pad.r);
  const sy = (v: number) => H - pad.b - ((v - y0) / (y1 - y0)) * (H - pad.t - pad.b);
  const wm = points.filter((p) => p.watermark_ms != null).sort((a, b) => a.arrival - b.arrival);
  return (
    <VizFrame
      title={title}
      source={source}
      testId="event-time-diagram"
      meta={`x: arrival at engine · y: event time · ${points.length} examples${boundMs != null ? ` · watermark bound ${formatMs(boundMs)}` : ''}`}
      summary={`${points.filter((p) => p.late).length} of ${points.length} example events arrived behind the watermark.`}
      csvName="event-time.csv"
      table={{ columns: [{ key: 'event_id', label: 'Event' }, { key: 'event_time', label: 'Event time', format: (v) => formatTimestamp(Number(v), true) }, { key: 'arrival', label: 'Arrival', format: (v) => formatTimestamp(Number(v), true) }, { key: 'watermark_ms', label: 'Watermark', format: (v) => (v == null ? 'Not recorded' : formatTimestamp(Number(v), true)) }, { key: 'late', label: 'Late', format: (v) => (v ? 'yes' : 'no') }], rows: points as unknown as Array<Record<string, unknown>> }}
      footer="An event is late when its event time is behind the engine's current watermark (dashed). Flink's watermark advances per record; Spark's is fixed per micro-batch."
    >
      <div className="fw-svgwrap">
        <svg viewBox={`0 0 ${W} ${H}`} className="fw-svgchart" aria-hidden="true">
          <line x1={pad.l} y1={H - pad.b} x2={W - pad.r} y2={H - pad.b} stroke="var(--border-strong)" />
          <line x1={pad.l} y1={pad.t} x2={pad.l} y2={H - pad.b} stroke="var(--border-strong)" />
          <text x={(W + pad.l) / 2} y={H - 6} textAnchor="middle" fontSize="11" fill="var(--text-muted)">arrival time →</text>
          <text x={12} y={(H - pad.b) / 2} fontSize="11" fill="var(--text-muted)" transform={`rotate(-90 12 ${(H - pad.b) / 2})`} textAnchor="middle">event time →</text>
          <text x={pad.l} y={H - pad.b + 14} fontSize="10" fill="var(--text-muted)">{formatTimestamp(x0)}</text>
          <text x={W - pad.r} y={H - pad.b + 14} fontSize="10" fill="var(--text-muted)" textAnchor="end">{formatTimestamp(x1)}</text>
          {wm.length > 1 && <polyline fill="none" stroke="var(--text-muted)" strokeDasharray="5 4" points={wm.map((p) => `${sx(p.arrival)},${sy(p.watermark_ms!)}`).join(' ')} />}
          {wm.length === 1 && <circle cx={sx(wm[0]!.arrival)} cy={sy(wm[0]!.watermark_ms!)} r={3} fill="none" stroke="var(--text-muted)" />}
          {points.map((p) => (
            <g key={p.event_id}>
              {p.watermark_ms != null && <line x1={sx(p.arrival)} x2={sx(p.arrival)} y1={sy(p.event_time)} y2={sy(p.watermark_ms)} stroke={p.late ? 'var(--late)' : 'var(--border)'} strokeWidth={1} />}
              <circle cx={sx(p.arrival)} cy={sy(p.event_time)} r={4} fill={p.late ? 'var(--late)' : 'var(--chart-1)'} stroke="var(--surface)" />
            </g>
          ))}
        </svg>
        <ul className="fw-svglegend fw-small">
          <li><span className="fw-dot" style={{ background: 'var(--chart-1)' }} /> On time</li>
          <li><span className="fw-dot" style={{ background: 'var(--late)' }} /> Late (behind watermark)</li>
          <li><span className="fw-dash" /> Watermark at arrival</li>
        </ul>
      </div>
    </VizFrame>
  );
}

/* ---------------- Checkpoint / recovery timeline ---------------- */

export interface TimelineMark {
  at: number | null;
  label: string;
  kind: 'checkpoint' | 'kill' | 'restart' | 'running' | 'info' | 'error';
  detail?: string;
}

export function EventTimeline({ marks, title = 'Checkpoint and recovery timeline', source = 'saved', meta }: { marks: TimelineMark[]; title?: string; source?: SourceKind; meta?: string }) {
  const known = marks.filter((m) => m.at != null).sort((a, b) => a.at! - b.at!);
  const unknown = marks.filter((m) => m.at == null);
  if (!known.length) {
    return (
      <VizFrame title={title} source={source} summary="No timestamps.">
        <VizEmpty kind="no_data" title="No recorded timeline events" />
      </VizFrame>
    );
  }
  const t0 = known[0]!.at!;
  const span = Math.max(1, known[known.length - 1]!.at! - t0);
  return (
    <VizFrame
      title={title}
      source={source}
      testId="event-timeline"
      meta={[`total ${formatMs(span)}`, meta].filter(Boolean).join(' · ')}
      summary={known.map((m) => `${m.label} at +${formatMs(m.at! - t0)}`).join(', ')}
      csvName="timeline.csv"
      table={{ columns: [{ key: 'label', label: 'Event' }, { key: 'kind', label: 'Kind' }, { key: 'at', label: 'Time', format: (v) => (v == null ? 'Not recorded' : formatTimestamp(Number(v), true)) }, { key: 'detail', label: 'Detail' }], rows: marks as unknown as Array<Record<string, unknown>> }}
    >
      <div className="fw-tl">
        <div className="fw-tl-axis" aria-hidden="true">
          {known.map((m, i) => (
            <span key={i} className="fw-tl-mark" data-kind={m.kind} style={{ left: `${(100 * (m.at! - t0)) / span}%` }} />
          ))}
        </div>
        <ol className="fw-tl-list">
          {known.map((m, i) => (
            <li key={i} data-kind={m.kind}>
              <span className="fw-tl-dot" aria-hidden="true" />
              <strong>{m.label}</strong>
              <span className="fw-num fw-small fw-muted"> +{formatMs(m.at! - t0)} · {formatTimestamp(m.at, true)}</span>
              {m.detail && <div className="fw-small fw-muted">{m.detail}</div>}
            </li>
          ))}
          {unknown.map((m, i) => (
            <li key={`u${i}`} data-kind="unknown">
              <span className="fw-tl-dot" aria-hidden="true" />
              <strong>{m.label}</strong> <span className="fw-small fw-muted">Not recorded</span>
            </li>
          ))}
        </ol>
      </div>
    </VizFrame>
  );
}
