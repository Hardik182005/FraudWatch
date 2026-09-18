import { useState, type ReactNode } from 'react';
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, ReferenceDot, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatInt, formatNumber, formatPct } from '../format';
import { confusionMetrics, reconcileConfusion, rowNormalize, type Confusion } from './math';
import { VizEmpty, VizFrame, type SourceKind } from './VizFrame';

const AXIS = { fontSize: 12, fill: 'var(--text-muted)' };

/* ---------------- Confusion matrix heatmap ---------------- */

export interface EvalMeta {
  modelVersion?: string | null;
  dataset?: string | null;
  split?: string | null;
  threshold?: number | null;
  evaluatedAt?: string | null;
  artifact?: string | null;
}

export function ConfusionHeatmap({
  cm,
  reported,
  meta,
  source = 'heldout',
  title = 'Confusion matrix at the fixed threshold',
}: {
  cm: Confusion;
  reported?: { n?: number | null; positives?: number | null; precision?: number | null; recall?: number | null; f1?: number | null };
  meta: EvalMeta;
  source?: SourceKind;
  title?: string;
}) {
  const [mode, setMode] = useState<'counts' | 'row'>('counts');
  const m = confusionMetrics(cm);
  const pct = rowNormalize(cm);
  const check = reconcileConfusion(cm, reported ?? {});
  const max = Math.max(cm.tn, cm.fp, cm.fn, cm.tp, 1);
  const intensity = (k: keyof Confusion) => (mode === 'counts' ? Math.sqrt(cm[k] / max) : (pct[k] ?? 0));
  const cells: Array<{ k: keyof Confusion; name: string; desc: string }> = [
    { k: 'tn', name: 'TN', desc: 'Correctly ordinary' },
    { k: 'fp', name: 'FP', desc: 'False alert' },
    { k: 'fn', name: 'FN', desc: 'Missed fraud' },
    { k: 'tp', name: 'TP', desc: 'Correctly flagged fraud' },
  ];
  const summary = `Confusion matrix: ${formatInt(cm.tn)} true negatives, ${formatInt(cm.fp)} false positives, ${formatInt(cm.fn)} false negatives, ${formatInt(cm.tp)} true positives, total ${formatInt(m.total)}.`;
  return (
    <VizFrame
      title={title}
      source={source}
      testId="confusion-heatmap"
      summary={summary}
      csvName="confusion-matrix.csv"
      meta={<EvalMetaLine meta={meta} n={m.total} />}
      table={{
        columns: [
          { key: 'actual', label: 'Actual' },
          { key: 'pred_ordinary', label: 'Predicted ordinary', align: 'right' },
          { key: 'pred_fraud', label: 'Predicted fraud', align: 'right' },
          { key: 'support', label: 'Support', align: 'right' },
        ],
        rows: [
          { actual: 'Ordinary', pred_ordinary: cm.tn, pred_fraud: cm.fp, support: m.actualOrdinary },
          { actual: 'Fraud', pred_ordinary: cm.fn, pred_fraud: cm.tp, support: m.actualFraud },
        ],
      }}
      actions={
        <div className="fw-segmented" role="group" aria-label="Matrix values">
          <button type="button" aria-pressed={mode === 'counts'} onClick={() => setMode('counts')}>
            Counts
          </button>
          <button type="button" aria-pressed={mode === 'row'} onClick={() => setMode('row')}>
            % of actual class
          </button>
        </div>
      }
      footer={
        check.ok ? (
          <span data-testid="cm-reconciled">Cells sum to {formatInt(m.total)} evaluated rows; precision, recall and F1 below are recomputed from these cells and match the artifact.</span>
        ) : (
          <span style={{ color: 'var(--alert)' }} data-testid="cm-mismatch">
            Reconciliation problem: {check.problems.join(' ')}
          </span>
        )
      }
    >
      <div className="fw-cm-layout">
        <div className="fw-cm" role="table" aria-label="Confusion matrix">
          <div role="row" className="fw-cm-row fw-cm-headrow">
            <span role="columnheader" />
            <span role="columnheader">Predicted ordinary</span>
            <span role="columnheader">Predicted fraud</span>
          </div>
          {[0, 1].map((r) => (
            <div role="row" className="fw-cm-row" key={r}>
              <span role="rowheader" className="fw-cm-rowhead">
                {r === 0 ? 'Actual ordinary' : 'Actual fraud'}
                <em>{formatInt(r === 0 ? m.actualOrdinary : m.actualFraud)} rows</em>
              </span>
              {cells.slice(r * 2, r * 2 + 2).map((c) => {
                const t = intensity(c.k);
                const good = c.k === 'tn' || c.k === 'tp';
                return (
                  <span
                    role="cell"
                    key={c.k}
                    className="fw-cm-cell"
                    data-dark={t > 0.55 || undefined}
                    data-kind={good ? 'correct' : 'error'}
                    style={{ ['--t' as string]: String(0.06 + t * 0.94) }}
                    title={`${c.desc}: ${formatInt(cm[c.k])} (${pct[c.k] === null ? 'N/A' : formatPct(pct[c.k]!)} of actual class)`}
                  >
                    <span className="fw-cm-tag">{c.name}</span>
                    <strong className="fw-cm-value">{mode === 'counts' ? formatInt(cm[c.k]) : pct[c.k] === null ? 'N/A' : formatPct(pct[c.k]!)}</strong>
                    <span className="fw-cm-desc">{c.desc}</span>
                  </span>
                );
              })}
            </div>
          ))}
          <div className="fw-cm-legend" aria-hidden="true">
            <span>{mode === 'counts' ? '0' : '0%'}</span>
            <span className="fw-cm-ramp" />
            <span>{mode === 'counts' ? `${formatInt(max)} (sqrt scale)` : '100%'}</span>
          </div>
        </div>
        <dl className="fw-cm-metrics">
          <div>
            <dt>Precision</dt>
            <dd>{m.precision === null ? 'N/A' : formatNumber(m.precision, 3)}</dd>
            <dd className="fw-small fw-muted">TP / (TP + FP)</dd>
          </div>
          <div>
            <dt>Recall</dt>
            <dd>{m.recall === null ? 'N/A' : formatNumber(m.recall, 3)}</dd>
            <dd className="fw-small fw-muted">TP / (TP + FN)</dd>
          </div>
          <div>
            <dt>F1</dt>
            <dd>{m.f1 === null ? 'N/A' : formatNumber(m.f1, 3)}</dd>
            <dd className="fw-small fw-muted">Harmonic mean</dd>
          </div>
          <div>
            <dt>Support</dt>
            <dd>
              {formatInt(m.actualFraud)} fraud / {formatInt(m.actualOrdinary)} ordinary
            </dd>
          </div>
        </dl>
      </div>
      <p className="fw-small fw-muted">No labelled row-level examples are published by the API, so cells show aggregates only (no drill-down).</p>
    </VizFrame>
  );
}

export function EvalMetaLine({ meta, n }: { meta: EvalMeta; n?: number | null }) {
  const parts: ReactNode[] = [];
  if (meta.dataset) parts.push(meta.dataset);
  if (meta.split) parts.push(meta.split);
  if (n != null) parts.push(`n = ${formatInt(n)}`);
  if (meta.modelVersion) parts.push(`model ${meta.modelVersion}`);
  if (meta.threshold != null) parts.push(`threshold ${formatNumber(meta.threshold, 4)}`);
  if (meta.evaluatedAt) parts.push(meta.evaluatedAt);
  if (meta.artifact) parts.push(meta.artifact);
  return <>{parts.map((p, i) => (<span key={i}>{i > 0 ? ' · ' : ''}{p}</span>))}</>;
}

/* ---------------- Precision-recall ---------------- */

export interface CurveSeries {
  id: string;
  label: string;
  /** [recall, precision] */
  points: Array<[number, number]>;
  color: string;
  operating?: { recall: number; precision: number; threshold: number } | null;
  auc?: number | null;
}

export function PRCurve({ series, prevalence, meta, source = 'heldout' }: { series: CurveSeries[]; prevalence: number | null; meta: EvalMeta; source?: SourceKind }) {
  const usable = series.filter((s) => s.points.length > 1);
  const markers = series.filter((s) => s.operating);
  if (!usable.length && !markers.length) {
    return (
      <VizFrame title="Precision-recall curve" source={source} summary="No curve points available.">
        <VizEmpty kind="unavailable" title="No precision-recall points in the evaluation artifact">The model results file does not include curve points for this split.</VizEmpty>
      </VizFrame>
    );
  }
  const rows = usable.flatMap((s) => s.points.map(([recall, precision]) => ({ model: s.label, recall, precision })));
  return (
    <VizFrame
      title="Precision-recall curve"
      source={source}
      testId="pr-curve"
      meta={<EvalMetaLine meta={meta} />}
      csvName="precision-recall.csv"
      summary={`Precision-recall curves for ${(usable.length ? usable : markers).map((s) => `${s.label} (PR-AUC ${s.auc != null ? formatNumber(s.auc, 3) : 'not reported'})`).join(', ')}. Prevalence reference ${prevalence != null ? formatPct(prevalence) : 'not reported'}.`}
      table={{ columns: [{ key: 'model', label: 'Model' }, { key: 'recall', label: 'Recall', align: 'right', format: (v) => formatNumber(Number(v), 4) }, { key: 'precision', label: 'Precision', align: 'right', format: (v) => formatNumber(Number(v), 4) }], rows }}
      footer={prevalence != null ? `Dashed line: prevalence (${formatPct(prevalence, 2)}) = precision of random ranking. Dot: fixed operating threshold chosen on validation.` : undefined}
    >
      <div className="fw-chart fw-chart-square" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart margin={{ top: 8, right: 16, bottom: 18, left: 4 }}>
            <CartesianGrid stroke="var(--chart-grid)" />
            <XAxis type="number" dataKey="x" domain={[0, 1]} tick={AXIS} label={{ value: 'Recall', position: 'insideBottom', offset: -8, fill: 'var(--text-muted)', fontSize: 12 }} allowDuplicatedCategory={false} />
            <YAxis type="number" dataKey="y" domain={[0, 1]} tick={AXIS} width={44} label={{ value: 'Precision', angle: -90, position: 'insideLeft', fill: 'var(--text-muted)', fontSize: 12 }} />
            <Tooltip formatter={(v) => formatNumber(Number(v), 3)} labelFormatter={(l) => `Recall ${formatNumber(Number(l), 3)}`} />
            <Legend verticalAlign="top" height={24} />
            {usable.map((s) => (
              <Line key={s.id} data={s.points.map(([x, y]) => ({ x, y }))} dataKey="y" name={`${s.label}${s.auc != null ? ` (AUC ${formatNumber(s.auc, 3)})` : ''}`} stroke={s.color} dot={false} strokeWidth={2} isAnimationActive={false} type="linear" />
            ))}
            {prevalence != null && <ReferenceLine y={prevalence} stroke="var(--text-muted)" strokeDasharray="5 4" />}
            {markers.map((s) => s.operating && <ReferenceDot key={`${s.id}-op`} x={s.operating.recall} y={s.operating.precision} r={6} fill={s.color} stroke="var(--surface)" strokeWidth={2} />)}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Class balance ---------------- */

export function ClassBalance({ ordinary, fraud, meta, source = 'heldout' }: { ordinary: number; fraud: number; meta: EvalMeta; source?: SourceKind }) {
  const total = ordinary + fraud;
  const rows = [
    { cls: 'Ordinary', count: ordinary, share: total ? ordinary / total : null },
    { cls: 'Fraud', count: fraud, share: total ? fraud / total : null },
  ];
  return (
    <VizFrame
      title="Class balance"
      source={source}
      testId="class-balance"
      meta={<EvalMetaLine meta={meta} n={total} />}
      summary={`${formatInt(ordinary)} ordinary and ${formatInt(fraud)} fraud rows (${total ? formatPct(fraud / total, 2) : 'N/A'} fraud).`}
      csvName="class-balance.csv"
      table={{ columns: [{ key: 'cls', label: 'Class' }, { key: 'count', label: 'Rows', align: 'right', format: (v) => formatInt(Number(v)) }, { key: 'share', label: 'Share', align: 'right', format: (v) => (v == null ? 'N/A' : formatPct(Number(v), 2)) }], rows }}
    >
      <div className="fw-balance" aria-hidden="true">
        {rows.map((r) => (
          <div key={r.cls} className="fw-balance-row">
            <span className="fw-balance-label">{r.cls}</span>
            <span className="fw-balance-track">
              <span className="fw-balance-bar" data-cls={r.cls.toLowerCase()} style={{ width: `${Math.max(0.4, (r.share ?? 0) * 100)}%` }} />
            </span>
            <span className="fw-num fw-balance-val">
              {formatInt(r.count)} <em>{r.share == null ? 'N/A' : formatPct(r.share, 2)}</em>
            </span>
          </div>
        ))}
      </div>
    </VizFrame>
  );
}

/* ---------------- Model comparison ---------------- */

export interface ModelMetricRow {
  model: string;
  pr_auc: number | null;
  roc_auc: number | null;
  precision: number | null;
  recall: number | null;
  f1: number | null;
  note?: string;
}

export function ModelComparison({ rows, meta, source = 'heldout', title = 'Baseline vs LightGBM' }: { rows: ModelMetricRow[]; meta: EvalMeta; source?: SourceKind; title?: string }) {
  const metrics: Array<{ key: keyof ModelMetricRow; label: string }> = [
    { key: 'pr_auc', label: 'PR-AUC' },
    { key: 'roc_auc', label: 'ROC-AUC' },
    { key: 'precision', label: 'Precision' },
    { key: 'recall', label: 'Recall' },
    { key: 'f1', label: 'F1' },
  ];
  const data = metrics.map((m) => ({ metric: m.label, ...Object.fromEntries(rows.map((r) => [r.model, r[m.key] as number | null])) }));
  const colors = ['var(--unscored)', 'var(--chart-1)', 'var(--chart-4)'];
  return (
    <VizFrame
      title={title}
      source={source}
      testId="model-comparison"
      meta={<EvalMetaLine meta={meta} />}
      summary={rows.map((r) => `${r.model}: PR-AUC ${r.pr_auc != null ? formatNumber(r.pr_auc, 3) : 'N/A'}, F1 ${r.f1 != null ? formatNumber(r.f1, 3) : 'N/A'}`).join('; ')}
      csvName="model-comparison.csv"
      table={{
        columns: [{ key: 'model', label: 'Model' }, ...metrics.map((m) => ({ key: m.key as string, label: m.label, align: 'right' as const, format: (v: unknown) => (v == null ? 'N/A' : formatNumber(Number(v), 3)) }))],
        rows: rows as unknown as Array<Record<string, unknown>>,
      }}
      footer="Bars start at zero. Same held-out split and each model's own validation-selected threshold."
    >
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--chart-grid)" vertical={false} />
            <XAxis dataKey="metric" tick={AXIS} />
            <YAxis domain={[0, 1]} tick={AXIS} width={40} />
            <Tooltip formatter={(v) => (v == null ? 'N/A' : formatNumber(Number(v), 3))} />
            <Legend verticalAlign="top" height={24} />
            {rows.map((r, i) => (
              <Bar key={r.model} dataKey={r.model} fill={colors[i % colors.length]} isAnimationActive={false} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Threshold tradeoff (validation only) ---------------- */

export interface ThresholdPoint {
  threshold: number;
  precision: number | null;
  recall: number | null;
  flagged?: number | null;
}

export function ThresholdTradeoff({ points, selected, meta }: { points: ThresholdPoint[] | null; selected?: number | null; meta: EvalMeta }) {
  if (!points || points.length < 2) {
    return (
      <VizFrame title="Threshold tradeoff" source="validation" summary="Validation threshold sweep not available.">
        <VizEmpty kind="not_instrumented" title="Validation threshold sweep not published">
          The evaluation artifact only contains held-out curves. Threshold exploration must use validation data, so it is not drawn from the held-out test set.
        </VizEmpty>
      </VizFrame>
    );
  }
  return (
    <VizFrame
      title="Threshold tradeoff"
      source="validation"
      testId="threshold-tradeoff"
      meta={<EvalMetaLine meta={meta} />}
      summary={`Precision and recall across ${points.length} validation thresholds.`}
      csvName="threshold-tradeoff.csv"
      table={{ columns: [{ key: 'threshold', label: 'Threshold', align: 'right' }, { key: 'precision', label: 'Precision', align: 'right' }, { key: 'recall', label: 'Recall', align: 'right' }, { key: 'flagged', label: 'Flagged rows', align: 'right' }], rows: points as unknown as Array<Record<string, unknown>> }}
    >
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={points} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--chart-grid)" />
            <XAxis dataKey="threshold" type="number" domain={[0, 1]} tick={AXIS} />
            <YAxis domain={[0, 1]} tick={AXIS} width={40} />
            <Tooltip formatter={(v) => formatNumber(Number(v), 3)} />
            <Legend verticalAlign="top" height={24} />
            <Line dataKey="precision" name="Precision" stroke="var(--chart-4)" dot={false} isAnimationActive={false} />
            <Line dataKey="recall" name="Recall" stroke="var(--chart-1)" dot={false} isAnimationActive={false} />
            {selected != null && <ReferenceLine x={selected} stroke="var(--text)" strokeDasharray="4 3" label={{ value: 'selected', fontSize: 11, fill: 'var(--text-muted)' }} />}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/* ---------------- Feature contributions (SHAP) ---------------- */

export interface Contribution {
  feature: string;
  value: number | string | null;
  contribution: number;
}

export function ContributionBars({ items, baseValue, method, source = 'live', title = 'Feature contributions for this prediction' }: { items: Contribution[]; baseValue?: number | null; method?: string | null; source?: SourceKind; title?: string }) {
  if (!items.length) {
    return (
      <VizFrame title={title} source={source} summary="No attribution values recorded.">
        <VizEmpty kind="unavailable" title="No feature attributions recorded for this prediction" />
      </VizFrame>
    );
  }
  const sorted = [...items].sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution)).slice(0, 12);
  const data = sorted.map((c) => ({ ...c, label: `${c.feature}${c.value !== null && c.value !== undefined ? ` = ${typeof c.value === 'number' ? formatNumber(c.value, 3) : c.value}` : ''}` }));
  return (
    <VizFrame
      title={title}
      source={source}
      testId="contribution-bars"
      meta={`${method ?? 'Attribution'} · log-odds units${baseValue != null ? ` · base value ${formatNumber(baseValue, 3)}` : ''}`}
      summary={`Top contributions: ${sorted.slice(0, 3).map((c) => `${c.feature} ${c.contribution >= 0 ? '+' : ''}${formatNumber(c.contribution, 3)}`).join(', ')} (log-odds).`}
      csvName="feature-contributions.csv"
      table={{ columns: [{ key: 'feature', label: 'Feature' }, { key: 'value', label: 'Value' }, { key: 'contribution', label: 'Contribution (log-odds)', align: 'right', format: (v) => formatNumber(Number(v), 4) }], rows: sorted as unknown as Array<Record<string, unknown>> }}
      footer="Positive values push the model score up, negative values down, in log-odds (not percentage points). Attributions describe the model, not causes of fraud."
    >
      <div className="fw-chart" style={{ height: Math.max(160, data.length * 30 + 40) }} aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ top: 4, right: 24, bottom: 0, left: 8 }}>
            <CartesianGrid stroke="var(--chart-grid)" horizontal={false} />
            <XAxis type="number" tick={AXIS} />
            <YAxis type="category" dataKey="label" width={190} tick={{ ...AXIS, fontSize: 11 }} />
            <Tooltip formatter={(v) => `${formatNumber(Number(v), 4)} log-odds`} />
            <ReferenceLine x={0} stroke="var(--text-muted)" />
            <Bar dataKey="contribution" isAnimationActive={false}>
              {data.map((d, i) => (
                <Cell key={i} fill={d.contribution >= 0 ? 'var(--alert)' : 'var(--chart-1)'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}

/** Global importance (gain) — explicitly not a per-prediction explanation. */
export function GlobalImportance({ items, meta, title = 'Global feature importance (gain)' }: { items: Array<{ feature: string; gain: number }>; meta: EvalMeta; title?: string }) {
  if (!items.length) return null;
  const data = [...items].sort((a, b) => b.gain - a.gain).slice(0, 12);
  return (
    <VizFrame
      title={title}
      source="saved"
      sourceText="Training artifact"
      meta={<EvalMetaLine meta={meta} />}
      summary={`Top features by total split gain: ${data.slice(0, 3).map((d) => d.feature).join(', ')}.`}
      csvName="feature-importance.csv"
      table={{ columns: [{ key: 'feature', label: 'Feature' }, { key: 'gain', label: 'Gain', align: 'right', format: (v) => formatNumber(Number(v), 1) }], rows: data }}
      footer="Total LightGBM split gain over the training population. This is not the explanation for any single transaction."
    >
      <div className="fw-chart" style={{ height: data.length * 26 + 30 }} aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ top: 4, right: 24, bottom: 0, left: 8 }}>
            <CartesianGrid stroke="var(--chart-grid)" horizontal={false} />
            <XAxis type="number" tick={AXIS} domain={[0, 'auto']} />
            <YAxis type="category" dataKey="feature" width={170} tick={{ ...AXIS, fontSize: 11 }} />
            <Tooltip formatter={(v) => formatNumber(Number(v), 1)} />
            <Bar dataKey="gain" fill="var(--chart-4)" isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </VizFrame>
  );
}
