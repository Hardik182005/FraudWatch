/* Pure helpers behind the shared visualizations. Everything here is unit tested. */

export interface Confusion {
  tn: number;
  fp: number;
  fn: number;
  tp: number;
}

export interface ConfusionMetrics {
  total: number;
  actualOrdinary: number;
  actualFraud: number;
  predictedFraud: number;
  precision: number | null;
  recall: number | null;
  f1: number | null;
  accuracy: number | null;
  specificity: number | null;
}

const ratio = (a: number, b: number): number | null => (b > 0 ? a / b : null);

export function confusionMetrics(cm: Confusion): ConfusionMetrics {
  const total = cm.tn + cm.fp + cm.fn + cm.tp;
  const precision = ratio(cm.tp, cm.tp + cm.fp);
  const recall = ratio(cm.tp, cm.tp + cm.fn);
  const f1 = precision !== null && recall !== null && precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : precision === null || recall === null ? null : 0;
  return {
    total,
    actualOrdinary: cm.tn + cm.fp,
    actualFraud: cm.fn + cm.tp,
    predictedFraud: cm.fp + cm.tp,
    precision,
    recall,
    f1,
    accuracy: ratio(cm.tn + cm.tp, total),
    specificity: ratio(cm.tn, cm.tn + cm.fp),
  };
}

/** Row-normalized matrix (percentage of the actual class). An empty row yields nulls (shown as N/A). */
export function rowNormalize(cm: Confusion): { tn: number | null; fp: number | null; fn: number | null; tp: number | null } {
  const ord = cm.tn + cm.fp;
  const fraud = cm.fn + cm.tp;
  return { tn: ratio(cm.tn, ord), fp: ratio(cm.fp, ord), fn: ratio(cm.fn, fraud), tp: ratio(cm.tp, fraud) };
}

export interface ReconcileCheck {
  ok: boolean;
  problems: string[];
}

/**
 * Checks that the matrix sums to the evaluated sample count, that class support matches, and that
 * reported metrics agree with the matrix (within `tol`).
 */
export function reconcileConfusion(
  cm: Confusion,
  reported: { n?: number | null; positives?: number | null; precision?: number | null; recall?: number | null; f1?: number | null },
  tol = 1e-6,
): ReconcileCheck {
  const m = confusionMetrics(cm);
  const problems: string[] = [];
  if (reported.n != null && reported.n !== m.total) problems.push(`Cells sum to ${m.total}, but the artifact reports n = ${reported.n}.`);
  if (reported.positives != null && reported.positives !== m.actualFraud) problems.push(`Actual fraud row sums to ${m.actualFraud}, but the artifact reports ${reported.positives} positives.`);
  const near = (a: number | null, b: number | null | undefined) => b == null || a == null || Math.abs(a - b) <= tol;
  if (!near(m.precision, reported.precision)) problems.push('Reported precision does not match the matrix.');
  if (!near(m.recall, reported.recall)) problems.push('Reported recall does not match the matrix.');
  if (!near(m.f1, reported.f1)) problems.push('Reported F1 does not match the matrix.');
  return { ok: problems.length === 0, problems };
}

export interface HistogramBin {
  x0: number;
  x1: number;
  count: number;
}

/** Fixed-width histogram over [min, max]; the top edge is inclusive. Non-finite values are ignored. */
export function histogram(values: Array<number | null | undefined>, bins: number, min = 0, max = 1): HistogramBin[] {
  const width = (max - min) / bins;
  const out: HistogramBin[] = Array.from({ length: bins }, (_, i) => ({ x0: min + i * width, x1: min + (i + 1) * width, count: 0 }));
  for (const v of values) {
    if (typeof v !== 'number' || !Number.isFinite(v) || v < min || v > max) continue;
    const i = v === max ? bins - 1 : Math.floor((v - min) / width);
    out[i]!.count++;
  }
  return out;
}

/** Nearest-rank percentile on a copy; null when there are no observations. */
export function percentile(values: number[], p: number): number | null {
  const xs = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!xs.length) return null;
  const rank = Math.ceil((p / 100) * xs.length);
  return xs[Math.min(xs.length - 1, Math.max(0, rank - 1))]!;
}

/**
 * Min/max-preserving downsampling into `buckets` groups (keeps spikes). Returns the input unchanged when
 * already small enough.
 */
export function downsampleMinMax<T>(points: T[], buckets: number, value: (p: T) => number): T[] {
  if (points.length <= buckets * 2) return points;
  const size = points.length / buckets;
  const out: T[] = [];
  for (let b = 0; b < buckets; b++) {
    const slice = points.slice(Math.floor(b * size), Math.floor((b + 1) * size));
    if (!slice.length) continue;
    let lo: T = slice[0] as T;
    let hi: T = slice[0] as T;
    for (const p of slice) {
      if (value(p) < value(lo)) lo = p;
      if (value(p) > value(hi)) hi = p;
    }
    const a = slice.indexOf(lo);
    const c = slice.indexOf(hi);
    if (lo === hi) out.push(lo);
    else if (a < c) out.push(lo, hi);
    else out.push(hi, lo);
  }
  return out;
}

/** CSV with RFC 4180 quoting. */
export function toCsv(rows: Array<Record<string, unknown>>, columns?: string[]): string {
  const cols = columns ?? Array.from(rows.reduce((set, r) => { Object.keys(r).forEach((k) => set.add(k)); return set; }, new Set<string>()));
  const cell = (v: unknown) => {
    if (v === null || v === undefined) return '';
    const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(','), ...rows.map((r) => cols.map((c) => cell(r[c])).join(','))].join('\r\n');
}

/** Distinguishes a measured zero from "not recorded". */
export type Availability = 'value' | 'zero' | 'unavailable' | 'not_instrumented';
export function availability(v: unknown): Availability {
  if (v === 'not_instrumented') return 'not_instrumented';
  if (v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v))) return 'unavailable';
  if (v === 0) return 'zero';
  return 'value';
}

const SECRET_KEY = /(token|secret|password|passphrase|authorization|cookie|api[_-]?key|credential|dsn|connection[_-]?string)/i;
/** Client-side defence in depth: masks secret-looking keys in log payloads before display. */
export function redact(value: unknown, depth = 0): unknown {
  if (depth > 8) return '[depth limit]';
  if (Array.isArray(value)) return value.map((v) => redact(v, depth + 1));
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) out[k] = SECRET_KEY.test(k) ? '[redacted]' : redact(v, depth + 1);
    return out;
  }
  if (typeof value === 'string') return value.replace(/(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}/g, '$1[redacted]');
  return value;
}
