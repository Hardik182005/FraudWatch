const inr = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR' });
const inrCompact = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', notation: 'compact', maximumFractionDigits: 1 });
const intFmt = new Intl.NumberFormat('en-IN');

export const DASH = '—';

function isNum(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

/** INR using Indian digit grouping, e.g. ₹1,23,456.50 */
export function formatINR(value: number | null | undefined, opts: { compact?: boolean } = {}): string {
  if (!isNum(value)) return DASH;
  return opts.compact ? inrCompact.format(value) : inr.format(value);
}

export function formatInt(value: number | null | undefined): string {
  if (!isNum(value)) return DASH;
  return intFmt.format(Math.round(value));
}

export function formatNumber(value: number | null | undefined, digits = 2): string {
  if (!isNum(value)) return DASH;
  return new Intl.NumberFormat('en-IN', { maximumFractionDigits: digits, minimumFractionDigits: 0 }).format(value);
}

/** Durations in ms: <1 ms with 2 decimals, <1 s as ms, then s / min. */
export function formatMs(ms: number | null | undefined): string {
  if (!isNum(ms)) return DASH;
  const neg = ms < 0;
  const a = Math.abs(ms);
  let out: string;
  if (a < 1) out = `${a.toFixed(2)} ms`;
  else if (a < 1000) out = `${a < 10 ? a.toFixed(1) : Math.round(a)} ms`;
  else if (a < 60_000) out = `${(a / 1000).toFixed(a < 10_000 ? 2 : 1)} s`;
  else if (a < 3_600_000) out = `${Math.floor(a / 60_000)} min ${Math.round((a % 60_000) / 1000)} s`;
  else out = `${Math.floor(a / 3_600_000)} h ${Math.round((a % 3_600_000) / 60_000)} min`;
  return neg ? `-${out}` : out;
}

/** Relative time such as "12 s ago" or "in 3 min". */
export function formatRelativeTime(ts: number | null | undefined, now: number = Date.now()): string {
  if (!isNum(ts)) return DASH;
  const diff = ts - now;
  const a = Math.abs(diff);
  const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto', style: 'short' });
  if (a < 5_000) return 'just now';
  if (a < 60_000) return rtf.format(Math.round(diff / 1000), 'second');
  if (a < 3_600_000) return rtf.format(Math.round(diff / 60_000), 'minute');
  if (a < 86_400_000) return rtf.format(Math.round(diff / 3_600_000), 'hour');
  return rtf.format(Math.round(diff / 86_400_000), 'day');
}

export function formatTimestamp(ts: number | null | undefined, withDate = false): string {
  if (!isNum(ts)) return DASH;
  const d = new Date(ts);
  const time = d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  if (!withDate) return `${time}.${String(d.getMilliseconds()).padStart(3, '0')}`;
  return `${d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })} ${time}`;
}

export function formatScore(score: number | null | undefined, digits = 3): string {
  if (!isNum(score)) return DASH;
  return score.toFixed(digits);
}

export function formatPct(fraction: number | null | undefined, digits = 1): string {
  if (!isNum(fraction)) return DASH;
  return `${(fraction * 100).toFixed(digits)}%`;
}

export function formatEps(eps: number | null | undefined): string {
  if (!isNum(eps)) return DASH;
  return `${eps < 10 ? eps.toFixed(1) : Math.round(eps)} ev/s`;
}

export function shortId(id: string | null | undefined, len = 8): string {
  if (!id) return DASH;
  return id.length <= len ? id : id.slice(0, len);
}

export function humanize(key: string): string {
  const s = key.replace(/_/g, ' ');
  return s.charAt(0).toUpperCase() + s.slice(1);
}

export function formatValue(v: unknown): string {
  if (v === null || v === undefined || v === '') return DASH;
  if (typeof v === 'number') return Number.isInteger(v) ? formatInt(v) : formatNumber(v, 4);
  if (typeof v === 'boolean') return v ? 'yes' : 'no';
  if (typeof v === 'string') return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
