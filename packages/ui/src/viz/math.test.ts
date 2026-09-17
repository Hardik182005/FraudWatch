import { describe, expect, it } from 'vitest';
import { availability, confusionMetrics, downsampleMinMax, histogram, percentile, reconcileConfusion, redact, rowNormalize, toCsv } from './math';

const cm = { tn: 10427, fp: 210, fn: 264, tp: 60 };

describe('confusion matrix', () => {
  it('computes metrics from the same cells', () => {
    const m = confusionMetrics(cm);
    expect(m.total).toBe(10961);
    expect(m.actualFraud).toBe(324);
    expect(m.precision).toBeCloseTo(60 / 270, 10);
    expect(m.recall).toBeCloseTo(60 / 324, 10);
    expect(m.f1).toBeCloseTo(0.202020202020202, 10);
  });
  it('row-normalizes by actual class and handles empty rows', () => {
    const r = rowNormalize(cm);
    expect(r.tn! + r.fp!).toBeCloseTo(1, 12);
    expect(r.fn! + r.tp!).toBeCloseTo(1, 12);
    const empty = rowNormalize({ tn: 5, fp: 5, fn: 0, tp: 0 });
    expect(empty.fn).toBeNull();
    expect(empty.tp).toBeNull();
  });
  it('handles zero denominators', () => {
    const m = confusionMetrics({ tn: 3, fp: 0, fn: 0, tp: 0 });
    expect(m.precision).toBeNull();
    expect(m.recall).toBeNull();
    expect(m.f1).toBeNull();
  });
  it('reconciles against reported artifact values', () => {
    expect(reconcileConfusion(cm, { n: 10961, positives: 324, precision: 0.2222222222222222, recall: 0.18518518518518517, f1: 0.202020202020202 }).ok).toBe(true);
    const bad = reconcileConfusion(cm, { n: 11000, positives: 324 });
    expect(bad.ok).toBe(false);
    expect(bad.problems[0]).toMatch(/10961/);
  });
});

describe('histogram & percentile', () => {
  it('bins with an inclusive top edge and skips invalid values', () => {
    const h = histogram([0, 0.05, 0.1, 0.99, 1, null, Number.NaN, 2], 10);
    expect(h.map((b) => b.count)).toEqual([2, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
  });
  it('uses nearest rank', () => {
    expect(percentile([5, 1, 3, 2, 4], 50)).toBe(3);
    expect(percentile([5, 1, 3, 2, 4], 95)).toBe(5);
    expect(percentile([], 50)).toBeNull();
  });
});

describe('downsampleMinMax', () => {
  it('preserves spikes', () => {
    const pts = Array.from({ length: 1000 }, (_, i) => ({ i, v: i === 777 ? 999 : 1 }));
    const out = downsampleMinMax(pts, 50, (p) => p.v);
    expect(out.length).toBeLessThanOrEqual(100);
    expect(out.some((p) => p.v === 999)).toBe(true);
  });
});

describe('toCsv, availability, redact', () => {
  it('quotes CSV cells', () => {
    expect(toCsv([{ a: 'x,y', b: 'say "hi"', c: null }])).toBe('a,b,c\r\n"x,y","say ""hi""",');
  });
  it('separates zero from unavailable', () => {
    expect(availability(0)).toBe('zero');
    expect(availability(null)).toBe('unavailable');
    expect(availability('not_instrumented')).toBe('not_instrumented');
    expect(availability(3)).toBe('value');
  });
  it('masks secret-looking keys and bearer tokens', () => {
    expect(redact({ token: 'abc', nested: { Authorization: 'x', ok: 1 }, msg: 'Bearer abcdefghijkl' })).toEqual({
      token: '[redacted]',
      nested: { Authorization: '[redacted]', ok: 1 },
      msg: 'Bearer [redacted]',
    });
  });
});
