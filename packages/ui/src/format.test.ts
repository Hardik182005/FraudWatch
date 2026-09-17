import { describe, expect, it } from 'vitest';
import { DASH, formatINR, formatInt, formatMs, formatPct, formatRelativeTime, formatScore, formatValue, humanize, shortId } from './format';
import { nextPreference, readPreference, resolveTheme, writePreference, applyTheme, THEME_STORAGE_KEY, type StorageLike } from './theme';

describe('formatINR', () => {
  it('uses Indian grouping and the rupee symbol', () => {
    expect(formatINR(123456.5)).toBe('₹1,23,456.50');
    expect(formatINR(0)).toBe('₹0.00');
    expect(formatINR(10000000)).toBe('₹1,00,00,000.00');
  });
  it('handles missing values', () => {
    expect(formatINR(null)).toBe(DASH);
    expect(formatINR(Number.NaN)).toBe(DASH);
  });
  it('supports compact notation', () => {
    expect(formatINR(1250000, { compact: true })).toMatch(/₹12\.5\s?L/);
  });
});

describe('formatMs', () => {
  it('formats sub-ms, ms, seconds and minutes', () => {
    expect(formatMs(0.456)).toBe('0.46 ms');
    expect(formatMs(4.25)).toBe('4.3 ms');
    expect(formatMs(245.4)).toBe('245 ms');
    expect(formatMs(1500)).toBe('1.50 s');
    expect(formatMs(15500)).toBe('15.5 s');
    expect(formatMs(125000)).toBe('2 min 5 s');
    expect(formatMs(-20)).toBe('-20 ms');
    expect(formatMs(undefined)).toBe(DASH);
  });
});

describe('formatRelativeTime', () => {
  const now = 1_800_000_000_000;
  it('describes past and future', () => {
    expect(formatRelativeTime(now - 2000, now)).toBe('just now');
    expect(formatRelativeTime(now - 30_000, now)).toBe('30 sec. ago');
    expect(formatRelativeTime(now - 5 * 60_000, now)).toBe('5 min. ago');
    expect(formatRelativeTime(now + 2 * 3_600_000, now)).toBe('in 2 hr.');
    expect(formatRelativeTime(null, now)).toBe(DASH);
  });
});

describe('misc formatters', () => {
  it('formats numbers, scores and percentages', () => {
    expect(formatInt(1234567)).toBe('12,34,567');
    expect(formatScore(0.12099)).toBe('0.121');
    expect(formatPct(0.0348)).toBe('3.5%');
    expect(shortId('abcdef1234567')).toBe('abcdef12');
    expect(humanize('prior_count_60s')).toBe('Prior count 60s');
    expect(formatValue(true)).toBe('yes');
    expect(formatValue(null)).toBe(DASH);
  });
});

describe('theme logic', () => {
  function mem(): StorageLike & { data: Record<string, string> } {
    const data: Record<string, string> = {};
    return { data, getItem: (k) => data[k] ?? null, setItem: (k, v) => void (data[k] = v) };
  }
  it('defaults to system and resolves from the OS preference', () => {
    const s = mem();
    expect(readPreference(s)).toBe('system');
    expect(resolveTheme('system', true)).toBe('dark');
    expect(resolveTheme('system', false)).toBe('light');
    expect(resolveTheme('light', true)).toBe('light');
  });
  it('persists a valid preference and ignores garbage', () => {
    const s = mem();
    writePreference(s, 'dark');
    expect(s.data[THEME_STORAGE_KEY]).toBe('dark');
    expect(readPreference(s)).toBe('dark');
    s.data[THEME_STORAGE_KEY] = 'neon';
    expect(readPreference(s)).toBe('system');
  });
  it('survives blocked storage', () => {
    const blocked: StorageLike = {
      getItem: () => {
        throw new Error('blocked');
      },
      setItem: () => {
        throw new Error('blocked');
      },
    };
    expect(readPreference(blocked)).toBe('system');
    expect(() => writePreference(blocked, 'light')).not.toThrow();
  });
  it('cycles preferences and applies attributes', () => {
    expect(nextPreference('system')).toBe('light');
    expect(nextPreference('light')).toBe('dark');
    expect(nextPreference('dark')).toBe('system');
    const attrs: Record<string, string> = {};
    applyTheme({ setAttribute: (n, v) => void (attrs[n] = v) }, 'dark', 'system');
    expect(attrs).toEqual({ 'data-theme': 'dark', 'data-theme-preference': 'system' });
  });
});
