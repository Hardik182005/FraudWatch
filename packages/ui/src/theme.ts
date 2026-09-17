import { useCallback, useEffect, useState } from 'react';

export type ThemePreference = 'light' | 'dark' | 'system';
export type ResolvedTheme = 'light' | 'dark';

/** localStorage is used ONLY for this per-viewer display preference (never for shared state). */
export const THEME_STORAGE_KEY = 'fraudwatch.theme';

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export function readPreference(storage: StorageLike | null | undefined): ThemePreference {
  try {
    const v = storage?.getItem(THEME_STORAGE_KEY);
    return v === 'light' || v === 'dark' || v === 'system' ? v : 'system';
  } catch {
    return 'system';
  }
}

export function writePreference(storage: StorageLike | null | undefined, pref: ThemePreference): void {
  try {
    storage?.setItem(THEME_STORAGE_KEY, pref);
  } catch {
    /* storage blocked: preference lasts for this page view only */
  }
}

export function resolveTheme(pref: ThemePreference, systemPrefersDark: boolean): ResolvedTheme {
  if (pref === 'system') return systemPrefersDark ? 'dark' : 'light';
  return pref;
}

export function applyTheme(root: { setAttribute(n: string, v: string): void } | null | undefined, theme: ResolvedTheme, pref: ThemePreference) {
  root?.setAttribute('data-theme', theme);
  root?.setAttribute('data-theme-preference', pref);
}

export function nextPreference(pref: ThemePreference): ThemePreference {
  return pref === 'system' ? 'light' : pref === 'light' ? 'dark' : 'system';
}

function safeStorage(): StorageLike | null {
  try {
    return typeof window !== 'undefined' ? window.localStorage : null;
  } catch {
    return null;
  }
}

const DARK_QUERY = '(prefers-color-scheme: dark)';

/** Call once before first render to avoid a flash of the wrong theme. */
export function initTheme(): void {
  if (typeof window === 'undefined') return;
  const pref = readPreference(safeStorage());
  const dark = window.matchMedia?.(DARK_QUERY).matches ?? false;
  applyTheme(document.documentElement, resolveTheme(pref, dark), pref);
}

export function useTheme() {
  const [preference, setPref] = useState<ThemePreference>(() => readPreference(safeStorage()));
  const [systemDark, setSystemDark] = useState<boolean>(() => (typeof window !== 'undefined' ? window.matchMedia?.(DARK_QUERY).matches ?? false : false));

  useEffect(() => {
    const mq = window.matchMedia?.(DARK_QUERY);
    if (!mq) return;
    const onChange = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, []);

  const resolved = resolveTheme(preference, systemDark);

  useEffect(() => {
    applyTheme(document.documentElement, resolved, preference);
  }, [resolved, preference]);

  const setPreference = useCallback((p: ThemePreference) => {
    writePreference(safeStorage(), p);
    setPref(p);
  }, []);

  return { preference, resolved, setPreference };
}
