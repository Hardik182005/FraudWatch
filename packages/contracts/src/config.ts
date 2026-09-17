/**
 * Runtime configuration. In containers `/config.js` (written at start from API_BASE_URL, LIVE_APP_URL,
 * LAB_APP_URL) sets window.__FRAUDWATCH_CONFIG__. In dev we fall back to VITE_API_BASE_URL.
 * No secrets ever live here.
 */
export interface RuntimeConfig {
  apiBaseUrl: string;
  liveUrl: string | null;
  labUrl: string | null;
}

declare global {
  interface Window {
    __FRAUDWATCH_CONFIG__?: Partial<RuntimeConfig>;
  }
}

export const DEFAULT_API_BASE_URL = 'http://localhost:8000/api/v1';

function clean(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const v = value.trim();
  // Unsubstituted template placeholders count as missing.
  if (!v || v.startsWith('${') || v.startsWith('__')) return null;
  return v.replace(/\/+$/, '');
}

export function resolveRuntimeConfig(
  injected: Partial<RuntimeConfig> | undefined,
  env: { VITE_API_BASE_URL?: string; VITE_LIVE_APP_URL?: string; VITE_LAB_APP_URL?: string } = {},
): RuntimeConfig {
  return {
    apiBaseUrl: clean(injected?.apiBaseUrl) ?? clean(env.VITE_API_BASE_URL) ?? DEFAULT_API_BASE_URL,
    liveUrl: clean(injected?.liveUrl) ?? clean(env.VITE_LIVE_APP_URL),
    labUrl: clean(injected?.labUrl) ?? clean(env.VITE_LAB_APP_URL),
  };
}

let cached: RuntimeConfig | null = null;

export function getRuntimeConfig(): RuntimeConfig {
  if (cached) return cached;
  const injected = typeof window !== 'undefined' ? window.__FRAUDWATCH_CONFIG__ : undefined;
  let env: Record<string, string | undefined> = {};
  try {
    env = (import.meta as unknown as { env?: Record<string, string | undefined> }).env ?? {};
  } catch {
    env = {};
  }
  cached = resolveRuntimeConfig(injected, env);
  return cached;
}

/** Test helper. */
export function __setRuntimeConfig(cfg: RuntimeConfig | null): void {
  cached = cfg;
}
