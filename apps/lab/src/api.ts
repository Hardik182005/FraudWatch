import { FraudWatchClient, getRuntimeConfig } from '@fraudwatch/contracts';

/**
 * Credentials for this tab only (sessionStorage via lab.tsx): an optional view token from a Live share link,
 * and an optional operator token after login. Neither is written to localStorage.
 */
const auth: { viewToken: string | null; operatorToken: string | null } = { viewToken: null, operatorToken: null };

export function setViewToken(token: string | null) {
  auth.viewToken = token;
}
export function setOperatorToken(token: string | null) {
  auth.operatorToken = token;
}

export const api = new FraudWatchClient({
  baseUrl: getRuntimeConfig().apiBaseUrl,
  getToken: () => auth.viewToken,
  getOperatorToken: () => auth.operatorToken,
});

export function readStored<T>(key: string): T | null {
  try {
    const raw = window.sessionStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

export function writeStored(key: string, value: unknown) {
  try {
    if (value === null) window.sessionStorage.removeItem(key);
    else window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* storage unavailable: state lasts for this page view only */
  }
}
