import { FraudWatchClient, getRuntimeConfig } from '@fraudwatch/contracts';

/**
 * Credentials for this browser tab. The session/view token and the optional operator token are kept in
 * sessionStorage (tab-scoped, cleared when the tab closes) so a page refresh keeps them. They are
 * credentials, not shared application state, and they are never written to localStorage.
 */
const auth: { token: string | null; operatorToken: string | null } = { token: null, operatorToken: null };

export function setAuthToken(token: string | null) {
  auth.token = token;
}

export function setOperatorToken(token: string | null) {
  auth.operatorToken = token;
}

export const api = new FraudWatchClient({
  baseUrl: getRuntimeConfig().apiBaseUrl,
  getToken: () => auth.token,
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
    /* storage unavailable: the session lasts for this page view only */
  }
}
