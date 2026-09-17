import { getRuntimeConfig, type AppConfig, type Engine, type Health, type OperatorLogin } from '@fraudwatch/contracts';
import { useApi, type QueryState } from '@fraudwatch/contracts/react';
import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { api, readStored, setOperatorToken, setViewToken, writeStored } from './api';

const VIEW_KEY = 'fraudwatch.lab.view';
const OPERATOR_KEY = 'fraudwatch.lab.operator';

export interface LabView {
  sessionId: string;
  viewToken: string | null;
  run: string | null;
  engine: Engine | null;
}

/** Reads `?session=&vt=&run=&engine=` from a Live share link once at start-up. */
function viewFromUrl(): LabView | null {
  if (typeof window === 'undefined') return null;
  const q = new URLSearchParams(window.location.search);
  const sessionId = q.get('session');
  const run = q.get('run');
  if (!sessionId && !run) return null;
  const engine = q.get('engine');
  return { sessionId: sessionId ?? '', viewToken: q.get('vt'), run, engine: engine === 'flink' || engine === 'spark' ? engine : null };
}

interface LabContextValue {
  config: QueryState<AppConfig> & { refetch: () => void };
  health: QueryState<Health> & { refetch: () => void };
  view: LabView | null;
  clearView: () => void;
  operator: OperatorLogin | null;
  login: (passphrase: string) => Promise<void>;
  logout: () => void;
  liveUrl: string | null;
  /** Live link that carries the shared session context back, when present. */
  liveLink: (path?: string) => string | null;
}

const Ctx = createContext<LabContextValue | null>(null);

export function useLab(): LabContextValue {
  const v = useContext(Ctx);
  if (!v) throw new Error('useLab must be used inside <LabProvider>');
  return v;
}

export function LabProvider({ children }: { children: ReactNode }) {
  const [view, setView] = useState<LabView | null>(() => {
    const fromUrl = viewFromUrl();
    if (fromUrl) {
      writeStored(VIEW_KEY, fromUrl);
      return fromUrl;
    }
    return readStored<LabView>(VIEW_KEY);
  });
  const [operator, setOperator] = useState<OperatorLogin | null>(() => {
    const stored = readStored<OperatorLogin>(OPERATOR_KEY);
    return stored && stored.expires_at > Date.now() ? stored : null;
  });
  setViewToken(view?.viewToken ?? null);
  setOperatorToken(operator?.token ?? null);

  // Drop the operator token when it expires.
  useEffect(() => {
    if (!operator) return;
    const ms = operator.expires_at - Date.now();
    const h = setTimeout(() => {
      writeStored(OPERATOR_KEY, null);
      setOperator(null);
    }, Math.max(0, Math.min(ms, 2 ** 31 - 1)));
    return () => clearTimeout(h);
  }, [operator]);

  const config = useApi(() => api.getConfig(), []);
  const health = useApi((signal) => api.getHealth(signal), [], { refreshMs: 15000 });

  const login = useCallback(async (passphrase: string) => {
    const res = await api.operatorLogin(passphrase);
    writeStored(OPERATOR_KEY, res);
    setOperatorToken(res.token);
    setOperator(res);
  }, []);
  const logout = useCallback(() => {
    writeStored(OPERATOR_KEY, null);
    setOperatorToken(null);
    setOperator(null);
  }, []);
  const clearView = useCallback(() => {
    writeStored(VIEW_KEY, null);
    setView(null);
    const url = new URL(window.location.href);
    ['session', 'vt', 'run', 'engine', 'action'].forEach((k) => url.searchParams.delete(k));
    window.history.replaceState(null, '', url.toString());
  }, []);

  const liveUrl = config.data?.live_url ?? getRuntimeConfig().liveUrl;
  const liveLink = useCallback(
    (path = '/') => {
      if (!liveUrl) return null;
      const url = new URL(path, liveUrl.endsWith('/') ? liveUrl : `${liveUrl}/`);
      if (view?.sessionId && view.viewToken) {
        url.searchParams.set('session', view.sessionId);
        url.searchParams.set('vt', view.viewToken);
        if (view.run) url.searchParams.set('run', view.run);
        if (view.engine) url.searchParams.set('engine', view.engine);
      }
      return url.toString();
    },
    [liveUrl, view],
  );

  return <Ctx.Provider value={{ config, health, view, clearView, operator, login, logout, liveUrl, liveLink }}>{children}</Ctx.Provider>;
}
