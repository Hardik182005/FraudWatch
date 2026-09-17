import { ApiError, type AppConfig, type Engine, type Health, type Session, type StreamEventMap, type StreamEventType } from '@fraudwatch/contracts';
import { useApi, useEventStream, type QueryState } from '@fraudwatch/contracts/react';
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { api, readStored, setAuthToken, writeStored } from './api';

const SESSION_KEY = 'fraudwatch.live.session';
const VIEW_KEY = 'fraudwatch.live.view';

interface StoredSession {
  id: string;
  token: string;
}

export interface ViewContext {
  sessionId: string;
  viewToken: string;
  run: string | null;
  engine: Engine | null;
  action: string | null;
}

/** Reads `?session=&vt=&run=&engine=&action=` (a share link) once at start-up. */
function viewFromUrl(): ViewContext | null {
  if (typeof window === 'undefined') return null;
  const q = new URLSearchParams(window.location.search);
  const sessionId = q.get('session');
  const viewToken = q.get('vt');
  if (!sessionId || !viewToken) return null;
  const engine = q.get('engine');
  return {
    sessionId,
    viewToken,
    run: q.get('run'),
    engine: engine === 'flink' || engine === 'spark' ? engine : null,
    action: q.get('action'),
  };
}

type Listener = <K extends StreamEventType>(type: K, data: StreamEventMap[K]) => void;

interface SessionContextValue {
  config: QueryState<AppConfig> & { refetch: () => void };
  mode: 'none' | 'owner' | 'view';
  sessionId: string | null;
  token: string | null;
  view: ViewContext | null;
  session: Session | undefined;
  sessionState: QueryState<Session> & { refetch: () => void };
  startDemo: () => Promise<void>;
  starting: boolean;
  startError: unknown;
  resetSession: () => Promise<void>;
  exitView: () => void;
  engine: Engine;
  setEngine: (e: Engine) => void;
  runId: string | null;
  setRunId: (id: string | null) => void;
  stream: ReturnType<typeof useEventStream> & { reconnect: () => void };
  subscribe: (fn: Listener) => () => void;
  health: QueryState<Health> & { refetch: () => void };
}

const Ctx = createContext<SessionContextValue | null>(null);

export function useSession(): SessionContextValue {
  const v = useContext(Ctx);
  if (!v) throw new Error('useSession must be used inside <SessionProvider>');
  return v;
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const [view, setView] = useState<ViewContext | null>(() => {
    const fromUrl = viewFromUrl();
    if (fromUrl) {
      writeStored(VIEW_KEY, fromUrl);
      return fromUrl;
    }
    return readStored<ViewContext>(VIEW_KEY);
  });
  const [owned, setOwned] = useState<StoredSession | null>(() => readStored<StoredSession>(SESSION_KEY));
  const mode: SessionContextValue['mode'] = view ? 'view' : owned ? 'owner' : 'none';
  const sessionId = view?.sessionId ?? owned?.id ?? null;
  const token = view?.viewToken ?? owned?.token ?? null;
  setAuthToken(token);

  const [engine, setEngine] = useState<Engine>(view?.engine ?? 'flink');
  const [runOverride, setRunId] = useState<string | null>(view?.run ?? null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<unknown>(null);

  const config = useApi((signal) => api.request<AppConfig>('/config', { signal }), []);
  const health = useApi((signal) => api.getHealth(signal), [], { refreshMs: 15000 });
  const sessionState = useApi((signal) => api.request<Session>(`/sessions/${encodeURIComponent(sessionId ?? '')}`, { signal, token }), [sessionId, token], {
    enabled: !!sessionId,
    refreshMs: 10000,
  });

  // A stored session that the API no longer accepts (expired / unknown) is dropped; the visitor can start again.
  useEffect(() => {
    const err = sessionState.error;
    if (sessionState.status === 'error' && err instanceof ApiError && (err.isUnauthorized || err.isNotFound)) {
      if (mode === 'owner') {
        writeStored(SESSION_KEY, null);
        setOwned(null);
        setStartError(new ApiError({ kind: 'http', status: err.status, code: err.code, message: 'Your previous demo session has expired. Start a new one.' }));
      }
    }
  }, [sessionState.status, sessionState.error, mode]);

  const startDemo = useCallback(async () => {
    setStarting(true);
    setStartError(null);
    try {
      const res = await api.createSession();
      const stored = { id: res.session.id, token: res.token };
      writeStored(SESSION_KEY, stored);
      writeStored(VIEW_KEY, null);
      setView(null);
      setOwned(stored);
      setRunId(null);
    } catch (e) {
      setStartError(e);
    } finally {
      setStarting(false);
    }
  }, []);

  const resetSession = useCallback(async () => {
    const res = await api.resetSession();
    const stored = { id: res.session.id, token: res.token };
    writeStored(SESSION_KEY, stored);
    setOwned(stored);
    setRunId(null);
  }, []);

  const exitView = useCallback(() => {
    writeStored(VIEW_KEY, null);
    setView(null);
    setRunId(null);
    const url = new URL(window.location.href);
    ['session', 'vt', 'run', 'engine', 'action'].forEach((k) => url.searchParams.delete(k));
    window.history.replaceState(null, '', url.toString());
  }, []);

  /* ---------- Live stream (one EventSource per session, shared by all views) ---------- */
  const listeners = useRef(new Set<Listener>());
  const subscribe = useCallback((fn: Listener) => {
    listeners.current.add(fn);
    return () => {
      listeners.current.delete(fn);
    };
  }, []);
  const [streamGen, setStreamGen] = useState(0);
  const [streamHealth, setStreamHealth] = useState<Health | null>(null);
  const refetchSession = sessionState.refetch;
  const stream = useEventStream({
    enabled: !!sessionId && !!token,
    resetKey: `${sessionId}:${token}:${streamGen}`,
    buildUrl: (cursor) => api.streamUrl(sessionId ?? '', token ?? '', cursor),
    onEvent: (type, data) => {
      if (type === 'health') setStreamHealth(data as Health);
      if (type === 'run') refetchSession();
      listeners.current.forEach((fn) => fn(type, data));
    },
    onSnapshotNeeded: (reason) => {
      listeners.current.forEach((fn) => fn('reset', { reason }));
      refetchSession();
    },
  });

  const session = sessionState.status === 'error' && !sessionId ? undefined : sessionState.data;
  const runId = runOverride ?? session?.active_run_id ?? null;

  // Health: prefer the newest of the polled snapshot and SSE health events.
  const healthValue = useMemo(() => {
    if (streamHealth && (!health.data || streamHealth.as_of > health.data.as_of)) {
      return { ...health, status: 'success' as const, data: streamHealth, error: null };
    }
    return health;
  }, [health, streamHealth]) as SessionContextValue['health'];

  const value: SessionContextValue = {
    config,
    mode,
    sessionId,
    token,
    view,
    session: sessionId ? session : undefined,
    sessionState,
    startDemo,
    starting,
    startError,
    resetSession,
    exitView,
    engine,
    setEngine,
    runId,
    setRunId,
    stream: { ...stream, reconnect: () => setStreamGen((g) => g + 1) },
    subscribe,
    health: healthValue,
  };
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

/** Subscribe to live stream events for the lifetime of a component. */
export function useStreamListener(fn: Listener) {
  const { subscribe } = useSession();
  const ref = useRef(fn);
  ref.current = fn;
  useEffect(() => subscribe((type, data) => ref.current(type, data)), [subscribe]);
}
