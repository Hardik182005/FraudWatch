import { useCallback, useEffect, useRef, useState } from 'react';
import { ApiError } from './client';
import { StreamClient, type StreamStatus } from './sse';
import type { StreamEventMap, StreamEventType } from './types';

export type QueryState<T> =
  | { status: 'loading'; data: T | undefined; error: null }
  | { status: 'success'; data: T; error: null }
  | { status: 'error'; data: T | undefined; error: unknown };

/** Window event fired when the cloud backend has just become ready (after a wake). Every useApi refetches. */
export const BACKEND_READY_EVENT = 'fraudwatch:backend-ready';

export function notifyBackendReady(): void {
  if (typeof window !== 'undefined') window.dispatchEvent(new Event(BACKEND_READY_EVENT));
}

export interface UseApiOptions {
  enabled?: boolean;
  /** Poll interval in ms (keeps last good data while refetching). */
  refreshMs?: number;
}

/**
 * Small data hook: loading -> success | error, with refetch and optional polling.
 * Data always comes from the real API; nothing is substituted on failure.
 */
export function useApi<T>(fetcher: (signal: AbortSignal) => Promise<T>, deps: unknown[], options: UseApiOptions = {}) {
  const { enabled = true, refreshMs } = options;
  const [state, setState] = useState<QueryState<T>>({ status: 'loading', data: undefined, error: null });
  const [tick, setTick] = useState(0);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const dataRef = useRef<T | undefined>(undefined);

  const refetch = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    if (dataRef.current === undefined) setState({ status: 'loading', data: undefined, error: null });
    fetcherRef
      .current(controller.signal)
      .then((data) => {
        if (controller.signal.aborted) return;
        dataRef.current = data;
        setState({ status: 'success', data, error: null });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || (error instanceof ApiError && error.kind === 'aborted')) return;
        setState({ status: 'error', data: dataRef.current, error });
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, tick, ...deps]);

  // Reset cached data when identity deps change.
  const depsKey = JSON.stringify(deps);
  const lastDepsKey = useRef(depsKey);
  if (lastDepsKey.current !== depsKey) {
    lastDepsKey.current = depsKey;
    dataRef.current = undefined;
  }

  // A wake that completes while this view is open reloads it, instead of leaving a stale "waking" error.
  useEffect(() => {
    if (!enabled || typeof window === 'undefined') return;
    const on = () => setTick((t) => t + 1);
    window.addEventListener(BACKEND_READY_EVENT, on);
    return () => window.removeEventListener(BACKEND_READY_EVENT, on);
  }, [enabled]);

  useEffect(() => {
    if (!enabled || !refreshMs) return;
    const h = setInterval(() => {
      if (typeof document === 'undefined' || document.visibilityState !== 'hidden') setTick((t) => t + 1);
    }, refreshMs);
    return () => clearInterval(h);
  }, [enabled, refreshMs]);

  return { ...state, refetch };
}

export interface UseStreamArgs {
  enabled: boolean;
  buildUrl: (cursor: string | null) => string;
  onEvent: <K extends StreamEventType>(type: K, data: StreamEventMap[K]) => void;
  onSnapshotNeeded?: (reason: string) => void;
  resetKey: string;
}

export function useEventStream({ enabled, buildUrl, onEvent, onSnapshotNeeded, resetKey }: UseStreamArgs) {
  const [status, setStatus] = useState<StreamStatus>('idle');
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const [nextRetryMs, setNextRetryMs] = useState<number | null>(null);
  const handlers = useRef({ onEvent, onSnapshotNeeded, buildUrl });
  handlers.current = { onEvent, onSnapshotNeeded, buildUrl };

  useEffect(() => {
    if (!enabled || typeof EventSource === 'undefined') {
      setStatus(enabled ? 'closed' : 'idle');
      return;
    }
    const client = new StreamClient({
      buildUrl: (c) => handlers.current.buildUrl(c),
      onEvent: (type, data) => {
        setLastMessageAt(Date.now());
        handlers.current.onEvent(type, data);
      },
      onStatus: (s, info) => {
        setStatus(s);
        setNextRetryMs(info.nextRetryMs);
      },
      onSnapshotNeeded: (reason) => handlers.current.onSnapshotNeeded?.(reason),
    });
    client.start();
    return () => client.stop();
  }, [enabled, resetKey]);

  return { status, lastMessageAt, nextRetryMs };
}
