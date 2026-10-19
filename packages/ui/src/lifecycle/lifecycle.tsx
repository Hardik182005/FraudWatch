import { ApiError, LifecycleClient, type LifecycleHistoryItem, type LifecycleStatus } from '@fraudwatch/contracts';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { formatMs, formatRelativeTime, formatTimestamp } from '../format';
import { useNow } from '../hooks';
import { VizEmpty } from '../viz/VizFrame';

export type WakePhase = 'local' | 'checking' | 'sleeping' | 'starting_vm' | 'starting_services' | 'restoring' | 'ready' | 'stopping' | 'failed' | 'unreachable';

export const WAKE_STEPS: Array<{ id: WakePhase; label: string }> = [
  { id: 'sleeping', label: 'Sleeping' },
  { id: 'starting_vm', label: 'Starting VM' },
  { id: 'starting_services', label: 'Starting services' },
  { id: 'restoring', label: 'Restoring processing' },
  { id: 'ready', label: 'Ready' },
];

const KNOWN: WakePhase[] = ['sleeping', 'starting_vm', 'starting_services', 'restoring', 'ready', 'stopping', 'failed'];

function toPhase(state: string | null | undefined): WakePhase {
  return state && (KNOWN as string[]).includes(state) ? (state as WakePhase) : 'checking';
}

/** Extracts a wake state from an API 503 `{error:{code:"pipeline_<state>", lifecycle}}`. */
export function wakePhaseFromError(error: unknown): WakePhase | null {
  if (!(error instanceof ApiError) || !error.isPipelineWaking) return null;
  const fromLifecycle = typeof error.lifecycle?.state === 'string' ? (error.lifecycle.state as string) : null;
  const fromCode = error.code?.replace(/^pipeline_/, '') ?? null;
  const p = toPhase(fromLifecycle ?? fromCode);
  return p === 'checking' ? 'starting_services' : p;
}

export interface LifecycleView {
  phase: WakePhase;
  status: LifecycleStatus | null;
  error: string | null;
  /** True when the controller is configured (cloud deployment). */
  managed: boolean;
  retry: () => void;
  refresh: () => void;
  client: LifecycleClient | null;
}

/**
 * Cloud wake orchestration. Calls ensure-ready once per page load (and per retry) when a controller URL is
 * configured and the document is visible, with or without a demo session: a first-time visitor has no
 * session yet, and every API call is a 503 until the VM is up. Then polls status until ready or failed.
 * Without a controller (local development) the phase is `local` and nothing is called.
 * `onReady` fires when the phase moves into `ready` from a non-ready state (a wake just finished).
 */
export function useLifecycle({
  controlUrl,
  sessionId = null,
  pollMs = 3000,
  onReady,
}: {
  controlUrl: string | null | undefined;
  sessionId?: string | null;
  pollMs?: number;
  onReady?: () => void;
}): LifecycleView {
  const client = useMemo(() => (controlUrl ? new LifecycleClient(controlUrl) : null), [controlUrl]);
  const [status, setStatus] = useState<LifecycleStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [phase, setPhase] = useState<WakePhase>(client ? 'checking' : 'local');
  const [attempt, setAttempt] = useState(0);
  const ensured = useRef<string | null>(null);

  const apply = useCallback((s: LifecycleStatus) => {
    setStatus(s);
    setError(null);
    setPhase(toPhase(s.state));
  }, []);

  // ensure-ready: once per attempt, only while visible. The session id is informational for the controller.
  const sessionRef = useRef(sessionId);
  sessionRef.current = sessionId;
  useEffect(() => {
    if (!client) return;
    const key = String(attempt);
    const run = () => {
      if (document.visibilityState !== 'visible' || ensured.current === key) return;
      ensured.current = key;
      client
        .ensureReady(sessionRef.current ?? null)
        .then((s) => s && apply(s))
        .catch((e: unknown) => {
          setError(e instanceof Error ? e.message : 'Could not reach the wake controller');
          setPhase('unreachable');
        });
    };
    run();
    document.addEventListener('visibilitychange', run);
    return () => document.removeEventListener('visibilitychange', run);
  }, [client, attempt, apply]);

  // Announce a completed wake once per transition so data views can reload.
  const prevPhase = useRef<WakePhase>(phase);
  const onReadyRef = useRef(onReady);
  onReadyRef.current = onReady;
  useEffect(() => {
    const prev = prevPhase.current;
    prevPhase.current = phase;
    if (phase === 'ready' && prev !== 'ready' && prev !== 'checking' && prev !== 'local') onReadyRef.current?.();
  }, [phase]);

  const refresh = useCallback(() => {
    if (!client) return;
    client
      .status()
      .then(apply)
      .catch((e: unknown) => {
        setError(e instanceof Error ? e.message : 'Could not reach the wake controller');
        setPhase('unreachable');
      });
  }, [client, apply]);

  // Poll quickly while transitioning, slowly once settled.
  useEffect(() => {
    if (!client) return;
    refresh();
    const settled = phase === 'ready' || phase === 'failed' || phase === 'sleeping';
    const h = setInterval(() => document.visibilityState === 'visible' && refresh(), settled ? 60000 : pollMs);
    return () => clearInterval(h);
  }, [client, refresh, phase, pollMs]);

  const retry = useCallback(() => {
    setError(null);
    setPhase('checking');
    setAttempt((a) => a + 1);
  }, []);

  return { phase: client ? phase : 'local', status, error, managed: !!client, retry, refresh, client };
}

/** Sends at most one activity ping per `throttleMs` after real pointer/keyboard interaction. */
export function useActivityPings(client: LifecycleClient | null, sessionId: string | null | undefined, throttleMs = 60000) {
  const sessionRef = useRef(sessionId ?? null);
  sessionRef.current = sessionId ?? null;
  useEffect(() => {
    if (!client) return;
    let last = 0;
    const on = (e: Event) => {
      if (!e.isTrusted || document.visibilityState !== 'visible') return;
      const now = Date.now();
      if (now - last < throttleMs) return;
      last = now;
      client.activity(sessionRef.current, e.type === 'keydown' ? 'keyboard' : 'pointer').catch(() => undefined);
    };
    window.addEventListener('pointerdown', on, { passive: true });
    window.addEventListener('keydown', on);
    return () => {
      window.removeEventListener('pointerdown', on);
      window.removeEventListener('keydown', on);
    };
  }, [client, throttleMs]);
}

/** Wake progress banner. Indeterminate progress only; never a fake percentage. */
export function WakeBanner({ view }: { view: LifecycleView }) {
  const now = useNow(15000);
  const { phase, status, error } = view;
  if (phase === 'local' || phase === 'checking') return null;
  if (phase === 'ready') {
    const deadline = status?.idle_deadline_at ?? null;
    if (deadline && deadline - now < 5 * 60000 && deadline > now) {
      return (
        <div className="fw-wake fw-wake-idle" role="status" data-testid="idle-notice">
          Cloud demo will sleep {formatRelativeTime(deadline, now).replace(' ago', '')} after inactivity. Interacting keeps it awake.
        </div>
      );
    }
    return null;
  }
  const current = WAKE_STEPS.findIndex((s) => s.id === phase);
  const failed = phase === 'failed' || phase === 'unreachable';
  return (
    <div className="fw-wake" role="status" aria-live="polite" data-testid="wake-banner" data-phase={phase}>
      <div className="fw-wake-head">
        <strong>
          {phase === 'sleeping' && 'The cloud demo is asleep'}
          {phase === 'stopping' && 'The cloud demo is going to sleep'}
          {phase === 'failed' && 'Wake-up failed'}
          {phase === 'unreachable' && 'Wake controller unreachable'}
          {current > 0 && current < 4 && 'Waking the cloud demo'}
        </strong>
        {status?.wake_started_at && !failed && <span className="fw-small fw-muted"> · started {formatRelativeTime(status.wake_started_at, now)}</span>}
        {status?.last_wake_duration_ms != null && !failed && <span className="fw-small fw-muted"> · last wake took {formatMs(status.last_wake_duration_ms)}</span>}
      </div>
      <ol className="fw-wake-steps">
        {WAKE_STEPS.map((s, i) => (
          <li key={s.id} data-state={failed ? (i <= Math.max(current, 0) ? 'failed' : 'todo') : i < current ? 'done' : i === current ? 'current' : 'todo'} aria-current={i === current ? 'step' : undefined}>
            {s.label}
          </li>
        ))}
      </ol>
      {!failed && phase !== 'sleeping' && <div className="fw-indeterminate" aria-hidden="true" />}
      {phase === 'stopping' && <p className="fw-small fw-muted">Stage: {status?.stage ?? 'draining'}. It will wake again when requested.</p>}
      {failed && (
        <p className="fw-small">
          {status?.error ?? error ?? 'The controller reported a failure.'}{' '}
          <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={view.retry}>
            Retry
          </button>
        </p>
      )}
      {phase === 'sleeping' && (
        <p className="fw-small">
          <button type="button" className="fw-btn fw-btn-primary fw-btn-sm" onClick={view.retry}>
            Wake the demo
          </button>
        </p>
      )}
    </div>
  );
}

const ALL_STATES = ['sleeping', 'starting_vm', 'starting_services', 'restoring', 'ready', 'stopping', 'sleeping'];
const STATE_LABEL: Record<string, string> = {
  sleeping: 'Sleeping',
  starting_vm: 'Starting VM',
  starting_services: 'Starting services',
  restoring: 'Restoring',
  ready: 'Ready',
  stopping: 'Draining',
  failed: 'Failed',
};

/** Cloud lifecycle diagram + recorded history from the controller. */
export function LifecycleTimeline({ controlUrl }: { controlUrl: string | null | undefined }) {
  const client = useMemo(() => (controlUrl ? new LifecycleClient(controlUrl) : null), [controlUrl]);
  const [status, setStatus] = useState<LifecycleStatus | null>(null);
  const [items, setItems] = useState<LifecycleHistoryItem[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    if (!client) return;
    let alive = true;
    const load = () => {
      Promise.all([client.status(), client.history(40).catch(() => ({ items: [] as LifecycleHistoryItem[] }))])
        .then(([s, h]) => {
          if (!alive) return;
          setStatus(s);
          setItems(h?.items ?? []);
          setErr(null);
        })
        .catch((e: unknown) => alive && setErr(e instanceof Error ? e.message : 'Unavailable'));
    };
    load();
    const h = setInterval(load, 15000);
    return () => {
      alive = false;
      clearInterval(h);
    };
  }, [client]);

  const current = status?.state ?? null;
  return (
    <figure className="fw-viz" data-testid="lifecycle-timeline" aria-labelledby="fw-lc-title">
      <div className="fw-viz-head">
        <div className="fw-viz-titles">
          <h3 id="fw-lc-title" className="fw-viz-title">
            Cloud lifecycle
          </h3>
          <div className="fw-viz-meta">
            <span className="fw-source" data-kind={client ? 'live' : 'illustration'} data-testid="viz-source">
              <span className="fw-source-dot" aria-hidden="true" />
              {client ? 'Live measurements' : 'Illustration'}
            </span>
            <span className="fw-small fw-muted">{client ? `controller status${status?.as_of ? ` as of ${formatTimestamp(status.as_of)}` : ''}` : 'no wake controller configured (local run)'}</span>
          </div>
        </div>
      </div>
      <ol className="fw-lc-flow" aria-label="Lifecycle states">
        {ALL_STATES.map((s, i) => (
          <li key={i} data-current={(current === s && (i < 6 || current !== 'sleeping')) || undefined}>
            {STATE_LABEL[s]}
          </li>
        ))}
      </ol>
      {!client ? (
        <p className="fw-small fw-muted">In the cloud deployment a controller starts the VM on the first visit and stops it after the idle timeout. Locally the stack runs continuously, so there is nothing to show.</p>
      ) : err ? (
        <VizEmpty kind="unavailable" title="Controller unreachable">{err}</VizEmpty>
      ) : (
        <>
          <dl className="fw-kv fw-lc-kv">
            <div><dt>State</dt><dd>{current ? STATE_LABEL[current] ?? current : 'Checking'}{status?.stage ? ` (${status.stage})` : ''}</dd></div>
            <div><dt>Idle timeout</dt><dd>{status?.idle_timeout_minutes != null ? `${status.idle_timeout_minutes} min` : 'Not reported'}</dd></div>
            <div><dt>Last activity</dt><dd>{status?.last_activity_at ? formatTimestamp(status.last_activity_at, true) : 'Not recorded'}</dd></div>
            <div><dt>Sleeps after</dt><dd>{status?.idle_deadline_at ? formatTimestamp(status.idle_deadline_at, true) : 'Not scheduled'}</dd></div>
            <div><dt>Last wake duration</dt><dd>{status?.last_wake_duration_ms != null ? formatMs(status.last_wake_duration_ms) : 'Not recorded'}</dd></div>
            <div><dt>Keep awake until</dt><dd>{status?.keep_awake_until ? formatTimestamp(status.keep_awake_until, true) : 'Not set'}</dd></div>
            <div><dt>VM status (observed)</dt><dd>{status?.vm_status_observed ?? 'Not reported'}</dd></div>
          </dl>
          {items && items.length > 0 ? (
            <ol className="fw-tl-list" aria-label="Lifecycle history">
              {items.slice(0, 20).map((it, i) => (
                <li key={i} data-kind={String(it.event ?? '').includes('fail') ? 'error' : 'info'}>
                  <span className="fw-tl-dot" aria-hidden="true" />
                  <strong>{String(it.event ?? 'event').replace(/_/g, ' ')}</strong>
                  <span className="fw-num fw-small fw-muted"> {formatTimestamp(it.at, true)}</span>
                </li>
              ))}
            </ol>
          ) : (
            <p className="fw-small fw-muted">No lifecycle history recorded yet.</p>
          )}
        </>
      )}
    </figure>
  );
}
