import { STREAM_EVENT_TYPES, type StreamEventMap, type StreamEventType } from './types';

export type StreamStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'stale' | 'closed';

/** Minimal EventSource surface so tests can inject a fake. */
export interface EventSourceLike {
  addEventListener(type: string, listener: (ev: MessageEvent) => void): void;
  close(): void;
  onopen: ((ev: Event) => void) | null;
  onerror: ((ev: Event) => void) | null;
}

export interface Timers {
  setTimeout: (fn: () => void, ms: number) => unknown;
  clearTimeout: (handle: unknown) => void;
  setInterval: (fn: () => void, ms: number) => unknown;
  clearInterval: (handle: unknown) => void;
  now: () => number;
}

export interface StreamClientOptions {
  /** Builds the SSE URL for the given cursor (null on first connect). */
  buildUrl: (cursor: string | null) => string;
  createEventSource?: (url: string) => EventSourceLike;
  onEvent?: <K extends StreamEventType>(type: K, data: StreamEventMap[K], cursor: string | null) => void;
  onStatus?: (status: StreamStatus, info: { attempt: number; nextRetryMs: number | null; lastMessageAt: number | null }) => void;
  /** Called when the server sends `reset`, and after a reconnect following a stale/failed stream. */
  onSnapshotNeeded?: (reason: string) => void;
  initialCursor?: string | null;
  minBackoffMs?: number;
  maxBackoffMs?: number;
  staleAfterMs?: number;
  staleCheckMs?: number;
  timers?: Timers;
}

export const MIN_BACKOFF_MS = 1000;
export const MAX_BACKOFF_MS = 30000;
export const STALE_AFTER_MS = 30000;

/** Exponential backoff: 1 s, 2 s, 4 s ... capped at 30 s. `attempt` starts at 0. */
export function backoffDelay(attempt: number, minMs = MIN_BACKOFF_MS, maxMs = MAX_BACKOFF_MS): number {
  const safe = Math.max(0, Math.min(attempt, 30));
  return Math.min(maxMs, minMs * 2 ** safe);
}

const realTimers: Timers = {
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms),
  clearTimeout: (h) => globalThis.clearTimeout(h as ReturnType<typeof setTimeout>),
  setInterval: (fn, ms) => globalThis.setInterval(fn, ms),
  clearInterval: (h) => globalThis.clearInterval(h as ReturnType<typeof setInterval>),
  now: () => Date.now(),
};

/**
 * SSE client with cursor resume, exponential backoff and stale detection.
 * The server closes streams after ~5 minutes; we reconnect with the last cursor.
 */
export class StreamClient {
  private readonly o: Required<Omit<StreamClientOptions, 'onEvent' | 'onStatus' | 'onSnapshotNeeded' | 'initialCursor' | 'createEventSource'>> &
    StreamClientOptions;
  private es: EventSourceLike | null = null;
  private retryHandle: unknown = null;
  private staleHandle: unknown = null;
  private attempt = 0;
  private stopped = true;
  private hadFailure = false;
  status: StreamStatus = 'idle';
  cursor: string | null;
  lastMessageAt: number | null = null;

  constructor(opts: StreamClientOptions) {
    this.o = {
      minBackoffMs: MIN_BACKOFF_MS,
      maxBackoffMs: MAX_BACKOFF_MS,
      staleAfterMs: STALE_AFTER_MS,
      staleCheckMs: 5000,
      timers: realTimers,
      ...opts,
    };
    this.cursor = opts.initialCursor ?? null;
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect('connecting');
    this.staleHandle = this.o.timers.setInterval(() => this.checkStale(), this.o.staleCheckMs);
  }

  stop(): void {
    this.stopped = true;
    this.teardown();
    if (this.retryHandle !== null) this.o.timers.clearTimeout(this.retryHandle);
    if (this.staleHandle !== null) this.o.timers.clearInterval(this.staleHandle);
    this.retryHandle = null;
    this.staleHandle = null;
    this.setStatus('closed', null);
  }

  private setStatus(status: StreamStatus, nextRetryMs: number | null) {
    this.status = status;
    this.o.onStatus?.(status, { attempt: this.attempt, nextRetryMs, lastMessageAt: this.lastMessageAt });
  }

  private teardown() {
    if (this.es) {
      this.es.onopen = null;
      this.es.onerror = null;
      this.es.close();
      this.es = null;
    }
  }

  private connect(status: StreamStatus) {
    if (this.stopped) return;
    this.teardown();
    this.setStatus(status, null);
    const factory = this.o.createEventSource ?? ((url: string) => new EventSource(url) as unknown as EventSourceLike);
    let es: EventSourceLike;
    try {
      es = factory(this.o.buildUrl(this.cursor));
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.es = es;
    // Treat connection start as activity so stale detection measures silence after connecting.
    this.lastMessageAt = this.o.timers.now();
    es.onopen = () => {
      if (this.es !== es) return;
      this.attempt = 0;
      this.setStatus('open', null);
      if (this.hadFailure) {
        this.hadFailure = false;
        // Cursor resume covers most gaps; a snapshot refetch keeps aggregates consistent.
        this.o.onSnapshotNeeded?.('reconnected');
      }
    };
    es.onerror = () => {
      if (this.es !== es) return;
      this.hadFailure = true;
      this.teardown();
      this.scheduleReconnect();
    };
    for (const type of STREAM_EVENT_TYPES) {
      es.addEventListener(type, (ev: MessageEvent) => this.handle(type, ev, es));
    }
  }

  private handle(type: StreamEventType, ev: MessageEvent, es: EventSourceLike) {
    if (this.es !== es) return;
    this.lastMessageAt = this.o.timers.now();
    if (ev.lastEventId) this.cursor = ev.lastEventId;
    if (this.status !== 'open') {
      this.attempt = 0;
      this.setStatus('open', null);
    }
    let data: unknown = null;
    try {
      data = typeof ev.data === 'string' && ev.data ? JSON.parse(ev.data) : null;
    } catch {
      return; // ignore malformed payloads
    }
    if (type === 'reset') {
      this.o.onSnapshotNeeded?.((data as { reason?: string } | null)?.reason ?? 'reset');
    }
    this.o.onEvent?.(type, data as never, this.cursor);
  }

  private scheduleReconnect() {
    if (this.stopped) return;
    const delay = backoffDelay(this.attempt, this.o.minBackoffMs, this.o.maxBackoffMs);
    this.attempt += 1;
    this.setStatus('reconnecting', delay);
    if (this.retryHandle !== null) this.o.timers.clearTimeout(this.retryHandle);
    this.retryHandle = this.o.timers.setTimeout(() => {
      this.retryHandle = null;
      this.connect('reconnecting');
    }, delay);
  }

  private checkStale() {
    if (this.stopped || this.retryHandle !== null) return;
    const last = this.lastMessageAt;
    if (last !== null && this.o.timers.now() - last > this.o.staleAfterMs) {
      this.hadFailure = true;
      this.setStatus('stale', null);
      this.teardown();
      this.scheduleReconnect();
    }
  }
}
