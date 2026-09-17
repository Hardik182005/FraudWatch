import { describe, expect, it } from 'vitest';
import { StreamClient, backoffDelay, type EventSourceLike, type StreamStatus, type Timers } from './sse';

class FakeTimers implements Timers {
  t = 0;
  private seq = 0;
  private tasks = new Map<number, { at: number; fn: () => void; every?: number }>();
  now = () => this.t;
  setTimeout = (fn: () => void, ms: number) => {
    const id = ++this.seq;
    this.tasks.set(id, { at: this.t + ms, fn });
    return id;
  };
  clearTimeout = (h: unknown) => {
    this.tasks.delete(h as number);
  };
  setInterval = (fn: () => void, ms: number) => {
    const id = ++this.seq;
    this.tasks.set(id, { at: this.t + ms, fn, every: ms });
    return id;
  };
  clearInterval = (h: unknown) => {
    this.tasks.delete(h as number);
  };
  advance(ms: number) {
    const end = this.t + ms;
    for (;;) {
      let nextId = -1;
      let next: { at: number; fn: () => void; every?: number } | undefined;
      for (const [id, task] of this.tasks) if (task.at <= end && (!next || task.at < next.at)) {
        next = task;
        nextId = id;
      }
      if (!next) break;
      this.t = next.at;
      if (next.every) next.at += next.every;
      else this.tasks.delete(nextId);
      next.fn();
    }
    this.t = end;
  }
}

class FakeES implements EventSourceLike {
  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  closed = false;
  listeners = new Map<string, (ev: MessageEvent) => void>();
  constructor(public url: string) {}
  addEventListener(type: string, l: (ev: MessageEvent) => void) {
    this.listeners.set(type, l);
  }
  close() {
    this.closed = true;
  }
  open() {
    this.onopen?.(new Event('open'));
  }
  fail() {
    this.onerror?.(new Event('error'));
  }
  emit(type: string, data: unknown, id: string) {
    this.listeners.get(type)?.({ data: JSON.stringify(data), lastEventId: id } as MessageEvent);
  }
}

function setup(initialCursor: string | null = null) {
  const timers = new FakeTimers();
  const sources: FakeES[] = [];
  const statuses: StreamStatus[] = [];
  const events: Array<[string, unknown]> = [];
  const snapshots: string[] = [];
  const client = new StreamClient({
    buildUrl: (cursor) => `http://api/stream?token=t${cursor ? `&cursor=${cursor}` : ''}`,
    createEventSource: (url) => {
      const es = new FakeES(url);
      sources.push(es);
      return es;
    },
    onEvent: (type, data) => events.push([type, data]),
    onStatus: (s) => statuses.push(s),
    onSnapshotNeeded: (r) => snapshots.push(r),
    initialCursor,
    timers,
  });
  return { timers, sources, statuses, events, snapshots, client };
}

describe('backoffDelay', () => {
  it('doubles from 1 s and caps at 30 s', () => {
    expect([0, 1, 2, 3, 4, 5, 6, 10].map((a) => backoffDelay(a))).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  });
});

describe('StreamClient', () => {
  it('connects, dispatches events and tracks the cursor', () => {
    const { client, sources, events, statuses } = setup();
    client.start();
    expect(sources[0]!.url).toBe('http://api/stream?token=t');
    sources[0]!.open();
    sources[0]!.emit('txn', { event_id: 'e1' }, 'c-10');
    expect(events).toEqual([['txn', { event_id: 'e1' }]]);
    expect(client.cursor).toBe('c-10');
    expect(statuses).toContain('open');
    client.stop();
    expect(sources[0]!.closed).toBe(true);
  });

  it('reconnects with exponential backoff and resumes from the last cursor', () => {
    const { client, sources, timers, statuses, snapshots } = setup();
    client.start();
    sources[0]!.open();
    sources[0]!.emit('ping', { as_of: 1 }, 'c-5');
    sources[0]!.fail();
    expect(statuses.at(-1)).toBe('reconnecting');
    timers.advance(999);
    expect(sources).toHaveLength(1);
    timers.advance(1);
    expect(sources).toHaveLength(2);
    expect(sources[1]!.url).toContain('cursor=c-5');
    // second failure waits 2 s
    sources[1]!.fail();
    timers.advance(1999);
    expect(sources).toHaveLength(2);
    timers.advance(1);
    expect(sources).toHaveLength(3);
    // successful open resets backoff and requests a snapshot
    sources[2]!.open();
    expect(snapshots).toEqual(['reconnected']);
    sources[2]!.fail();
    timers.advance(1000);
    expect(sources).toHaveLength(4);
    client.stop();
  });

  it('caps backoff at 30 s', () => {
    const { client, sources, timers } = setup();
    client.start();
    for (let i = 0; i < 8; i++) {
      sources.at(-1)!.fail();
      timers.advance(30000);
    }
    const count = sources.length;
    sources.at(-1)!.fail();
    timers.advance(29999);
    expect(sources.length).toBe(count);
    timers.advance(1);
    expect(sources.length).toBe(count + 1);
    client.stop();
  });

  it('marks the stream stale after 30 s of silence and reconnects', () => {
    const { client, sources, timers, statuses } = setup('c-1');
    client.start();
    expect(sources[0]!.url).toContain('cursor=c-1');
    sources[0]!.open();
    timers.advance(25000);
    expect(statuses).not.toContain('stale');
    timers.advance(10000);
    expect(statuses).toContain('stale');
    expect(sources[0]!.closed).toBe(true);
    timers.advance(1000);
    expect(sources).toHaveLength(2);
    client.stop();
  });

  it('asks for a snapshot on reset events', () => {
    const { client, sources, snapshots, events } = setup();
    client.start();
    sources[0]!.open();
    sources[0]!.emit('reset', { reason: 'cursor_expired' }, 'c-9');
    expect(snapshots).toEqual(['cursor_expired']);
    expect(events[0]![0]).toBe('reset');
    client.stop();
  });

  it('ignores events from a replaced source and malformed data', () => {
    const { client, sources, events, timers } = setup();
    client.start();
    const first = sources[0]!;
    first.fail();
    timers.advance(1000);
    first.emit('txn', { event_id: 'old' }, 'x');
    sources[1]!.listeners.get('txn')?.({ data: '{not json', lastEventId: 'y' } as MessageEvent);
    expect(events).toEqual([]);
    client.stop();
  });
});
