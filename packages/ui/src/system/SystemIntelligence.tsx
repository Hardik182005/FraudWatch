import { ApiError, describeError, type DataQuality, type FraudWatchClient, type Health, type LogEntry, type LogQuery, type SystemOperations, type SystemTrace } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import { ChevronDown, ChevronRight, Download, Pause, Play, RefreshCw, Search } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { Banner, ErrorState, LoadingSkeleton } from '../components/states';
import { HealthDot, StatusPill } from '../components/status';
import { TabPanel, Tabs } from '../components/Tabs';
import { formatInt, formatMs, formatNumber, formatRelativeTime, formatTimestamp, humanize, shortId } from '../format';
import { useNow } from '../hooks';
import { LifecycleTimeline } from '../lifecycle/lifecycle';
import { PipelineDiagram } from '../pipeline/PipelineDiagram';
import { redact } from '../viz/math';
import { ErrorStrip, TraceWaterfall, type WaterfallStage } from '../viz/streamCharts';
import { Measure, SourceBadge, VizEmpty, downloadBlob } from '../viz/VizFrame';

export type SystemLinkKind = 'tx' | 'alert' | 'experiment' | 'run';
export type SystemTab = 'topology' | 'logs' | 'trace' | 'quality' | 'operations' | 'lifecycle';

export interface SystemIntelligenceProps {
  client: FraudWatchClient;
  /** Session or view token for SSE (?token=). Never rendered. */
  token: string | null;
  sessionId: string | null;
  runId?: string | null;
  controlUrl?: string | null;
  /** Router-aware link renderer (cross-site links should preserve ?session=&run=). */
  renderLink?: (kind: SystemLinkKind, id: string, label: ReactNode) => ReactNode;
  tab?: SystemTab;
  onTabChange?: (tab: SystemTab) => void;
  initialEventId?: string | null;
}

const TABS: Array<{ id: SystemTab; label: string }> = [
  { id: 'topology', label: 'Topology' },
  { id: 'logs', label: 'Logs' },
  { id: 'trace', label: 'Trace' },
  { id: 'quality', label: 'Data quality' },
  { id: 'operations', label: 'Operations' },
  { id: 'lifecycle', label: 'Lifecycle' },
];

export function SystemIntelligence(props: SystemIntelligenceProps) {
  const [localTab, setLocalTab] = useState<SystemTab>(props.tab ?? (props.initialEventId ? 'trace' : 'topology'));
  const tab = props.tab ?? localTab;
  const setTab = (t: SystemTab) => {
    setLocalTab(t);
    props.onTabChange?.(t);
  };
  const [traceId, setTraceId] = useState<string | null>(props.initialEventId ?? null);
  const openTrace = useCallback(
    (id: string) => {
      setTraceId(id);
      setLocalTab('trace');
      props.onTabChange?.('trace');
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [props.onTabChange],
  );
  return (
    <div className="fw-system" data-testid="system-intelligence">
      <Tabs items={TABS} value={tab} onChange={(id) => setTab(id as SystemTab)} label="System Intelligence sections" idBase="fw-si" />
      <TabPanel idBase="fw-si" id={tab}>
        {tab === 'topology' && <TopologyPanel {...props} />}
        {tab === 'logs' && <LogsPanel {...props} onTrace={openTrace} />}
        {tab === 'trace' && <TracePanel {...props} eventId={traceId} onEventId={setTraceId} />}
        {tab === 'quality' && <DataQualityPanel {...props} />}
        {tab === 'operations' && <OperationsPanel {...props} />}
        {tab === 'lifecycle' && <LifecycleTimeline controlUrl={props.controlUrl} />}
      </TabPanel>
    </div>
  );
}

/* ---------------- Topology ---------------- */

const STALE_MS = 60000;

function TopologyPanel({ client, runId }: SystemIntelligenceProps) {
  const now = useNow(5000);
  const health = useApi((s) => client.getHealth(s), [client], { refreshMs: 10000 });
  const arch = useApi(() => client.getArchitecture(), [client]);
  const dq = useApi((s) => client.dataQuality(runId ?? null, s), [client, runId], { refreshMs: 15000 });
  const h: Health | undefined = health.data;
  const checkedAge = h ? now - h.as_of : null;
  const stale = checkedAge != null && checkedAge > STALE_MS;
  const recon = dq.data?.reconciliation ?? [];
  const latest = recon[0];
  return (
    <div className="fw-stack">
      <div className="fw-row fw-row-wrap">
        <SourceBadge kind="live" />
        <span className="fw-small fw-muted">GET /health every 10 s · reconciliation from GET /system/data-quality</span>
        {h && (
          <span className="fw-small" data-testid="topology-freshness">
            Checked {formatRelativeTime(h.as_of, now)}
          </span>
        )}
      </div>
      {stale && <Banner tone="warn">Health data is stale (last successful check {formatRelativeTime(h!.as_of, now)}). Statuses below may no longer be true.</Banner>}
      {health.status === 'error' && !h && <ErrorState error={health.error} title="Health endpoint unreachable" onRetry={health.refetch} />}
      <div className="fw-grid fw-grid-2">
        <section className="fw-card" aria-labelledby="fw-reach">
          <div className="fw-card-head">
            <h3 id="fw-reach">Reachable</h3>
          </div>
          <div className="fw-card-body">
            <p className="fw-small fw-muted">Whether each component answered its health check. Reachable does not mean transactions are completing.</p>
            {!h ? (
              <LoadingSkeleton lines={4} />
            ) : (
              <ul className="fw-topo-list">
                {h.components.map((c) => (
                  <li key={c.id}>
                    <HealthDot status={stale ? 'unknown' : c.status} label={humanize(c.id)} />
                    <span className="fw-small fw-muted">{stale ? 'stale' : c.status}</span>
                    {c.detail && <span className="fw-small fw-muted fw-topo-detail">{c.detail}</span>}
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>
        <section className="fw-card" aria-labelledby="fw-complete">
          <div className="fw-card-head">
            <h3 id="fw-complete">Completing transactions</h3>
          </div>
          <div className="fw-card-body">
            <p className="fw-small fw-muted">Whether generated events actually reach a persisted result, per engine, for the most recent runs in scope.</p>
            {dq.status === 'error' && !dq.data ? (
              <VizEmpty kind="unavailable" title="Reconciliation unavailable">{describeError(dq.error)}</VizEmpty>
            ) : !dq.data ? (
              <LoadingSkeleton lines={3} />
            ) : !latest ? (
              <VizEmpty kind="no_data" title="No runs in scope">Start a run to see whether transactions complete end to end.</VizEmpty>
            ) : (
              <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Run reconciliation">
                <table className="fw-table fw-table-compact">
                  <thead>
                    <tr>
                      <th scope="col">Run</th>
                      <th scope="col">Engine</th>
                      <th scope="col" className="fw-num">Acked</th>
                      <th scope="col" className="fw-num">Persisted</th>
                      <th scope="col" className="fw-num">Missing</th>
                      <th scope="col">State</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recon.slice(0, 6).map((r) => (
                      <tr key={`${r.run_id}-${r.engine}`}>
                        <td><code>{shortId(r.run_id)}</code></td>
                        <td>{r.engine}</td>
                        <td className="fw-num"><Measure value={r.acked} /></td>
                        <td className="fw-num"><Measure value={r.persisted} /></td>
                        <td className="fw-num"><Measure value={r.missing} /></td>
                        <td><ReconState state={r.state} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </section>
      </div>
      <PipelineDiagram architecture={arch.data} health={stale ? null : h} loading={arch.status === 'loading'} error={arch.error} onRetry={arch.refetch} />
    </div>
  );
}

function ReconState({ state }: { state: string }) {
  const tone = state === 'reconciled' ? 'allow' : state === 'incomplete' ? 'alert' : state === 'draining' ? 'review' : 'info';
  return <StatusPill tone={tone}>{humanize(state)}</StatusPill>;
}

/* ---------------- Logs ---------------- */

const SEVERITIES = ['debug', 'info', 'warning', 'error', 'critical'];
const SERVICES = ['api', 'generator', 'scoring', 'result_consumer', 'ops_agent'];

function LogsPanel({ client, token, sessionId, runId, renderLink, onTrace }: SystemIntelligenceProps & { onTrace: (id: string) => void }) {
  const [draft, setDraft] = useState<LogQuery>({ severity: 'info', service: '', engine: '', q: '', run_id: runId ?? '' });
  const [query, setQuery] = useState<LogQuery>(draft);
  const [items, setItems] = useState<LogEntry[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);
  const [live, setLive] = useState(true);
  const [paused, setPaused] = useState(false);
  const [buffer, setBuffer] = useState<LogEntry[]>([]);
  const [behind, setBehind] = useState<number | null>(null);
  const [streamState, setStreamState] = useState<'connecting' | 'open' | 'error' | 'off'>('off');
  const [reconnectKey, setReconnectKey] = useState(0);
  const [expanded, setExpanded] = useState<string | null>(null);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  const clean = useMemo<LogQuery>(() => {
    const q: LogQuery = {};
    for (const [k, v] of Object.entries(query)) if (v !== '' && v != null) (q as Record<string, unknown>)[k] = v;
    return q;
  }, [query]);

  const loadPage = useCallback(
    (after: string | null) => {
      setLoading(true);
      client
        .systemLogs({ ...clean, limit: 100, cursor: after })
        .then((res) => {
          setItems((prev) => (after ? [...prev, ...res.items] : res.items));
          setCursor(res.next_cursor);
          setError(null);
        })
        .catch(setError)
        .finally(() => setLoading(false));
    },
    [client, clean],
  );

  useEffect(() => {
    setBehind(null);
    setBuffer([]);
    loadPage(null);
  }, [loadPage]);

  // Live tail
  useEffect(() => {
    if (!live || typeof EventSource === 'undefined') {
      setStreamState('off');
      return;
    }
    let es: EventSource | null = null;
    let retry: ReturnType<typeof setTimeout> | null = null;
    let lastId: string | null = null;
    let attempt = 0;
    let closed = false;
    const connect = () => {
      setStreamState('connecting');
      const url = client.systemLogsStreamUrl({ ...clean }, token);
      es = new EventSource(lastId ? `${url}${url.includes('?') ? '&' : '?'}after=${encodeURIComponent(lastId)}` : url);
      es.onopen = () => {
        attempt = 0;
        setStreamState('open');
      };
      es.addEventListener('log', (ev) => {
        try {
          const entry = JSON.parse((ev as MessageEvent).data) as LogEntry;
          lastId = entry.id;
          if (pausedRef.current) setBuffer((b) => [entry, ...b].slice(0, 500));
          else setItems((prev) => (prev.some((p) => p.id === entry.id) ? prev : [entry, ...prev].slice(0, 1000)));
        } catch {
          /* ignore malformed frame */
        }
      });
      es.addEventListener('behind', (ev) => {
        try {
          setBehind((JSON.parse((ev as MessageEvent).data) as { skipped: number }).skipped);
        } catch {
          setBehind(-1);
        }
      });
      es.onerror = () => {
        es?.close();
        if (closed) return;
        setStreamState('error');
        const delay = Math.min(30000, 1000 * 2 ** attempt++);
        retry = setTimeout(connect, delay);
      };
    };
    connect();
    return () => {
      closed = true;
      es?.close();
      if (retry) clearTimeout(retry);
    };
  }, [live, client, clean, token, reconnectKey]);

  const resume = () => {
    setItems((prev) => {
      const ids = new Set(prev.map((p) => p.id));
      return [...buffer.filter((b) => !ids.has(b.id)), ...prev].slice(0, 1000);
    });
    setBuffer([]);
    setPaused(false);
  };

  const exportCsv = async () => {
    try {
      const blob = await client.fetchBlob('/system/logs/export.csv', { ...clean } as Record<string, string | number | null | undefined>);
      downloadBlob(blob, `fraudwatch-logs-${Date.now()}.csv`);
    } catch (e) {
      setError(e);
    }
  };

  const setD = (k: keyof LogQuery, v: string) => setDraft((d) => ({ ...d, [k]: v }));

  return (
    <div className="fw-stack" data-testid="logs-panel">
      <form
        className="fw-logfilters"
        onSubmit={(e) => {
          e.preventDefault();
          setQuery(draft);
        }}
        aria-label="Log filters"
      >
        <label>
          <span>Min severity</span>
          <select value={draft.severity ?? ''} onChange={(e) => setD('severity', e.target.value)}>
            {SEVERITIES.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Service</span>
          <select value={draft.service ?? ''} onChange={(e) => setD('service', e.target.value)}>
            <option value="">All</option>
            {SERVICES.map((s) => (
              <option key={s} value={s}>{humanize(s)}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Engine</span>
          <select value={draft.engine ?? ''} onChange={(e) => setD('engine', e.target.value)}>
            <option value="">All</option>
            <option value="flink">Flink</option>
            <option value="spark">Spark</option>
          </select>
        </label>
        <label>
          <span>Run ID</span>
          <input value={draft.run_id ?? ''} onChange={(e) => setD('run_id', e.target.value)} placeholder="any" spellCheck={false} />
        </label>
        <label className="fw-logfilters-q">
          <span>Search message or error code</span>
          <input value={draft.q ?? ''} onChange={(e) => setD('q', e.target.value)} placeholder="e.g. timeout" />
        </label>
        <button type="submit" className="fw-btn fw-btn-primary fw-btn-sm">
          <Search size={14} aria-hidden="true" /> Apply
        </button>
      </form>

      <div className="fw-row fw-row-wrap fw-logbar">
        <SourceBadge kind="live" />
        <span className="fw-small fw-muted">
          {sessionId ? 'Your demo session plus public experiment logs' : 'Public experiment logs'} · server-side filters · newest first
        </span>
        <span className="fw-spacer" />
        <span className="fw-small" data-testid="tail-state" data-state={streamState}>
          <HealthDot status={streamState === 'open' ? (paused ? 'idle' : 'ok') : streamState === 'error' ? 'down' : streamState === 'off' ? 'idle' : 'unknown'} label={streamState === 'open' ? (paused ? `Tail paused (${buffer.length} buffered)` : 'Live tail') : streamState === 'error' ? 'Tail disconnected, retrying' : streamState === 'off' ? 'Tail off' : 'Connecting'} />
        </span>
        {live && (paused ? (
          <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={resume}>
            <Play size={14} aria-hidden="true" /> Resume
          </button>
        ) : (
          <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={() => setPaused(true)}>
            <Pause size={14} aria-hidden="true" /> Pause
          </button>
        ))}
        <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" onClick={() => (live ? setReconnectKey((k) => k + 1) : setLive(true))}>
          <RefreshCw size={14} aria-hidden="true" /> {live ? 'Reconnect' : 'Start tail'}
        </button>
        <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" onClick={exportCsv}>
          <Download size={14} aria-hidden="true" /> CSV
        </button>
      </div>

      {behind != null && (
        <Banner tone="warn">
          The live tail fell behind{behind > 0 ? ` and skipped ${formatInt(behind)} rows` : ''}.{' '}
          <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={() => { setBehind(null); loadPage(null); }}>
            Reload history
          </button>
        </Banner>
      )}
      {error != null && <ErrorState error={error} title="Could not load logs" onRetry={() => loadPage(null)} />}

      <ErrorStrip items={items} />

      {loading && !items.length ? (
        <LoadingSkeleton lines={6} />
      ) : !items.length ? (
        <VizEmpty kind="no_data" title="No log entries match these filters">Lower the minimum severity or clear the run filter.</VizEmpty>
      ) : (
        <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Log entries">
          <table className="fw-table fw-table-compact fw-logtable">
            <thead>
              <tr>
                <th scope="col"><span className="fw-sr-only">Expand</span></th>
                <th scope="col">Time (UTC ms)</th>
                <th scope="col">Severity</th>
                <th scope="col">Service</th>
                <th scope="col">Message</th>
                <th scope="col">Links</th>
              </tr>
            </thead>
            <tbody>
              {items.map((l) => {
                const open = expanded === l.id;
                return [
                  <tr key={l.id} data-sev={l.severity}>
                    <td>
                      <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" aria-expanded={open} aria-label={open ? 'Hide details' : 'Show details'} onClick={() => setExpanded(open ? null : l.id)}>
                        {open ? <ChevronDown size={14} aria-hidden="true" /> : <ChevronRight size={14} aria-hidden="true" />}
                      </button>
                    </td>
                    <td className="fw-num fw-nowrap">{formatTimestamp(l.timestamp_utc, true)}</td>
                    <td><span className="fw-sev" data-sev={l.severity}>{l.severity}</span></td>
                    <td className="fw-nowrap">{humanize(l.service)}{l.engine ? ` · ${l.engine}` : ''}</td>
                    <td className="fw-logmsg">
                      {l.message}
                      {l.error_code && <code className="fw-small"> {l.error_code}</code>}
                      {l.duration_ms != null && <span className="fw-small fw-muted"> · {formatMs(l.duration_ms)}</span>}
                    </td>
                    <td className="fw-nowrap fw-loglinks">
                      {l.event_id && (
                        <button type="button" className="fw-link" onClick={() => onTrace(l.event_id!)}>
                          trace
                        </button>
                      )}
                      {l.event_id && renderLink?.('tx', l.event_id, 'tx')}
                      {l.alert_id && renderLink?.('alert', l.alert_id, 'alert')}
                      {l.experiment_id && renderLink?.('experiment', l.experiment_id, 'experiment')}
                      {l.run_id && renderLink?.('run', l.run_id, `run ${shortId(l.run_id, 6)}`)}
                    </td>
                  </tr>,
                  open && (
                    <tr key={`${l.id}-d`} className="fw-logdetail">
                      <td colSpan={6}>
                        <dl className="fw-kv fw-kv-inline">
                          {(['id', 'observed_at', 'environment', 'session_id', 'run_id', 'event_id', 'trace_id', 'job_id', 'stage', 'status', 'retry_attempt'] as const).map((k) =>
                            l[k] != null ? (
                              <div key={k}>
                                <dt>{k}</dt>
                                <dd><code>{k === 'observed_at' ? formatTimestamp(l[k] as number, true) : String(l[k])}</code></dd>
                              </div>
                            ) : null,
                          )}
                        </dl>
                        <pre className="fw-code">{l.payload ? JSON.stringify(redact(l.payload), null, 2) : 'No payload'}</pre>
                      </td>
                    </tr>
                  ),
                ];
              })}
            </tbody>
          </table>
        </div>
      )}
      {cursor && (
        <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={() => loadPage(cursor)} disabled={loading}>
          {loading ? 'Loading…' : 'Load older entries'}
        </button>
      )}
    </div>
  );
}

/* ---------------- Trace ---------------- */

export const TRACE_STAGES: Array<{ id: string; label: string }> = [
  { id: 'generated', label: 'Generated' },
  { id: 'kafka_acked', label: 'Kafka acknowledged' },
  { id: 'engine_received', label: 'Engine received' },
  { id: 'features_ready', label: 'Features ready' },
  { id: 'scored', label: 'Model scored' },
  { id: 'engine_emitted', label: 'Engine emitted' },
  { id: 'persisted', label: 'Result persisted' },
  { id: 'alert_evaluated', label: 'Alert evaluated' },
];

export function traceLanes(trace: SystemTrace) {
  return (Object.entries(trace.engines) as Array<[string, SystemTrace['engines']['flink']]>)
    .filter(([, t]) => !!t)
    .map(([engine, t]) => ({
      id: engine,
      label: engine === 'flink' ? 'Flink' : engine === 'spark' ? 'Spark' : engine,
      stages: TRACE_STAGES.map<WaterfallStage>((s) => {
        const hit = t!.stages.find((x) => x.stage === s.id);
        return { stage: s.id, label: s.label, at_ms: hit?.at_ms ?? null, status: hit?.status ?? 'missing', detail: typeof hit?.detail === 'string' ? hit.detail : hit?.detail ? JSON.stringify(hit.detail) : null };
      }),
    }));
}

function TracePanel({ client, eventId, onEventId, renderLink }: SystemIntelligenceProps & { eventId: string | null; onEventId: (id: string) => void }) {
  const [draft, setDraft] = useState(eventId ?? '');
  useEffect(() => setDraft(eventId ?? ''), [eventId]);
  const trace = useApi((s) => client.systemTrace(eventId!, s), [client, eventId], { enabled: !!eventId });
  const notFound = trace.error instanceof ApiError && trace.error.status === 404;
  return (
    <div className="fw-stack" data-testid="trace-panel">
      <form
        className="fw-row fw-row-wrap"
        onSubmit={(e) => {
          e.preventDefault();
          if (draft.trim()) onEventId(draft.trim());
        }}
      >
        <label className="fw-inline-field">
          <span>Event ID</span>
          <input value={draft} onChange={(e) => setDraft(e.target.value)} placeholder="Paste an event ID from the logs or transaction table" spellCheck={false} />
        </label>
        <button type="submit" className="fw-btn fw-btn-primary fw-btn-sm">Trace event</button>
      </form>
      {!eventId ? (
        <VizEmpty kind="no_data" title="Pick an event to trace">Use the trace link next to a log entry, or paste an event ID. The explorer shows Generated → Kafka acknowledged → Engine received → Features ready → Model scored → Result persisted → Alert evaluated, with browser receipt kept separate.</VizEmpty>
      ) : trace.status === 'loading' ? (
        <LoadingSkeleton lines={6} />
      ) : notFound ? (
        <VizEmpty kind="no_data" title="No engine persisted this event yet">It may still be in flight, it may have failed before persistence, or the ID may belong to another session.</VizEmpty>
      ) : trace.status === 'error' ? (
        <ErrorState error={trace.error} onRetry={trace.refetch} />
      ) : trace.data ? (
        <>
          <div className="fw-row fw-row-wrap fw-small">
            <span>Event <code>{trace.data.event_id}</code></span>
            {trace.data.seq != null && <span className="fw-muted">seq {trace.data.seq}</span>}
            {renderLink?.('tx', trace.data.event_id, 'Open in inspector')}
            {trace.data.run_id && renderLink?.('run', trace.data.run_id, `Run ${shortId(trace.data.run_id)}`)}
          </div>
          <TraceWaterfall lanes={traceLanes(trace.data)} clientReceipt={trace.data.client_received ?? null} />
          {Object.entries(trace.data.engines).map(([e, t]) =>
            t && t.missing.length ? (
              <p key={e} className="fw-small fw-muted">
                {e}: not recorded for {t.missing.map((m) => TRACE_STAGES.find((s) => s.id === m)?.label ?? m).join(', ')}.
              </p>
            ) : null,
          )}
          {trace.data.logs.length > 0 && (
            <details className="fw-disclosure">
              <summary>Related log entries ({trace.data.logs.length})</summary>
              <ul className="fw-loglist">
                {trace.data.logs.map((l) => (
                  <li key={l.id}>
                    <span className="fw-num fw-small">{formatTimestamp(l.timestamp_utc, true)}</span> <span className="fw-sev" data-sev={l.severity}>{l.severity}</span> {humanize(l.service)}: {l.message}
                  </li>
                ))}
              </ul>
            </details>
          )}
        </>
      ) : null}
    </div>
  );
}

/* ---------------- Data quality ---------------- */

const COUNT_LABEL: Record<string, string> = {
  malformed: 'Malformed',
  schema_mismatch: 'Schema mismatch',
  missing_fields: 'Missing fields',
  duplicates: 'Duplicate deliveries',
  processing_failures: 'Processing failures',
  late: 'Late events',
  unmatched: 'Unmatched between engines',
  stale: 'Stale (no result after drain)',
};

function DataQualityPanel({ client, runId }: SystemIntelligenceProps) {
  const [scopeRun, setScopeRun] = useState(runId ?? '');
  const [applied, setApplied] = useState(runId ?? '');
  const dq = useApi((s) => client.dataQuality(applied || null, s), [client, applied], { refreshMs: 20000 });
  const d: DataQuality | undefined = dq.data;
  return (
    <div className="fw-stack" data-testid="quality-panel">
      <form className="fw-row fw-row-wrap" onSubmit={(e) => { e.preventDefault(); setApplied(scopeRun.trim()); }}>
        <label className="fw-inline-field">
          <span>Run ID (blank = your session)</span>
          <input value={scopeRun} onChange={(e) => setScopeRun(e.target.value)} spellCheck={false} />
        </label>
        <button type="submit" className="fw-btn fw-btn-secondary fw-btn-sm">Apply</button>
        <SourceBadge kind="live" />
      </form>
      {dq.status === 'error' && !d ? (
        <ErrorState error={dq.error} onRetry={dq.refetch} />
      ) : !d ? (
        <LoadingSkeleton lines={5} />
      ) : (
        <>
          <p className="fw-small fw-muted">
            Scope: {d.scope.run_id ? `run ${shortId(d.scope.run_id)}` : d.scope.session_id ? 'your demo session' : 'last 24 hours'}
            {d.scope.from ? ` · ${formatTimestamp(d.scope.from, true)} – ${formatTimestamp(d.scope.to, true)}` : ''}
          </p>
          <div className="fw-dq-grid">
            {Object.entries(COUNT_LABEL).map(([k, label]) => (
              <div key={k} className="fw-dq-cell" data-state={d.counts[k] == null ? 'na' : d.counts[k] === 0 ? 'zero' : 'value'}>
                <span className="fw-small fw-muted">{label}</span>
                <strong className="fw-dq-val">
                  <Measure value={d.counts[k]} unavailable={d.dlq?.restricted && ['malformed', 'schema_mismatch', 'missing_fields'].includes(k) ? 'Operator only' : 'Not recorded'} />
                </strong>
                {d.definitions?.[k] && <span className="fw-small fw-muted">{d.definitions[k]}</span>}
              </div>
            ))}
          </div>
          <h3 className="fw-h3">Reconciliation</h3>
          {!d.reconciliation?.length ? (
            <VizEmpty kind="no_data" title="No runs in scope" />
          ) : (
            <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Reconciliation">
              <table className="fw-table fw-table-compact">
                <thead>
                  <tr>
                    <th scope="col">Run</th><th scope="col">Engine</th><th scope="col" className="fw-num">Generated</th><th scope="col" className="fw-num">Acked</th><th scope="col" className="fw-num">Persisted</th><th scope="col" className="fw-num">Missing</th><th scope="col" className="fw-num">Duplicates</th><th scope="col">State</th>
                  </tr>
                </thead>
                <tbody>
                  {d.reconciliation.map((r) => (
                    <tr key={`${r.run_id}-${r.engine}`}>
                      <td><code>{shortId(r.run_id)}</code></td><td>{r.engine}</td>
                      <td className="fw-num"><Measure value={r.generated} /></td><td className="fw-num"><Measure value={r.acked} /></td><td className="fw-num"><Measure value={r.persisted} /></td><td className="fw-num"><Measure value={r.missing} /></td><td className="fw-num"><Measure value={r.duplicates} /></td>
                      <td><ReconState state={r.state} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <h3 className="fw-h3">Dead-letter queue</h3>
          {d.dlq?.restricted ? (
            <VizEmpty kind="not_applicable" title="Operator only">DLQ records carry no session attribution, so they are visible only to the operator.</VizEmpty>
          ) : !d.dlq?.items.length ? (
            <VizEmpty kind="no_data" title="No dead-letter records in scope" />
          ) : (
            <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Dead-letter records">
              <table className="fw-table fw-table-compact">
                <thead><tr><th scope="col">Received</th><th scope="col">Engine</th><th scope="col">Reason</th><th scope="col">Error</th><th scope="col">Excerpt</th></tr></thead>
                <tbody>
                  {d.dlq.items.map((x) => (
                    <tr key={x.id}>
                      <td className="fw-num fw-nowrap">{formatTimestamp(x.received_at, true)}</td><td>{x.engine ?? '—'}</td><td>{humanize(x.reason)}</td><td className="fw-small">{x.error}</td>
                      <td><code className="fw-small">{String(redact(x.raw_excerpt ?? '')).slice(0, 160)}</code></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

/* ---------------- Operations ---------------- */

function OperationsPanel({ client }: SystemIntelligenceProps) {
  const ops = useApi((s) => client.operations(s), [client], { refreshMs: 15000 });
  const now = useNow(5000);
  const o: SystemOperations | undefined = ops.data;
  if (ops.status === 'error' && !o) return <ErrorState error={ops.error} onRetry={ops.refetch} />;
  if (!o) return <LoadingSkeleton lines={6} />;
  const kv = (label: string, value: ReactNode) => (
    <div key={label}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
  const lastCp = o.flink?.last_checkpoint as { id?: number; completed_at?: number; duration_ms?: number } | null | undefined;
  return (
    <div className="fw-stack" data-testid="operations-panel">
      <div className="fw-row fw-row-wrap">
        <SourceBadge kind="live" />
        <span className="fw-small fw-muted">as of {formatTimestamp(o.as_of, true)} ({formatRelativeTime(o.as_of, now)}) · refreshed every 15 s</span>
      </div>
      {!!o.incidents?.length && (
        <ul className="fw-incidents">
          {o.incidents.map((i) => (
            <li key={i.id} data-sev={i.severity}>
              <strong>{i.title}</strong> <span className="fw-small fw-muted">since {formatRelativeTime(i.since, now)}{i.source ? ` · ${i.source}` : ''}</span>
              {i.detail && <div className="fw-small">{i.detail}</div>}
            </li>
          ))}
        </ul>
      )}
      <div className="fw-grid fw-grid-3">
        <section className="fw-card">
          <div className="fw-card-head"><h3>Model</h3></div>
          <div className="fw-card-body">
            {!o.model ? <VizEmpty kind="unavailable" title="Model metadata unavailable" /> : (
              <dl className="fw-kv-rows">
                {kv('Version', o.model.version ?? 'Not reported')}
                {kv('Feature schema', o.model.feature_schema ?? 'Not reported')}
                {kv('Checksum', o.model.checksum ? <code>{shortId(o.model.checksum, 12)}</code> : 'Not reported')}
                {kv('Thresholds', o.model.thresholds ? `review ${formatNumber(o.model.thresholds.review, 3)} · alert ${formatNumber(o.model.thresholds.alert, 3)}` : 'Not reported')}
                {kv('Inference errors', <Measure value={o.model.inference_errors} />)}
                {kv('Inference time', o.model.inference_ms ? `p50 ${formatMs(o.model.inference_ms.p50)} · p95 ${formatMs(o.model.inference_ms.p95)} (n = ${formatInt(o.model.inference_ms.n)})` : <Measure value={null} unavailable="Not instrumented" />)}
                {kv('Features', `${o.model.features.length} inputs`)}
              </dl>
            )}
          </div>
        </section>
        <section className="fw-card">
          <div className="fw-card-head"><h3>Flink</h3></div>
          <div className="fw-card-body">
            {!o.flink ? <VizEmpty kind="unavailable" title="Flink metrics unavailable" /> : (
              <dl className="fw-kv-rows">
                {kv('Job', o.flink.job_id ? <code>{shortId(o.flink.job_id, 12)}</code> : 'No job')}
                {kv('State', o.flink.state ?? 'Not reported')}
                {kv('Restarts', <Measure value={o.flink.restarts} />)}
                {kv('Checkpoint age', o.flink.checkpoint_age_ms != null ? formatMs(o.flink.checkpoint_age_ms) : 'No checkpoint yet')}
                {kv('Last checkpoint', lastCp?.id != null ? `#${lastCp.id}${lastCp.duration_ms != null ? ` · ${formatMs(lastCp.duration_ms)}` : ''}` : 'Not recorded')}
                {kv('Watermarks', o.flink.watermarks === 'not_instrumented' ? <Measure value="not_instrumented" /> : o.flink.watermarks.length ? o.flink.watermarks.map((w) => `${w.vertex}: ${w.current_input_watermark_ms != null ? formatTimestamp(w.current_input_watermark_ms, true) : 'none'}`).join('; ') : 'None reported')}
                {kv('Backpressure', o.flink.backpressure === 'not_instrumented' ? <Measure value="not_instrumented" /> : o.flink.backpressure.length ? o.flink.backpressure.map((b) => `${b.vertex}: ${b.level}`).join('; ') : 'None reported')}
              </dl>
            )}
          </div>
        </section>
        <section className="fw-card">
          <div className="fw-card-head"><h3>Spark</h3></div>
          <div className="fw-card-body">
            {!o.spark ? <VizEmpty kind="unavailable" title="Spark metrics unavailable" /> : (
              <dl className="fw-kv-rows">
                {kv('State', o.spark.state ?? 'Not reported')}
                {kv('Query / app', o.spark.app_id ? <code>{shortId(o.spark.app_id, 14)}</code> : 'Not running')}
                {kv('Run label', o.spark.run_label ?? '—')}
                {kv('Trigger', o.spark.trigger ?? 'Not reported')}
                {kv('Last batch', o.spark.last_batch_id != null ? `#${o.spark.last_batch_id} ${o.spark.last_batch_at ? formatRelativeTime(o.spark.last_batch_at, now) : ''}` : 'No batch yet')}
                {kv('Batch duration', o.spark.batch_duration_ms != null ? formatMs(o.spark.batch_duration_ms) : <Measure value={null} />)}
                {kv('Input rows/s', <Measure value={o.spark.input_rows_per_second} format={(n) => formatNumber(n, 1)} />)}
                {kv('Processed rows/s', <Measure value={o.spark.processed_rows_per_second} format={(n) => formatNumber(n, 1)} />)}
              </dl>
            )}
          </div>
        </section>
      </div>
      <section className="fw-card">
        <div className="fw-card-head"><h3>Kafka consumer lag</h3></div>
        <div className="fw-card-body">
          {o.kafka_lag === 'not_instrumented' || o.kafka_lag == null ? (
            <VizEmpty kind="not_instrumented" title="Not instrumented" />
          ) : !o.kafka_lag.length ? (
            <VizEmpty kind="no_data" title="No committed consumer offsets reported" />
          ) : (
            <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Kafka lag">
              <table className="fw-table fw-table-compact">
                <thead><tr><th scope="col">Group</th><th scope="col">Topic</th><th scope="col" className="fw-num">Partition</th><th scope="col" className="fw-num">Committed</th><th scope="col" className="fw-num">End offset</th><th scope="col" className="fw-num">Lag</th></tr></thead>
                <tbody>
                  {o.kafka_lag.map((k) => (
                    <tr key={`${k.group}-${k.topic}-${k.partition}`}>
                      <td>{k.group}</td><td>{k.topic}</td><td className="fw-num">{k.partition}</td><td className="fw-num"><Measure value={k.committed} /></td><td className="fw-num"><Measure value={k.end_offset} /></td><td className="fw-num"><Measure value={k.lag} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {o.kafka_lag_notes?.map((n, i) => (
            <p key={i} className="fw-small fw-muted">{n}</p>
          ))}
        </div>
      </section>
    </div>
  );
}
