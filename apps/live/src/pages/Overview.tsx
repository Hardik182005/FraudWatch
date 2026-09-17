import type { Engine, Overview, TxnSummary, WindowAgg } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import {
  Banner,
  Button,
  Card,
  DataTable,
  EmptyState,
  ErrorState,
  LoadingSkeleton,
  Segmented,
  StaleBanner,
  Stat,
  formatEps,
  formatAmount,
  formatInt,
  formatMs,
  formatRelativeTime,
  formatTimestamp,
  shortId,
  useNow,
} from '@fraudwatch/ui';
import { Play } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api';
import { SeriesChart } from '../components/SeriesChart';
import { TxnTable } from '../components/TxnTable';
import { useSession, useStreamListener } from '../session';

export function OverviewPage() {
  const { mode } = useSession();
  return mode === 'none' ? <Intro /> : <Dashboard />;
}

function Intro() {
  const { startDemo, starting, startError, config } = useSession();
  return (
    <div className="fw-stack" style={{ gap: 20 }}>
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">Real-time fraud detection demo</div>
          <h1>FraudWatch Live</h1>
          <p>
            Start a private demo session, replay held-out IEEE-CIS transaction records through Kafka and Apache Flink, and watch each event get scored by the machine-learning model, checked by rules and
            routed to alert, review or allow — with the evidence for every decision.
          </p>
        </div>
      </div>
      <Card>
        <div className="fw-row" style={{ justifyContent: 'space-between', gap: 16 }}>
          <div className="fw-stack" style={{ gap: 4 }}>
            <strong>Your own isolated session</strong>
            <span className="fw-muted fw-small">
              {config.data
                ? `Each session is capped at ${formatInt(config.data.demo_max_events_per_session)} events and ${formatInt(config.data.demo_max_events_per_second)} events per second.`
                : 'Session caps are loaded from the API.'}
            </span>
          </div>
          <Button variant="primary" size="lg" icon={<Play size={18} aria-hidden="true" />} loading={starting} onClick={() => void startDemo()} data-testid="start-demo">
            {starting ? 'Starting…' : 'Start demo'}
          </Button>
        </div>
        {startError ? (
          <div style={{ marginTop: 12 }}>
            <ErrorState error={startError} title="Could not start a demo session" onRetry={() => void startDemo()} />
          </div>
        ) : null}
      </Card>
      <Banner tone="neutral">
        Educational demo only: not connected to any bank. Transactions are replayed records from the IEEE-CIS Fraud Detection dataset and the account id is a pseudonymous key derived from card1, addr1 and P_emaildomain. Alerts are model outputs, not confirmed fraud.
      </Banner>
    </div>
  );
}

const RECENT_LIMIT = 25;

function Dashboard() {
  const { sessionId, session, engine, setEngine, runId, stream, mode, config } = useSession();
  const now = useNow(5000);

  const overview = useApi((signal) => api.getOverview(sessionId!, { engine, run_id: runId }, signal), [sessionId, engine, runId], { enabled: !!sessionId, refreshMs: 5000 });
  const sparkProbe = useApi((signal) => api.getOverview(sessionId!, { engine: 'spark', run_id: runId }, signal), [sessionId, runId], { enabled: !!sessionId, refreshMs: 20000 });
  const sparkHasData = (sparkProbe.data?.counts.scored ?? 0) > 0;

  const snapshot = useApi((signal) => api.listTransactions(sessionId!, { engine, run_id: runId, limit: RECENT_LIMIT }, signal), [sessionId, engine, runId], { enabled: !!sessionId, refreshMs: 15000 });
  const [live, setLive] = useState<TxnSummary[]>([]);
  useEffect(() => setLive([]), [sessionId, engine, runId]);

  const refetchOverview = overview.refetch;
  const refetchSnapshot = snapshot.refetch;
  const lastRefetch = useRef(0);
  useStreamListener((type, data) => {
    if (type === 'txn') {
      const t = data as TxnSummary;
      if (t.engine !== engine || (runId && t.run_id !== runId)) return;
      setLive((xs) => [t, ...xs.filter((x) => x.event_id !== t.event_id)].slice(0, RECENT_LIMIT));
    }
    if (type === 'reset') {
      setLive([]);
      refetchOverview();
      refetchSnapshot();
      return;
    }
    if ((type === 'txn' || type === 'window' || type === 'alert') && Date.now() - lastRefetch.current > 2000) {
      lastRefetch.current = Date.now();
      refetchOverview();
    }
  });

  const recent = mergeRecent(live, snapshot.data?.items ?? []);
  const o = overview.data;

  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">{mode === 'view' ? 'Shared session (view only)' : 'Your demo session'}</div>
          <h1>Live overview</h1>
          <p className="fw-small fw-muted">
            Session <code>{shortId(sessionId, 12)}</code>
            {runId ? (
              <>
                {' '}
                · Run <code>{shortId(runId, 12)}</code>
              </>
            ) : (
              ' · All runs'
            )}
            {o ? ` · Updated ${formatRelativeTime(o.as_of, now)}` : ''}
          </p>
        </div>
        <div className="fw-row">
          <Segmented<Engine>
            label="Stream engine"
            value={engine}
            onChange={setEngine}
            options={[
              { value: 'flink', label: 'Flink' },
              { value: 'spark', label: 'Spark', disabled: !sparkHasData && engine !== 'spark', title: sparkHasData ? 'Spark results' : 'No Spark results for this session yet' },
            ]}
          />
        </div>
      </div>

      <StaleBanner status={stream.status} lastMessageAt={stream.lastMessageAt} nextRetryMs={stream.nextRetryMs} onReconnect={stream.reconnect} />

      {session && !session.active_run_id && !runId && mode === 'owner' && (
        <Banner tone="info">
          <span>
            No scenario is running. <Link to="/scenario">Open scenario controls</Link> to start one
            {config.data ? ` (up to ${formatInt(session.rate_cap_eps)} events per second).` : '.'}
          </span>
        </Banner>
      )}

      {overview.status === 'error' && !o ? (
        <Card>
          <ErrorState error={overview.error} title="Could not load the overview" onRetry={overview.refetch} />
        </Card>
      ) : !o ? (
        <Card>
          <LoadingSkeleton lines={4} label="Loading overview" />
        </Card>
      ) : (
        <OverviewBody o={o} />
      )}

      <Card title="Recent transactions" description={`Newest first, ${engine === 'flink' ? 'Flink' : 'Spark'} results. Select a row to inspect it.`} flush>
        {snapshot.status === 'error' && !recent.length ? (
          <ErrorState error={snapshot.error} title="Could not load transactions" onRetry={snapshot.refetch} />
        ) : snapshot.status === 'loading' && !recent.length ? (
          <LoadingSkeleton lines={5} label="Loading transactions" />
        ) : (
          <TxnTable
            rows={recent}
            caption="Recent transactions"
            empty={<EmptyState title="No transactions yet">Transactions appear here as soon as the engine persists them.</EmptyState>}
          />
        )}
      </Card>
    </div>
  );
}

function mergeRecent(live: TxnSummary[], snap: TxnSummary[]): TxnSummary[] {
  const seen = new Set<string>();
  const out: TxnSummary[] = [];
  for (const t of [...live, ...snap]) {
    if (seen.has(t.event_id)) continue;
    seen.add(t.event_id);
    out.push(t);
  }
  return out.sort((a, b) => b.persisted_at - a.persisted_at).slice(0, RECENT_LIMIT);
}

function OverviewBody({ o }: { o: Overview }) {
  const windows = [...o.windows].sort((a, b) => b.window_start - a.window_start).slice(0, 10);
  return (
    <>
      <div className="fw-grid fw-grid-4" data-testid="overview-stats">
        <Stat label="Scored" value={formatInt(o.counts.scored)} note={`${formatInt(o.counts.allow)} allowed · ${formatInt(o.counts.unscored)} unscored`} />
        <Stat label="Alerts" value={<span style={{ color: 'var(--alert)' }}>{formatInt(o.counts.alert)}</span>} note={`${formatInt(o.counts.late)} late events`} />
        <Stat label="Reviews" value={<span style={{ color: 'var(--review)' }}>{formatInt(o.counts.review)}</span>} note="Queued for a human decision" />
        <Stat label="Amount flagged" value={formatAmount(o.amount_flagged, { compact: (o.amount_flagged ?? 0) >= 1e7 })} note="Routed to alert or review. Not confirmed losses or money saved." />
      </div>
      <div className="fw-grid fw-grid-main-side">
        <Card title="Scored and flagged over time" description="From persisted results for this session.">
          <SeriesChart series={o.series} />
        </Card>
        <div className="fw-stack" style={{ gap: 12 }}>
          <Stat label="Throughput (last 1 min)" value={formatEps(o.throughput_eps_1m)} note={o.last_persisted_at ? `Last result persisted ${formatRelativeTime(o.last_persisted_at)}` : 'Nothing persisted yet'} />
          <Card title="End-to-end latency" description={`Produce → persisted, ${formatInt(o.latency_ms.n)} events${o.latency_ms.window_s ? `, last ${o.latency_ms.window_s} s` : ''}.`}>
            {o.latency_ms.n === 0 ? (
              <p className="fw-muted">No latency samples yet.</p>
            ) : (
              <div className="fw-grid fw-grid-3" style={{ gap: 8 }}>
                {(['p50', 'p95', 'p99'] as const).map((p) => (
                  <div key={p}>
                    <div className="fw-small fw-muted">{p}</div>
                    <div className="fw-num" style={{ fontWeight: 700, fontSize: '1.2rem' }}>
                      {formatMs(o.latency_ms[p])}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>
      </div>
      <Card title="1-minute event-time windows" description="Tumbling windows emitted by the engine after the watermark passes." flush>
        <DataTable<WindowAgg>
          caption="One-minute windows"
          compact
          rows={windows}
          rowKey={(w) => `${w.engine}:${w.window_start}`}
          empty={<EmptyState title="No closed windows yet">A window is emitted once the watermark passes its end.</EmptyState>}
          columns={[
            { key: 'w', header: 'Window', render: (w) => <span className="fw-num">{`${formatTimestamp(w.window_start).slice(0, 8)}–${formatTimestamp(w.window_end).slice(0, 8)}`}</span> },
            { key: 'n', header: 'Transactions', align: 'right', render: (w) => formatInt(w.txn_count) },
            { key: 'sum', header: 'Amount', align: 'right', render: (w) => formatAmount(w.amount_sum) },
            { key: 'a', header: 'Alerts', align: 'right', render: (w) => formatInt(w.alert_count) },
            { key: 'r', header: 'Reviews', align: 'right', render: (w) => formatInt(w.review_count) },
            { key: 'u', header: 'Unscored', align: 'right', render: (w) => formatInt(w.unscored_count) },
          ]}
        />
      </Card>
    </>
  );
}
