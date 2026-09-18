import { ApiError, type Alert, type AlertPage, type AlertStatus, type AuditEntry } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import {
  ActionBadge,
  AlertStatusPill,
  Banner,
  Button,
  Card,
  DataTable,
  Drawer,
  EmptyState,
  ErrorState,
  Field,
  KeyValue,
  LoadingSkeleton,
  StaleBanner,
  formatAmount,
  formatInt,
  formatRelativeTime,
  formatScore,
  formatTimestamp,
  humanize,
  shortId,
  useNow,
  useToast,
  type Column,
} from '@fraudwatch/ui';
import { Bell, Link2 } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { EvidenceBlocks, ScoreScale } from '../components/evidence';
import { useSession, useStreamListener } from '../session';

const STATUSES: AlertStatus[] = ['open', 'acknowledged', 'escalated', 'dismissed', 'confirmed_fraud_demo'];
const STATUS_LABEL: Record<AlertStatus, string> = {
  open: 'Open',
  acknowledged: 'Acknowledged',
  escalated: 'Escalated',
  dismissed: 'Dismissed',
  confirmed_fraud_demo: 'Confirmed fraud (demo)',
};
const DECISIONS: Array<{ status: Exclude<AlertStatus, 'open'>; label: string; variant: 'primary' | 'secondary' | 'danger' }> = [
  { status: 'acknowledged', label: 'Acknowledge', variant: 'secondary' },
  { status: 'escalated', label: 'Escalate', variant: 'secondary' },
  { status: 'dismissed', label: 'Dismiss', variant: 'secondary' },
  { status: 'confirmed_fraud_demo', label: 'Confirm fraud (demo)', variant: 'danger' },
];
const PAGE_SIZE = 50;

export function AlertsPage() {
  const { mode, sessionId, view, stream } = useSession();
  const [params, setParams] = useSearchParams();

  // A shared link can pre-select an action filter.
  const seeded = useRef(false);
  useEffect(() => {
    if (seeded.current) return;
    seeded.current = true;
    if (view?.action && !params.get('action')) {
      const next = new URLSearchParams(params);
      next.set('action', view.action);
      setParams(next, { replace: true });
    }
  }, [view, params, setParams]);

  const filters = {
    status: params.get('status') || null,
    action: params.get('action') || null,
    engine: params.get('engine') || null,
    min_score: params.get('min_score') ? Number(params.get('min_score')) : null,
  };
  const setFilter = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    next.delete('alert');
    setParams(next, { replace: true });
  };

  const [extra, setExtra] = useState<Alert[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState<unknown>(null);
  const filterKey = JSON.stringify(filters);
  const list = useApi((signal) => api.listAlerts(sessionId!, { ...filters, limit: PAGE_SIZE }, signal), [sessionId, filterKey], { enabled: !!sessionId, refreshMs: 15000 });
  useEffect(() => {
    setExtra([]);
    setCursor(null);
  }, [sessionId, filterKey]);

  const refetch = list.refetch;
  const lastRefetch = useRef(0);
  useStreamListener((type) => {
    if ((type === 'alert' || type === 'reset') && Date.now() - lastRefetch.current > 1500) {
      lastRefetch.current = Date.now();
      refetch();
    }
  });

  const loadMore = async (page: AlertPage) => {
    const from = cursor ?? page.next_cursor;
    if (!from || !sessionId) return;
    setLoadingMore(true);
    setMoreError(null);
    try {
      const res = await api.listAlerts(sessionId, { ...filters, limit: PAGE_SIZE, cursor: from });
      setExtra((xs) => [...xs, ...res.items]);
      setCursor(res.next_cursor ?? '');
    } catch (e) {
      setMoreError(e);
    } finally {
      setLoadingMore(false);
    }
  };

  const selectedId = params.get('alert');
  const [localUpdates, setLocalUpdates] = useState<Record<string, Alert>>({});
  const rows = dedupe([...(list.data?.items ?? []), ...extra]).map((a) => {
    const local = localUpdates[a.id];
    return local && local.updated_at >= a.updated_at ? local : a;
  });
  const selected = rows.find((a) => a.id === selectedId) ?? null;
  const select = (id: string | null) => {
    const next = new URLSearchParams(params);
    if (id) next.set('alert', id);
    else next.delete('alert');
    setParams(next);
  };

  if (mode === 'none') {
    // The gate still renders the page heading: every route keeps exactly one h1 so the document
    // outline and the screen-reader landmark stay correct even when there is nothing to show.
    return (
      <div className="fw-stack" style={{ gap: 16 }}>
        <div className="fw-page-head">
          <div>
            <div className="fw-eyebrow">Human review queue</div>
            <h1>Alerts</h1>
            <p>Transactions the review policy routed to alert or review. Open one to see its evidence and record a decision.</p>
          </div>
        </div>
        <Card>
          <EmptyState title="Start a demo to see alerts" icon={<Bell size={22} aria-hidden="true" />} action={<Link className="fw-btn fw-btn-primary" to="/console">Go to Start demo</Link>}>
            Alerts belong to a demo session. Start one from the overview.
          </EmptyState>
        </Card>
      </div>
    );
  }

  const hasMore = cursor === null ? !!list.data?.next_cursor : cursor !== '';
  const counts = list.data?.counts_by_status ?? {};

  const columns: Column<Alert>[] = [
    { key: 'created', header: 'Raised', render: (a) => <span className="fw-num">{formatTimestamp(a.created_at)}</span> },
    { key: 'action', header: 'Action', render: (a) => <ActionBadge action={a.action} /> },
    { key: 'status', header: 'Status', render: (a) => <AlertStatusPill status={a.status} /> },
    { key: 'amount', header: 'Amount', align: 'right', render: (a) => <span className="fw-num">{formatAmount(a.amount)}</span> },
    { key: 'score', header: 'Score', align: 'right', render: (a) => <span className="fw-num">{formatScore(a.score)}</span> },
    { key: 'source', header: 'Source record', render: (a) => a.source_ref ?? <span className="fw-muted">Not provided</span> },
    { key: 'engine', header: 'Engine', render: (a) => humanize(a.engine) },
    { key: 'rules', header: 'Rules fired', render: (a) => (a.rules_fired.length ? a.rules_fired.join(', ') : <span className="fw-muted">None</span>) },
  ];

  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">Human review queue</div>
          <h1>Alerts</h1>
          <p>Transactions the review policy routed to alert or review. Open one to see its evidence and record a decision.</p>
        </div>
      </div>

      <StaleBanner status={stream.status} lastMessageAt={stream.lastMessageAt} nextRetryMs={stream.nextRetryMs} onReconnect={stream.reconnect} />

      <Banner tone="neutral">Decisions are recorded in an audit trail for this demo. They never retrain or change the model.</Banner>
      {mode === 'view' && <Banner tone="info">You are viewing a shared session. Decisions are disabled in view-only mode.</Banner>}

      <Card title="Filters" as="div">
        <div className="fw-filters" role="group" aria-label="Alert filters">
          <Field label="Status" htmlFor="f-status">
            <select id="f-status" className="fw-select" value={filters.status ?? ''} onChange={(e) => setFilter('status', e.target.value)}>
              <option value="">All statuses</option>
              {STATUSES.map((s) => (
                <option key={s} value={s}>
                  {`${STATUS_LABEL[s]}${counts[s] !== undefined ? ` (${formatInt(counts[s])})` : ''}`}
                </option>
              ))}
            </select>
          </Field>
          <Field label="Action" htmlFor="f-action">
            <select id="f-action" className="fw-select" value={filters.action ?? ''} onChange={(e) => setFilter('action', e.target.value)}>
              <option value="">Alert and review</option>
              <option value="alert">Alert</option>
              <option value="review">Review</option>
            </select>
          </Field>
          <Field label="Engine" htmlFor="f-engine">
            <select id="f-engine" className="fw-select" value={filters.engine ?? ''} onChange={(e) => setFilter('engine', e.target.value)}>
              <option value="">Both engines</option>
              <option value="flink">Flink</option>
              <option value="spark">Spark</option>
            </select>
          </Field>
          <Field label="Minimum score" htmlFor="f-min" help="Between 0 and 1">
            <input
              id="f-min"
              className="fw-input"
              type="number"
              min={0}
              max={1}
              step={0.01}
              key={String(filters.min_score)}
              defaultValue={filters.min_score ?? ''}
              onBlur={(e) => setFilter('min_score', e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') setFilter('min_score', (e.target as HTMLInputElement).value);
              }}
            />
          </Field>
        </div>
      </Card>

      <Card title="Alert queue" description={list.data ? `${formatInt(rows.length)} shown${hasMore ? ', more available' : ''}.` : undefined} flush>
        {list.status === 'error' && !list.data ? (
          <ErrorState error={list.error} title="Could not load alerts" onRetry={list.refetch} />
        ) : !list.data ? (
          <LoadingSkeleton lines={5} label="Loading alerts" />
        ) : (
          <>
            <DataTable
              caption="Alerts"
              columns={columns}
              rows={rows}
              rowKey={(a) => a.id}
              selectedKey={selectedId}
              onRowActivate={(a) => select(a.id)}
              rowLabel={(a) => `${a.action} ${formatAmount(a.amount)}, score ${formatScore(a.score)}, status ${STATUS_LABEL[a.status] ?? a.status}. Open evidence.`}
              empty={<EmptyState title="No alerts match">Alerts appear when the policy routes a transaction to alert or review. Try clearing filters or starting a scenario.</EmptyState>}
            />
            {(hasMore || moreError) && (
              <div style={{ padding: 12 }} className="fw-stack">
                {moreError ? <ErrorState error={moreError} title="Could not load more alerts" onRetry={() => void loadMore(list.data!)} /> : null}
                {hasMore && (
                  <Button onClick={() => void loadMore(list.data!)} loading={loadingMore}>
                    Load more
                  </Button>
                )}
              </div>
            )}
          </>
        )}
      </Card>

      <Drawer
        open={!!selectedId}
        onClose={() => select(null)}
        title={selected ? `${selected.action === 'alert' ? 'Alert' : 'Review'} · ${formatAmount(selected.amount)}` : 'Alert'}
        description={selected ? `Event ${shortId(selected.event_id, 14)} · ${humanize(selected.engine)}` : undefined}
      >
        {selected ? (
          <AlertDetail
            alert={selected}
            readOnly={mode !== 'owner'}
            onUpdated={(a) => {
              setLocalUpdates((m) => ({ ...m, [a.id]: a }));
              refetch();
            }}
          />
        ) : list.data ? (
          <EmptyState title="Alert not in the current list">It may be filtered out. Clear the filters and try again.</EmptyState>
        ) : (
          <LoadingSkeleton lines={4} />
        )}
      </Drawer>
    </div>
  );
}

function dedupe(xs: Alert[]): Alert[] {
  const seen = new Set<string>();
  return xs.filter((a) => (seen.has(a.id) ? false : (seen.add(a.id), true)));
}

function AlertDetail({ alert, readOnly, onUpdated }: { alert: Alert; readOnly: boolean; onUpdated: (a: Alert) => void }) {
  const tx = useApi((signal) => api.getTransaction(alert.event_id, signal), [alert.event_id]);
  const audit = useApi(() => api.getAudit(alert.id), [alert.id, alert.updated_at]);
  const detail = tx.data?.engines[alert.engine] ?? null;
  const now = useNow(10000);

  return (
    <div className="fw-stack" style={{ gap: 20 }} data-testid="alert-detail">
      <div className="fw-row" style={{ gap: 8 }}>
        <ActionBadge action={alert.action} />
        <AlertStatusPill status={alert.status} />
        <Link className="fw-btn fw-btn-ghost fw-btn-sm" to={`/tx/${encodeURIComponent(alert.event_id)}?engine=${alert.engine}`}>
          <Link2 size={16} aria-hidden="true" /> Open in inspector
        </Link>
      </div>
      <KeyValue
        items={[
          { key: 'Raised', value: `${formatTimestamp(alert.created_at, true)} (${formatRelativeTime(alert.created_at, now)})` },
          { key: 'Account', value: <code>{alert.account_id}</code> },
          { key: 'Source record', value: alert.source_ref ?? 'Not provided' },
          { key: 'Last note', value: alert.decision_note ?? 'None' },
        ]}
      />

      <section aria-label="Score">
        <h3 style={{ fontSize: '1rem', marginBottom: 8 }}>Score</h3>
        <ScoreScale score={alert.score} thresholds={detail?.model?.thresholds} />
      </section>

      <section aria-label="Evidence">
        <h3 style={{ fontSize: '1rem', marginBottom: 8 }}>Why flagged?</h3>
        {tx.status === 'error' && !tx.data ? <ErrorState error={tx.error} title="Could not load evidence" onRetry={tx.refetch} /> : !tx.data ? <LoadingSkeleton lines={4} label="Loading evidence" /> : <EvidenceBlocks detail={detail} />}
      </section>

      <DecisionForm alert={alert} readOnly={readOnly} onUpdated={onUpdated} />

      <section aria-label="Audit trail">
        <h3 style={{ fontSize: '1rem', marginBottom: 8 }}>Audit trail</h3>
        {audit.status === 'error' && !audit.data ? <ErrorState error={audit.error} title="Could not load the audit trail" onRetry={audit.refetch} /> : !audit.data ? <LoadingSkeleton lines={3} label="Loading audit trail" /> : <AuditTrail entries={audit.data} />}
      </section>
    </div>
  );
}

function DecisionForm({ alert, readOnly, onUpdated }: { alert: Alert; readOnly: boolean; onUpdated: (a: Alert) => void }) {
  const toast = useToast();
  const [note, setNote] = useState('');
  const [busy, setBusy] = useState<AlertStatus | null>(null);
  const [error, setError] = useState<unknown>(null);
  const noteMissing = note.trim().length === 0;

  const decide = async (status: AlertStatus) => {
    setBusy(status);
    setError(null);
    try {
      const updated = await api.updateAlert(alert.id, status, note.trim());
      onUpdated(updated);
      setNote('');
      toast({ kind: 'success', title: `Alert marked ${STATUS_LABEL[status].toLowerCase()}`, message: 'Recorded in the audit trail. The model is not retrained.' });
    } catch (e) {
      setError(e);
    } finally {
      setBusy(null);
    }
  };

  return (
    <section aria-label="Decision" className="fw-stack" style={{ gap: 10 }}>
      <h3 style={{ fontSize: '1rem' }}>Decision</h3>
      {readOnly ? (
        <p className="fw-muted">Decisions can only be recorded by the session owner.</p>
      ) : (
        <>
          <Field label="Decision note" htmlFor="decision-note" help="Required. Stored with the decision in the audit trail.">
            <textarea id="decision-note" className="fw-textarea" rows={3} value={note} onChange={(e) => setNote(e.target.value)} maxLength={1000} />
          </Field>
          <div className="fw-row" style={{ gap: 8 }}>
            {DECISIONS.map((d) => (
              <Button key={d.status} variant={d.variant} disabled={noteMissing || alert.status === d.status || (busy !== null && busy !== d.status)} loading={busy === d.status} onClick={() => void decide(d.status)}>
                {d.label}
              </Button>
            ))}
          </div>
          <p className="fw-small fw-muted">“Confirm fraud (demo)” is a label for this demo session only. Decisions never retrain the model.</p>
          {error ? (
            <ErrorState
              error={error}
              title={error instanceof ApiError && error.status === 409 ? 'This alert changed since you opened it' : 'The decision was not saved'}
            />
          ) : null}
        </>
      )}
    </section>
  );
}

/** Shows each audit entry and checks that prev_hash links to the previous entry's hash. */
export function AuditTrail({ entries }: { entries: AuditEntry[] }) {
  if (!entries.length) return <p className="fw-muted">No decisions have been recorded for this alert yet.</p>;
  const sorted = [...entries].sort((a, b) => a.at - b.at);
  const links = sorted.map((e, i) => (i === 0 ? true : e.prev_hash === sorted[i - 1]!.hash));
  const intact = links.every(Boolean);
  return (
    <div className="fw-stack" style={{ gap: 10 }}>
      <Banner tone={intact ? 'neutral' : 'error'}>
        {intact
          ? `Hash chain links are consistent across ${sorted.length} entr${sorted.length === 1 ? 'y' : 'ies'} (each entry references the previous hash). Hashes are computed by the API.`
          : 'Hash chain break detected: an entry does not reference the previous entry’s hash.'}
      </Banner>
      <ol className="fw-timeline" data-testid="audit-trail">
        {sorted.map((e, i) => (
          <li key={e.id}>
            <div className="fw-row" style={{ justifyContent: 'space-between', gap: 8 }}>
              <strong>
                {humanize(e.action)}
                {e.to_status ? `: ${e.from_status ? `${humanize(e.from_status)} → ` : ''}${humanize(e.to_status)}` : ''}
              </strong>
              <span className="fw-small fw-muted fw-num">{formatTimestamp(e.at, true)}</span>
            </div>
            <div className="fw-small">by {e.actor}</div>
            {e.note && <div className="fw-small">“{e.note}”</div>}
            <div className="fw-hash">
              hash {shortId(e.hash, 16)}
              {e.prev_hash ? ` · prev ${shortId(e.prev_hash, 16)}` : ' · first entry'}
              {!links[i] && <strong style={{ color: 'var(--alert)' }}> · link broken</strong>}
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}
