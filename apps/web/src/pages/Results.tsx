import type { Action, AlertStatus, Overview, Run, RunStatus, TxnSummary } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import { Button, Card, EmptyState, ErrorState, LoadingSkeleton, StatusPill, formatAmount, formatInt, formatPct, formatRelativeTime, useNow } from '@fraudwatch/ui';
import { CheckCircle2, Clock, History, Play, ShieldAlert, UserSearch } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api } from '../api';
import { useModelEvidence } from '../lab/snapshot';
import { useSession } from '../session';

/** Plain-language results for someone who is not an engineer: what was checked, what was stopped, and every simulation so far. */

const ACTION_WORDS: Record<Action, { label: string; tone: 'alert' | 'review' | 'allow' | 'muted' }> = {
  alert: { label: 'Flagged as likely fraud', tone: 'alert' },
  review: { label: 'Sent to a person to check', tone: 'review' },
  allow: { label: 'Approved', tone: 'allow' },
  unscored: { label: 'Not checked', tone: 'muted' },
};

const RULE_WORDS: Record<string, string> = {
  R1_BURST: 'many payments from one account within a minute',
  R2_PROFILE_DEVIATION: 'far bigger than this account usually spends',
  R3_TEST_THEN_LARGE: 'tiny test payments, then a large one',
};

const STATUS_WORDS: Record<RunStatus, { label: string; tone: 'info' | 'allow' | 'neutral' | 'alert' | 'review' }> = {
  starting: { label: 'Starting', tone: 'info' },
  running: { label: 'Running now', tone: 'info' },
  paused: { label: 'Paused', tone: 'review' },
  completed: { label: 'Finished', tone: 'allow' },
  stopped: { label: 'Stopped early', tone: 'neutral' },
  failed: { label: 'Failed', tone: 'alert' },
};

const DECISION_WORDS: Partial<Record<AlertStatus, string>> = {
  open: 'waiting for a person',
  acknowledged: 'being looked at',
  escalated: 'escalated',
  confirmed_fraud_demo: 'confirmed as fraud',
  dismissed: 'cleared as genuine',
};

function reason(t: TxnSummary) {
  if (t.rules_fired.length) return t.rules_fired.map((r) => RULE_WORDS[r] ?? r.toLowerCase().replace(/_/g, ' ')).join(' · ');
  if (t.action === 'alert' || t.action === 'review') return 'the model saw a risky pattern';
  if (t.action === 'allow') return 'looked normal';
  return 'the checker was unavailable';
}

/** A single bar split into approved / check / flagged. */
function SplitBar({ c, big }: { c: Overview['counts']; big?: boolean }) {
  const total = c.allow + c.review + c.alert + c.unscored;
  const seg = (n: number) => `${total ? (n / total) * 100 : 0}%`;
  return (
    <div className="fw-split" data-big={big || undefined} role="img" aria-label={`${c.allow} approved, ${c.review} sent to a person, ${c.alert} flagged as likely fraud`}>
      <span data-k="allow" style={{ width: seg(c.allow) }} />
      <span data-k="review" style={{ width: seg(c.review) }} />
      <span data-k="alert" style={{ width: seg(c.alert) }} />
      <span data-k="unscored" style={{ width: seg(c.unscored) }} />
    </div>
  );
}

function BigNumber({ icon, value, label, tone }: { icon: ReactNode; value: string; label: string; tone: string }) {
  return (
    <div className="fw-bignum" data-tone={tone}>
      <span className="fw-bignum-icon">{icon}</span>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function ModelReportCard() {
  const ev = useModelEvidence();
  const stream = ev.data?.results.stream;
  const r = stream ? stream.test?.[stream.selected] ?? Object.values(stream.test ?? {})[0] : undefined;
  if (!r) return null;
  const p = r.at_threshold.precision, rec = r.at_threshold.recall;
  return (
    <Card title="How good is the checker?" description="Tested on past payments it had never seen">
      <div className="fw-grid fw-grid-2">
        <BigNumber icon={<ShieldAlert size={18} />} tone="alert" value={formatPct(rec, 0)} label="of real fraud it catches" />
        <BigNumber icon={<CheckCircle2 size={18} />} tone="allow" value={formatPct(p, 0)} label="of its flags turn out to be real fraud" />
      </div>
      <p className="fw-small fw-muted fw-space-top">
        In plain words: out of 10 fraudulent payments it stops about {Math.round(rec * 10)}; out of 10 payments it flags, about {Math.round(p * 10)} are really fraud. The rest go to a person, which is why people review.
      </p>
    </Card>
  );
}

function RunRow({ run, sessionId, names, onOpen }: { run: Run; sessionId: string; names: Map<string, string>; onOpen: () => void }) {
  const now = useNow(15000);
  const live = run.status === 'running' || run.status === 'starting';
  const ov = useApi((signal) => api.getOverview(sessionId, { run_id: run.id }, signal), [sessionId, run.id], { refreshMs: live ? 5000 : 0 });
  const c = ov.data?.counts;
  const st = STATUS_WORDS[run.status];
  const name = names.get(run.preset ?? '') ?? names.get(run.scenario) ?? run.scenario.replace(/_/g, ' ');
  return (
    <li className="fw-run" data-testid="results-run">
      <div className="fw-run-head">
        <strong>{name}</strong>
        <StatusPill tone={st.tone}>{st.label}</StatusPill>
        <span className="fw-small fw-muted">{run.started_at ? formatRelativeTime(run.started_at, now) : 'not started'}</span>
        <Button size="sm" variant="ghost" onClick={onOpen}>Details</Button>
      </div>
      {c ? (
        <>
          <SplitBar c={c} />
          <div className="fw-run-facts">
            <span><b>{formatInt(c.scored)}</b> checked</span>
            <span data-k="allow"><b>{formatInt(c.allow)}</b> approved</span>
            <span data-k="review"><b>{formatInt(c.review)}</b> to a person</span>
            <span data-k="alert"><b>{formatInt(c.alert)}</b> likely fraud</span>
            {ov.data?.amount_flagged != null && <span><b>{formatAmount(ov.data.amount_flagged, { compact: true })}</b> held back</span>}
          </div>
        </>
      ) : ov.error ? (
        <span className="fw-small fw-muted">Results unavailable right now.</span>
      ) : (
        <LoadingSkeleton lines={1} />
      )}
    </li>
  );
}

export function ResultsPage() {
  const { mode, sessionId, runId, setRunId, startDemo, starting, health } = useSession();
  const navigate = useNavigate();
  const now = useNow(5000);
  const on = mode !== 'none' && !!sessionId;
  const all = useApi((signal) => api.getOverview(sessionId!, {}, signal), [sessionId], { enabled: on, refreshMs: 5000 });
  const recent = useApi((signal) => api.listTransactions(sessionId!, { limit: 8 }, signal), [sessionId], { enabled: on, refreshMs: 4000 });
  const alerts = useApi((signal) => api.listAlerts(sessionId!, { limit: 1 }, signal), [sessionId], { enabled: on, refreshMs: 10000 });
  const runs = useApi(() => api.listRuns(sessionId!), [sessionId], { enabled: on, refreshMs: 10000 });
  const presets = useApi((signal) => api.listScenarios(signal), []);
  const names = new Map((presets.data ?? []).flatMap((p) => [[p.id, p.name] as [string, string], ...(p.scenario ? [[p.scenario, p.name] as [string, string]] : [])]));

  const head = (
    <div className="fw-page-head">
      <div>
        <div className="fw-eyebrow">Results</div>
        <h1>What FraudWatch found</h1>
        <p>Every payment is checked in under a second. Here is what happened, in plain words.</p>
      </div>
    </div>
  );

  if (!on) {
    return (
      <div className="fw-stack fw-results">
        {head}
        <Card>
          <EmptyState
            title="No simulation yet"
            icon={<Play size={20} />}
            action={<Button variant="primary" size="lg" icon={<Play size={18} />} loading={starting} onClick={() => void startDemo().then(() => navigate('/scenario'))} data-testid="results-start">Start a simulation</Button>}
          >
            We replay real, anonymised card payments. Nothing is charged.
          </EmptyState>
        </Card>
        <ModelReportCard />
      </div>
    );
  }

  const c = all.data?.counts;
  const statusCounts = alerts.data?.counts_by_status ?? {};
  const decided = Object.entries(statusCounts).filter(([, n]) => n);
  const runList = [...(runs.data ?? [])].sort((a, b) => (b.started_at ?? 0) - (a.started_at ?? 0));
  const running = runList.some((r) => r.status === 'running' || r.status === 'starting');
  const up = health.data?.overall === 'ok';

  return (
    <div className="fw-stack fw-results" data-testid="results-page">
      {head}

      <Card data-testid="results-summary">
        {all.error && !c ? (
          <ErrorState error={all.error} title="Results are not reachable yet" onRetry={all.refetch} />
        ) : !c ? (
          <LoadingSkeleton lines={3} />
        ) : c.scored === 0 ? (
          <EmptyState title="Nothing checked yet" icon={<Clock size={20} />} action={<Link className="fw-btn fw-btn-primary" to="/scenario">Run a simulation</Link>}>
            Start a scenario and payments appear here within seconds.
          </EmptyState>
        ) : (
          <div className="fw-stack" style={{ gap: 16 }}>
            <p className="fw-results-headline">
              <b>{formatInt(c.scored)}</b> payments checked. <span data-k="alert"><b>{formatInt(c.alert)}</b> flagged as likely fraud</span>, <span data-k="review"><b>{formatInt(c.review)}</b> sent to a person</span>, <span data-k="allow"><b>{formatInt(c.allow)}</b> approved</span>.
            </p>
            <SplitBar c={c} big />
            <div className="fw-grid fw-grid-4">
              <BigNumber icon={<CheckCircle2 size={18} />} tone="allow" value={formatPct(c.allow / c.scored, 0)} label="approved instantly" />
              <BigNumber icon={<UserSearch size={18} />} tone="review" value={formatInt(c.review)} label="need a human look" />
              <BigNumber icon={<ShieldAlert size={18} />} tone="alert" value={formatInt(c.alert)} label="likely fraud stopped" />
              <BigNumber icon={<Clock size={18} />} tone="info" value={all.data?.latency_ms?.p50 != null ? `${(all.data.latency_ms.p50 / 1000).toFixed(2)} s` : '—'} label="typical time to decide" />
            </div>
            <div className="fw-row fw-small fw-muted" style={{ flexWrap: 'wrap', gap: 10 }}>
              <StatusPill tone={running ? 'info' : 'neutral'}>{running ? 'Live: updating every few seconds' : 'No simulation running'}</StatusPill>
              {all.data?.amount_flagged != null && <span>{formatAmount(all.data.amount_flagged)} in flagged payments held back (not confirmed losses)</span>}
              {!up && health.data && <span>Some system parts are still starting.</span>}
            </div>
          </div>
        )}
      </Card>

      <div className="fw-grid fw-grid-2 fw-results-grid">
        <Card title="Happening right now" description="Newest payments first" actions={<Link to="/console" className="fw-small">Full console</Link>}>
          {!recent.data?.items.length ? (
            <p className="fw-muted fw-small">No payments yet.</p>
          ) : (
            <ul className="fw-feed" data-testid="results-feed">
              {recent.data.items.map((t) => {
                const w = ACTION_WORDS[t.action];
                return (
                  <li key={`${t.event_id}-${t.engine}`}>
                    <Link to={`/tx/${encodeURIComponent(t.event_id)}`} className="fw-feed-row" data-k={t.action}>
                      <span className="fw-feed-dot" />
                      <span className="fw-feed-main">
                        <b>{t.amount != null ? formatAmount(t.amount) : 'Payment'}</b> · {w.label}
                        <small>{reason(t)}</small>
                      </span>
                      <span className="fw-small fw-muted">{formatRelativeTime(t.persisted_at, now)}</span>
                    </Link>
                  </li>
                );
              })}
            </ul>
          )}
        </Card>

        <div className="fw-stack">
          <Card title="Human decisions" description="What reviewers did with the flags" actions={<Link to="/alerts" className="fw-small">Review queue</Link>}>
            {!decided.length ? (
              <p className="fw-muted fw-small">No flags yet.</p>
            ) : (
              <ul className="fw-decisions">
                {decided.map(([k, n]) => (
                  <li key={k}><b>{formatInt(n)}</b> {DECISION_WORDS[k as AlertStatus] ?? k}</li>
                ))}
              </ul>
            )}
          </Card>
          <ModelReportCard />
        </div>
      </div>

      <Card title="Simulation history" description="Every simulation in this session, newest first" actions={<History size={18} aria-hidden="true" />}>
        {runs.error && !runs.data ? (
          <ErrorState error={runs.error} title="History unavailable" onRetry={runs.refetch} />
        ) : !runList.length ? (
          <EmptyState title="No simulations yet" action={<Link className="fw-btn fw-btn-primary" to="/scenario">Run one</Link>}>Each scenario you run is kept here with its results.</EmptyState>
        ) : (
          <ol className="fw-runs" data-testid="results-history">
            {runList.map((r) => (
              <RunRow key={r.id} run={r} sessionId={sessionId!} names={names} onOpen={() => { setRunId(r.id); navigate('/console'); }} />
            ))}
          </ol>
        )}
        {runId && <p className="fw-small fw-muted fw-space-top">The console is currently focused on one simulation; "Details" switches it.</p>}
      </Card>

      <p className="fw-small fw-muted">Replayed, anonymised IEEE-CIS payments. A flag is the system's judgement, not proof of fraud.</p>
    </div>
  );
}
