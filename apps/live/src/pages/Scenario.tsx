import { ApiError, describeError, normalizeOptions, type Pattern, type Run } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import {
  Banner,
  Button,
  Card,
  DataTable,
  Dialog,
  EmptyState,
  ErrorState,
  Field,
  KeyValue,
  LoadingSkeleton,
  Meter,
  StatusPill,
  formatEps,
  formatInt,
  formatRelativeTime,
  formatTimestamp,
  humanize,
  shortId,
  useNow,
  useToast,
  type Column,
} from '@fraudwatch/ui';
import { Pause, Play, Repeat, RotateCcw, SlidersHorizontal, Square, Zap } from 'lucide-react';
import { useEffect, useId, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api';
import { useSession, useStreamListener } from '../session';

const ACTIVE: Run['status'][] = ['starting', 'running', 'paused'];

/** Human text for errors from run controls, with explicit handling for demo caps (HTTP 429). */
function controlErrorText(err: unknown): string {
  if (err instanceof ApiError && err.isRateLimited) {
    const wait = err.retryAfterS ? ` Try again in about ${Math.ceil(err.retryAfterS)} s.` : '';
    return `Demo limit reached: ${err.message}.${wait}`;
  }
  return describeError(err);
}

export function ScenarioPage() {
  const { mode, sessionId, session, sessionState, config, runId, setRunId, resetSession } = useSession();
  const toast = useToast();
  const now = useNow(5000);
  const readOnly = mode !== 'owner';

  const runs = useApi(() => api.listRuns(sessionId!), [sessionId], { enabled: !!sessionId, refreshMs: 10000 });
  const run = useApi(() => api.getRun(runId!), [runId], { enabled: !!runId, refreshMs: 3000 });
  const refetchRun = run.refetch;
  const refetchRuns = runs.refetch;
  useStreamListener((type) => {
    if (type === 'run') {
      refetchRun();
      refetchRuns();
    }
  });

  const scenarios = normalizeOptions(config.data?.scenarios);
  const patterns = normalizeOptions(config.data?.patterns);
  const rateCap = Math.max(1, Math.min(config.data?.demo_max_events_per_second ?? Infinity, session?.rate_cap_eps ?? Infinity));
  const capKnown = Number.isFinite(rateCap);

  const [scenario, setScenario] = useState('');
  const [rate, setRate] = useState<number | null>(null);
  const [seed, setSeed] = useState('');
  useEffect(() => {
    if (!scenario && scenarios.length) setScenario(scenarios[0]!.id);
  }, [scenario, scenarios]);
  const current = run.data && run.data.id === runId ? run.data : undefined;
  useEffect(() => {
    if (rate === null && capKnown) setRate(current?.rate_eps ?? Math.max(1, Math.round(rateCap / 2)));
  }, [rate, capKnown, rateCap, current]);

  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [resetOpen, setResetOpen] = useState(false);
  const rateId = useId();

  const act = async (label: string, fn: () => Promise<unknown>, success?: string) => {
    setBusy(label);
    setError(null);
    try {
      await fn();
      if (success) toast({ kind: 'success', title: success });
      refetchRun();
      refetchRuns();
      sessionState.refetch();
    } catch (e) {
      setError(e);
      toast({ kind: 'error', title: `${label} failed`, message: controlErrorText(e) });
    } finally {
      setBusy(null);
    }
  };

  if (mode === 'none') {
    return (
      <Card>
        <EmptyState title="Start a demo to control scenarios" icon={<SlidersHorizontal size={22} aria-hidden="true" />} action={<Link className="fw-btn fw-btn-primary" to="/console">Go to Start demo</Link>}>
          Scenario controls act on your own demo session.
        </EmptyState>
      </Card>
    );
  }

  const active = !!current && ACTIVE.includes(current.status);
  const effectiveRate = Math.min(rate ?? 1, capKnown ? rateCap : 1);

  const runColumns: Column<Run>[] = [
    { key: 'started', header: 'Started', render: (r) => <span className="fw-num">{r.started_at ? formatTimestamp(r.started_at) : 'Not started'}</span> },
    { key: 'scenario', header: 'Scenario', render: (r) => humanize(r.scenario) },
    { key: 'status', header: 'Status', render: (r) => <RunStatusPill status={r.status} /> },
    { key: 'sent', header: 'Events', align: 'right', render: (r) => `${formatInt(r.events_sent)} / ${formatInt(r.max_events)}` },
    { key: 'seed', header: 'Seed', align: 'right', render: (r) => <span className="fw-num">{r.seed}</span> },
    { key: 'replay', header: 'Replay of', render: (r) => (r.replay_of ? <code>{shortId(r.replay_of)}</code> : <span className="fw-muted">Original</span>) },
  ];

  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">Scenario controls</div>
          <h1>Scenario</h1>
          <p>Replay held-out IEEE-CIS records into your session, inject known delivery and fraud patterns, and replay a run with the same seed.</p>
        </div>
      </div>

      {readOnly && <Banner tone="info">You are viewing a shared session. Scenario controls are disabled in view-only mode.</Banner>}

      <div className="fw-grid fw-grid-2">
        <Card title="Session usage">
          {!session ? (
            sessionState.status === 'error' ? <ErrorState error={sessionState.error} title="Could not load the session" onRetry={sessionState.refetch} /> : <LoadingSkeleton lines={3} label="Loading session" />
          ) : (
            <div className="fw-stack" style={{ gap: 12 }}>
              <div>
                <div className="fw-row" style={{ justifyContent: 'space-between' }}>
                  <span>Events used</span>
                  <span className="fw-num" data-testid="events-used">
                    {formatInt(session.events_used)} of {formatInt(session.event_cap)}
                  </span>
                </div>
                <Meter value={session.events_used} max={session.event_cap} label="Events used of the session cap" />
              </div>
              <KeyValue
                items={[
                  { key: 'Session', value: <code>{shortId(session.id, 12)}</code> },
                  { key: 'Rate cap', value: formatEps(session.rate_cap_eps) },
                  { key: 'Expires', value: `${formatTimestamp(session.expires_at, true)} (${formatRelativeTime(session.expires_at, now)})` },
                  { key: 'Status', value: humanize(session.status) },
                ]}
              />
              {session.events_used >= session.event_cap && <Banner tone="warn">This session has used its event allowance. Reset the session to start a fresh one.</Banner>}
              {!readOnly && (
                <div>
                  <Button icon={<RotateCcw size={16} aria-hidden="true" />} onClick={() => setResetOpen(true)}>
                    Reset session
                  </Button>
                </div>
              )}
            </div>
          )}
        </Card>

        <Card title="Current run" description={current ? `Run ${shortId(current.id, 12)}` : undefined}>
          {!runId ? (
            <EmptyState title="No run selected">Start a scenario below, or pick a previous run from the list.</EmptyState>
          ) : run.status === 'error' && !current ? (
            <ErrorState error={run.error} title="Could not load the run" onRetry={run.refetch} />
          ) : !current ? (
            <LoadingSkeleton lines={3} label="Loading run" />
          ) : (
            <div className="fw-stack" style={{ gap: 12 }} data-testid="current-run">
              <div className="fw-row">
                <RunStatusPill status={current.status} />
                <span className="fw-small fw-muted">{humanize(current.scenario)}</span>
              </div>
              <Meter value={current.events_sent} max={current.max_events} label="Events sent in this run" />
              <KeyValue
                items={[
                  { key: 'Events sent', value: `${formatInt(current.events_sent)} of ${formatInt(current.max_events)}` },
                  { key: 'Rate', value: formatEps(current.rate_eps) },
                  { key: 'Seed', value: <span className="fw-num" data-testid="run-seed">{current.seed}</span> },
                  { key: 'Engines', value: current.engines.map(humanize).join(', ') },
                  { key: 'Replay of', value: current.replay_of ? <code>{shortId(current.replay_of, 12)}</code> : 'Original run' },
                  { key: 'Started', value: current.started_at ? formatTimestamp(current.started_at, true) : 'Not started' },
                ]}
              />
              {current.error && <Banner tone="error">{current.error}</Banner>}
              {!readOnly && (
                <div className="fw-row" style={{ gap: 8 }}>
                  {current.status === 'paused' ? (
                    <Button icon={<Play size={16} aria-hidden="true" />} loading={busy === 'Resume'} disabled={!!busy} onClick={() => void act('Resume', () => api.runControl(current.id, 'resume'))}>
                      Resume
                    </Button>
                  ) : (
                    <Button icon={<Pause size={16} aria-hidden="true" />} loading={busy === 'Pause'} disabled={!!busy || current.status !== 'running'} onClick={() => void act('Pause', () => api.runControl(current.id, 'pause'))}>
                      Pause
                    </Button>
                  )}
                  <Button icon={<Square size={16} aria-hidden="true" />} loading={busy === 'Stop'} disabled={!!busy || !active} onClick={() => void act('Stop', () => api.runControl(current.id, 'stop'), 'Run stopped')}>
                    Stop
                  </Button>
                  <Button icon={<Repeat size={16} aria-hidden="true" />} loading={busy === 'Replay'} disabled={!!busy || active} onClick={() => void act('Replay', async () => setRunId((await api.replayRun(current.id)).id), 'Replay started with the same seed')}>
                    Replay run
                  </Button>
                </div>
              )}
              <p className="fw-small fw-muted">Replay re-generates the same events from the recorded seed and control log, then processes them again. It is not a recording.</p>
            </div>
          )}
        </Card>
      </div>

      {!readOnly && (
        <Card title={active ? 'Adjust the running scenario' : 'Start a scenario'}>
          {config.status === 'error' && !config.data ? (
            <ErrorState error={config.error} title="Could not load scenario options" onRetry={config.refetch} />
          ) : !config.data ? (
            <LoadingSkeleton lines={3} label="Loading scenario options" />
          ) : (
            <form
              className="fw-stack"
              style={{ gap: 16 }}
              onSubmit={(e) => {
                e.preventDefault();
                if (active && current) void act('Set rate', () => api.setRunRate(current.id, effectiveRate), `Rate set to ${formatEps(effectiveRate)}`);
                else
                  void act(
                    'Start',
                    async () => {
                      const created = await api.createRun({ scenario, rate_eps: effectiveRate, ...(seed.trim() ? { seed: Number(seed) } : {}) });
                      setRunId(created.id);
                    },
                    'Scenario started',
                  );
              }}
            >
              <div className="fw-filters">
                <Field label="Scenario" htmlFor="sc-scenario" help={scenarios.find((s) => s.id === scenario)?.description}>
                  {scenarios.length ? (
                    <select id="sc-scenario" className="fw-select" value={scenario} disabled={active} onChange={(e) => setScenario(e.target.value)}>
                      {scenarios.map((s) => (
                        <option key={s.id} value={s.id}>
                          {s.label}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <p className="fw-muted">The API did not list any scenarios.</p>
                  )}
                </Field>
                <Field label={`Rate: ${formatEps(effectiveRate)}`} htmlFor={rateId} help={`Up to ${formatEps(rateCap)} for this session.`}>
                  <input
                    id={rateId}
                    type="range"
                    min={1}
                    max={capKnown ? rateCap : 1}
                    step={1}
                    value={effectiveRate}
                    onChange={(e) => setRate(Number(e.target.value))}
                    aria-valuetext={formatEps(effectiveRate)}
                    data-testid="rate-slider"
                  />
                </Field>
                <Field label="Seed (optional)" htmlFor="sc-seed" help="Leave empty to let the API choose; it is always shown on the run.">
                  <input id="sc-seed" className="fw-input" inputMode="numeric" pattern="[0-9]*" value={seed} disabled={active} onChange={(e) => setSeed(e.target.value.replace(/[^0-9]/g, ''))} />
                </Field>
              </div>
              <div className="fw-row">
                {active ? (
                  <Button type="submit" loading={busy === 'Set rate'} disabled={!!busy || current?.rate_eps === effectiveRate}>
                    Apply rate
                  </Button>
                ) : (
                  <Button type="submit" variant="primary" icon={<Play size={16} aria-hidden="true" />} loading={busy === 'Start'} disabled={!!busy || !scenario || (session ? session.events_used >= session.event_cap : false)} data-testid="start-run">
                    Start scenario
                  </Button>
                )}
              </div>
            </form>
          )}
        </Card>
      )}

      {!readOnly && (
        <Card title="Inject a fraud pattern" description="Schedules a short, labelled sequence of replayed records into the running scenario.">
          {!active || !current ? (
            <p className="fw-muted">Start or resume a run to inject patterns.</p>
          ) : patterns.length === 0 ? (
            <p className="fw-muted">The API did not list any injectable patterns.</p>
          ) : (
            <div className="fw-row" style={{ gap: 8 }}>
              {patterns.map((p) => (
                <Button
                  key={p.id}
                  icon={<Zap size={16} aria-hidden="true" />}
                  loading={busy === `Inject ${p.id}`}
                  disabled={!!busy || current.status !== 'running'}
                  title={p.description}
                  onClick={() =>
                    void act(`Inject ${p.id}`, async () => {
                      const res = await api.inject(current.id, p.id as Pattern);
                      toast({ kind: 'success', title: `Injected ${humanize(res.pattern)}`, message: `${formatInt(res.events)} events for account ${shortId(res.account_id, 10)}.` });
                    })
                  }
                >
                  {humanize(p.label ?? p.id)}
                </Button>
              ))}
            </div>
          )}
        </Card>
      )}

      {error ? (
        <Banner tone="error">
          <span role="alert">{controlErrorText(error)}</span>
        </Banner>
      ) : null}

      <Card title="Runs in this session" flush>
        {runs.status === 'error' && !runs.data ? (
          <ErrorState error={runs.error} title="Could not load runs" onRetry={runs.refetch} />
        ) : !runs.data ? (
          <LoadingSkeleton lines={3} label="Loading runs" />
        ) : (
          <DataTable
            caption="Runs in this session"
            columns={runColumns}
            rows={[...runs.data].sort((a, b) => (b.started_at ?? 0) - (a.started_at ?? 0))}
            rowKey={(r) => r.id}
            selectedKey={runId}
            onRowActivate={(r) => setRunId(r.id)}
            rowLabel={(r) => `${humanize(r.scenario)} run, ${r.status}. Select to view it on the overview.`}
            empty={<EmptyState title="No runs yet">Start a scenario to create the first run.</EmptyState>}
          />
        )}
      </Card>

      <Dialog
        open={resetOpen}
        onClose={() => setResetOpen(false)}
        title="Reset demo session?"
        description="This ends the current session and gives you a new one with a fresh event allowance."
        footer={
          <div className="fw-row" style={{ justifyContent: 'flex-end', width: '100%' }}>
            <Button onClick={() => setResetOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              loading={busy === 'Reset'}
              onClick={() =>
                void act('Reset', async () => {
                  await resetSession();
                  setResetOpen(false);
                }, 'New session started')
              }
            >
              Reset session
            </Button>
          </div>
        }
      >
        <p>Previous runs, alerts and decisions stay with the old session and will no longer be shown here.</p>
      </Dialog>
    </div>
  );
}

function RunStatusPill({ status }: { status: Run['status'] }) {
  const tone = status === 'running' ? 'allow' : status === 'paused' || status === 'starting' ? 'review' : status === 'failed' ? 'alert' : 'neutral';
  return <StatusPill tone={tone}>{humanize(status)}</StatusPill>;
}
