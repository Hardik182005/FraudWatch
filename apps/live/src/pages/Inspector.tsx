import type { Engine, ModelMetadata, TraceStep, TransactionDetailResponse, TxnDetail, WhatIfRequest, WhatIfResponse } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import {
  ActionBadge,
  Banner,
  Button,
  Card,
  DASH,
  Disclosure,
  EmptyState,
  ErrorState,
  Field,
  KeyValue,
  LateBadge,
  LoadingSkeleton,
  ObjectKeyValue,
  TRACE_STAGE_LABEL,
  Tabs,
  TabPanel,
  formatAmount,
  formatMs,
  formatNumber,
  formatScore,
  formatTimestamp,
  formatValue,
  humanize,
  shortId,
} from '@fraudwatch/ui';
import { ArrowLeft, FlaskConical, GitCompare, Network } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { EvidenceBlocks, ScoreScale } from '../components/evidence';

const ENGINES: Engine[] = ['flink', 'spark'];
const ENGINE_LABEL: Record<Engine, string> = { flink: 'Flink', spark: 'Spark' };

export function InspectorPage() {
  const { eventId = '' } = useParams();
  const [params, setParams] = useSearchParams();
  const tx = useApi((signal) => api.getTransaction(eventId, signal), [eventId], { enabled: !!eventId });
  const data = tx.data;

  // Keep polling while an expected engine result has not been persisted yet.
  const pending = !!data && (data.event.target_engines ?? []).some((e) => !data.engines[e]);
  useEffect(() => {
    if (!pending) return;
    const h = setInterval(tx.refetch, 4000);
    return () => clearInterval(h);
  }, [pending, tx.refetch]);

  const available = ENGINES.filter((e) => data?.engines[e]);
  const requested = params.get('engine');
  const engine: Engine = requested === 'spark' || requested === 'flink' ? requested : (available[0] ?? 'flink');

  if (tx.status === 'error' && !data) {
    return (
      <div className="fw-stack" style={{ gap: 16 }}>
        <BackLink />
        <Card>
          <ErrorState error={tx.error} title="Could not load this transaction" onRetry={tx.refetch} />
        </Card>
      </div>
    );
  }
  if (!data) {
    return (
      <div className="fw-stack" style={{ gap: 16 }}>
        <BackLink />
        <Card>
          <LoadingSkeleton lines={6} label="Loading transaction" />
        </Card>
      </div>
    );
  }

  const detail = data.engines[engine] ?? null;
  const ev = data.event;

  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <BackLink />
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">Transaction inspector</div>
          <h1 style={{ overflowWrap: 'anywhere' }}>{formatAmount(ev.amount)} · {ev.product_cd}</h1>
          <p className="fw-small fw-muted">
            Event <code>{ev.event_id}</code> · account <code>{shortId(ev.account_id, 12)}</code> · event time {formatTimestamp(ev.event_time, true)}
          </p>
        </div>
        <div className="fw-row">
          {detail && <ActionBadge action={detail.policy?.action ?? (detail.model ? null : 'unscored')} />}
          {detail?.late && <LateBadge late />}
          <Link className="fw-btn fw-btn-secondary" to={`/pipeline?trace=${encodeURIComponent(ev.event_id)}`}>
            <Network size={16} aria-hidden="true" /> Show on pipeline
          </Link>
        </div>
      </div>

      {pending && <Banner tone="info">Waiting for {data.event.target_engines.filter((e) => !data.engines[e]).map((e) => ENGINE_LABEL[e]).join(' and ')} to persist this event. This page refreshes automatically.</Banner>}

      <div className="fw-grid fw-grid-main-side">
        <div className="fw-stack" style={{ gap: 16 }}>
          <Card title="Engine result">
            <Tabs
              label="Engine"
              idBase="inspector-engine"
              value={engine}
              onChange={(id) => {
                const next = new URLSearchParams(params);
                next.set('engine', id);
                setParams(next, { replace: true });
              }}
              items={ENGINES.map((e) => ({ id: e, label: data.engines[e] ? ENGINE_LABEL[e] : `${ENGINE_LABEL[e]} (no result)` }))}
            />
            <TabPanel idBase="inspector-engine" id={engine}>
              {!detail ? (
                <EmptyState title={`No ${ENGINE_LABEL[engine]} result`}>
                  {ev.target_engines.includes(engine) ? 'The engine has not persisted this event yet.' : `This event was not routed to ${ENGINE_LABEL[engine]}.`}
                </EmptyState>
              ) : (
                <EngineResult detail={detail} eventTime={ev.event_time} />
              )}
            </TabPanel>
          </Card>

          <Card title="Why flagged?" description="Three separate kinds of evidence. The model explains its score; rules and the review policy are deterministic.">
            <EvidenceBlocks detail={detail} />
          </Card>

          <Card title="Flink vs Spark" description="The same event as processed by each engine, where both results exist.">
            <EngineComparison data={data} />
          </Card>
        </div>

        <div className="fw-stack" style={{ gap: 16 }}>
          <Card title="Event">
            <KeyValue
              items={[
                { key: 'Amount', value: formatAmount(ev.amount) },
                { key: 'Source record', value: ev.source_ref ?? DASH },
                { key: 'Product code', value: ev.product_cd },
                { key: 'Card', value: `${ev.card_network} · ${ev.card_type}` },
                { key: 'Email domain', value: ev.email_domain },
                { key: 'Device', value: ev.device_type },
                { key: 'Identity record', value: ev.has_identity ? 'Yes' : 'No' },
                { key: 'Scenario', value: ev.scenario ?? DASH },
                { key: 'Injected pattern', value: ev.pattern ? humanize(ev.pattern) : 'None' },
                { key: 'Arrival delay', value: ev.arrival_delay_ms ? formatMs(ev.arrival_delay_ms) : 'None' },
                { key: 'Run', value: <code>{shortId(ev.run_id, 12)}</code> },
                { key: 'Sequence', value: formatValue(ev.seq) },
              ]}
            />
            <p className="fw-small fw-muted" style={{ marginTop: 8 }}>
              Account id is a pseudonymous key derived from the IEEE-CIS record (card1 + addr1 + email domain), not a real customer.
            </p>
          </Card>

          <Card title="Follow this transaction" description="Each stage timestamp recorded along the pipeline, with the delay since the previous stage.">
            <TraceTimeline trace={data.trace} />
          </Card>
        </div>
      </div>

      <WhatIfPanel event={ev} detail={detail} />
    </div>
  );
}

function BackLink() {
  return (
    <div>
      <Link to="/console" className="fw-btn fw-btn-ghost fw-btn-sm">
        <ArrowLeft size={16} aria-hidden="true" /> Back to overview
      </Link>
    </div>
  );
}

function EngineResult({ detail, eventTime }: { detail: TxnDetail; eventTime: number }) {
  const lag = detail.watermark_ms !== null ? detail.watermark_ms - eventTime : null;
  const t = detail.timing;
  const e2e = t.persisted_at !== null && t.produced_at !== null ? t.persisted_at - t.produced_at : null;
  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <div>
        <h3 style={{ fontSize: '1rem', marginBottom: 8 }}>Score vs thresholds</h3>
        <ScoreScale score={detail.model?.score ?? null} thresholds={detail.model?.thresholds} />
      </div>
      <KeyValue
        items={[
          { key: 'Model version', value: detail.model?.version ?? 'Not scored' },
          {
            key: 'Scoring',
            value: (
              <span>
                {detail.scoring.status} · {detail.scoring.attempts} attempt{detail.scoring.attempts === 1 ? '' : 's'}
                {detail.scoring.error ? <span className="fw-muted"> · {detail.scoring.error}</span> : null}
              </span>
            ),
          },
          {
            key: 'Late event',
            value: detail.late ? (
              <span>
                <LateBadge late /> arrived after the watermark
              </span>
            ) : (
              'No'
            ),
          },
          { key: 'Watermark at processing', value: detail.watermark_ms !== null ? `${formatTimestamp(detail.watermark_ms, true)}${lag !== null ? ` (${lag >= 0 ? `${formatMs(lag)} past` : `${formatMs(-lag)} before`} event time)` : ''}` : 'Not reported' },
          { key: 'Kafka', value: detail.kafka ? `partition ${detail.kafka.partition}, offset ${detail.kafka.offset}` : DASH },
          { key: 'Engine job', value: detail.engine_job_id ? <code>{shortId(detail.engine_job_id, 16)}</code> : DASH },
          { key: 'End-to-end', value: formatMs(e2e) },
          { key: 'Duplicate deliveries', value: formatValue(detail.duplicates) },
        ]}
      />
      <Disclosure summary="Raw stream features">
        <ObjectKeyValue data={detail.raw_features} empty="No raw features were recorded." />
      </Disclosure>
      <Disclosure summary="Model inputs">
        <ObjectKeyValue data={detail.model_inputs} empty="No model inputs were recorded (the event may not have been scored)." />
      </Disclosure>
    </div>
  );
}

function TraceTimeline({ trace }: { trace: TraceStep[] }) {
  if (!trace.length) return <EmptyState title="No trace recorded">Stage timestamps appear once the event has been processed.</EmptyState>;
  const steps = [...trace].sort((a, b) => a.at_ms - b.at_ms);
  return (
    <ol className="fw-timeline" data-testid="trace-timeline">
      {steps.map((s, i) => (
        <li key={`${s.stage}-${s.engine ?? 'x'}-${i}`}>
          <div className="fw-row" style={{ justifyContent: 'space-between', gap: 8 }}>
            <strong>
              {TRACE_STAGE_LABEL[s.stage] ?? humanize(s.stage)}
              {s.engine ? ` · ${ENGINE_LABEL[s.engine]}` : ''}
            </strong>
            <span className="fw-num fw-small">{s.delta_ms !== null ? `+${formatMs(s.delta_ms)}` : 'start'}</span>
          </div>
          <div className="fw-small fw-muted fw-num">{formatTimestamp(s.at_ms, true)}</div>
          {s.detail ? <div className="fw-small fw-muted">{typeof s.detail === 'string' ? s.detail : Object.entries(s.detail).map(([k, v]) => `${humanize(k)}: ${formatValue(v)}`).join(' · ')}</div> : null}
        </li>
      ))}
    </ol>
  );
}

function EngineComparison({ data }: { data: TransactionDetailResponse }) {
  const f = data.engines.flink;
  const s = data.engines.spark;
  if (!f || !s) {
    return (
      <EmptyState title="Side-by-side needs both engines" icon={<GitCompare size={22} aria-hidden="true" />}>
        {f ? 'Only Flink has processed this event.' : s ? 'Only Spark has processed this event.' : 'No engine has processed this event yet.'} Spark runs only when an operator starts it for a comparison.
      </EmptyState>
    );
  }
  const e2e = (d: TxnDetail) => (d.timing.persisted_at !== null && d.timing.produced_at !== null ? d.timing.persisted_at - d.timing.produced_at : null);
  const featureKeys = Array.from(new Set([...Object.keys(f.raw_features), ...Object.keys(s.raw_features)]));
  const mismatched = featureKeys.filter((k) => !sameNumber(f.raw_features[k], s.raw_features[k]));
  const rows: Array<{ label: string; flink: React.ReactNode; spark: React.ReactNode; differs: boolean }> = [
    { label: 'Action', flink: <ActionBadge action={f.policy?.action ?? 'unscored'} />, spark: <ActionBadge action={s.policy?.action ?? 'unscored'} />, differs: f.policy?.action !== s.policy?.action },
    { label: 'Score', flink: formatScore(f.model?.score), spark: formatScore(s.model?.score), differs: !sameNumber(f.model?.score, s.model?.score) },
    { label: 'Late', flink: f.late ? 'Yes' : 'No', spark: s.late ? 'Yes' : 'No', differs: f.late !== s.late },
    { label: 'Rules fired', flink: String(f.rules?.results.filter((r) => r.fired).length ?? 0), spark: String(s.rules?.results.filter((r) => r.fired).length ?? 0), differs: false },
    { label: 'End-to-end', flink: formatMs(e2e(f)), spark: formatMs(e2e(s)), differs: false },
    { label: 'Duplicates', flink: String(f.duplicates), spark: String(s.duplicates), differs: f.duplicates !== s.duplicates },
  ];
  return (
    <div className="fw-stack" style={{ gap: 12 }}>
      <div className="fw-table-wrap">
        <table className="fw-table fw-table-compact">
          <caption className="fw-sr-only">Flink and Spark results for this event</caption>
          <thead>
            <tr>
              <th scope="col">Field</th>
              <th scope="col">Flink</th>
              <th scope="col">Spark</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.label}>
                <th scope="row">
                  {r.label}
                  {r.differs && <span className="fw-small" style={{ color: 'var(--review)' }}> · differs</span>}
                </th>
                <td className="fw-num">{r.flink}</td>
                <td className="fw-num">{r.spark}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="fw-small">
        {mismatched.length === 0 ? (
          `All ${featureKeys.length} raw stream features match between the engines.`
        ) : (
          <>
            {mismatched.length} raw feature{mismatched.length === 1 ? '' : 's'} differ: {mismatched.map(humanize).join(', ')}. Differences usually come from micro-batch timing versus per-event processing.
          </>
        )}
      </p>
    </div>
  );
}

function sameNumber(a: unknown, b: unknown): boolean {
  if (typeof a === 'number' && typeof b === 'number') return Math.abs(a - b) <= 1e-6 * Math.max(1, Math.abs(a), Math.abs(b));
  return a === b || (a == null && b == null);
}

/* ---------- What-if ---------- */

const CATEGORICAL: Array<keyof Omit<WhatIfRequest['event'], 'amount' | 'has_identity'>> = ['product_cd', 'card_network', 'card_type', 'email_domain', 'device_type'];

function WhatIfPanel({ event, detail }: { event: TransactionDetailResponse['event']; detail: TxnDetail | null }) {
  const model = useApi(() => api.getModel(), []);
  const initial = useMemo<WhatIfRequest | null>(() => {
    if (!detail) return null;
    return {
      event: {
        amount: event.amount,
        product_cd: event.product_cd,
        card_network: event.card_network,
        card_type: event.card_type,
        email_domain: event.email_domain,
        device_type: event.device_type,
        has_identity: event.has_identity,
      },
      raw: { ...detail.raw_features },
    };
  }, [event, detail]);
  const [draft, setDraft] = useState<WhatIfRequest | null>(initial);
  const [result, setResult] = useState<WhatIfResponse | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    setDraft(initial);
    setResult(null);
    setError(null);
  }, [initial]);

  const run = async () => {
    if (!draft) return;
    setBusy(true);
    setError(null);
    try {
      setResult(await api.whatIf(draft));
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card
      title="What-if: model sensitivity"
      description="Change inputs and re-score them with the deployed model. This shows how sensitive the model is to an input, not what caused fraud."
      actions={<FlaskConical size={18} aria-hidden="true" />}
    >
      {!draft || !detail ? (
        <EmptyState title="Nothing to re-score yet">What-if needs the raw stream features persisted by an engine for this event.</EmptyState>
      ) : (
        <form
          className="fw-stack"
          style={{ gap: 16 }}
          onSubmit={(e) => {
            e.preventDefault();
            void run();
          }}
        >
          <div className="fw-filters">
            <Field label="Amount" htmlFor="wi-amount">
              <input
                id="wi-amount"
                className="fw-input"
                type="number"
                min={0}
                step="any"
                value={draft.event.amount ?? ''}
                onChange={(e) => setDraft({ ...draft, event: { ...draft.event, amount: e.target.value === '' ? null : Number(e.target.value) } })}
              />
            </Field>
            {CATEGORICAL.map((key) => (
              <CategoricalField key={key} name={key} value={draft.event[key]} model={model.data} onChange={(v) => setDraft({ ...draft, event: { ...draft.event, [key]: v } })} />
            ))}
            <Field label="Identity record" htmlFor="wi-identity">
              <select id="wi-identity" className="fw-select" value={draft.event.has_identity ? 'yes' : 'no'} onChange={(e) => setDraft({ ...draft, event: { ...draft.event, has_identity: e.target.value === 'yes' } })}>
                <option value="yes">Yes</option>
                <option value="no">No</option>
              </select>
            </Field>
          </div>
          <Disclosure summary="Stream features">
            <div className="fw-filters">
              {Object.keys(draft.raw).map((k) => (
                <Field key={k} label={humanize(k)} htmlFor={`wi-raw-${k}`}>
                  <input
                    id={`wi-raw-${k}`}
                    className="fw-input"
                    type="number"
                    step="any"
                    value={draft.raw[k] ?? ''}
                    placeholder="Missing"
                    onChange={(e) => setDraft({ ...draft, raw: { ...draft.raw, [k]: e.target.value === '' ? null : Number(e.target.value) } })}
                  />
                </Field>
              ))}
            </div>
          </Disclosure>
          <div className="fw-row">
            <Button type="submit" variant="primary" loading={busy}>
              {busy ? 'Scoring…' : 'Re-score'}
            </Button>
            <Button
              variant="ghost"
              onClick={() => {
                setDraft(initial);
                setResult(null);
              }}
            >
              Restore original values
            </Button>
          </div>
          {error ? <ErrorState error={error} title="What-if scoring failed" onRetry={() => void run()} /> : null}
          {result && (
            <div className="fw-stack" style={{ gap: 12 }} aria-live="polite" data-testid="whatif-result">
              <div className="fw-row">
                <strong>Re-scored:</strong>
                <span className="fw-num">{formatScore(result.model.score)}</span>
                {detail.model && (
                  <span className="fw-small fw-muted fw-num">
                    (original {formatScore(detail.model.score)}, change {result.model.score - detail.model.score >= 0 ? '+' : ''}
                    {formatNumber(result.model.score - detail.model.score, 4)})
                  </span>
                )}
                {result.policy && <ActionBadge action={result.policy.action} />}
              </div>
              <ScoreScale score={result.model.score} thresholds={result.model.thresholds} />
              <Banner tone="warn">{result.disclaimer || 'Sensitivity, not causality: a different score here does not mean the changed input caused fraud.'}</Banner>
            </div>
          )}
          {!result && (
            <p className="fw-small fw-muted">
              Sensitivity, not causality. Changing an input and seeing the score move says nothing about real-world cause. Nothing here changes the stored decision or the model.
            </p>
          )}
        </form>
      )}
    </Card>
  );
}

function CategoricalField({ name, value, model, onChange }: { name: string; value: string; model: ModelMetadata | undefined; onChange: (v: string) => void }) {
  const id = `wi-${name}`;
  const options = model?.features?.categories?.[name];
  if (options && options.length) {
    const list = options.includes(value) ? options : [value, ...options];
    return (
      <Field label={humanize(name)} htmlFor={id}>
        <select id={id} className="fw-select" value={value} onChange={(e) => onChange(e.target.value)}>
          {list.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </Field>
    );
  }
  return (
    <Field label={humanize(name)} htmlFor={id} help={model ? 'Allowed values were not published by the API.' : undefined}>
      <input id={id} className="fw-input" value={value} onChange={(e) => onChange(e.target.value)} />
    </Field>
  );
}

