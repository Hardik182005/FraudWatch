import type { ModelEvidence, PolicyDecision, RuleResult, TxnDetail } from '@fraudwatch/contracts';
import { ActionBadge, EmptyState, StatusPill, formatNumber, formatScore, formatValue, humanize } from '@fraudwatch/ui';
import { CircleCheck, CircleMinus } from 'lucide-react';

/** Score position against the deployed thresholds (all three values come from the API). */
export function ScoreScale({ score, thresholds }: { score: number | null; thresholds: { alert: number; review: number } | null | undefined }) {
  if (score === null || !thresholds) return <p className="fw-muted">No model score is available for this event.</p>;
  // Scores are probabilities; the thresholds sit near the low end, so a square-root scale keeps them readable.
  const pos = (v: number) => `${Math.sqrt(Math.min(1, Math.max(0, v))) * 100}%`;
  return (
    <div>
      <div
        className="fw-score-scale"
        role="img"
        aria-label={`Score ${formatScore(score)}; review threshold ${formatScore(thresholds.review)}; alert threshold ${formatScore(thresholds.alert)}`}
      >
        <div className="fw-score-marker" style={{ left: pos(thresholds.review), background: 'var(--review)' }}>
          <span>review ≥ {formatScore(thresholds.review)}</span>
        </div>
        <div className="fw-score-marker" style={{ left: pos(thresholds.alert), background: 'var(--alert)' }}>
          <span style={{ top: -22 }}>alert ≥ {formatScore(thresholds.alert)}</span>
        </div>
        <div className="fw-score-dot" style={{ left: pos(score) }}>
          <span>{formatScore(score)}</span>
        </div>
      </div>
      <p className="fw-small fw-muted">Square-root scale from 0 to 1. The score is the model’s estimated fraud probability, not a certainty.</p>
    </div>
  );
}

function EvidenceFrame({ kind, title, badge, children, testId }: { kind: string; title: string; badge?: React.ReactNode; children: React.ReactNode; testId: string }) {
  return (
    <section className="fw-evidence" aria-label={`${kind}: ${title}`} data-testid={testId}>
      <div className="fw-evidence-head">
        <div>
          <div className="fw-evidence-kind">{kind}</div>
          <h3 style={{ fontSize: '1rem', marginTop: 2 }}>{title}</h3>
        </div>
        {badge}
      </div>
      <div className="fw-evidence-body">{children}</div>
    </section>
  );
}

export function ModelEvidenceBlock({ model }: { model: ModelEvidence | null }) {
  const top = model?.explanation?.top ?? [];
  const maxAbs = Math.max(1e-9, ...top.map((t) => Math.abs(t.contribution)));
  return (
    <EvidenceFrame kind="Model evidence" title="Model attribution (TreeSHAP)" testId="evidence-model" badge={model ? <StatusPill tone="neutral">{model.version}</StatusPill> : undefined}>
      {!model ? (
        <p className="fw-muted">The model did not score this event (see scoring status).</p>
      ) : (
        <div className="fw-stack" style={{ gap: 10 }}>
          <p className="fw-small">
            How much each input moved <em>this model’s</em> score, in log-odds, relative to its average output
            {model.explanation ? ` (base value ${formatNumber(model.explanation.base_value, 3)})` : ''}. This is model attribution: it explains the model, not the real-world cause.
          </p>
          {top.length === 0 ? (
            <p className="fw-muted">No attribution was returned for this event.</p>
          ) : (
            <div role="list" aria-label="Top feature contributions">
              {top.map((t) => {
                const w = `${(Math.abs(t.contribution) / maxAbs) * 50}%`;
                return (
                  <div className="fw-bar-row" role="listitem" key={t.feature}>
                    <span style={{ overflowWrap: 'anywhere' }}>
                      {humanize(t.feature)} <span className="fw-muted fw-small">= {formatValue(t.value)}</span>
                    </span>
                    <span className="fw-bar-track" aria-hidden="true">
                      <span className={`fw-bar ${t.contribution >= 0 ? 'fw-bar-pos' : 'fw-bar-neg'}`} style={{ width: w }} />
                    </span>
                    <span className="fw-num fw-right" style={{ textAlign: 'right' }}>
                      {t.contribution >= 0 ? '+' : ''}
                      {formatNumber(t.contribution, 3)}
                      <span className="fw-sr-only">{t.contribution >= 0 ? ' towards fraud' : ' towards legitimate'}</span>
                    </span>
                  </div>
                );
              })}
            </div>
          )}
          {model.explanation?.method && <p className="fw-small fw-muted">Method: {model.explanation.method}. Red bars push towards fraud, teal bars towards legitimate.</p>}
        </div>
      )}
    </EvidenceFrame>
  );
}

export function RuleEvidenceBlock({ rules }: { rules: { version: string; results: RuleResult[] } | null }) {
  const fired = rules?.results.filter((r) => r.fired).length ?? 0;
  return (
    <EvidenceFrame
      kind="Rule evidence"
      title="Deterministic rules"
      testId="evidence-rules"
      badge={rules ? <StatusPill tone={fired ? 'review' : 'neutral'}>{`${fired} of ${rules.results.length} fired · ${rules.version}`}</StatusPill> : undefined}
    >
      {!rules || rules.results.length === 0 ? (
        <p className="fw-muted">No rule results were recorded for this event.</p>
      ) : (
        <ul className="fw-list-plain fw-stack" style={{ gap: 8 }}>
          {rules.results.map((r) => (
            <li key={r.rule_id} className="fw-row" style={{ alignItems: 'flex-start', flexWrap: 'nowrap' }}>
              {r.fired ? <CircleCheck size={18} aria-hidden="true" style={{ color: 'var(--review)', flex: 'none', marginTop: 3 }} /> : <CircleMinus size={18} aria-hidden="true" style={{ color: 'var(--text-muted)', flex: 'none', marginTop: 3 }} />}
              <div>
                <strong>{r.title}</strong> <code className="fw-small">{r.rule_id}</code> <span className="fw-small">{r.fired ? '— fired' : '— not fired'}</span>
                {r.evidence && <div className="fw-small fw-muted">{r.evidence}</div>}
              </div>
            </li>
          ))}
        </ul>
      )}
      <p className="fw-small fw-muted" style={{ marginTop: 10 }}>
        Rules are transparent checks on the stream features, labelled separately from the model score.
      </p>
    </EvidenceFrame>
  );
}

export function PolicyEvidenceBlock({ policy }: { policy: PolicyDecision | null }) {
  return (
    <EvidenceFrame kind="Review policy decision" title="Action taken" testId="evidence-policy" badge={policy ? <ActionBadge action={policy.action} /> : undefined}>
      {!policy ? (
        <p className="fw-muted">No policy decision was recorded.</p>
      ) : (
        <div className="fw-stack" style={{ gap: 8 }}>
          <p className="fw-small">
            The review policy <code>{policy.policy_version}</code> combines the model score and rule results into one action.
          </p>
          {policy.reasons.length ? (
            <ul style={{ margin: 0, paddingLeft: 20 }}>
              {policy.reasons.map((r) => (
                <li key={r}>{r}</li>
              ))}
            </ul>
          ) : (
            <p className="fw-muted">No reasons were returned.</p>
          )}
        </div>
      )}
    </EvidenceFrame>
  );
}

/** "Why flagged?" as three separate blocks: model attribution, rules, and the policy decision. */
export function EvidenceBlocks({ detail }: { detail: TxnDetail | null | undefined }) {
  if (!detail) return <EmptyState title="No engine result yet">This event has not been persisted by the selected engine.</EmptyState>;
  return (
    <div className="fw-stack" style={{ gap: 12 }}>
      <ModelEvidenceBlock model={detail.model} />
      <RuleEvidenceBlock rules={detail.rules} />
      <PolicyEvidenceBlock policy={detail.policy} />
    </div>
  );
}
