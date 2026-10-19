import type { Health } from '@fraudwatch/contracts';
import {
  BrandMark, LandingHeader, LandingHero, LandingPage, LandingSection, SceneStage, StepCard, TEAM_LINE, WakeBanner, formatMs, formatNumber, formatPct, type SceneNode,
} from '@fraudwatch/ui';
import { Activity, ArrowRight, Brain, Command, GitCompareArrows, Play } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { HEADLINE } from '../lab/measured';
import { useModelEvidence } from '../lab/snapshot';
import { CloudChip, useCloud } from '../lifecycle';
import { useSession } from '../session';

/** Health component id backing each scene node, so no status is ever invented. */
const NODE_SOURCE: Record<string, string> = { source: 'generator', kafka: 'kafka', flink: 'flink', scoring: 'scoring', queue: 'result_consumer', alerts: 'api' };

function buildNodes(health: Health | undefined): SceneNode[] {
  const node = (id: string, label: string, sub: string, role: string, lane?: SceneNode['lane']): SceneNode => {
    const c = health?.components.find((x) => x.id === NODE_SOURCE[id]) ?? null;
    return { id, label, sub, role, lane, status: c ? c.status : null, statusDetail: c?.detail ?? null };
  };
  return [
    node('source', 'Transaction source', 'replayed events', 'Replays IEEE-CIS records at a capped rate. No real payments.'),
    node('kafka', 'Kafka', 'transactions topic', 'The shared event log both engines read independently.'),
    node('flink', 'Apache Flink', 'per-event state', 'Event-time processing with keyed per-account state.'),
    node('scoring', 'Scoring service', 'model + rules', 'LightGBM score, deterministic rules, then the review policy.'),
    node('queue', 'Result store', 'scored events', 'Writes every scored event and its evidence to Postgres.'),
    node('alerts', 'Alerts and review', 'API and stream', 'Serves the alert queue, evidence and the live stream.', 'alert'),
  ];
}

function TrackCard({ to, icon, tag, title, stats, cta }: { to: string; icon: ReactNode; tag: string; title: string; stats: Array<{ v: string; l: string }>; cta: string }) {
  return (
    <Link to={to} className="fw-track" data-testid={`track-${tag.toLowerCase()}`}>
      <span className="fw-track-tag">{icon}{tag}</span>
      <h3>{title}</h3>
      <div className="fw-track-stats">
        {stats.map((s) => (
          <div key={s.l}><strong>{s.v}</strong><span>{s.l}</span></div>
        ))}
      </div>
      <span className="fw-track-cta">{cta} <ArrowRight size={16} aria-hidden="true" /></span>
    </Link>
  );
}

export function HomeLanding() {
  const { health, session, runId, mode, startDemo, starting } = useSession();
  const cloud = useCloud();
  const navigate = useNavigate();
  const ev = useModelEvidence();
  const run = runId ?? session?.active_run_id ?? null;
  const attached = mode !== 'none' && !!session && !!run;
  const stream = ev.data?.results.stream;
  const report = stream ? stream.test?.[stream.selected] ?? Object.values(stream.test ?? {})[0] : undefined;
  const dash = '—';
  const go = async () => {
    if (mode !== 'owner') await startDemo();
    navigate('/console');
  };

  return (
    <LandingPage
      header={
        <LandingHeader
          brand={<Link to="/" className="fw-brand" aria-label="FraudWatch home"><BrandMark /></Link>}
          links={[
            <Link key="ca1" to="/ca1">CA1 · Model</Link>,
            <Link key="ca2" to="/ca2">CA2 · Streaming</Link>,
            <Link key="system" to="/system">System</Link>,
            <a key="how" href="#how">How it works</a>,
            <CloudChip key="cloud" />,
          ]}
          cta={<Link to="/console" className="fw-btn fw-btn-primary fw-btn-sm">Console <ArrowRight size={16} aria-hidden="true" /></Link>}
        />
      }
      footer={
        <>
          <span>Educational project. Replayed IEEE-CIS records, pseudonymous accounts, no real payments. Alerts are model outputs, not confirmed fraud.</span>
          <span>{TEAM_LINE}</span>
        </>
      }
    >
      {cloud.managed && cloud.phase !== 'ready' && cloud.phase !== 'checking' && (
        <div className="fw-landing-wake"><WakeBanner view={cloud} /></div>
      )}
      <LandingHero
        eyebrow="CA1 + CA2 · one live system"
        title={['Score fraud in flight.', 'Prove it with evidence.']}
        lead="Kafka → Flink → LightGBM + rules → review queue, with Spark as a measured micro-batch rival."
        actions={
          <>
            <button type="button" className="fw-btn fw-btn-primary fw-btn-lg" onClick={() => void go()} disabled={starting} data-testid="hero-start">
              <Play size={18} aria-hidden="true" />
              {mode === 'owner' ? 'Open my live demo' : starting ? 'Starting…' : 'Start live demo'}
            </button>
            <Link to="/ca1" className="fw-btn fw-btn-secondary fw-btn-lg">CA1</Link>
            <Link to="/ca2" className="fw-btn fw-btn-secondary fw-btn-lg">CA2</Link>
          </>
        }
        meta={
          <span className="fw-small fw-muted fw-row">
            <Command size={14} aria-hidden="true" /> Press <kbd>Ctrl K</kbd> to jump anywhere
          </span>
        }
        scene={
          <SceneStage
            variant="live"
            nodes={buildNodes(health.data)}
            live={attached}
            modeLabel={attached ? `Live · run ${run}` : 'Illustration'}
            initialSelected="flink"
            alertActive
            caption="Topology and order only; timing is decorative."
          />
        }
      />

      <LandingSection id="tracks" eyebrow="Two assessments, one site" title="Pick a track">
        <div className="fw-grid fw-grid-2">
          <TrackCard
            to="/ca1"
            icon={<Brain size={15} aria-hidden="true" />}
            tag="CA1"
            title="Fraud detection with reproducible ML"
            stats={[
              { v: report ? formatNumber(report.pr_auc, 3) : dash, l: 'test PR-AUC' },
              { v: report ? formatPct(report.at_threshold.precision) : dash, l: 'precision' },
              { v: report ? formatPct(report.at_threshold.recall) : dash, l: 'recall' },
            ]}
            cta="Model, SHAP, policy"
          />
          <TrackCard
            to="/ca2"
            icon={<GitCompareArrows size={15} aria-hidden="true" />}
            tag="CA2"
            title="Flink vs Spark on the same stream"
            stats={[
              { v: formatMs(HEADLINE.flink.p50), l: 'Flink p50' },
              { v: formatMs(HEADLINE.spark.p50), l: 'Spark p50' },
              { v: `${HEADLINE.parity.same}/${HEADLINE.parity.both}`, l: 'same decision' },
            ]}
            cta="Latency, parity, late events"
          />
        </div>
        <p className="fw-small fw-muted fw-space-top">
          CA1 figures: {ev.source === 'api' ? 'live model API' : ev.source === 'snapshot' ? 'bundled model artifact' : 'loading'}. CA2 figures: prerecorded GCP run <code>{HEADLINE.id}</code>.
        </p>
      </LandingSection>

      <LandingSection id="how" eyebrow="One event, four steps" title="How a transaction becomes a decision">
        <div className="fw-grid fw-grid-4">
          <StepCard n={1} title="Replay">IEEE-CIS record → Kafka, with event time.</StepCard>
          <StepCard n={2} title="Stream">Flink keyed state, watermarks, late output.</StepCard>
          <StepCard n={3} title="Score">LightGBM + rules → allow, review or alert.</StepCard>
          <StepCard n={4} title="Evidence">SHAP, rule reasons and trace, per event.</StepCard>
        </div>
      </LandingSection>

      <LandingSection id="features" eyebrow="Inside the console" title="What you can do">
        <div className="fw-feature-grid">
          {[
            { to: '/console', t: 'Live stream', d: 'Scored events and KPIs over SSE' },
            { to: '/alerts', t: 'Review queue', d: 'Decide, with an audit trail' },
            { to: '/scenario', t: 'Scenario control', d: 'Fraud mix, rate, bursts' },
            { to: '/system?tab=trace', t: 'Event trace', d: 'Every hop, timed' },
            { to: '/system?tab=logs', t: 'Live logs', d: 'Filter by run, event, level' },
            { to: '/system?tab=quality', t: 'Data quality', d: 'Malformed, duplicate, late, unmatched' },
            { to: '/experiments', t: 'Experiments', d: 'Parity and late records for your run' },
            { to: '/architecture', t: '3D architecture', d: 'Live component status' },
          ].map((f) => (
            <Link key={f.t} to={f.to} className="fw-feature">
              <Activity size={16} aria-hidden="true" />
              <strong>{f.t}</strong>
              <span>{f.d}</span>
            </Link>
          ))}
        </div>
      </LandingSection>

      <LandingSection eyebrow="Honest limits" title="Not a bank, not a benchmark">
        <p className="fw-muted fw-small">
          Replayed competition data, capped demo rates, small instances. Scores rank risk; they are not calibrated probabilities. Reviews never retrain the model. <Link to="/evidence">Full method Q&amp;A →</Link>
        </p>
      </LandingSection>
    </LandingPage>
  );
}
