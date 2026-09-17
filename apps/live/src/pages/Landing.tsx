import { getRuntimeConfig, type Health } from '@fraudwatch/contracts';
import {
  BrandMark, LandingHeader, LandingHero, LandingPage, LandingSection, SceneStage, StepCard, TEAM_LINE, type SceneNode,
} from '@fraudwatch/ui';
import { ArrowRight, ExternalLink, FlaskConical } from 'lucide-react';
import { Link } from 'react-router-dom';
import { useSession } from '../session';

/** Health component id backing each scene node, so no status is ever invented. */
const NODE_SOURCE: Record<string, string> = {
  source: 'generator',
  kafka: 'kafka',
  flink: 'flink',
  scoring: 'scoring',
  queue: 'result_consumer',
  alerts: 'api',
};

function buildNodes(health: Health | undefined): SceneNode[] {
  const find = (id: string) => health?.components.find((c) => c.id === NODE_SOURCE[id]) ?? null;
  const node = (id: string, label: string, sub: string, role: string, lane?: SceneNode['lane']): SceneNode => {
    const c = find(id);
    return { id, label, sub, role, lane, status: c ? c.status : null, statusDetail: c?.detail ?? null };
  };
  return [
    node('source', 'Transaction source', 'replayed events', 'Replays records from the IEEE-CIS Fraud Detection dataset as a stream of events at a capped rate. Nothing here is a real payment.'),
    node('kafka', 'Kafka', 'transactions topic', 'The event log every consumer reads from. It keeps event order per key and lets the two processing engines read the same records independently.'),
    node('flink', 'Apache Flink', 'per-event state', 'Processes one event at a time on event time, keeps keyed state for the account key, and emits enriched features for scoring.'),
    node('scoring', 'Scoring service', 'model + rules', 'Runs the versioned LightGBM model, then the deterministic rules, then the review policy that turns a score plus rule results into allow, review or alert.'),
    node('queue', 'Result store', 'scored events', 'The consumer that writes every scored event and its evidence to Postgres, which is what both websites read.'),
    node('alerts', 'Alerts and review', 'API and stream', 'The API that serves the console: the alert queue, the per-event evidence, and the live stream the dashboard subscribes to.', 'alert'),
  ];
}

export function LiveLandingPage() {
  const { health, session, runId, config, mode } = useSession();
  const nodes = buildNodes(health.data);
  const labUrl = config.data?.lab_url ?? getRuntimeConfig().labUrl;
  const run = runId ?? session?.active_run_id ?? null;
  const attached = mode !== 'none' && !!session && !!run;

  return (
    <LandingPage
      header={
        <LandingHeader
          brand={
            <Link to="/" className="fw-brand" aria-label="FraudWatch Live home">
              <BrandMark product="Live" />
            </Link>
          }
          links={[
            <a key="how" href="#how">How it works</a>,
            <a key="measured" href="#measured">What is measured</a>,
            <a key="limits" href="#limits">Limitations</a>,
            ...(labUrl ? [<a key="lab" href={labUrl}>Lab site</a>] : []),
          ]}
          cta={
            <Link to="/console" className="fw-btn fw-btn-primary fw-btn-sm">
              Open the console
              <ArrowRight size={16} aria-hidden="true" />
            </Link>
          }
        />
      }
      footer={
        <>
          <span>
            Educational project, not a banking integration. Transactions are replayed records from the IEEE-CIS Fraud Detection dataset (anonymized Vesta competition data); the account id is a pseudonymous key derived from card1, addr1 and P_emaildomain, and <code>TransactionAmt</code> is shown in its source unit because the dataset does not document a currency. No real payments are processed and alerts are model outputs, not confirmed fraud.
          </span>
          <span>{TEAM_LINE}</span>
        </>
      }
    >
      <LandingHero
        eyebrow="Streaming fraud detection, end to end"
        title={['Watch a transaction', 'get scored in flight']}
        lead={
          <>
            FraudWatch Live replays the IEEE-CIS Fraud Detection dataset through Kafka and Apache Flink, scores each event with a versioned model plus explicit rules, and shows the evidence behind every decision. Start a capped demo session and follow a single event from the stream to the review queue.
          </>
        }
        actions={
          <>
            <Link to="/console" className="fw-btn fw-btn-primary fw-btn-lg">
              Open the console
              <ArrowRight size={18} aria-hidden="true" />
            </Link>
            <Link to="/pipeline" className="fw-btn fw-btn-secondary fw-btn-lg">
              See the pipeline
            </Link>
            {labUrl && (
              <a className="fw-btn fw-btn-ghost fw-btn-lg" href={labUrl}>
                <FlaskConical size={18} aria-hidden="true" />
                FraudWatch Lab
              </a>
            )}
          </>
        }
        meta={
          <span className="fw-small fw-muted">
            Demo sessions are capped in both event count and rate. Component status below comes from the API health probe; when the probe has not answered, status is shown as not checked.
          </span>
        }
        scene={
          <SceneStage
            variant="live"
            nodes={nodes}
            live={attached}
            modeLabel={attached ? `Live · run ${run}` : 'Illustration'}
            initialSelected="flink"
            alertActive
            caption="The moving markers show the order that events travel in, not their speed: the animation timing is decorative. Measured latency is on the console, per run."
          />
        }
      />

      <LandingSection
        id="how"
        eyebrow="One event, six steps"
        title="How a transaction becomes a decision"
        lead="Every event follows the same path. Nothing is precomputed for display; the console reads what the pipeline actually wrote."
      >
        <div className="fw-grid fw-grid-3">
          <StepCard n={1} title="Replay">
            A record from the IEEE-CIS dataset is published to the Kafka transactions topic with an event time and a pseudonymous account key derived from card1, addr1 and P_emaildomain.
          </StepCard>
          <StepCard n={2} title="Stream processing">
            Flink consumes the topic on event time, keeps keyed state per account, and derives the features the model expects. Watermarks decide when a window is complete and which records count as late.
          </StepCard>
          <StepCard n={3} title="Model score">
            A pinned LightGBM artifact scores the feature vector. The score is a ranking signal, not a calibrated probability of fraud.
          </StepCard>
          <StepCard n={4} title="Rules">
            Deterministic rules (burst, profile deviation, card testing) run against explicit state. Their reasons stay separate from the model attribution so the two are never confused.
          </StepCard>
          <StepCard n={5} title="Policy">
            Thresholds plus rule results produce one of allow, review or alert. The policy version is recorded with the decision.
          </StepCard>
          <StepCard n={6} title="Evidence">
            The scored event, its model inputs, TreeSHAP contributions, rule results and policy outcome are written to Postgres and served to the console and the inspector.
          </StepCard>
        </div>
      </LandingSection>

      <LandingSection
        id="measured"
        eyebrow="Measured vs illustrative"
        title="What the numbers on this project mean"
        lead="The landing page states no metrics. Every number in the product is read back from a run or from the served model metadata."
      >
        <div className="fw-grid fw-grid-2">
          <StepCard n={1} title="Measured in the product">
            Event and alert counts, per-stage latency, engine parity between Flink and Spark, late-record counts, model version and the held-out evaluation report. All of it is scoped to a run id and refreshed from the API.
          </StepCard>
          <StepCard n={2} title="Illustrative on this page">
            The 3D scene. It shows topology and ordering only. Marker speed, spacing and colour are decorative, and the scene shows component status only when the health probe has answered.
          </StepCard>
        </div>
      </LandingSection>

      <LandingSection
        id="limits"
        eyebrow="Honest limits"
        title="What this system is not"
        lead="Stated plainly, because a fraud demo that overclaims is worse than no demo."
      >
        <div className="fw-grid fw-grid-3">
          <StepCard n={1} title="Not a bank integration">
            Nothing connects to a payment network or a real account. Every event is a replayed dataset record and no money moves.
          </StepCard>
          <StepCard n={2} title="Not confirmed fraud">
            An alert means the model and rules ranked the event highly. Labels in the dataset are the competition&rsquo;s own; the system does not observe a real chargeback outcome.
          </StepCard>
          <StepCard n={3} title="Not an anonymization claim">
            The IEEE-CIS features are already anonymized by the competition. The account key is a pseudonymous grouping for demonstration, not a re-identification-safe identifier.
          </StepCard>
          <StepCard n={4} title="Not a benchmark">
            The demo runs with capped event counts and rates on small instances. Throughput and latency observed here describe this deployment only.
          </StepCard>
          <StepCard n={5} title="Not retrained by review">
            Review decisions in the console produce an audit trail. They do not feed back into the model.
          </StepCard>
          <StepCard n={6} title="Not a currency figure">
            <code>TransactionAmt</code> is displayed in its source unit. The dataset does not document a currency, so none is shown.
          </StepCard>
        </div>
      </LandingSection>

      {labUrl && (
        <LandingSection
          eyebrow="The companion site"
          title="FraudWatch Lab"
          lead="The same pipeline, studied instead of operated: dataset provenance, model evaluation, and a side-by-side Flink versus Spark comparison on the same event contract."
        >
          <p>
            <a className="fw-btn fw-btn-secondary" href={labUrl}>
              <ExternalLink size={16} aria-hidden="true" />
              Open FraudWatch Lab
            </a>
          </p>
        </LandingSection>
      )}

      <LandingSection
        eyebrow="Start here"
        title="Run a capped demo session"
        lead="A session generates a bounded number of events at a bounded rate, then stops on its own."
      >
        <p>
          <Link to="/console" className="fw-btn fw-btn-primary fw-btn-lg">
            Open the console
            <ArrowRight size={18} aria-hidden="true" />
          </Link>
        </p>
      </LandingSection>
    </LandingPage>
  );
}
