import type { Health } from '@fraudwatch/contracts';
import { BrandMark, LandingHeader, LandingHero, LandingPage, LandingSection, SceneStage, StepCard, TEAM_LINE, type SceneNode } from '@fraudwatch/ui';
import { ArrowRight, ExternalLink } from 'lucide-react';
import { Link } from 'react-router-dom';
import { useLab } from './lab';

/** Health component id backing each scene node, so no status is ever invented. */
const NODE_SOURCE: Record<string, string> = { kafka: 'kafka', flink: 'flink', spark: 'spark', measure: 'api' };

function buildNodes(health: Health | undefined): SceneNode[] {
  const node = (id: string, label: string, sub: string, lane: SceneNode['lane'], role: string): SceneNode => {
    const c = health?.components.find((x) => x.id === NODE_SOURCE[id]) ?? null;
    return { id, label, sub, role, lane, status: c ? c.status : null, statusDetail: c?.detail ?? null };
  };
  return [
    node('kafka', 'Kafka topic', 'one shared log', 'main', 'Both engines read the same transactions topic independently, so a comparison is about the engines and not about different input.'),
    node('flink', 'Flink lane', 'per event', 'flink', 'Apache Flink processes one record at a time on event time, with keyed state and watermarks deciding completeness and lateness. Checkpoints make that state recoverable.'),
    node('spark', 'Spark lane', 'micro-batch', 'spark', 'Spark Structured Streaming collects records into a micro-batch, then processes the batch as a unit. Records wait for the batch boundary before they move.'),
    node('measure', 'Measurement', 'parity and late records', 'main', 'Both lanes write scored events under the same event contract, which is what makes per-run parity, latency and late-record counts comparable at all.'),
  ];
}

export function LabLandingPage() {
  const { health, view, liveLink } = useLab();
  const nodes = buildNodes(health.data);
  const attached = !!view?.run;
  const live = liveLink('/');
  const liveConsole = liveLink('/console');

  return <LandingPage
    header={<LandingHeader
      brand={<Link to="/" className="fw-brand" aria-label="FraudWatch Lab home"><BrandMark product="Lab" /></Link>}
      links={[
        <a key="engines" href="#engines">Two engines</a>,
        <a key="study" href="#study">What you can study</a>,
        <a key="limits" href="#limits">Limitations</a>,
        ...(live ? [<a key="live" href={live}>Live site</a>] : []),
      ]}
      cta={<Link to="/study" className="fw-btn fw-btn-primary fw-btn-sm">Enter the study<ArrowRight size={16} aria-hidden="true" /></Link>}
    />}
    footer={<>
      <span>Academic companion for FraudWatch. Measured evidence is labelled separately from explanation. The data is the IEEE-CIS Fraud Detection dataset (anonymized Vesta competition data); the account id is a pseudonymous key derived from card1, addr1 and P_emaildomain, and <code>TransactionAmt</code> is shown in its source unit because the dataset does not document a currency.</span>
      <span>{TEAM_LINE}</span>
    </>}
  >
    <LandingHero
      eyebrow="The study companion to FraudWatch Live"
      title={['One event contract,', 'two streaming engines']}
      lead={<>FraudWatch Lab is where the system is explained and checked rather than operated: how the model was built and evaluated, and what actually differs when the same Kafka topic is processed by Apache Flink per event and by Spark Structured Streaming in micro-batches.</>}
      actions={<>
        <Link to="/study" className="fw-btn fw-btn-primary fw-btn-lg">Enter the study<ArrowRight size={18} aria-hidden="true" /></Link>
        <Link to="/architecture" className="fw-btn fw-btn-secondary fw-btn-lg">Explore the architecture</Link>
        {live && <a className="fw-btn fw-btn-ghost fw-btn-lg" href={live}><ExternalLink size={18} aria-hidden="true" />FraudWatch Live</a>}
      </>}
      meta={<span className="fw-small fw-muted">Component status comes from the API health probe; until it answers, status reads as not checked. Every figure in the study pages is read back from a run or from the served model metadata.</span>}
      scene={<SceneStage
        variant="lab"
        nodes={nodes}
        live={attached}
        modeLabel={attached ? `Live · run ${view!.run}` : 'Illustration'}
        initialSelected="flink"
        caption="The two lanes show the shape of the difference: continuous records against records that wait for a batch boundary. The timing of the animation is decorative — measured latency and parity live in the experiment pages, per run."
      />}
    />

    <LandingSection id="engines" eyebrow="Where the lanes differ" title="Per-event and micro-batch, side by side" lead="Both lanes consume the same topic and emit the same event contract. That is the only reason a comparison means anything.">
      <div className="fw-grid fw-grid-3">
        <StepCard n={1} title="Ingest">The same Kafka partition assignment feeds both lanes. Neither engine sees a filtered or reordered stream.</StepCard>
        <StepCard n={2} title="Time">Flink advances event time with watermarks and can emit as soon as a record is handled. Spark advances at batch boundaries, so a record waits for its batch.</StepCard>
        <StepCard n={3} title="State">Flink keeps keyed state per account key with checkpointed recovery. Spark keeps state across batches under its own store and commit protocol.</StepCard>
        <StepCard n={4} title="Lateness">A record later than the watermark allowance is recorded as late instead of being silently dropped, so late counts are part of the evidence rather than a gap in it.</StepCard>
        <StepCard n={5} title="Output">Both write scored events with model score, rule results and policy decision under one schema, keyed by run id and engine.</StepCard>
        <StepCard n={6} title="Comparison">Parity is computed over the records both engines actually produced for the same run: agreement on decisions, and where they diverge.</StepCard>
      </div>
    </LandingSection>

    <LandingSection id="study" eyebrow="What is here" title="What you can study" lead="Six entry points, each backed by data the API serves rather than by text written into the page.">
      <div className="fw-grid fw-grid-3">
        <StepCard n={1} title="CA1 · Fraud detection">Dataset provenance, time-aware preprocessing, model selection and the held-out evaluation report, plus the rules and review policy that sit around the score.</StepCard>
        <StepCard n={2} title="CA2 · Stream processing">Event time, watermarks, keyed state, checkpoints and late events, explained against the pipeline that is actually running.</StepCard>
        <StepCard n={3} title="Experiments">Per-run engine parity and late-record inspection. Starting a bounded benchmark needs operator access; reading the results does not.</StepCard>
        <StepCard n={4} title="Architecture">The shared Kafka-to-websites pipeline with each component&rsquo;s role, configuration and probed status, in 2D or 3D.</StepCard>
        <StepCard n={5} title="Evidence">Machine-readable model, experiment and test evidence, each carrying its provenance label.</StepCard>
        <StepCard n={6} title="Carried context">Opening this site from a Live session carries the session and run across, so the experiment pages query that exact run.</StepCard>
      </div>
    </LandingSection>

    <LandingSection id="limits" eyebrow="Honest limits" title="What this comparison is not" lead="The engines run on small instances under capped demo load, which bounds what any measurement here can claim.">
      <div className="fw-grid fw-grid-3">
        <StepCard n={1} title="Not a benchmark result">Numbers describe this deployment, this configuration and the run they are labelled with. They do not rank Flink against Spark in general.</StepCard>
        <StepCard n={2} title="Not confirmed fraud">Labels come from the IEEE-CIS competition data. A decision is a model and rule output, not an observed chargeback.</StepCard>
        <StepCard n={3} title="Not invented display data">No merchant, city or currency is fabricated for display: every shown field comes from the dataset record or from measured runtime state.</StepCard>
      </div>
    </LandingSection>

    {(liveConsole || live) && <LandingSection eyebrow="The operational side" title="FraudWatch Live" lead="The same pipeline, operated: start a capped demo session, watch events get scored, and open the evidence behind any single decision.">
      <p><a className="fw-btn fw-btn-secondary" href={liveConsole ?? live!}><ExternalLink size={16} aria-hidden="true" />Open FraudWatch Live</a></p>
    </LandingSection>}

    <LandingSection eyebrow="Start here" title="Enter the study" lead="The study index links each assessment area to the evidence behind it.">
      <p><Link to="/study" className="fw-btn fw-btn-primary fw-btn-lg">Enter the study<ArrowRight size={18} aria-hidden="true" /></Link></p>
    </LandingSection>
  </LandingPage>;
}
