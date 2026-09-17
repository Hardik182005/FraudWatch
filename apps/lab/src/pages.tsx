import type { EvalReport, Experiment, ExperimentKind, ModelMetadata, ModelResultsResponse, RunComparison, RunLate } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import {
  Banner, Button, Card, Disclosure, EmptyState, ErrorState, Field, HealthDot, KeyValue,
  LoadingSkeleton, ObjectKeyValue, PipelineDiagram, Stat, StatusPill, buttonClass,
  formatDate, formatInt, formatMs, formatNumber, formatPct, formatTimestamp,
} from '@fraudwatch/ui';
import { ArrowRight, BookOpen, Download, ExternalLink, FlaskConical, LockKeyhole, Network, Play, RefreshCw, ShieldCheck } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from './api';
import { useLab } from './lab';

function PageHead({ eyebrow, title, children, actions }: { eyebrow: string; title: string; children: React.ReactNode; actions?: React.ReactNode }) {
  return <div className="fw-page-head"><div><div className="fw-eyebrow">{eyebrow}</div><h1>{title}</h1><p>{children}</p></div>{actions && <div className="fw-row">{actions}</div>}</div>;
}

export function HomePage() {
  const { view, liveLink, health } = useLab();
  const model = useApi(() => api.getModel(), []);
  return <div className="fw-stack fw-lab-page">
    <PageHead eyebrow="Interactive academic companion" title="Learn the system through its evidence">
      CA1 builds the fraud model and shared detection policy. CA2 runs the same event contract through Apache Flink and Spark Structured Streaming.
    </PageHead>
    {view?.run ? <Banner tone="info"><strong>Run selected from Live.</strong> The experiment views below query run <code>{view.run}</code> from the shared backend.</Banner> :
      <Banner tone="neutral">Start a session in FraudWatch Live, then choose <strong>Open in Lab</strong> to carry its session and run here.</Banner>}
    <div className="fw-grid fw-grid-3">
      <Stat label="Deployed model" value={model.data?.model_version ?? '—'} note={model.status === 'error' ? 'API unavailable' : 'Loaded from model metadata'} />
      <Stat label="Live run context" value={view?.run ? 'Selected' : 'None'} note={view?.run ?? 'Waiting for a shared run'} />
      <Stat label="Runtime health" value={health.data?.overall ?? 'Unknown'} note={health.data ? `${health.data.components.length} components reported` : 'No runtime snapshot'} />
    </div>
    <div className="fw-lab-map">
      <StudyTile to="/ca1" step="01" title="CA1 · Fraud detection" text="Dataset provenance, time-aware preprocessing, model selection, held-out results and the decision policy." />
      <StudyTile to="/ca2" step="02" title="CA2 · Stream processing" text="Event time, watermarks, keyed state, checkpoints, late events and the Spark micro-batch comparison." />
      <StudyTile to="/experiments" step="03" title="Experiment lab" text="Inspect real run parity and late records. Operator access is required to start bounded benchmarks." />
      <StudyTile to="/architecture" step="04" title="Architecture explorer" text="Inspect the shared Kafka-to-websites pipeline in accessible 2D or interactive 3D." />
      <StudyTile to="/viva" step="05" title="Viva walkthrough" text="A concise demonstration route and focused questions with expandable answers." />
      <StudyTile to="/evidence" step="06" title="Evidence & status" text="Machine-readable model, experiment and test evidence, with honest provenance labels." />
    </div>
    {liveLink('/') && <Card title="Continue in FraudWatch Live" description="The link preserves the selected session and run."><a href={liveLink('/')!} className={buttonClass('primary')}>Open operational dashboard <ExternalLink size={16} /></a></Card>}
  </div>;
}

function StudyTile({ to, step, title, text }: { to: string; step: string; title: string; text: string }) {
  return <Link to={to} className="fw-study-tile"><span>{step}</span><h2>{title}</h2><p>{text}</p><strong>Explore <ArrowRight size={16} /></strong></Link>;
}

export function Ca1Page() {
  const model = useApi(() => api.getModel(), []);
  const results = useApi(() => api.getModelResults(), []);
  const stream = results.data?.stream;
  const report = stream ? (stream.test?.[stream.selected] ?? Object.values(stream.test ?? {})[0]) : undefined;
  return <div className="fw-stack fw-lab-page">
    <PageHead eyebrow="Continuous Assessment 1" title="Fraud detection with reproducible ML">The dashboard uses a versioned LightGBM score, separate deterministic rules, and an explicit review policy. A score is not presented as a calibrated probability.</PageHead>
    {model.status === 'loading' || results.status === 'loading' ? <LoadingSkeleton lines={5} /> : null}
    {model.status === 'error' ? <ErrorState error={model.error} onRetry={model.refetch} /> : null}
    {results.status === 'error' ? <ErrorState error={results.error} title="Evaluation results unavailable" onRetry={results.refetch} /> : null}
    {model.data && <Dataset model={model.data} />}
    {stream && report && <Evaluation results={stream} report={report} />}
    <div className="fw-grid fw-grid-3">
      <Concept n="1" title="Model evidence">TreeSHAP contributions explain which model inputs moved this model score. They are associations, not causes.</Concept>
      <Concept n="2" title="Rule evidence">Burst, profile-deviation and card-testing rules inspect explicit state. Their reasons stay separate from model attribution.</Concept>
      <Concept n="3" title="Review policy">Thresholds and rule results combine into alert, review or allow. Human review decisions create an audit trail and do not retrain the model.</Concept>
    </div>
  </div>;
}

function Dataset({ model }: { model: ModelMetadata }) {
  const ds = model.dataset ?? {};
  return <Card title="Dataset and feature discipline" description="Metadata served with the deployed artifact">
    <KeyValue items={[
      { key: 'dataset', label: 'Source', value: String(ds.dataset ?? 'Not reported') },
      { key: 'rows', label: 'Rows', value: formatInt(ds.rows as number | undefined) },
      { key: 'prevalence', label: 'Fraud prevalence', value: typeof ds.fraud_rate === 'number' ? formatPct(ds.fraud_rate) : '—' },
      { key: 'split', label: 'Split', value: String((ds.split as Record<string, unknown> | undefined)?.method ?? 'Not reported') },
      { key: 'account', label: 'Account proxy', value: String(ds.uid_definition ?? 'Not reported') },
      { key: 'version', label: 'Feature schema', value: String(model.feature_spec ?? '—') },
    ]} />
    <Banner tone="warn" className="fw-space-top">The IEEE-CIS dataset is competition data. Its pseudo-account is not a bank account, and synthetic merchant/location display fields are not claimed as original dataset features.</Banner>
  </Card>;
}

function Evaluation({ results, report }: { results: NonNullable<ModelResultsResponse['stream']>; report: EvalReport }) {
  const t = report.at_threshold;
  return <>
    <div className="fw-grid fw-grid-4">
      <Stat label="Test PR-AUC" value={formatNumber(report.pr_auc, 3)} note="Primary imbalanced-class ranking metric" />
      <Stat label="Precision" value={formatPct(t.precision)} note={`At threshold ${formatNumber(t.threshold, 4)}`} />
      <Stat label="Recall" value={formatPct(t.recall)} note={`${formatInt(t.confusion_matrix.tp)} true positives`} />
      <Stat label="F1" value={formatNumber(t.f1, 3)} note={`${formatPct(report.prevalence)} held-out prevalence`} />
    </div>
    <Card title="Model selection" description={results.selection_rule}>
      <div className="fw-table-wrap"><table className="fw-table"><thead><tr><th>Candidate</th><th>Validation PR-AUC</th><th>Imbalance handling</th><th>Selected</th></tr></thead><tbody>
        {Object.entries(results.candidates).map(([name, c]) => <tr key={name}><td><strong>{name}</strong></td><td>{formatNumber(c.validation_pr_auc, 4)}</td><td>{c.imbalance_handling ?? '—'}</td><td>{name === results.selected ? <StatusPill tone="allow">Selected</StatusPill> : '—'}</td></tr>)}
      </tbody></table></div>
    </Card>
    <Card title="Held-out confusion matrix" description={`Final test set · n=${formatInt(report.n)}`}>
      <div className="fw-matrix"><div className="fw-matrix-h" /><div className="fw-matrix-h">Predicted ordinary</div><div className="fw-matrix-h">Predicted fraud</div><div className="fw-matrix-h">Actual ordinary</div><div><strong>{formatInt(t.confusion_matrix.tn)}</strong>TN</div><div><strong>{formatInt(t.confusion_matrix.fp)}</strong>FP</div><div className="fw-matrix-h">Actual fraud</div><div><strong>{formatInt(t.confusion_matrix.fn)}</strong>FN</div><div><strong>{formatInt(t.confusion_matrix.tp)}</strong>TP</div></div>
      <p className="fw-muted fw-small fw-space-top">Accuracy alone is misleading because only {formatPct(report.prevalence)} of this held-out set is fraud. PR-AUC, precision and recall show the useful minority-class trade-off.</p>
    </Card>
  </>;
}

function Concept({ n, title, children }: { n: string; title: string; children: React.ReactNode }) {
  return <Card headingLevel={3}><div className="fw-concept-number">{n}</div><h3>{title}</h3><p className="fw-muted">{children}</p></Card>;
}

export function Ca2Page() {
  return <div className="fw-stack fw-lab-page">
    <PageHead eyebrow="Continuous Assessment 2" title="True stream processing and a fair micro-batch comparison">The same Kafka events, feature contract, scoring artifact and persistence rules feed Flink and the pinned Spark Structured Streaming job.</PageHead>
    <div className="fw-grid fw-grid-2">
      <Card title="Apache Flink" description="Long-running event-at-a-time pipeline"><ul className="fw-check-list"><li>Bounded out-of-orderness watermarks</li><li>Keyed per-account transaction state</li><li>Event-time windows and visible late-event output</li><li>Periodic checkpoints and restart strategy</li><li>Stable event IDs at the idempotent sink</li></ul></Card>
      <Card title="Spark Structured Streaming" description="Explicit micro-batch configuration"><ul className="fw-check-list"><li>Independent Kafka consumer group</li><li>Configured trigger interval</li><li>Equivalent state feature semantics</li><li>Separate result topic and checkpoint directory</li><li>Operator-triggered to reduce idle cost</li></ul></Card>
    </div>
    <Card title="Event-time lifecycle"><div className="fw-event-flow"><FlowStep n="1" title="Timestamp">Generator assigns event time; Kafka records arrival independently.</FlowStep><FlowStep n="2" title="Watermark">The engine estimates completed event time while allowing bounded disorder.</FlowStep><FlowStep n="3" title="Keyed state">Earlier account events produce burst and profile context without using labels.</FlowStep><FlowStep n="4" title="Late policy">Late records remain visible and are counted; finalized-window behavior is reported separately.</FlowStep></div></Card>
    <div className="fw-grid fw-grid-2">
      <Card title="Checkpointing is not a blanket exactly-once claim"><p>Flink checkpoints coordinate Kafka offsets and managed state. The HTTP scoring call and durable database writes cross external systems, so FraudWatch separately records retries and uses stable IDs plus database upserts for idempotency.</p></Card>
      <Card title="Comparison boundary"><p>End-to-end delay starts at the documented ingestion timestamp and ends when the result becomes durable. Browser refresh time is excluded. Both engines must process matching IDs before latency is interpreted.</p></Card>
    </div>
    <Card title="Inspect actual behavior" actions={<Link className={buttonClass('primary')} to="/experiments">Open experiment lab <ArrowRight size={16} /></Link>}><p className="fw-muted">A selected Live run enables parity, latency and late-event inspection. Until an experiment completes, charts stay empty instead of displaying invented results.</p></Card>
  </div>;
}

function FlowStep({ n, title, children }: { n: string; title: string; children: React.ReactNode }) { return <div><span>{n}</span><h3>{title}</h3><p>{children}</p></div>; }

export function ExperimentsPage() {
  const { view, operator, login, logout, liveLink } = useLab();
  const experiments = useApi(() => api.listExperiments(), [operator?.token], { refreshMs: 10000 });
  const comparison = useApi(() => api.getComparison(view?.run ?? ''), [view?.run], { enabled: !!view?.run });
  const late = useApi(() => api.getLate(view?.run ?? ''), [view?.run], { enabled: !!view?.run });
  const [passphrase, setPassphrase] = useState('');
  const [loginError, setLoginError] = useState<unknown>(null);
  const [creating, setCreating] = useState<ExperimentKind | null>(null);
  const [createError, setCreateError] = useState<unknown>(null);
  const submitLogin = async (e: React.FormEvent) => { e.preventDefault(); setLoginError(null); try { await login(passphrase); setPassphrase(''); } catch (err) { setLoginError(err); } };
  const start = async (kind: ExperimentKind) => { setCreating(kind); setCreateError(null); try { await api.createExperiment(kind, { profile: 'demo', run_id: view?.run ?? null, repeats: kind === 'comparison' ? 3 : 1 }); experiments.refetch(); } catch (err) { setCreateError(err); } finally { setCreating(null); } };
  return <div className="fw-stack fw-lab-page">
    <PageHead eyebrow="Measured evidence" title="Experiment lab">Inspect a shared run without operator access. Starting bounded benchmark, late-event or recovery work requires the operator role.</PageHead>
    {!view?.run ? <EmptyState title="No run selected" icon={<FlaskConical size={18} />}>Open a run from FraudWatch Live to compare engines and inspect late records.{liveLink('/') && <div className="fw-space-top"><a className={buttonClass('primary')} href={liveLink('/')!}>Open Live</a></div>}</EmptyState> : <>
      <Banner tone="info">Inspecting run <code>{view.run}</code>. Values below come from the shared durable results.</Banner>
      <ComparisonView data={comparison.data} error={comparison.error} loading={comparison.status === 'loading'} onRetry={comparison.refetch} />
      <LateView data={late.data} error={late.error} loading={late.status === 'loading'} onRetry={late.refetch} />
    </>}
    <Card title="Operator controls" description="Protected workload controls">
      {!operator ? <form className="fw-login" onSubmit={submitLogin}><Field label="Operator passphrase" htmlFor="operator-pass"><input id="operator-pass" className="fw-input" type="password" autoComplete="current-password" value={passphrase} onChange={(e) => setPassphrase(e.target.value)} /></Field><Button type="submit" variant="primary" icon={<LockKeyhole size={16} />} disabled={!passphrase}>Sign in</Button>{loginError && <ErrorState error={loginError} title="Sign-in failed" />}</form> : <div className="fw-stack"><div className="fw-row"><StatusPill tone="allow">Operator authenticated</StatusPill><span className="fw-muted fw-small">Expires {formatTimestamp(operator.expires_at)}</span><Button size="sm" onClick={logout}>Sign out</Button></div><div className="fw-row"><Button loading={creating === 'comparison'} onClick={() => void start('comparison')}>Run engine comparison</Button><Button loading={creating === 'late_events'} onClick={() => void start('late_events')}>Run late-event test</Button><Button variant="danger" loading={creating === 'recovery'} onClick={() => void start('recovery')}>Run controlled recovery</Button></div>{createError && <ErrorState error={createError} title="Experiment did not start" />}</div>}
    </Card>
    <ExperimentList data={experiments.data} error={experiments.error} loading={experiments.status === 'loading'} onRetry={experiments.refetch} />
  </div>;
}

function ComparisonView({ data, error, loading, onRetry }: { data?: RunComparison; error: unknown; loading: boolean; onRetry: () => void }) {
  if (loading) return <LoadingSkeleton lines={4} label="Loading comparison" />;
  if (error) return <ErrorState error={error} title="Comparison unavailable" onRetry={onRetry} />;
  if (!data) return null;
  const f = data.engines.flink, s = data.engines.spark;
  return <Card title="Same stream, two engines" description="Matching by stable event_id"><div className="fw-grid fw-grid-4"><Stat label="Both engines" value={formatInt(data.matching.both)} /><Stat label="Action agreement" value={formatInt(data.matching.action_agree)} note={`${formatInt(data.matching.action_disagree)} disagreements`} /><Stat label="Only Flink" value={formatInt(data.matching.only_flink)} /><Stat label="Only Spark" value={formatInt(data.matching.only_spark)} /></div><div className="fw-table-wrap fw-space-top"><table className="fw-table"><thead><tr><th>Engine</th><th>Persisted</th><th>Late</th><th>Duplicates</th><th>p95 durable delay</th></tr></thead><tbody><EngineRow name="Flink" stats={f} /><EngineRow name="Spark" stats={s} /></tbody></table></div></Card>;
}
function EngineRow({ name, stats }: { name: string; stats: RunComparison['engines']['flink'] }) { return <tr><td><strong>{name}</strong></td><td>{formatInt(stats?.count)}</td><td>{formatInt(stats?.late)}</td><td>{formatInt(stats?.duplicates)}</td><td>{formatMs(stats?.latency_ms.p95)}</td></tr>; }
function LateView({ data, error, loading, onRetry }: { data?: RunLate; error: unknown; loading: boolean; onRetry: () => void }) { if (loading) return <LoadingSkeleton lines={3} />; if (error) return <ErrorState error={error} title="Late-event evidence unavailable" onRetry={onRetry} />; if (!data) return null; return <Card title="Late-event evidence" description="Runtime counters, not an animation"><div className="fw-grid fw-grid-2">{(['flink','spark'] as const).map((e) => <div key={e}><h3>{e === 'flink' ? 'Flink' : 'Spark'}</h3><KeyValue items={[{key:'late',value:formatInt(data.engines[e]?.late)},{key:'dropped',label:'Finalized-window drops',value:formatInt(data.engines[e]?.windows_late_dropped)}]} /></div>)}</div></Card>; }

function ExperimentList({ data, error, loading, onRetry }: { data?: Experiment[]; error: unknown; loading: boolean; onRetry: () => void }) {
  if (loading && !data) return <LoadingSkeleton lines={4} label="Loading experiments" />;
  if (error && !data) return <ErrorState error={error} title="Experiments unavailable" onRetry={onRetry} />;
  return <Card title="Experiment history" description="Latest server-owned records" actions={<Button size="sm" icon={<RefreshCw size={15} />} onClick={onRetry}>Refresh</Button>} flush>{!data?.length ? <EmptyState title="No recorded experiments"><p>Run an authorized experiment to create measured evidence.</p></EmptyState> : <div className="fw-table-wrap"><table className="fw-table"><thead><tr><th>Experiment</th><th>Kind</th><th>Status</th><th>Created</th><th>Evidence</th></tr></thead><tbody>{data.map((e) => <tr key={e.id}><td><code>{e.id}</code></td><td>{e.kind.replace('_',' ')}</td><td><StatusPill tone={e.status === 'completed' ? 'allow' : e.status === 'failed' ? 'alert' : 'info'}>{e.status}</StatusPill></td><td>{formatDate(e.created_at)}</td><td>{e.status === 'completed' ? <span className="fw-row"><a href={api.experimentEvidenceUrl(e.id)} download>JSON</a><a href={api.experimentRawCsvUrl(e.id)} download>CSV</a></span> : '—'}</td></tr>)}</tbody></table></div>}</Card>;
}

export function ArchitecturePage() {
  const { health } = useLab();
  const architecture = useApi(() => api.getArchitecture(), []);
  const explanations: Record<string, string> = { generator:'Produces reproducible scenarios and applies session caps.',kafka:'Durable event log shared by independent engine consumer groups.',flink:'Primary true-stream processor with event-time state and checkpoints.',spark:'Operator-triggered Structured Streaming micro-batch comparison.',scoring:'Loads the versioned preprocessing and LightGBM artifact.',result_consumer:'Deduplicates stable record IDs and persists results.',postgres:'Server-owned sessions, runs, predictions, alerts, audits and evidence.',api:'Authenticated HTTP and resumable SSE boundary for both websites.',live:'Operational monitoring and investigation interface.',lab:'Academic concepts, experiments, evidence and viva guide.' };
  return <div className="fw-stack fw-lab-page"><PageHead eyebrow="Shared system" title="Architecture explorer">Select any component to see its runtime status, configuration and role. The 3D scene lazy-loads; 2D is always available.</PageHead><PipelineDiagram architecture={architecture.data} health={health.data} loading={architecture.status === 'loading'} error={architecture.error} onRetry={() => { architecture.refetch(); health.refetch(); }} explain={(id) => <p className="fw-muted fw-small">{explanations[id] ?? 'Runtime component reported by the architecture API.'}</p>} /></div>;
}

const QUESTIONS = [
  ['Why is accuracy insufficient?', 'Fraud is rare. A model that predicts every transaction as ordinary can have high accuracy while catching no fraud. Precision, recall and PR-AUC expose minority-class performance.'],
  ['What prevents target leakage?', 'The split is chronological, preprocessing is fitted on training data, state features use only earlier events, and labels never enter the feature calculation.'],
  ['Is the model score a probability?', 'No. The interface calls it a model score because calibration must be assessed before probability language is justified.'],
  ['Why keep rules separate from the model?', 'It preserves provenance. TreeSHAP explains the model; explicit evidence explains a rule; policy combines both into a review action.'],
  ['What is event time?', 'The time the transaction occurred, distinct from when Kafka or an engine received it. It lets windows reflect the source timeline.'],
  ['What does a watermark mean?', 'It is the engine’s estimate of event-time progress. Records behind it may be classified as late according to the configured policy.'],
  ['Why keyed state?', 'Burst and profile features need prior events for the same synthetic account, so events are partitioned by account ID and state is bounded with TTL.'],
  ['Does checkpointing guarantee end-to-end exactly once?', 'No. It coordinates Flink source offsets and managed state. External scoring and database effects need retry analysis, stable IDs and idempotent writes.'],
  ['How is Spark compared fairly?', 'Both engines receive matching event IDs through independent consumer groups, use the same scoring model, aligned event-time assumptions and distinct outputs.'],
  ['Why can Spark be idle?', 'It is launched for an authorized comparison to reduce idle cloud cost. The UI reports idle rather than pretending it is continuously processing.'],
  ['How are duplicate alerts prevented?', 'Results use stable logical IDs and the consumer persists with database uniqueness constraints/upserts. Duplicate delivery is still counted as evidence.'],
  ['What does p95 latency mean?', 'Ninety-five percent of measured durable outputs completed at or below that delay within the stated run interval.'],
  ['What is a late event?', 'An event whose event time falls behind the processor watermark. It is recorded separately from ordinary on-time processing.'],
  ['Does a review decision retrain the model?', 'No. It creates an auditable demo decision only. Retraining requires a separate governed dataset and training workflow.'],
  ['What is the main dataset limitation?', 'IEEE-CIS is anonymized competition data with a constructed pseudo-account. It does not prove performance on a real bank’s current traffic.'],
  ['Why use synthetic display context?', 'It makes the demo understandable, but merchant and location fields are labelled synthetic and are not claimed as original training features.'],
  ['How are visitors isolated?', 'Each visitor receives a signed session token; APIs scope runs and results to that session. Global experiments and infrastructure controls require operator credentials.'],
  ['What happens after reconnect?', 'SSE resumes from a cursor when possible; if history is unavailable, the client fetches a consistent server snapshot.'],
  ['What is measured versus illustrative?', 'Predictions, traces and completed benchmark records come from runtime storage. Diagram flow particles are explicitly only an illustration.'],
  ['What would production require?', 'Multi-node availability, stronger identity and authorization, managed secrets, monitoring, governance, drift checks, calibrated decisions and validation on institution-specific data.'],
] as const;

export function VivaPage() { return <div className="fw-stack fw-lab-page"><PageHead eyebrow="Five-to-seven-minute guide" title="Viva mode">Use the guided route for the demonstration, then open any question for a short defensible answer.</PageHead><Card title="Walkthrough"><ol className="fw-steps"><li><div><strong>Problem and CA1</strong><p className="fw-muted">Show rarity, chronological split, model selection and held-out metrics.</p></div></li><li><div><strong>Start the Live demo</strong><p className="fw-muted">Run an ordinary scenario, inject suspicious behavior and follow one persisted transaction.</p></div></li><li><div><strong>Explain the decision</strong><p className="fw-muted">Separate model attribution, explicit rules and the final review policy.</p></div></li><li><div><strong>Show CA2</strong><p className="fw-muted">Explain event time, watermark, keyed state, late handling and checkpoints.</p></div></li><li><div><strong>Compare engines</strong><p className="fw-muted">Use matching event IDs and measured evidence; do not promise one engine always wins.</p></div></li><li><div><strong>Close with limits</strong><p className="fw-muted">Academic dataset, single-machine deployment, synthetic context and no real payment blocking.</p></div></li></ol></Card><Card title="Focused questions"><div>{QUESTIONS.map(([q,a], i) => <Disclosure key={q} summary={`${i+1}. ${q}`}><p>{a}</p></Disclosure>)}</div></Card><Card title="Team"><p>Group 4 · Hardik Hinduja · Avinash Gehi · Siddharth Dey · Sahil Deshmukh</p><p className="fw-muted fw-small">Responsibilities are intentionally not claimed until the group confirms completed contributions.</p></Card></div>; }

export function EvidencePage() {
  const evidence = useApi(() => api.listEvidence(), []);
  const model = useApi(() => api.getModel(), []);
  return <div className="fw-stack fw-lab-page"><PageHead eyebrow="Provenance and completion" title="Evidence & status">Downloadable evidence is generated by the model and experiment workflows. The formatted report is deferred until the teacher supplies the required template.</PageHead><Banner tone="warn"><strong>Report template pending.</strong> No final PDF is claimed or generated yet.</Banner><Card title="Deployed model record"><ObjectKeyValue data={model.data ? { model_version:model.data.model_version, model_type:model.data.model_type, score_semantics:model.data.score_semantics, feature_spec:model.data.feature_spec, trained_at:model.data.trained_at_unix ? formatTimestamp(model.data.trained_at_unix * 1000) : null, training_command:model.data.training_command } : null} />{model.status === 'error' && <ErrorState error={model.error} onRetry={model.refetch} />}</Card><Card title="Evidence index" description="Provenance is returned by the API" flush>{evidence.status === 'loading' ? <LoadingSkeleton lines={4} /> : evidence.status === 'error' ? <ErrorState error={evidence.error} onRetry={evidence.refetch} /> : !evidence.data?.length ? <EmptyState title="No evidence records yet">Complete a model or experiment workflow to populate this index.</EmptyState> : <div className="fw-table-wrap"><table className="fw-table"><thead><tr><th>Item</th><th>Kind</th><th>Provenance</th><th>Created</th><th>Download</th></tr></thead><tbody>{evidence.data.map((e) => <tr key={e.id}><td><strong>{e.title}</strong><div><code>{e.id}</code></div></td><td>{e.kind}</td><td><StatusPill tone={e.provenance === 'measured' ? 'allow' : e.provenance === 'prerecorded' ? 'review' : 'neutral'}>{e.provenance}</StatusPill></td><td>{formatDate(e.created_at)}</td><td>{e.url ? <a href={e.url} className={buttonClass('secondary','sm')} download><Download size={15}/>Download</a> : '—'}</td></tr>)}</tbody></table></div>}</Card></div>;
}

export function NotFoundPage() { return <EmptyState title="Page not found" icon={<BookOpen size={18} />} action={<Link to="/" className={buttonClass('primary')}>Return to study lab</Link>}>The requested Lab route does not exist.</EmptyState>; }
