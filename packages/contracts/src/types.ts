/**
 * TypeScript mirror of docs/contracts.md section 6 (Public HTTP API, base path /api/v1).
 * All times are integer epoch milliseconds (UTC). Amounts are INR.
 * Fields the contract leaves open are typed loosely (Record<string, unknown>) on purpose.
 */

export type Engine = 'flink' | 'spark';
export type Action = 'alert' | 'review' | 'allow' | 'unscored';
export type Pattern = 'burst' | 'profile_deviation' | 'card_testing' | 'late_burst';

export interface ApiErrorBody {
  error: { code: string; message: string };
}

/* ---------- Config & sessions ---------- */

export interface ScenarioOption {
  id: string;
  label?: string;
  description?: string;
}

export interface AppConfig {
  live_url: string | null;
  lab_url: string | null;
  demo_max_events_per_session: number;
  demo_max_events_per_second: number;
  ai_enabled: boolean;
  ai_provider_order: string[];
  model_version: string | null;
  /** Contract leaves the element shape open: strings or {id,label}. Use normalizeOptions(). */
  scenarios: Array<string | ScenarioOption>;
  patterns: Array<string | ScenarioOption>;
}

export interface Session {
  id: string;
  label: string | null;
  created_at: number;
  expires_at: number;
  status: 'active' | 'expired';
  event_cap: number;
  events_used: number;
  rate_cap_eps: number;
  active_run_id: string | null;
}

export interface SessionWithToken {
  session: Session;
  token: string;
}

export interface ShareLink {
  view_token: string;
  expires_at: number;
  lab_url: string | null;
  live_url: string | null;
}

export interface OperatorLogin {
  token: string;
  expires_at: number;
}

/* ---------- Runs ---------- */

export type RunStatus = 'starting' | 'running' | 'paused' | 'completed' | 'stopped' | 'failed';

export interface ControlLogEntry {
  at_base_index: number;
  action: string;
  value: unknown;
}

export interface Run {
  id: string;
  session_id: string;
  scenario: string;
  seed: number;
  rate_eps: number;
  engines: Engine[];
  status: RunStatus;
  max_events: number;
  events_sent: number;
  started_at: number | null;
  ended_at: number | null;
  replay_of: string | null;
  control_log: ControlLogEntry[];
  created_by: 'visitor' | 'operator' | 'experiment';
  error: string | null;
}

export interface CreateRunRequest {
  scenario: string;
  rate_eps: number;
  max_events?: number;
  seed?: number;
  engines?: Engine[];
}

export interface InjectResult {
  pattern: Pattern;
  account_id: string;
  events: number;
  injection: Record<string, unknown>;
}

/* ---------- Live data ---------- */

export interface LatencySummary {
  p50: number | null;
  p95: number | null;
  p99: number | null;
  n: number;
  window_s?: number;
}

export interface SeriesPoint {
  t: number;
  scored: number;
  flagged: number;
}

export interface WindowAgg {
  engine: Engine;
  session_id: string;
  window_start: number;
  window_end: number;
  txn_count: number;
  amount_sum_inr: number;
  alert_count: number;
  review_count: number;
  unscored_count: number;
  emitted_at: number;
  persisted_at: number;
}

export interface Overview {
  as_of: number;
  engine: Engine;
  counts: { scored: number; alert: number; review: number; allow: number; unscored: number; late: number };
  amount_flagged_inr: number;
  throughput_eps_1m: number | null;
  latency_ms: LatencySummary;
  last_persisted_at: number | null;
  series: SeriesPoint[];
  windows: WindowAgg[];
}

export interface TxnSummary {
  event_id: string;
  engine: Engine;
  session_id: string;
  run_id: string;
  seq: number;
  account_id: string;
  event_time: number;
  amount_inr: number;
  merchant_name: string | null;
  merchant_category: string | null;
  city: string | null;
  action: Action;
  score: number | null;
  late: boolean;
  pattern: string | null;
  injected: boolean | null;
  rules_fired: string[];
  persisted_at: number;
  e2e_ms: number | null;
}

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
}

export interface ShapContribution {
  feature: string;
  value: number | string | null;
  contribution: number;
}

export interface ModelEvidence {
  version: string;
  score: number;
  thresholds: { alert: number; review: number };
  explanation: { method: string; base_value: number; top: ShapContribution[] } | null;
}

export interface RuleResult {
  rule_id: string;
  title: string;
  fired: boolean;
  evidence: string | null;
}

export interface PolicyDecision {
  policy_version: string;
  action: Action;
  reasons: string[];
}

export interface TxnTiming {
  produced_at: number | null;
  kafka_ts: number | null;
  engine_received_at: number | null;
  scoring_started_at: number | null;
  scoring_completed_at: number | null;
  emitted_at: number | null;
  persisted_at: number | null;
}

export interface TxnDetail {
  engine: Engine;
  engine_job_id: string | null;
  kafka: { partition: number; offset: number; timestamp: number } | null;
  late: boolean;
  watermark_ms: number | null;
  raw_features: Record<string, number | null>;
  model_inputs: Record<string, number | string | null> | null;
  model: ModelEvidence | null;
  rules: { version: string; results: RuleResult[] } | null;
  policy: PolicyDecision | null;
  scoring: { status: 'ok' | 'failed' | string; attempts: number; error: string | null };
  timing: TxnTiming;
  duplicates: number;
}

export type TraceStage = 'generated' | 'kafka_ingest' | 'engine_received' | 'scoring' | 'engine_emitted' | 'persisted';

export interface TraceStep {
  stage: TraceStage;
  engine: Engine | null;
  at_ms: number;
  delta_ms: number | null;
  detail: string | Record<string, unknown> | null;
}

/** Input event fields (docs/contracts.md section 2). Optional display-only fields may be absent. */
export interface TxnEvent {
  schema_version: string;
  kind: 'txn';
  event_id: string;
  demo_session_id: string;
  run_id: string;
  seq: number;
  account_id: string;
  event_time: number;
  produced_at: number;
  amount_inr: number;
  product_cd: string;
  card_network: string;
  card_type: string;
  email_domain: string;
  device_type: string;
  has_identity: boolean;
  target_engines: Engine[];
  merchant_id?: string;
  merchant_name?: string;
  merchant_category?: string;
  city?: string;
  scenario?: string;
  pattern?: string | null;
  injected?: boolean;
  arrival_delay_ms?: number;
}

export interface TransactionDetailResponse {
  event: TxnEvent;
  engines: Partial<Record<Engine, TxnDetail>>;
  trace: TraceStep[];
}

/* ---------- Alerts ---------- */

export type AlertStatus = 'open' | 'acknowledged' | 'escalated' | 'dismissed' | 'confirmed_fraud_demo';

export interface Alert {
  id: string;
  event_id: string;
  engine: Engine;
  session_id: string;
  run_id: string;
  account_id: string;
  amount_inr: number;
  merchant_name: string | null;
  action: 'alert' | 'review';
  score: number;
  rules_fired: string[];
  created_at: number;
  status: AlertStatus;
  decision_note: string | null;
  updated_at: number;
}

export interface AlertPage extends Page<Alert> {
  counts_by_status: Partial<Record<AlertStatus, number>>;
}

export interface AuditEntry {
  id: string;
  at: number;
  actor: string;
  action: string;
  entity_type: string;
  entity_id: string;
  from_status: string | null;
  to_status: string | null;
  note: string | null;
  prev_hash: string | null;
  hash: string;
}

/* ---------- Model ---------- */

/** GET /model: deployed metadata. Loosely typed beyond the fields the UI reads. */
export interface ModelMetadata {
  model_version: string;
  model_type: string;
  score_semantics?: string;
  feature_spec?: string;
  features?: { numeric: string[]; categorical: string[]; categories?: Record<string, string[]> };
  thresholds?: { alert: number; review: number; alert_method?: string; review_method?: string };
  ruleset_version?: string;
  policy_version?: string;
  seed?: number;
  dataset?: Record<string, unknown> & {
    dataset?: string;
    rows?: number;
    fraud_rate?: number;
    uid_definition?: string;
    split?: Record<string, unknown>;
    feature_history_note?: string;
    licence?: string;
    license?: string;
    limitations?: string[] | string;
  };
  explanations?: string;
  feature_importance_gain?: Array<{ feature: string; gain: number }>;
  training_command?: string;
  environment?: Record<string, string>;
  trained_at_unix?: number;
  [k: string]: unknown;
}

export interface ConfusionMatrix { tn: number; fp: number; fn: number; tp: number }

export interface CalibrationBin { bin: number; count: number; mean_score: number; observed_fraud_rate: number }

export interface EvalReport {
  n: number;
  positives: number;
  prevalence: number;
  pr_auc: number;
  roc_auc: number | null;
  at_threshold: {
    threshold: number;
    precision: number;
    recall: number;
    f1: number;
    accuracy: number;
    flagged_rate: number;
    confusion_matrix: ConfusionMatrix;
  };
  all_legitimate_baseline_accuracy: number;
  calibration?: { brier: number; ece: number; bins: CalibrationBin[] };
  /** pr points are [recall, precision]; roc points are [fpr, tpr] */
  curves?: { pr: Array<[number, number]>; roc: Array<[number, number]> };
  validation_selected_threshold?: number;
}

export interface StreamModelResults {
  model_version: string;
  selected: string;
  selection_rule: string;
  candidates: Record<string, { validation_pr_auc: number; imbalance_handling?: string; best_iteration?: number }>;
  thresholds: { alert: number; review: number; alert_method?: string; review_method?: string };
  test: Record<string, EvalReport>;
  test_unseen_accounts?: EvalReport;
  test_policy?: {
    alert?: { count: number; share: number; fraud_rate_within: number };
    review?: { count: number; share: number; fraud_rate_within: number };
    allow?: { count: number; share: number; fraud_rate_within: number };
    flagged_recall?: number;
    rule_fire_counts?: Record<string, number>;
  };
  seconds?: number;
}

export interface ReferenceModelResults {
  model: string;
  purpose: string;
  n_features: number;
  split?: string;
  best_iteration?: number;
  validation_pr_auc: number;
  test: EvalReport;
  top_features_gain?: Array<{ feature: string; gain: number }>;
  seed?: number;
}

export interface ModelResultsResponse {
  stream: StreamModelResults | null;
  reference: ReferenceModelResults | null;
}

export interface WhatIfRequest {
  event: {
    amount_inr: number;
    product_cd: string;
    card_network: string;
    card_type: string;
    email_domain: string;
    device_type: string;
    has_identity: boolean;
  };
  raw: Record<string, number | null>;
}

export interface WhatIfResponse {
  event_id?: string;
  model: ModelEvidence;
  rules: { version: string; results: RuleResult[] } | null;
  policy: PolicyDecision | null;
  model_inputs?: Record<string, number | string | null>;
  disclaimer: string;
}

/* ---------- Health & architecture ---------- */

export type ComponentId = 'api' | 'postgres' | 'kafka' | 'generator' | 'scoring' | 'result_consumer' | 'flink' | 'spark';
export type ComponentStatus = 'ok' | 'degraded' | 'down' | 'idle' | 'unknown';

export interface HealthComponent {
  id: ComponentId | string;
  status: ComponentStatus;
  detail: string | null;
  checked_at: number | null;
  metrics: Record<string, unknown> | null;
}

export interface FlinkMetrics {
  job_id?: string | null;
  job_state?: string | null;
  uptime_ms?: number | null;
  restarts?: number | null;
  last_checkpoint?: { id: number; completed_at: number; duration_ms: number; state_size: number } | null;
}

export interface SparkMetrics {
  state?: 'idle' | 'running';
  app_id?: string | null;
  run_label?: string | null;
  last_batch_id?: number | null;
  last_batch_at?: number | null;
}

export interface Health {
  as_of: number;
  overall: 'ok' | 'degraded' | 'down';
  components: HealthComponent[];
}

export type ArchNodeId = 'generator' | 'kafka' | 'flink' | 'spark' | 'scoring' | 'result_consumer' | 'postgres' | 'api' | 'live' | 'lab';

export interface ArchitectureNode {
  id: ArchNodeId | string;
  label: string;
  role: string;
  config: Record<string, unknown>;
  status: ComponentStatus | string;
}

export interface Architecture {
  nodes: ArchitectureNode[];
  edges: Array<{ from: string; to: string; label: string }>;
}

/* ---------- Comparison, late, experiments ---------- */

export interface EngineStats {
  count: number;
  late: number;
  actions: { alert: number; review: number; allow: number; unscored: number };
  duplicates: number;
  latency_ms: LatencySummary;
  first_persisted_at: number | null;
  last_persisted_at: number | null;
}

export interface RunComparison {
  run_id: string;
  engines: Partial<Record<Engine, EngineStats>>;
  matching: {
    both: number;
    only_flink: number;
    only_spark: number;
    action_agree: number;
    action_disagree: number;
    raw_feature_mismatch: number;
    late_disagree: number;
  };
  disagreements: Array<{ event_id: string; flink_action: Action | null; spark_action: Action | null; flink_late: boolean | null; spark_late: boolean | null }>;
}

export interface LateExample {
  event_id: string;
  event_time: number;
  watermark_ms: number | null;
  lag_ms: number | null;
  pattern: string | null;
}

export interface RunLate {
  engines: Partial<Record<Engine, { late: number; windows_late_dropped: number; examples: LateExample[] }>>;
}

export type ExperimentKind = 'comparison' | 'late_events' | 'recovery';
export type ExperimentStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface ComparisonEngineResult {
  offered_eps: number;
  processed_eps: number;
  sent: number;
  persisted: number;
  missing: number;
  duplicates: number;
  failed_scoring: number;
  late: number;
  latency_ms: { p50: number | null; p95: number | null; p99: number | null; mean: number | null; n: number; interval_ms?: number };
  backlog_max: number | null;
  cpu_pct: number | null;
  mem_mb: number | null;
  repeats: unknown[];
}

export interface Experiment {
  id: string;
  kind: ExperimentKind;
  status: ExperimentStatus;
  params: Record<string, unknown>;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  created_by: string;
  run_ids: string[];
  results: (Record<string, unknown> & { engines?: Partial<Record<Engine, ComparisonEngineResult>> }) | null;
  error: string | null;
  environment: Record<string, unknown> | null;
}

export interface EvidenceItem {
  id: string;
  title: string;
  kind: 'model' | 'experiment' | 'test' | 'deployment' | 'report';
  created_at: number;
  config: Record<string, unknown> | null;
  url: string | null;
  provenance: 'measured' | 'prerecorded' | 'illustrative';
}

export interface AssistRequest {
  topic: 'alert' | 'architecture' | 'experiment' | 'viva';
  ref_id?: string;
  question: string;
}

export interface AssistResponse {
  provider: 'groq' | 'gemini' | 'openai' | 'deterministic';
  model: string | null;
  answer: string;
  citations: Array<{ type: string; id: string; label: string }>;
  disclaimer: string;
}

/* ---------- SSE ---------- */

export interface StreamEventMap {
  txn: TxnSummary;
  alert: Alert;
  window: WindowAgg;
  run: Run;
  health: Health;
  ping: { as_of: number };
  reset: { reason: string };
}

export type StreamEventType = keyof StreamEventMap;

export const STREAM_EVENT_TYPES: StreamEventType[] = ['txn', 'alert', 'window', 'run', 'health', 'ping', 'reset'];

/** Normalises the open-ended scenarios/patterns arrays from GET /config. */
export function normalizeOptions(list: Array<string | ScenarioOption> | undefined | null): ScenarioOption[] {
  if (!Array.isArray(list)) return [];
  return list.map((item) => (typeof item === 'string' ? { id: item, label: item.replace(/_/g, ' ') } : { ...item, label: item.label ?? item.id.replace(/_/g, ' ') }));
}
