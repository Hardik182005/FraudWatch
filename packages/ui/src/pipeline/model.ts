import type { Architecture, ArchitectureNode, Health, HealthComponent, TraceStep } from '@fraudwatch/contracts';

/**
 * Canonical layout of the FraudWatch pipeline:
 * Transactions -> Kafka -> Flink / Spark -> Scoring -> Alerts & Storage -> Both websites.
 * Only the layout and short labels are static. Role, configuration and status come from
 * GET /architecture and GET /health at runtime.
 */
export const PIPELINE_STAGES: Array<{ id: string; title: string; nodes: string[] }> = [
  { id: 'transactions', title: 'Transactions', nodes: ['generator'] },
  { id: 'kafka', title: 'Kafka', nodes: ['kafka'] },
  { id: 'engines', title: 'Flink / Spark', nodes: ['flink', 'spark'] },
  { id: 'scoring', title: 'Scoring', nodes: ['scoring'] },
  { id: 'storage', title: 'Alerts & Storage', nodes: ['result_consumer', 'postgres'] },
  { id: 'websites', title: 'Both websites', nodes: ['api', 'live', 'lab'] },
];

export const SHORT_LABELS: Record<string, string> = {
  generator: 'Generator',
  kafka: 'Kafka',
  flink: 'Flink',
  spark: 'Spark',
  scoring: 'Scoring',
  result_consumer: 'Result consumer',
  postgres: 'PostgreSQL',
  api: 'API',
  live: 'FraudWatch Live',
  lab: 'FraudWatch Lab',
};

/** Used only for drawing when GET /architecture is unavailable; flagged in the UI. */
export const CANONICAL_EDGES: Array<{ from: string; to: string; label: string }> = [
  { from: 'generator', to: 'kafka', label: 'transactions' },
  { from: 'kafka', to: 'flink', label: 'consume' },
  { from: 'kafka', to: 'spark', label: 'consume' },
  { from: 'flink', to: 'scoring', label: 'score' },
  { from: 'spark', to: 'scoring', label: 'score' },
  { from: 'flink', to: 'result_consumer', label: 'results topic' },
  { from: 'spark', to: 'result_consumer', label: 'results topic' },
  { from: 'result_consumer', to: 'postgres', label: 'upsert' },
  { from: 'postgres', to: 'api', label: 'query' },
  { from: 'api', to: 'live', label: 'HTTP + SSE' },
  { from: 'api', to: 'lab', label: 'HTTP' },
];

export interface PipelineNodeView {
  id: string;
  label: string;
  stage: number;
  row: number;
  rowsInStage: number;
  arch: ArchitectureNode | null;
  health: HealthComponent | null;
  status: string;
}

export function buildNodeViews(architecture: Architecture | null | undefined, health: Health | null | undefined): PipelineNodeView[] {
  const archById = new Map((architecture?.nodes ?? []).map((n) => [n.id, n]));
  const healthById = new Map((health?.components ?? []).map((c) => [c.id, c]));
  const views: PipelineNodeView[] = [];
  PIPELINE_STAGES.forEach((stage, s) => {
    stage.nodes.forEach((id, r) => {
      const arch = archById.get(id) ?? null;
      const h = healthById.get(id) ?? null;
      views.push({
        id,
        label: SHORT_LABELS[id] ?? arch?.label ?? id,
        stage: s,
        row: r,
        rowsInStage: stage.nodes.length,
        arch,
        health: h,
        status: h?.status ?? arch?.status ?? 'unknown',
      });
    });
  });
  // Any extra nodes reported by the API are appended to the last stage so nothing is hidden.
  for (const n of architecture?.nodes ?? []) {
    if (!views.some((v) => v.id === n.id)) {
      views.push({ id: n.id, label: n.label, stage: PIPELINE_STAGES.length - 1, row: 3, rowsInStage: 4, arch: n, health: healthById.get(n.id) ?? null, status: healthById.get(n.id)?.status ?? n.status ?? 'unknown' });
    }
  }
  return views;
}

/** Maps a real trace step to the node that handled it. */
export function traceStepNode(step: TraceStep): string | null {
  switch (step.stage) {
    case 'generated':
      return 'generator';
    case 'kafka_ingest':
      return 'kafka';
    case 'engine_received':
    case 'engine_emitted':
      return step.engine ?? 'flink';
    case 'scoring':
      return 'scoring';
    case 'persisted':
      return 'postgres';
    default:
      return null;
  }
}

export const TRACE_STAGE_LABEL: Record<string, string> = {
  generated: 'Generated',
  kafka_ingest: 'Kafka ingest',
  engine_received: 'Engine received',
  scoring: 'Scored',
  engine_emitted: 'Engine emitted',
  persisted: 'Persisted',
};
