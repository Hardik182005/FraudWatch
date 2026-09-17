import { Box, Square } from 'lucide-react';
import { Suspense, lazy, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { Architecture, FlinkMetrics, Health, SparkMetrics, TraceStep } from '@fraudwatch/contracts';
import { Banner, ErrorState, LoadingSkeleton } from '../components/states';
import { ErrorBoundary } from '../components/overlay';
import { ObjectKeyValue, KeyValue } from '../components/basics';
import { HealthDot } from '../components/status';
import { Segmented } from '../components/Tabs';
import { detectWebGL, useMediaQuery, useReducedMotion } from '../hooks';
import { formatMs, formatRelativeTime, formatTimestamp, formatInt, humanize } from '../format';
import { CANONICAL_EDGES, PIPELINE_STAGES, TRACE_STAGE_LABEL, buildNodeViews, traceStepNode, type PipelineNodeView } from './model';

const Pipeline3D = lazy(() => import('./Pipeline3D'));

export interface PipelineDiagramProps {
  architecture: Architecture | null | undefined;
  health: Health | null | undefined;
  loading?: boolean;
  error?: unknown;
  onRetry?: () => void;
  trace?: TraceStep[] | null;
  traceLabel?: ReactNode;
  /** Extra explanation for the selected node (Lab architecture explorer). */
  explain?: (nodeId: string) => ReactNode;
  initialSelected?: string;
}

export type ViewMode = '2d' | '3d';

export function PipelineDiagram({ architecture, health, loading, error, onRetry, trace, traceLabel, explain, initialSelected = 'flink' }: PipelineDiagramProps) {
  const isMobile = useMediaQuery('(max-width: 767px)');
  const reducedMotion = useReducedMotion();
  const [webglOk, setWebglOk] = useState<boolean | null>(null);
  const [runtimeFailure, setRuntimeFailure] = useState<string | null>(null);
  const [requested, setRequested] = useState<ViewMode>('3d');
  const [selected, setSelected] = useState<string>(initialSelected);
  const [particles, setParticles] = useState(true);

  useEffect(() => setWebglOk(detectWebGL()), []);

  const fallbackReason = isMobile
    ? 'Showing the 2D diagram on small screens.'
    : reducedMotion
      ? 'Showing the 2D diagram because reduced motion is requested.'
      : webglOk === false
        ? 'Showing the 2D diagram because WebGL is not available in this browser.'
        : runtimeFailure
          ? `Showing the 2D diagram because the 3D scene failed to start (${runtimeFailure}).`
          : null;
  const can3d = !fallbackReason && webglOk === true;
  const mode: ViewMode = can3d ? requested : '2d';

  const nodes = useMemo(() => buildNodeViews(architecture, health), [architecture, health]);
  const edges = architecture?.edges?.length ? architecture.edges : CANONICAL_EDGES;
  const traceNodes = useMemo(() => new Set((trace ?? []).map(traceStepNode).filter((x): x is string => !!x)), [trace]);
  const selectedNode = nodes.find((n) => n.id === selected) ?? null;

  return (
    <div className="fw-pipeline" data-view={mode}>
      <div className="fw-pipeline-toolbar">
        <Segmented<ViewMode>
          label="Diagram view"
          value={mode}
          onChange={setRequested}
          options={[
            { value: '3d', label: (<><Box size={16} aria-hidden="true" />3D</>), disabled: !can3d, title: fallbackReason ?? '3D view' },
            { value: '2d', label: (<><Square size={16} aria-hidden="true" />2D</>) },
          ]}
        />
        {mode === '3d' && (
          <label className="fw-row fw-small" style={{ gap: 6 }}>
            <input type="checkbox" checked={particles} onChange={(e) => setParticles(e.target.checked)} />
            Flow illustration
          </label>
        )}
      </div>
      {fallbackReason && <p className="fw-small fw-muted" data-testid="pipeline-fallback-reason">{fallbackReason}</p>}
      {!architecture && !loading && !error && <Banner tone="neutral">Architecture metadata not loaded yet.</Banner>}
      {mode === '3d' && !architecture?.edges?.length && !loading && (
        <p className="fw-small fw-muted">Connections drawn from the documented pipeline layout; the API did not return edge data.</p>
      )}
      {error ? <ErrorState error={error} title="Architecture or health data unavailable" onRetry={onRetry} /> : null}

      <div className="fw-pipeline-layout">
        <div className="fw-card" style={{ minWidth: 0 }}>
          {loading && !architecture ? (
            <LoadingSkeleton lines={4} label="Loading architecture" />
          ) : mode === '3d' ? (
            <ErrorBoundary
              onError={(e) => setRuntimeFailure(e.message || 'WebGL error')}
              fallback={() => <LoadingSkeleton lines={2} label="Switching to 2D" />}
            >
              <Suspense fallback={<LoadingSkeleton lines={4} label="Loading 3D scene" />}>
                <Pipeline3D nodes={nodes} edges={edges} selectedId={selected} onSelect={setSelected} traceNodes={traceNodes} showParticles={particles} onFailure={setRuntimeFailure} />
              </Suspense>
              <NodePicker nodes={nodes} selected={selected} onSelect={setSelected} />
            </ErrorBoundary>
          ) : (
            <Flow2D nodes={nodes} selected={selected} onSelect={setSelected} traceNodes={traceNodes} />
          )}
        </div>
        <NodeDetails node={selectedNode} explain={explain} trace={trace} traceLabel={traceLabel} traceNodes={traceNodes} />
      </div>
    </div>
  );
}

function NodePicker({ nodes, selected, onSelect }: { nodes: PipelineNodeView[]; selected: string; onSelect: (id: string) => void }) {
  return (
    <div style={{ padding: 12, borderTop: '1px solid var(--border)' }}>
      <div className="fw-small fw-muted" id="fw-node-picker-label" style={{ marginBottom: 6 }}>
        Select a component (keyboard alternative to clicking the scene)
      </div>
      <div className="fw-row" role="group" aria-labelledby="fw-node-picker-label">
        {nodes.map((n) => (
          <button key={n.id} type="button" className="fw-btn fw-btn-secondary fw-btn-sm" aria-pressed={selected === n.id} onClick={() => onSelect(n.id)}>
            {n.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function Flow2D({ nodes, selected, onSelect, traceNodes }: { nodes: PipelineNodeView[]; selected: string; onSelect: (id: string) => void; traceNodes: Set<string> }) {
  return (
    <ol className="fw-flow" aria-label="FraudWatch pipeline, in processing order" data-testid="pipeline-2d">
      {PIPELINE_STAGES.map((stage, s) => (
        <li key={stage.id} className="fw-flow-stage">
          <span className="fw-flow-stage-title">
            {s + 1}. {stage.title}
          </span>
          <div className="fw-flow-stage-nodes">
            {nodes
              .filter((n) => n.stage === s)
              .map((n) => (
                <button key={n.id} type="button" className="fw-node" aria-pressed={selected === n.id} data-trace={traceNodes.has(n.id) || undefined} onClick={() => onSelect(n.id)}>
                  <span className="fw-node-label">{n.label}</span>
                  <HealthDot status={n.status} />
                  {traceNodes.has(n.id) && <span className="fw-small" style={{ color: 'var(--info)' }}>On event path</span>}
                </button>
              ))}
          </div>
        </li>
      ))}
    </ol>
  );
}

function NodeDetails({ node, explain, trace, traceLabel, traceNodes }: { node: PipelineNodeView | null; explain?: (id: string) => ReactNode; trace?: TraceStep[] | null; traceLabel?: ReactNode; traceNodes: Set<string> }) {
  const metrics = node?.health?.metrics ?? null;
  return (
    <aside className="fw-card" aria-live="polite" aria-label="Selected component details">
      <div className="fw-card-head">
        <div>
          <div className="fw-eyebrow">Selected component</div>
          <h3 style={{ marginTop: 4 }}>{node?.arch?.label ?? node?.label ?? 'None'}</h3>
        </div>
        {node && <HealthDot status={node.status} />}
      </div>
      <div className="fw-card-body fw-stack" style={{ gap: 14 }}>
        {!node ? (
          <p className="fw-muted">Select a component to see its role, configuration and runtime status.</p>
        ) : (
          <>
            <div>
              <div className="fw-label">Role</div>
              <p>{node.arch?.role ?? <span className="fw-muted">Not reported by GET /architecture.</span>}</p>
            </div>
            {explain && <div>{explain(node.id)}</div>}
            <div>
              <div className="fw-label">Runtime status</div>
              {node.health ? (
                <KeyValue
                  items={[
                    { key: 'status', label: 'Status', value: <HealthDot status={node.health.status} /> },
                    { key: 'detail', label: 'Detail', value: node.health.detail ?? '—' },
                    { key: 'checked', label: 'Checked', value: formatRelativeTime(node.health.checked_at) },
                  ]}
                />
              ) : (
                <p className="fw-muted fw-small">{node.id === 'live' || node.id === 'lab' ? 'Websites are not probed by GET /health; status comes from GET /architecture.' : 'No health component reported for this node.'}</p>
              )}
            </div>
            {node.id === 'flink' && metrics && <FlinkMetricsView m={metrics as FlinkMetrics} />}
            {node.id === 'spark' && metrics && <SparkMetricsView m={metrics as SparkMetrics} />}
            {metrics && node.id !== 'flink' && node.id !== 'spark' && Object.keys(metrics).length > 0 && (
              <div>
                <div className="fw-label">Metrics</div>
                <ObjectKeyValue data={metrics} />
              </div>
            )}
            <div>
              <div className="fw-label">Configuration</div>
              <ObjectKeyValue data={node.arch?.config} empty="No configuration reported." />
            </div>
          </>
        )}
        {trace && trace.length > 0 && (
          <div>
            <div className="fw-label">{traceLabel ?? 'Event journey (real trace)'}</div>
            <ol className="fw-timeline" style={{ marginTop: 8 }}>
              {trace.map((st, i) => (
                <li key={`${st.stage}-${st.engine}-${i}`}>
                  <strong>{TRACE_STAGE_LABEL[st.stage] ?? humanize(st.stage)}</strong>
                  {st.engine && <span className="fw-muted"> · {st.engine}</span>}
                  <div className="fw-small fw-muted fw-num">
                    {formatTimestamp(st.at_ms)} {st.delta_ms !== null && st.delta_ms !== undefined && <>(+{formatMs(st.delta_ms)})</>}
                  </div>
                </li>
              ))}
            </ol>
            <p className="fw-small fw-muted">Highlighted nodes: {Array.from(traceNodes).join(', ') || 'none'}.</p>
          </div>
        )}
      </div>
    </aside>
  );
}

function FlinkMetricsView({ m }: { m: FlinkMetrics }) {
  return (
    <div>
      <div className="fw-label">Flink job</div>
      <KeyValue
        items={[
          { key: 'job_id', label: 'Job ID', value: <code>{m.job_id ?? '—'}</code> },
          { key: 'job_state', label: 'Job state', value: m.job_state ?? '—' },
          { key: 'uptime', label: 'Uptime', value: formatMs(m.uptime_ms) },
          { key: 'restarts', label: 'Restarts', value: formatInt(m.restarts) },
          { key: 'cp', label: 'Last checkpoint', value: m.last_checkpoint ? `#${m.last_checkpoint.id}, ${formatRelativeTime(m.last_checkpoint.completed_at)} (${formatMs(m.last_checkpoint.duration_ms)})` : 'None reported' },
        ]}
      />
    </div>
  );
}

function SparkMetricsView({ m }: { m: SparkMetrics }) {
  return (
    <div>
      <div className="fw-label">Spark application</div>
      <KeyValue
        items={[
          { key: 'state', label: 'State', value: m.state ?? '—' },
          { key: 'app', label: 'App ID', value: <code>{m.app_id ?? '—'}</code> },
          { key: 'label', label: 'Run label', value: m.run_label ?? '—' },
          { key: 'batch', label: 'Last batch', value: m.last_batch_id !== null && m.last_batch_id !== undefined ? `#${m.last_batch_id}, ${formatRelativeTime(m.last_batch_at)}` : 'None' },
        ]}
      />
    </div>
  );
}
