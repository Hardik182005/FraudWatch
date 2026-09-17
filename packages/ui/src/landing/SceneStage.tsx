import { Box, Pause, Play, Square } from 'lucide-react';
import { Suspense, lazy, useEffect, useRef, useState, type ReactNode } from 'react';
import { ErrorBoundary } from '../components/overlay';
import { LoadingSkeleton } from '../components/states';
import { HealthDot } from '../components/status';
import { Segmented } from '../components/Tabs';
import { detectWebGL, useMediaQuery, useReducedMotion } from '../hooks';
import type { SceneNode } from './types';

const LiveScene3D = lazy(() => import('./LiveScene3D'));
const LabScene3D = lazy(() => import('./LabScene3D'));

export interface SceneStageProps {
  variant: 'live' | 'lab';
  nodes: SceneNode[];
  /** "Illustration" when nothing is running, or "Live · run <id>" when a real run is attached. */
  modeLabel: ReactNode;
  live: boolean;
  initialSelected?: string;
  alertActive?: boolean;
  /** Short disclaimer under the scene (e.g. decorative timing note). */
  caption?: ReactNode;
}

/**
 * Landing scene container: lazy 3D with a 2D toggle, pauses when offscreen or under reduced motion,
 * falls back to 2D when WebGL is unavailable or fails, and exposes an accessible node list + detail panel.
 */
export function SceneStage({ variant, nodes, modeLabel, live, initialSelected, alertActive, caption }: SceneStageProps) {
  const small = useMediaQuery('(max-width: 640px)');
  const reducedMotion = useReducedMotion();
  const [webgl, setWebgl] = useState<boolean | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const [requested, setRequested] = useState<'3d' | '2d' | null>(null);
  const [selected, setSelected] = useState<string | null>(initialSelected ?? nodes[0]?.id ?? null);
  const [visible, setVisible] = useState(true);
  const [userPaused, setUserPaused] = useState(false);
  const wrap = useRef<HTMLDivElement>(null);

  useEffect(() => setWebgl(detectWebGL()), []);
  useEffect(() => {
    const el = wrap.current;
    if (!el || typeof IntersectionObserver === 'undefined') return;
    const io = new IntersectionObserver((entries) => setVisible(entries.some((e) => e.isIntersecting)), { threshold: 0.05 });
    io.observe(el);
    return () => io.disconnect();
  }, []);
  useEffect(() => {
    const on = () => setVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', on);
    return () => document.removeEventListener('visibilitychange', on);
  }, []);

  const can3d = webgl === true && !failure;
  const fallbackReason = webgl === false ? 'WebGL is not available in this browser, so the 2D view is shown.' : failure ? `The 3D view stopped (${failure}), so the 2D view is shown.` : null;
  const mode: '3d' | '2d' = !can3d ? '2d' : (requested ?? (small ? '2d' : '3d'));
  const animate = visible && !reducedMotion && !userPaused;
  const node = nodes.find((n) => n.id === selected) ?? null;
  const Scene = variant === 'live' ? LiveScene3D : LabScene3D;
  const ariaLabel =
    variant === 'live'
      ? `3D diagram of the FraudWatch Live pipeline: ${nodes.map((n) => n.label).join(', ')}. Use the component list to select with the keyboard.`
      : `3D diagram of one Kafka stream split into a Flink per-event lane and a Spark micro-batch lane that join at a measurement area. Use the component list to select with the keyboard.`;

  return (
    <div className="fw-scene" ref={wrap} data-variant={variant}>
      <div className="fw-scene-bar">
        <span className="fw-scene-mode" data-live={live || undefined} data-testid="scene-mode">
          <span className="fw-scene-mode-dot" aria-hidden="true" />
          {modeLabel}
        </span>
        <div className="fw-row" style={{ gap: 6 }}>
          {mode === '3d' && !reducedMotion && (
            <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" aria-pressed={userPaused} onClick={() => setUserPaused((p) => !p)}>
              {userPaused ? <Play size={14} aria-hidden="true" /> : <Pause size={14} aria-hidden="true" />}
              {userPaused ? 'Play motion' : 'Pause motion'}
            </button>
          )}
          <Segmented<'3d' | '2d'>
            label="Scene view"
            value={mode}
            onChange={setRequested}
            options={[
              { value: '3d', label: (<><Box size={14} aria-hidden="true" />3D</>), disabled: !can3d, title: fallbackReason ?? '3D view' },
              { value: '2d', label: (<><Square size={14} aria-hidden="true" />2D</>) },
            ]}
          />
        </div>
      </div>
      {fallbackReason && <p className="fw-small fw-muted" data-testid="scene-fallback">{fallbackReason}</p>}
      {mode === '3d' ? (
        <ErrorBoundary onError={(e) => setFailure(e.message || 'WebGL error')} fallback={() => <LoadingSkeleton lines={3} label="Switching to 2D" />}>
          <Suspense fallback={<div className="fw-canvas-wrap fw-scene-canvas fw-scene-loading"><LoadingSkeleton lines={3} label="Loading 3D scene" /></div>}>
            <Scene nodes={nodes} selectedId={selected} onSelect={setSelected} animate={animate} alertActive={alertActive} onFailure={setFailure} ariaLabel={ariaLabel} />
          </Suspense>
        </ErrorBoundary>
      ) : (
        <Scene2D variant={variant} nodes={nodes} selected={selected} onSelect={setSelected} />
      )}
      {caption && <p className="fw-small fw-muted fw-scene-caption">{caption}</p>}
      <div className="fw-scene-detail">
        <div role="group" aria-label="Scene components" className="fw-scene-nodes">
          {nodes.map((n) => (
            <button key={n.id} type="button" className="fw-scene-node" aria-pressed={selected === n.id} onClick={() => setSelected(n.id)}>
              <HealthDot status={n.status ?? 'unknown'} />
              <span>{n.label}</span>
            </button>
          ))}
        </div>
        <div className="fw-scene-panel" aria-live="polite" data-testid="scene-detail">
          {node ? (
            <>
              <div className="fw-row" style={{ justifyContent: 'space-between' }}>
                <strong>{node.label}</strong>
                <HealthDot status={node.status ?? 'unknown'} label={statusLabel(node.status)} />
              </div>
              <p className="fw-small">{node.role}</p>
              <p className="fw-small fw-muted">{node.statusDetail ?? (node.status ? null : 'Status not checked yet — the API health probe has not answered.')}</p>
              {node.extra}
            </>
          ) : (
            <p className="fw-small fw-muted">Select a component to see its role and current status.</p>
          )}
        </div>
      </div>
    </div>
  );
}

function statusLabel(status: string | null): string {
  if (!status) return 'Not checked';
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function Scene2D({ variant, nodes, selected, onSelect }: { variant: 'live' | 'lab'; nodes: SceneNode[]; selected: string | null; onSelect: (id: string) => void }) {
  const btn = (n: SceneNode) => (
    <button key={n.id} type="button" className="fw-scene2d-node" aria-pressed={selected === n.id} data-lane={n.lane} onClick={() => onSelect(n.id)}>
      <span className="fw-scene2d-label">{n.label}</span>
      {n.sub && <span className="fw-small fw-muted">{n.sub}</span>}
      <HealthDot status={n.status ?? 'unknown'} />
    </button>
  );
  if (variant === 'lab') {
    const main = nodes.filter((n) => !n.lane || n.lane === 'main');
    const head = main[0];
    const tail = main.slice(1);
    return (
      <div className="fw-scene2d fw-scene2d-lab" data-testid="scene-2d">
        {head && btn(head)}
        <div className="fw-scene2d-lanes">
          <div className="fw-scene2d-lane" data-lane="flink">{nodes.filter((n) => n.lane === 'flink').map(btn)}</div>
          <div className="fw-scene2d-lane" data-lane="spark">{nodes.filter((n) => n.lane === 'spark').map(btn)}</div>
        </div>
        {tail.map(btn)}
      </div>
    );
  }
  const main = nodes.filter((n) => n.lane !== 'alert');
  const alerts = nodes.filter((n) => n.lane === 'alert');
  return (
    <div className="fw-scene2d" data-testid="scene-2d">
      <ol className="fw-scene2d-flow" aria-label="Processing order">
        {main.map((n) => (
          <li key={n.id}>{btn(n)}</li>
        ))}
      </ol>
      {alerts.length > 0 && (
        <div className="fw-scene2d-branch">
          <span className="fw-small fw-muted">Alert branch</span>
          {alerts.map(btn)}
        </div>
      )}
    </div>
  );
}
