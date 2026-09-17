import { Html, Line, OrbitControls } from '@react-three/drei';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { RotateCcw, RotateCw, ZoomIn, ZoomOut, Maximize } from 'lucide-react';
import { useEffect, useMemo, useRef, useState, type ComponentRef, type RefObject } from 'react';
import type { InstancedMesh, OrthographicCamera } from 'three';
import { Object3D, Vector3 } from 'three';
import { Button } from '../components/basics';
import { PIPELINE_STAGES, type PipelineNodeView } from './model';

export interface Pipeline3DProps {
  nodes: PipelineNodeView[];
  edges: Array<{ from: string; to: string; label: string }>;
  selectedId: string;
  onSelect: (id: string) => void;
  traceNodes: Set<string>;
  showParticles: boolean;
  onFailure: (reason: string) => void;
}

interface Palette {
  surface: string;
  border: string;
  text: string;
  primary: string;
  ok: string;
  degraded: string;
  down: string;
  idle: string;
  unknown: string;
  info: string;
}

function readPalette(): Palette {
  const s = getComputedStyle(document.documentElement);
  const v = (name: string, fallback: string) => s.getPropertyValue(name).trim() || fallback;
  return {
    surface: v('--surface', '#ffffff'),
    border: v('--border-strong', '#c9c9cf'),
    text: v('--text', '#0a0a0a'),
    primary: v('--primary', '#0a0a0a'),
    ok: v('--ok', '#15803d'),
    degraded: v('--degraded', '#b45309'),
    down: v('--down', '#b91c1c'),
    idle: v('--idle', '#52525b'),
    unknown: v('--unknown', '#71717a'),
    info: v('--info', '#1d4ed8'),
  };
}

function usePalette(): Palette {
  const [palette, setPalette] = useState(readPalette);
  useEffect(() => {
    const obs = new MutationObserver(() => setPalette(readPalette()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);
  return palette;
}

const STAGE_GAP = 3.4;
const ROW_GAP = 2.6;

function nodePosition(n: PipelineNodeView): [number, number, number] {
  const x = (n.stage - (PIPELINE_STAGES.length - 1) / 2) * STAGE_GAP;
  const z = (n.row - (n.rowsInStage - 1) / 2) * ROW_GAP;
  return [x, 0, z];
}

function statusColor(p: Palette, status: string): string {
  if (status === 'ok') return p.ok;
  if (status === 'degraded') return p.degraded;
  if (status === 'down') return p.down;
  if (status === 'idle') return p.idle;
  return p.unknown;
}

const INITIAL_ZOOM = 38;

type OrbitControlsImpl = ComponentRef<typeof OrbitControls>;

type ControlAction = 'zoom-in' | 'zoom-out' | 'rotate-left' | 'rotate-right' | 'reset';

/** Applies toolbar commands to the camera/controls from inside the R3F tree. */
function ControlsBridge({ command, controls }: { command: { action: ControlAction; seq: number } | null; controls: RefObject<OrbitControlsImpl | null> }) {
  const camera = useThree((s) => s.camera) as OrthographicCamera;
  useEffect(() => {
    if (!command) return;
    const c = controls.current;
    if (command.action === 'reset') {
      c?.reset();
      camera.zoom = INITIAL_ZOOM;
    } else if (command.action === 'zoom-in' || command.action === 'zoom-out') {
      const factor = command.action === 'zoom-in' ? 1.2 : 1 / 1.2;
      camera.zoom = Math.min(120, Math.max(12, camera.zoom * factor));
    } else {
      const target = c?.target ?? new Vector3();
      const angle = (command.action === 'rotate-left' ? 1 : -1) * (Math.PI / 8);
      const offset = camera.position.clone().sub(target);
      offset.applyAxisAngle(new Vector3(0, 1, 0), angle);
      camera.position.copy(target.clone().add(offset));
      camera.lookAt(target);
    }
    camera.updateProjectionMatrix();
    c?.update();
  }, [command, camera, controls]);
  return null;
}

function NodeBox({ node, palette, selected, traced, onSelect }: { node: PipelineNodeView; palette: Palette; selected: boolean; traced: boolean; onSelect: (id: string) => void }) {
  const [hover, setHover] = useState(false);
  const pos = nodePosition(node);
  const edge = selected ? palette.primary : traced ? palette.info : palette.border;
  return (
    <group position={pos}>
      <mesh
        position={[0, 0.35, 0]}
        onClick={(e) => {
          e.stopPropagation();
          onSelect(node.id);
        }}
        onPointerOver={(e) => {
          e.stopPropagation();
          setHover(true);
          document.body.style.cursor = 'pointer';
        }}
        onPointerOut={() => {
          setHover(false);
          document.body.style.cursor = '';
        }}
      >
        <boxGeometry args={[1.9, 0.7, 1.5]} />
        <meshStandardMaterial color={hover || selected ? edge : palette.surface} roughness={0.85} metalness={0} />
      </mesh>
      {/* Status plate on top: colour mirrors the runtime status from GET /health. */}
      <mesh position={[0, 0.72, 0]}>
        <boxGeometry args={[1.5, 0.04, 1.1]} />
        <meshStandardMaterial color={statusColor(palette, node.status)} />
      </mesh>
      <mesh position={[0, 0.001, 0]} rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[2.2, 1.8]} />
        <meshBasicMaterial color={edge} transparent opacity={selected || traced ? 0.35 : 0.12} />
      </mesh>
      <Html position={[0, 1.25, 0]} center zIndexRange={[20, 0]}>
        <span className="fw-3d-label" data-selected={selected || undefined} aria-hidden="true">
          {node.label}
        </span>
      </Html>
    </group>
  );
}

function Particles({ segments, color }: { segments: Array<[Vector3, Vector3]>; color: string }) {
  const ref = useRef<InstancedMesh>(null);
  const perEdge = 3;
  const count = segments.length * perEdge;
  const dummy = useMemo(() => new Object3D(), []);
  useFrame(({ clock }) => {
    const mesh = ref.current;
    if (!mesh) return;
    const t = clock.getElapsedTime();
    let i = 0;
    for (const [a, b] of segments) {
      for (let k = 0; k < perEdge; k++) {
        const phase = (t * 0.35 + k / perEdge) % 1;
        dummy.position.lerpVectors(a, b, phase);
        dummy.updateMatrix();
        mesh.setMatrixAt(i++, dummy.matrix);
      }
    }
    mesh.instanceMatrix.needsUpdate = true;
  });
  if (!count) return null;
  return (
    <instancedMesh ref={ref} args={[undefined, undefined, count]}>
      <sphereGeometry args={[0.07, 10, 10]} />
      <meshBasicMaterial color={color} />
    </instancedMesh>
  );
}

export default function Pipeline3D({ nodes, edges, selectedId, onSelect, traceNodes, showParticles, onFailure }: Pipeline3DProps) {
  const palette = usePalette();
  const controls = useRef<OrbitControlsImpl | null>(null);
  const [command, setCommand] = useState<{ action: ControlAction; seq: number } | null>(null);
  const send = (action: ControlAction) => setCommand((c) => ({ action, seq: (c?.seq ?? 0) + 1 }));

  const byId = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const segments = useMemo(() => {
    const out: Array<{ key: string; a: Vector3; b: Vector3; traced: boolean }> = [];
    for (const e of edges) {
      const from = byId.get(e.from);
      const to = byId.get(e.to);
      if (!from || !to) continue;
      const [ax, , az] = nodePosition(from);
      const [bx, , bz] = nodePosition(to);
      out.push({ key: `${e.from}->${e.to}`, a: new Vector3(ax, 0.35, az), b: new Vector3(bx, 0.35, bz), traced: traceNodes.has(e.from) && traceNodes.has(e.to) });
    }
    return out;
  }, [edges, byId, traceNodes]);

  const selectedLabel = byId.get(selectedId)?.label ?? 'none';

  return (
    <div>
      <div
        className="fw-canvas-wrap"
        role="img"
        aria-label={`3D isometric view of the FraudWatch pipeline with ${nodes.length} components. Selected: ${selectedLabel}. Use the component buttons below to select with the keyboard.`}
        data-testid="pipeline-3d"
      >
        <Canvas
          orthographic
          dpr={[1, 2]}
          camera={{ position: [14, 14, 14], zoom: INITIAL_ZOOM, near: 0.1, far: 200 }}
          gl={{ antialias: true, alpha: true, failIfMajorPerformanceCaveat: false }}
          onCreated={({ gl }) => {
            gl.domElement.addEventListener('webglcontextlost', (ev) => {
              ev.preventDefault();
              onFailure('WebGL context lost');
            });
          }}
          onPointerMissed={() => undefined}
        >
          <ambientLight intensity={0.9} />
          <directionalLight position={[8, 16, 6]} intensity={1.1} />
          {segments.map((s) => (
            <Line key={s.key} points={[s.a, s.b]} color={s.traced ? palette.info : palette.border} lineWidth={s.traced ? 3 : 1.5} />
          ))}
          {nodes.map((n) => (
            <NodeBox key={n.id} node={n} palette={palette} selected={n.id === selectedId} traced={traceNodes.has(n.id)} onSelect={onSelect} />
          ))}
          {showParticles && <Particles segments={segments.map((s) => [s.a, s.b])} color={palette.text} />}
          <OrbitControls ref={controls} makeDefault enablePan={false} minZoom={12} maxZoom={120} maxPolarAngle={Math.PI / 2.2} />
          <ControlsBridge command={command} controls={controls} />
        </Canvas>
        <div className="fw-canvas-controls" role="toolbar" aria-label="3D view controls">
          <Button variant="secondary" size="sm" iconOnly aria-label="Rotate left" title="Rotate left" onClick={() => send('rotate-left')} icon={<RotateCcw size={16} aria-hidden="true" />} />
          <Button variant="secondary" size="sm" iconOnly aria-label="Rotate right" title="Rotate right" onClick={() => send('rotate-right')} icon={<RotateCw size={16} aria-hidden="true" />} />
          <Button variant="secondary" size="sm" iconOnly aria-label="Zoom in" title="Zoom in" onClick={() => send('zoom-in')} icon={<ZoomIn size={16} aria-hidden="true" />} />
          <Button variant="secondary" size="sm" iconOnly aria-label="Zoom out" title="Zoom out" onClick={() => send('zoom-out')} icon={<ZoomOut size={16} aria-hidden="true" />} />
          <Button variant="secondary" size="sm" iconOnly aria-label="Reset view" title="Reset view" onClick={() => send('reset')} icon={<Maximize size={16} aria-hidden="true" />} />
        </div>
        {showParticles && <p className="fw-canvas-caption">Illustration — not one particle per transaction</p>}
      </div>
    </div>
  );
}
