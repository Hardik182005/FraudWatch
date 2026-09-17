import { Html, Line, OrbitControls } from '@react-three/drei';
import { useFrame, useThree } from '@react-three/fiber';
import { Maximize, RotateCcw, RotateCw, ZoomIn, ZoomOut } from 'lucide-react';
import { useEffect, useMemo, useRef, useState, type ComponentRef, type RefObject } from 'react';
import { Color, Object3D, Vector3, type InstancedMesh, type OrthographicCamera } from 'three';
import { Button } from '../components/basics';

/* Shared building blocks for the two landing scenes. Only imported by lazily loaded modules. */

export interface ScenePalette {
  surface: string;
  surface2: string;
  border: string;
  text: string;
  muted: string;
  primary: string;
  flow: string;
  flow2: string;
  alert: string;
  review: string;
  ok: string;
  degraded: string;
  down: string;
  idle: string;
  unknown: string;
}

function readPalette(): ScenePalette {
  const s = getComputedStyle(document.documentElement);
  const v = (name: string, fallback: string) => s.getPropertyValue(name).trim() || fallback;
  return {
    surface: v('--surface', '#ffffff'),
    surface2: v('--surface-2', '#f4f4f5'),
    border: v('--border-strong', '#c9c9cf'),
    text: v('--text', '#0a0a0a'),
    muted: v('--text-muted', '#52525b'),
    primary: v('--primary', '#0a0a0a'),
    flow: v('--chart-1', '#0f766e'),
    flow2: v('--chart-4', '#1d4ed8'),
    alert: v('--alert', '#b91c1c'),
    review: v('--review', '#92400e'),
    ok: v('--ok', '#15803d'),
    degraded: v('--degraded', '#b45309'),
    down: v('--down', '#b91c1c'),
    idle: v('--idle', '#52525b'),
    unknown: v('--unknown', '#71717a'),
  };
}

export function usePalette(): ScenePalette {
  const [palette, setPalette] = useState(readPalette);
  useEffect(() => {
    const obs = new MutationObserver(() => setPalette(readPalette()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    const mq = window.matchMedia?.('(prefers-color-scheme: dark)');
    const on = () => setPalette(readPalette());
    mq?.addEventListener('change', on);
    return () => {
      obs.disconnect();
      mq?.removeEventListener('change', on);
    };
  }, []);
  return palette;
}

export function statusColor(p: ScenePalette, status: string | null | undefined): string {
  if (status === 'ok') return p.ok;
  if (status === 'degraded') return p.degraded;
  if (status === 'down') return p.down;
  if (status === 'idle') return p.idle;
  return p.unknown;
}

type OrbitControlsImpl = ComponentRef<typeof OrbitControls>;
export type ControlAction = 'zoom-in' | 'zoom-out' | 'rotate-left' | 'rotate-right' | 'reset';
export type ControlCommand = { action: ControlAction; seq: number } | null;

/** Keeps the whole scene in view: zoom follows the canvas width (and resets to it). */
export function FitZoom({ sceneWidth, command }: { sceneWidth: number; command: ControlCommand }) {
  const size = useThree((s) => s.size);
  const camera = useThree((s) => s.camera) as OrthographicCamera;
  const invalidate = useThree((s) => s.invalidate);
  useEffect(() => {
    const fit = Math.max(14, Math.min(90, Math.min(size.width / sceneWidth, size.height / (sceneWidth * 0.62))));
    camera.zoom = fit;
    camera.updateProjectionMatrix();
    invalidate();
  }, [size.width, size.height, sceneWidth, camera, invalidate, command?.action === 'reset' ? command.seq : 0]);
  return null;
}

export function ControlsBridge({ command, controls }: { command: ControlCommand; controls: RefObject<OrbitControlsImpl | null> }) {
  const camera = useThree((s) => s.camera) as OrthographicCamera;
  const invalidate = useThree((s) => s.invalidate);
  useEffect(() => {
    if (!command) return;
    const c = controls.current;
    if (command.action === 'reset') {
      c?.reset();
    } else if (command.action === 'zoom-in' || command.action === 'zoom-out') {
      const factor = command.action === 'zoom-in' ? 1.2 : 1 / 1.2;
      camera.zoom = Math.min(160, Math.max(10, camera.zoom * factor));
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
    invalidate();
  }, [command, camera, controls, invalidate]);
  return null;
}

export function SceneControls({ controls, command }: { controls: RefObject<OrbitControlsImpl | null>; command: ControlCommand }) {
  return (
    <>
      <OrbitControls ref={controls} makeDefault enablePan={false} minZoom={10} maxZoom={160} maxPolarAngle={Math.PI / 2.15} />
      <ControlsBridge command={command} controls={controls} />
    </>
  );
}

export function useControlCommands() {
  const controls = useRef<OrbitControlsImpl | null>(null);
  const [command, setCommand] = useState<ControlCommand>(null);
  const send = (action: ControlAction) => setCommand((c) => ({ action, seq: (c?.seq ?? 0) + 1 }));
  return { controls, command, send };
}

export function SceneToolbar({ send }: { send: (a: ControlAction) => void }) {
  return (
    <div className="fw-canvas-controls" role="toolbar" aria-label="3D view controls">
      <Button variant="secondary" size="sm" iconOnly aria-label="Rotate left" title="Rotate left" onClick={() => send('rotate-left')} icon={<RotateCcw size={16} aria-hidden="true" />} />
      <Button variant="secondary" size="sm" iconOnly aria-label="Rotate right" title="Rotate right" onClick={() => send('rotate-right')} icon={<RotateCw size={16} aria-hidden="true" />} />
      <Button variant="secondary" size="sm" iconOnly aria-label="Zoom in" title="Zoom in" onClick={() => send('zoom-in')} icon={<ZoomIn size={16} aria-hidden="true" />} />
      <Button variant="secondary" size="sm" iconOnly aria-label="Zoom out" title="Zoom out" onClick={() => send('zoom-out')} icon={<ZoomOut size={16} aria-hidden="true" />} />
      <Button variant="secondary" size="sm" iconOnly aria-label="Reset view" title="Reset view" onClick={() => send('reset')} icon={<Maximize size={16} aria-hidden="true" />} />
    </div>
  );
}

export interface BlockProps {
  id: string;
  label: string;
  sub?: string;
  position: [number, number, number];
  size?: [number, number, number];
  status?: string | null;
  accent?: string;
  selected: boolean;
  palette: ScenePalette;
  onSelect: (id: string) => void;
  badge?: string | null;
}

/** An architectural block: body, status plate (real status colour) and a legible HTML label. */
export function Block({ id, label, sub, position, size = [1.7, 0.8, 1.4], status, accent, selected, palette, onSelect, badge }: BlockProps) {
  const [hover, setHover] = useState(false);
  const [w, h, d] = size;
  const edge = selected ? palette.primary : (accent ?? palette.border);
  return (
    <group position={position}>
      <mesh
        position={[0, h / 2, 0]}
        onClick={(e) => {
          e.stopPropagation();
          onSelect(id);
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
        <boxGeometry args={[w, h, d]} />
        <meshStandardMaterial color={hover || selected ? palette.surface2 : palette.surface} roughness={0.9} metalness={0} />
      </mesh>
      <BoxEdges size={size} color={edge} />
      <mesh position={[0, h + 0.02, 0]}>
        <boxGeometry args={[w * 0.72, 0.04, d * 0.62]} />
        <meshStandardMaterial color={status ? statusColor(palette, status) : (accent ?? palette.muted)} />
      </mesh>
      <mesh position={[0, 0.002, 0]} rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[w + 0.5, d + 0.5]} />
        <meshBasicMaterial color={edge} transparent opacity={selected ? 0.22 : 0.07} />
      </mesh>
      <Html position={[0, h + 0.55, 0]} center zIndexRange={[20, 0]}>
        <span className="fw-scene-label" data-selected={selected || undefined} aria-hidden="true">
          <strong>{label}</strong>
          {sub && <em>{sub}</em>}
          {badge && <b>{badge}</b>}
        </span>
      </Html>
    </group>
  );
}

function BoxEdges({ size, color }: { size: [number, number, number]; color: string }) {
  const [w, h, d] = size;
  const pts = useMemo(() => {
    const x = w / 2;
    const z = d / 2;
    const c = (px: number, py: number, pz: number) => new Vector3(px, py, pz);
    return [
      [c(-x, h, -z), c(x, h, -z), c(x, h, z), c(-x, h, z), c(-x, h, -z)],
      [c(-x, 0, -z), c(x, 0, -z), c(x, 0, z), c(-x, 0, z), c(-x, 0, -z)],
    ];
  }, [w, h, d]);
  return (
    <>
      {pts.map((p, i) => (
        <Line key={i} points={p} color={color} lineWidth={1.2} />
      ))}
    </>
  );
}

/** A flat pathway between points, drawn as a line on the floor. */
export function Path({ points, color, width = 2, dashed }: { points: Array<[number, number, number]>; color: string; width?: number; dashed?: boolean }) {
  const pts = useMemo(() => points.map((p) => new Vector3(p[0], p[1], p[2])), [points]);
  return <Line points={pts} color={color} lineWidth={width} dashed={dashed} dashSize={0.25} gapSize={0.18} />;
}

export interface Mover {
  /** Polyline the particle follows. */
  path: Vector3[];
  color: string;
  /** Phase offset 0..1. */
  offset: number;
  /** Seconds per full traversal. */
  period: number;
  /** Optional gating: returns progress 0..1 given raw progress and time (used for batch release). */
  gate?: (raw: number, t: number) => number;
}

function pointOn(path: Vector3[], lengths: number[], total: number, f: number, out: Vector3) {
  let dist = f * total;
  for (let i = 0; i < lengths.length; i++) {
    const seg = lengths[i]!;
    if (dist <= seg || i === lengths.length - 1) {
      out.lerpVectors(path[i]!, path[i + 1]!, seg === 0 ? 0 : Math.min(1, dist / seg));
      return;
    }
    dist -= seg;
  }
}

/** Instanced particles. Rendering only advances while the canvas frameloop runs (paused offscreen / reduced motion). */
export function Movers({ movers, radius = 0.09 }: { movers: Mover[]; radius?: number }) {
  const ref = useRef<InstancedMesh>(null);
  const dummy = useMemo(() => new Object3D(), []);
  const tmp = useMemo(() => new Vector3(), []);
  const geo = useMemo(
    () =>
      movers.map((m) => {
        const lengths = m.path.slice(1).map((p, i) => p.distanceTo(m.path[i]!));
        return { lengths, total: lengths.reduce((a, b) => a + b, 0) };
      }),
    [movers],
  );
  useEffect(() => {
    const mesh = ref.current;
    if (!mesh) return;
    const c = new Color();
    movers.forEach((m, i) => mesh.setColorAt(i, c.set(m.color)));
    if (mesh.instanceColor) mesh.instanceColor.needsUpdate = true;
  }, [movers]);
  useFrame(({ clock }) => {
    const mesh = ref.current;
    if (!mesh) return;
    const t = clock.getElapsedTime();
    movers.forEach((m, i) => {
      const raw = (t / m.period + m.offset) % 1;
      const f = m.gate ? m.gate(raw, t) : raw;
      const g = geo[i]!;
      pointOn(m.path, g.lengths, g.total, f, tmp);
      dummy.position.copy(tmp);
      dummy.updateMatrix();
      mesh.setMatrixAt(i, dummy.matrix);
    });
    mesh.instanceMatrix.needsUpdate = true;
  });
  if (!movers.length) return null;
  return (
    <instancedMesh ref={ref} args={[undefined, undefined, movers.length]}>
      <sphereGeometry args={[radius, 12, 12]} />
      <meshBasicMaterial />
    </instancedMesh>
  );
}

export const v3 = (x: number, y: number, z: number) => new Vector3(x, y, z);
