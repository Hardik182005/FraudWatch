import { Canvas } from '@react-three/fiber';
import { useMemo } from 'react';
import { Block, FitZoom, Movers, Path, SceneControls, SceneToolbar, useControlCommands, usePalette, v3, type Mover } from './sceneKit';
import type { Scene3DProps } from './types';

/* Live: Transaction source -> Kafka -> Flink -> LightGBM scoring -> Review queue, with an alert branch. */

const POS: Record<string, [number, number, number]> = {
  source: [-7.2, 0, 0],
  kafka: [-3.6, 0, 0],
  flink: [0, 0, 0],
  scoring: [3.6, 0, 0],
  queue: [7.2, 0, -1.4],
  alerts: [7.2, 0, 2.2],
};

const Y = 0.35;

export default function LiveScene3D({ nodes, selectedId, onSelect, animate, alertActive = true, onFailure, ariaLabel }: Scene3DProps) {
  const palette = usePalette();
  const { controls, command, send } = useControlCommands();
  const byId = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  const mainPath = useMemo(() => [v3(-7.2, Y, 0), v3(3.6, Y, 0), v3(5.4, Y, 0), v3(5.4, Y, -1.4), v3(7.2, Y, -1.4)], []);
  const alertPath = useMemo(() => [v3(-7.2, Y, 0), v3(3.6, Y, 0), v3(5.4, Y, 0), v3(5.4, Y, 2.2), v3(7.2, Y, 2.2)], []);

  const movers = useMemo<Mover[]>(() => {
    const out: Mover[] = [];
    for (let i = 0; i < 9; i++) out.push({ path: mainPath, color: palette.flow, offset: i / 9, period: 7 });
    for (let i = 0; i < 3; i++) out.push({ path: mainPath, color: palette.flow2, offset: (i + 0.5) / 3, period: 7 });
    if (alertActive) for (let i = 0; i < 2; i++) out.push({ path: alertPath, color: palette.alert, offset: (i + 0.25) / 2, period: 9 });
    return out;
  }, [mainPath, alertPath, palette.flow, palette.flow2, palette.alert, alertActive]);

  const block = (id: string, size?: [number, number, number], accent?: string, sub?: string) => {
    const n = byId.get(id);
    if (!n) return null;
    return (
      <Block
        key={id}
        id={id}
        label={n.label}
        sub={sub ?? n.sub}
        position={POS[id] ?? [0, 0, 0]}
        size={size}
        status={n.status}
        accent={accent}
        selected={selectedId === id}
        palette={palette}
        onSelect={onSelect}
      />
    );
  };

  return (
    <div className="fw-canvas-wrap fw-scene-canvas" role="img" aria-label={ariaLabel} data-testid="scene-3d">
      <Canvas
        orthographic
        dpr={[1, 2]}
        frameloop={animate ? 'always' : 'demand'}
        camera={{ position: [10, 11, 13], zoom: 40, near: 0.1, far: 200 }}
        gl={{ antialias: true, alpha: true, failIfMajorPerformanceCaveat: false }}
        onCreated={({ gl }) => {
          gl.domElement.addEventListener('webglcontextlost', (ev) => {
            ev.preventDefault();
            onFailure('WebGL context lost');
          });
        }}
      >
        <ambientLight intensity={0.95} />
        <directionalLight position={[6, 14, 8]} intensity={1.05} />
        <FitZoom sceneWidth={19} command={command} />
        <Path points={[[-7.2, 0.02, 0], [3.6, 0.02, 0], [5.4, 0.02, 0], [5.4, 0.02, -1.4], [7.2, 0.02, -1.4]]} color={palette.flow} width={2.4} />
        <Path points={[[5.4, 0.02, 0], [5.4, 0.02, 2.2], [7.2, 0.02, 2.2]]} color={palette.alert} width={2.4} dashed />
        {block('source', [1.7, 0.7, 1.5])}
        {block('kafka', [1.9, 0.9, 1.5], palette.flow2)}
        {block('flink', [1.9, 1.1, 1.6], palette.flow)}
        {block('scoring', [1.9, 0.9, 1.6], palette.flow2)}
        {block('queue', [1.7, 0.7, 1.3], palette.review)}
        {block('alerts', [1.7, 0.7, 1.3], palette.alert)}
        {animate && <Movers movers={movers} />}
        <SceneControls controls={controls} command={command} />
      </Canvas>
      <SceneToolbar send={send} />
    </div>
  );
}
