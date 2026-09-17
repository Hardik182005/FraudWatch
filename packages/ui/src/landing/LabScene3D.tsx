import { Canvas } from '@react-three/fiber';
import { useMemo } from 'react';
import { Block, FitZoom, Movers, Path, SceneControls, SceneToolbar, useControlCommands, usePalette, v3, type Mover } from './sceneKit';
import type { Scene3DProps } from './types';

/*
 * Lab: one Kafka topic splits into a Flink per-event lane and a Spark micro-batch lane that join at a
 * measurement area. Motion timing is decorative and is labelled as such by the stage; no numbers are drawn.
 */

const POS: Record<string, [number, number, number]> = {
  kafka: [-7, 0, 0],
  flink: [0, 0, -2.4],
  spark: [0, 0, 2.4],
  measure: [7, 0, 0],
};
const Y = 0.35;

export default function LabScene3D({ nodes, selectedId, onSelect, animate, onFailure, ariaLabel }: Scene3DProps) {
  const palette = usePalette();
  const { controls, command, send } = useControlCommands();
  const byId = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  const flinkPath = useMemo(() => [v3(-7, Y, 0), v3(-4.6, Y, 0), v3(-3.4, Y, -2.4), v3(3.4, Y, -2.4), v3(4.6, Y, 0), v3(7, Y, 0)], []);
  const sparkPath = useMemo(() => [v3(-7, Y, 0), v3(-4.6, Y, 0), v3(-3.4, Y, 2.4), v3(3.4, Y, 2.4), v3(4.6, Y, 0), v3(7, Y, 0)], []);

  const movers = useMemo<Mover[]>(() => {
    const out: Mover[] = [];
    // Flink: continuous, evenly spaced (one record at a time).
    for (let i = 0; i < 10; i++) out.push({ path: flinkPath, color: palette.flow, offset: i / 10, period: 8 });
    // Spark: records collect before the batch block, then leave together.
    const period = 6;
    for (let i = 0; i < 8; i++) {
      const o = (i / 8) * 0.22;
      out.push({
        path: sparkPath,
        color: palette.flow2,
        offset: 0,
        period,
        gate: (_raw, t) => {
          const c = (t / period) % 1;
          const hold = 0.47 - o;
          if (c < 0.6) return Math.max(0, Math.min(hold, (c / 0.6) * 0.5 - o * 0.5));
          return hold + ((c - 0.6) / 0.4) * (1 - hold);
        },
      });
    }
    return out;
  }, [flinkPath, sparkPath, palette.flow, palette.flow2]);

  const block = (id: string, size: [number, number, number], accent: string) => {
    const n = byId.get(id);
    if (!n) return null;
    return (
      <Block key={id} id={id} label={n.label} sub={n.sub} position={POS[id] ?? [0, 0, 0]} size={size} status={n.status} accent={accent} selected={selectedId === id} palette={palette} onSelect={onSelect} />
    );
  };

  return (
    <div className="fw-canvas-wrap fw-scene-canvas" role="img" aria-label={ariaLabel} data-testid="scene-3d">
      <Canvas
        orthographic
        dpr={[1, 2]}
        frameloop={animate ? 'always' : 'demand'}
        camera={{ position: [9, 13, 13], zoom: 40, near: 0.1, far: 200 }}
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
        <FitZoom sceneWidth={18} command={command} />
        <Path points={flinkPath.map((p) => [p.x, 0.02, p.z] as [number, number, number])} color={palette.flow} width={2.4} />
        <Path points={sparkPath.map((p) => [p.x, 0.02, p.z] as [number, number, number])} color={palette.flow2} width={2.4} />
        {/* Lane floor plates */}
        <mesh position={[0, 0.001, -2.4]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[7.4, 1.5]} />
          <meshBasicMaterial color={palette.flow} transparent opacity={0.06} />
        </mesh>
        <mesh position={[0, 0.001, 2.4]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[7.4, 1.5]} />
          <meshBasicMaterial color={palette.flow2} transparent opacity={0.06} />
        </mesh>
        {block('kafka', [1.9, 1, 1.8], palette.muted)}
        {block('flink', [1.9, 1, 1.3], palette.flow)}
        {block('spark', [2.4, 1, 1.3], palette.flow2)}
        {block('measure', [2, 0.8, 2.2], palette.text)}
        {animate && <Movers movers={movers} />}
        <SceneControls controls={controls} command={command} />
      </Canvas>
      <SceneToolbar send={send} />
    </div>
  );
}
