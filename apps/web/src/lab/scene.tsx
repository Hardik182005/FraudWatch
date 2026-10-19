import type { Health } from '@fraudwatch/contracts';
import { SceneStage, type SceneNode } from '@fraudwatch/ui';

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

/** The two-lane Flink / Spark scene (CA2). Topology and ordering only; timing is decorative. */
export function EngineScene({ health, run }: { health: Health | undefined; run: string | null }) {
  return (
    <SceneStage
      variant="lab"
      nodes={buildNodes(health)}
      live={!!run}
      modeLabel={run ? `Live · run ${run}` : 'Illustration'}
      initialSelected="flink"
      caption="The two lanes show the shape of the difference: continuous records against records that wait for a batch boundary. The animation timing is decorative; measured latency and parity are in the tables, per run."
    />
  );
}
