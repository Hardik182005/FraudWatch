import type { ReactNode } from 'react';

/** A node in a landing-page scene. Status must come from a real health probe, or be null (unknown). */
export interface SceneNode {
  id: string;
  label: string;
  /** Short secondary label rendered under the name (e.g. "per event"). */
  sub?: string;
  /** Plain-language role, shown in the detail panel. */
  role: string;
  /** ok | degraded | down | idle | unknown, or null when not checked. */
  status: string | null;
  /** Freeform status line from the probe (never invented). */
  statusDetail?: string | null;
  /** Lane for the lab scene 2D fallback grouping. */
  lane?: 'main' | 'flink' | 'spark' | 'alert';
  /** Extra detail content (key/values etc.). */
  extra?: ReactNode;
}

export interface Scene3DProps {
  nodes: SceneNode[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  /** When false the render loop runs on demand only (offscreen, reduced motion, paused). */
  animate: boolean;
  /** Show the alert branch flow (Live scene). */
  alertActive?: boolean;
  onFailure: (reason: string) => void;
  ariaLabel: string;
}
