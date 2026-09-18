import { useMemo, useRef, useState, type Ref } from 'react';
import { Download, ExternalLink } from 'lucide-react';
import { SourceBadge } from './VizFrame';

/* Exportable architecture figures (prompt section 11).
   Hand-authored inline SVG: the interactive PipelineDiagram is HTML, not SVG, so it cannot be
   serialised for export. Colours use CSS custom properties with literal fallbacks so the figure
   follows the app theme in place and still renders standalone once exported. */

export type ArchitectureFigureId = 'end-to-end' | 'websites' | 'model-paths' | 'engine-paths' | 'cloud-lifecycle';

type NodeKind = 'ui' | 'service' | 'store' | 'engine' | 'external' | 'artifact';
type EdgeKind = 'data' | 'control' | 'persist';

interface FigNode {
  id: string;
  x: number;
  y: number;
  w?: number;
  h?: number;
  label: string;
  sub?: string;
  kind: NodeKind;
}

interface FigEdge {
  from: string;
  to: string;
  label?: string;
  kind: EdgeKind;
  /** 'h' leaves the left/right face, 'v' leaves the top/bottom face. Default picks by geometry. */
  route?: 'h' | 'v';
  dx?: number;
  dy?: number;
}

interface FigZone {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
}

interface Figure {
  id: ArchitectureFigureId;
  title: string;
  caption: string;
  width: number;
  height: number;
  zones?: FigZone[];
  nodes: FigNode[];
  edges: FigEdge[];
}

const NW = 184;
const NH = 62;

const FIGURES: Figure[] = [
  {
    id: 'end-to-end',
    title: 'End-to-end processing architecture',
    caption:
      'Path of one replayed IEEE-CIS transaction from the replay pack to a decision on screen. Every box is a service that exists in the compose stack; nothing here is aspirational.',
    width: 1000,
    height: 520,
    nodes: [
      { id: 'pack', x: 16, y: 40, label: 'IEEE-CIS replay pack', sub: 'chronological rows on disk', kind: 'artifact' },
      { id: 'gen', x: 16, y: 170, label: 'Generator service', sub: 'rate-capped replay', kind: 'service' },
      { id: 'txn', x: 250, y: 170, label: 'Kafka topic', sub: 'fraudwatch.transactions', kind: 'store' },
      { id: 'flink', x: 484, y: 84, label: 'Flink job', sub: 'event at a time, watermarks', kind: 'engine' },
      { id: 'spark', x: 484, y: 256, label: 'Spark Structured Streaming', sub: 'micro-batch trigger', kind: 'engine' },
      { id: 'score', x: 484, y: 400, label: 'Scoring service', sub: 'LightGBM, TreeSHAP, rules', kind: 'service' },
      { id: 'res', x: 760, y: 170, label: 'Kafka results topics', sub: 'results.flink, results.spark', kind: 'store' },
      { id: 'cons', x: 760, y: 290, label: 'Result consumer', sub: 'idempotent upsert', kind: 'service' },
      { id: 'pg', x: 760, y: 410, label: 'Postgres', sub: 'events, decisions, runs, logs', kind: 'store' },
      { id: 'api', x: 250, y: 410, label: 'API service', sub: 'FastAPI REST and SSE', kind: 'service' },
      { id: 'web', x: 16, y: 290, label: 'Live and Lab websites', sub: 'React single-page apps', kind: 'ui' },
    ],
    edges: [
      { from: 'pack', to: 'gen', kind: 'data', label: 'read rows in order', route: 'v' },
      { from: 'gen', to: 'txn', kind: 'data', label: 'produce' },
      { from: 'txn', to: 'flink', kind: 'data', label: 'consume' },
      { from: 'txn', to: 'spark', kind: 'data', label: 'consume' },
      { from: 'flink', to: 'score', kind: 'control', label: 'score request', dx: -80 },
      { from: 'spark', to: 'score', kind: 'control', label: 'score request', route: 'v' },
      { from: 'flink', to: 'res', kind: 'data', label: 'decision' },
      { from: 'spark', to: 'res', kind: 'data', label: 'decision' },
      { from: 'res', to: 'cons', kind: 'data', label: 'consume', route: 'v' },
      { from: 'cons', to: 'pg', kind: 'persist', label: 'insert', route: 'v' },
      { from: 'pg', to: 'api', kind: 'persist', label: 'query' },
      { from: 'api', to: 'web', kind: 'data', label: 'JSON and server-sent events' },
      { from: 'api', to: 'gen', kind: 'control', label: 'run commands', route: 'v' },
    ],
  },
  {
    id: 'websites',
    title: 'The two websites and the shared backend',
    caption:
      'Both sites are separate Cloud Run services built from the same shared UI package. They hold no data of their own: session and run state live in Postgres behind the API, and a shared link carries only the session and run identifiers.',
    width: 1000,
    height: 470,
    zones: [
      { x: 8, y: 8, w: 400, h: 250, label: 'Browser (one visitor)' },
      { x: 448, y: 8, w: 544, h: 440, label: 'Google Cloud: Cloud Run plus the pipeline VM' },
    ],
    nodes: [
      { id: 'live', x: 32, y: 56, label: 'Live site', sub: 'console, alerts, scenario', kind: 'ui' },
      { id: 'lab', x: 32, y: 160, label: 'Lab site', sub: 'study, CA1, CA2, evidence', kind: 'ui' },
      { id: 'caddy', x: 480, y: 56, label: 'Caddy on the VM', sub: 'TLS termination', kind: 'service' },
      { id: 'api', x: 480, y: 180, label: 'API service', sub: 'sessions, view tokens, SSE', kind: 'service' },
      { id: 'pg', x: 480, y: 310, label: 'Postgres', sub: 'shared session, run and decision state', kind: 'store' },
      { id: 'ctrl', x: 760, y: 56, label: 'Controller on Cloud Run', sub: 'lifecycle endpoints', kind: 'service' },
      { id: 'artifacts', x: 760, y: 310, label: 'Model artifacts on the VM', sub: 'results and evaluation JSON', kind: 'artifact' },
      { id: 'share', x: 32, y: 330, label: 'Shared link', sub: 'session and run ids only', kind: 'artifact' },
    ],
    edges: [
      { from: 'live', to: 'caddy', kind: 'data', label: 'HTTPS' },
      { from: 'lab', to: 'caddy', kind: 'data', label: 'HTTPS' },
      { from: 'caddy', to: 'api', kind: 'data', label: 'reverse proxy', route: 'v' },
      { from: 'api', to: 'pg', kind: 'persist', label: 'read and write', route: 'v' },
      { from: 'api', to: 'artifacts', kind: 'data', label: 'serve evaluation artifacts', dy: -22 },
      { from: 'live', to: 'ctrl', kind: 'control', label: 'wake on visit, activity ping', dy: -28 },
      { from: 'lab', to: 'share', kind: 'control', label: 'copy link', route: 'v' },
      { from: 'share', to: 'live', kind: 'control', label: 'reopen the same session', route: 'v', dx: 60 },
    ],
  },
  {
    id: 'model-paths',
    title: 'Model training versus live inference',
    caption:
      'Training ran once, offline, and is not part of the running demo. The live path only loads the frozen artifact. The artifact directory is the single place where the two paths meet.',
    width: 1000,
    height: 500,
    zones: [
      { x: 8, y: 8, w: 470, h: 470, label: 'Offline training (ran once, not running in the demo)' },
      { x: 518, y: 8, w: 474, h: 470, label: 'Live inference (runs on every replayed event)' },
    ],
    nodes: [
      { id: 'raw', x: 32, y: 48, label: 'IEEE-CIS transaction rows', sub: 'public Vesta dataset', kind: 'artifact' },
      { id: 'feat', x: 32, y: 142, label: 'Feature build', sub: 'pseudonymous account key', kind: 'service' },
      { id: 'split', x: 32, y: 236, label: 'Chronological split', sub: '70 / 15 / 15 by transaction time', kind: 'service' },
      { id: 'train', x: 32, y: 330, label: 'LightGBM fit, LR baseline', sub: 'threshold chosen on validation', kind: 'service' },
      { id: 'art', x: 262, y: 190, label: 'Model artifact', sub: 'model plus evaluation JSON', kind: 'artifact' },
      { id: 'scoring', x: 548, y: 48, label: 'Scoring service', sub: 'loads the frozen artifact', kind: 'service' },
      { id: 'engine', x: 548, y: 160, label: 'Flink or Spark job', sub: 'calls scoring per event', kind: 'engine' },
      { id: 'rules', x: 780, y: 160, label: 'Deterministic rules', sub: 'evaluated separately', kind: 'service' },
      { id: 'dec', x: 548, y: 280, label: 'Decision and attributions', sub: 'alert, review or allow', kind: 'store' },
      { id: 'ui', x: 548, y: 392, label: 'Websites', sub: 'decision, contributions, CA1 metrics', kind: 'ui' },
    ],
    edges: [
      { from: 'raw', to: 'feat', kind: 'data', route: 'v' },
      { from: 'feat', to: 'split', kind: 'data', route: 'v' },
      { from: 'split', to: 'train', kind: 'data', route: 'v' },
      { from: 'train', to: 'art', kind: 'persist', label: 'write artifact' },
      { from: 'art', to: 'scoring', kind: 'data', label: 'load at start' },
      { from: 'engine', to: 'scoring', kind: 'control', label: 'score request', route: 'v' },
      { from: 'rules', to: 'engine', kind: 'data', label: 'rule hits' },
      { from: 'engine', to: 'dec', kind: 'data', label: 'threshold policy', route: 'v' },
      { from: 'dec', to: 'ui', kind: 'data', route: 'v' },
      { from: 'art', to: 'ui', kind: 'data', label: 'held-out metrics for CA1', dy: 40 },
    ],
  },
  {
    id: 'engine-paths',
    title: 'Flink and Spark experiment data paths',
    caption:
      'Both engines read the same transactions topic and write to their own results topic, so a run can be compared event by event. Control requests come from the API; checkpoints are the only engine state that survives a restart.',
    width: 1000,
    height: 560,
    nodes: [
      { id: 'txn', x: 16, y: 250, label: 'Kafka topic', sub: 'fraudwatch.transactions', kind: 'store' },
      { id: 'fck', x: 262, y: 16, label: 'Flink checkpoints', sub: 'durable volume', kind: 'store' },
      { id: 'flink', x: 262, y: 120, label: 'Flink job', sub: 'watermarks, event time', kind: 'engine' },
      { id: 'spark', x: 262, y: 380, label: 'Spark Structured Streaming', sub: 'micro-batch', kind: 'engine' },
      { id: 'sck', x: 262, y: 482, label: 'Spark checkpoint directory', sub: 'durable volume', kind: 'store' },
      { id: 'fres', x: 512, y: 120, label: 'results.flink', sub: 'Kafka topic', kind: 'store' },
      { id: 'sres', x: 512, y: 380, label: 'results.spark', sub: 'Kafka topic', kind: 'store' },
      { id: 'cons', x: 760, y: 250, label: 'Result consumer', sub: 'tags rows with engine and run', kind: 'service' },
      { id: 'pg', x: 760, y: 380, label: 'Postgres', sub: 'per-engine run rows', kind: 'store' },
      { id: 'api', x: 512, y: 250, label: 'API service', sub: 'run control and comparison', kind: 'service' },
      { id: 'ops', x: 16, y: 120, label: 'Ops agent', sub: 'restart and failure injection', kind: 'service' },
    ],
    edges: [
      { from: 'txn', to: 'flink', kind: 'data', label: 'consume' },
      { from: 'txn', to: 'spark', kind: 'data', label: 'consume' },
      { from: 'flink', to: 'fres', kind: 'data', label: 'produce' },
      { from: 'spark', to: 'sres', kind: 'data', label: 'produce' },
      { from: 'fres', to: 'cons', kind: 'data', label: 'consume' },
      { from: 'sres', to: 'cons', kind: 'data', label: 'consume' },
      { from: 'cons', to: 'pg', kind: 'persist', label: 'insert', route: 'v' },
      { from: 'pg', to: 'api', kind: 'persist', label: 'matched-event query' },
      { from: 'flink', to: 'fck', kind: 'persist', label: 'checkpoint', route: 'v' },
      { from: 'spark', to: 'sck', kind: 'persist', label: 'checkpoint', route: 'v' },
      { from: 'api', to: 'flink', kind: 'control', label: 'submit and poll job', dy: -24 },
      { from: 'api', to: 'ops', kind: 'control', label: 'protected restart request', dy: 30 },
      { from: 'ops', to: 'flink', kind: 'control', label: 'stop or restart', route: 'h' },
    ],
  },
  {
    id: 'cloud-lifecycle',
    title: 'Automatic cloud wake and idle shutdown',
    caption:
      'The pipeline VM is stopped when nobody is looking at it. A visit wakes it through the controller; inactivity stops it again. Only control requests cross into the Google Cloud APIs, and no pipeline data flows through the controller.',
    width: 1000,
    height: 470,
    nodes: [
      { id: 'visitor', x: 16, y: 32, label: 'Visitor opens a site', sub: 'Live or Lab', kind: 'external' },
      { id: 'site', x: 16, y: 160, label: 'Site on Cloud Run', sub: 'always available, cheap to serve', kind: 'ui' },
      { id: 'ctrl', x: 300, y: 160, label: 'Controller on Cloud Run', sub: 'status and ensure-ready', kind: 'service' },
      { id: 'gce', x: 584, y: 32, label: 'Compute Engine API', sub: 'start and stop the instance', kind: 'external' },
      { id: 'vm', x: 584, y: 160, label: 'Pipeline VM', sub: 'compose stack: Kafka, engines, API', kind: 'service' },
      { id: 'idle', x: 300, y: 330, label: 'Idle timer', sub: 'last activity plus timeout', kind: 'service' },
      { id: 'api', x: 16, y: 330, label: 'API on the VM', sub: 'reachable only once ready', kind: 'service' },
    ],
    edges: [
      { from: 'visitor', to: 'site', kind: 'control', label: 'page load', route: 'v' },
      { from: 'site', to: 'ctrl', kind: 'control', label: 'ensure-ready, poll status' },
      { from: 'ctrl', to: 'gce', kind: 'control', label: 'start instance' },
      { from: 'gce', to: 'vm', kind: 'control', label: 'boot and run compose', route: 'v' },
      { from: 'vm', to: 'ctrl', kind: 'data', label: 'observed state and readiness', dy: 28 },
      { from: 'site', to: 'idle', kind: 'control', label: 'activity ping while watching', dx: 40 },
      { from: 'idle', to: 'ctrl', kind: 'control', label: 'timeout reached', route: 'v' },
      { from: 'site', to: 'api', kind: 'data', label: 'pipeline requests skip the controller', route: 'v', dx: 120 },
    ],
  },
];

export const ARCHITECTURE_FIGURES: Array<{ id: ArchitectureFigureId; title: string }> = FIGURES.map((f) => ({ id: f.id, title: f.title }));

const FIG_STYLE = `
.fig-zone { fill: none; stroke-dasharray: 6 5; }
.fig-box { stroke-width: 1.5; }
.fig-edge { fill: none; stroke-width: 1.8; }
.fig-edge-control { stroke-dasharray: 7 4; }
.fig-edge-persist { stroke-dasharray: 2 3; }
`;

const C = {
  bg: 'var(--surface, #ffffff)',
  panel: 'var(--surface-2, #f4f4f5)',
  soft: 'var(--surface-3, #e9e9ec)',
  border: 'var(--border, #d4d4d8)',
  borderStrong: 'var(--border-strong, #c9c9cf)',
  text: 'var(--text, #18181b)',
  muted: 'var(--text-muted, #52525b)',
  data: 'var(--chart-4, #1d4ed8)',
  persist: 'var(--chart-1, #0f766e)',
  control: 'var(--chart-3, #92400e)',
};

const NODE_STYLE: Record<NodeKind, { fill: string; stroke: string; width: number; dash?: string }> = {
  ui: { fill: C.panel, stroke: C.data, width: 2 },
  service: { fill: C.panel, stroke: C.borderStrong, width: 1.5 },
  store: { fill: C.soft, stroke: C.control, width: 2 },
  engine: { fill: C.panel, stroke: C.persist, width: 2.5 },
  external: { fill: C.bg, stroke: C.borderStrong, width: 1.5, dash: '5 4' },
  artifact: { fill: C.soft, stroke: C.borderStrong, width: 1.5 },
};

const EDGE_COLOR: Record<EdgeKind, string> = { data: C.data, control: C.control, persist: C.persist };

function box(n: FigNode) {
  return { x: n.x, y: n.y, w: n.w ?? NW, h: n.h ?? NH };
}

function edgePath(a: FigNode, b: FigNode, route: 'h' | 'v'): { d: string; mid: { x: number; y: number } } {
  const A = box(a);
  const B = box(b);
  if (route === 'v') {
    const down = B.y >= A.y;
    const y1 = down ? A.y + A.h : A.y;
    const y2 = down ? B.y : B.y + B.h;
    const x1 = A.x + A.w / 2;
    const x2 = B.x + B.w / 2;
    const my = (y1 + y2) / 2;
    if (Math.abs(x1 - x2) < 2) return { d: `M ${x1} ${y1} L ${x2} ${y2}`, mid: { x: x1, y: my } };
    return { d: `M ${x1} ${y1} L ${x1} ${my} L ${x2} ${my} L ${x2} ${y2}`, mid: { x: (x1 + x2) / 2, y: my } };
  }
  const right = B.x >= A.x;
  const x1 = right ? A.x + A.w : A.x;
  const x2 = right ? B.x : B.x + B.w;
  const y1 = A.y + A.h / 2;
  const y2 = B.y + B.h / 2;
  const mx = (x1 + x2) / 2;
  if (Math.abs(y1 - y2) < 2) return { d: `M ${x1} ${y1} L ${x2} ${y2}`, mid: { x: mx, y: y1 } };
  return { d: `M ${x1} ${y1} L ${mx} ${y1} L ${mx} ${y2} L ${x2} ${y2}`, mid: { x: mx, y: (y1 + y2) / 2 } };
}

function pickRoute(a: FigNode, b: FigNode): 'h' | 'v' {
  const A = box(a);
  const B = box(b);
  return Math.abs(A.y - B.y) > Math.abs(A.x - B.x) ? 'v' : 'h';
}

function wrap(text: string, max: number): string[] {
  const lines: string[] = [];
  let line = '';
  for (const word of text.split(' ')) {
    const next = line ? `${line} ${word}` : word;
    if (next.length > max && line) {
      lines.push(line);
      line = word;
    } else line = next;
  }
  if (line) lines.push(line);
  return lines;
}

function FigureSvg({ fig, svgRef }: { fig: Figure; svgRef?: Ref<SVGSVGElement> }) {
  const byId = useMemo(() => new Map(fig.nodes.map((n) => [n.id, n])), [fig]);
  const head = 34;
  const key = 34;
  const total = fig.height + head + key;
  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${fig.width} ${total}`}
      width="100%"
      role="img"
      aria-label={`${fig.title}. ${fig.caption}`}
      preserveAspectRatio="xMidYMid meet"
      style={{ display: 'block', maxWidth: '100%', height: 'auto' }}
    >
      <style>{FIG_STYLE}</style>
      <defs>
        {(['data', 'control', 'persist'] as EdgeKind[]).map((k) => (
          <marker key={k} id={`fig-${fig.id}-${k}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill={EDGE_COLOR[k]} />
          </marker>
        ))}
      </defs>
      <rect x="0" y="0" width={fig.width} height={total} fill={C.bg} />
      <text x="8" y="22" fill={C.text} fontSize="17" fontWeight="700" fontFamily="system-ui, sans-serif">
        {fig.title}
      </text>
      <g transform={`translate(0 ${head})`} fontFamily="system-ui, sans-serif">
        {fig.zones?.map((z, i) => (
          <g key={`z${i}`}>
            <rect className="fig-zone" x={z.x} y={z.y} width={z.w} height={z.h} rx={14} stroke={C.border} />
            <text x={z.x + 12} y={z.y + 20} fill={C.muted} fontSize="13" fontWeight="600">
              {z.label}
            </text>
          </g>
        ))}
        {fig.edges.map((e, i) => {
          const a = byId.get(e.from);
          const b = byId.get(e.to);
          if (!a || !b) return null;
          const { d, mid } = edgePath(a, b, e.route ?? pickRoute(a, b));
          const lx = mid.x + (e.dx ?? 0);
          const ly = mid.y + (e.dy ?? 0);
          const w = e.label ? e.label.length * 6 + 8 : 0;
          return (
            <g key={`e${i}`}>
              <path className={`fig-edge fig-edge-${e.kind}`} d={d} stroke={EDGE_COLOR[e.kind]} markerEnd={`url(#fig-${fig.id}-${e.kind})`} />
              {e.label && (
                <>
                  <rect x={lx - w / 2} y={ly - 10} width={w} height={16} rx={3} fill={C.bg} />
                  <text x={lx} y={ly + 2} textAnchor="middle" fill={C.muted} fontSize="11.5">
                    {e.label}
                  </text>
                </>
              )}
            </g>
          );
        })}
        {fig.nodes.map((n) => {
          const B = box(n);
          const st = NODE_STYLE[n.kind];
          const lines = wrap(n.label, 24);
          const subs = n.sub ? wrap(n.sub, 30) : [];
          const startY = B.y + B.h / 2 - (lines.length * 16 + subs.length * 13) / 2 + 12;
          return (
            <g key={n.id}>
              <rect className="fig-box" x={B.x} y={B.y} width={B.w} height={B.h} rx={10} fill={st.fill} stroke={st.stroke} strokeWidth={st.width} strokeDasharray={st.dash} />
              {lines.map((l, i) => (
                <text key={`l${i}`} x={B.x + B.w / 2} y={startY + i * 16} textAnchor="middle" fill={C.text} fontSize="14" fontWeight="600">
                  {l}
                </text>
              ))}
              {subs.map((l, i) => (
                <text key={`s${i}`} x={B.x + B.w / 2} y={startY + lines.length * 16 + i * 13} textAnchor="middle" fill={C.muted} fontSize="11.5">
                  {l}
                </text>
              ))}
            </g>
          );
        })}
      </g>
      <g transform={`translate(10 ${head + fig.height + 20})`} fontFamily="system-ui, sans-serif">
        {([
          ['data', 'data flow', 0],
          ['control', 'control request', 150],
          ['persist', 'persistence', 320],
        ] as Array<[EdgeKind, string, number]>).map(([k, label, x]) => (
          <g key={k}>
            <path className={`fig-edge fig-edge-${k}`} d={`M ${x} -4 L ${x + 26} -4`} stroke={EDGE_COLOR[k]} markerEnd={`url(#fig-${fig.id}-${k})`} />
            <text x={x + 34} y={0} fill={C.muted} fontSize="11.5">
              {label}
            </text>
          </g>
        ))}
      </g>
    </svg>
  );
}

function serialize(svg: SVGSVGElement): string {
  const clone = svg.cloneNode(true) as SVGSVGElement;
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
  const vb = clone.getAttribute('viewBox')?.split(/\s+/) ?? [];
  if (vb.length === 4) {
    clone.setAttribute('width', vb[2]);
    clone.setAttribute('height', vb[3]);
  }
  clone.removeAttribute('style');
  return `<?xml version="1.0" encoding="UTF-8"?>\n${new XMLSerializer().serializeToString(clone)}`;
}

/** Hands a file to the browser. Embedded viewers can block page-initiated downloads, so the
 *  caller also offers an explicit open-in-a-new-tab path and reports failure honestly. */
function offer(blob: Blob, filename: string): boolean {
  try {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.rel = 'noopener';
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 10000);
    return true;
  } catch {
    return false;
  }
}

const BLOCKED = 'The browser blocked that download. Use "Open in a new tab" and save the figure from there.';

export function ArchitectureFigures({ only }: { only?: ArchitectureFigureId[] }) {
  const figures = only ? FIGURES.filter((f) => only.includes(f.id)) : FIGURES;
  const [active, setActive] = useState<ArchitectureFigureId>(figures[0]?.id ?? 'end-to-end');
  const ref = useRef<SVGSVGElement | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const fig = figures.find((f) => f.id === active) ?? figures[0];
  if (!fig) return null;

  const text = () => (ref.current ? serialize(ref.current) : null);

  const downloadSvg = () => {
    const svg = text();
    if (!svg) return;
    setNote(offer(new Blob([svg], { type: 'image/svg+xml;charset=utf-8' }), `fraudwatch-${fig.id}.svg`) ? null : BLOCKED);
  };

  const openTab = () => {
    const svg = text();
    if (!svg) return;
    const url = URL.createObjectURL(new Blob([svg], { type: 'image/svg+xml;charset=utf-8' }));
    const win = window.open(url, '_blank', 'noopener');
    setNote(win ? null : 'The browser blocked the new tab. Open this page outside the embedded viewer to export the figure.');
    window.setTimeout(() => URL.revokeObjectURL(url), 30000);
  };

  const downloadPng = () => {
    const svg = text();
    if (!svg) return;
    const vb = ref.current?.getAttribute('viewBox')?.split(/\s+/) ?? [];
    const w = Number(vb[2] ?? fig.width) * 2;
    const h = Number(vb[3] ?? fig.height) * 2;
    const img = new Image();
    const fail = () => setNote('This browser could not rasterise the figure. Download the SVG instead.');
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext('2d');
      if (!ctx) return fail();
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, w, h);
      ctx.drawImage(img, 0, 0, w, h);
      canvas.toBlob((blob) => {
        if (!blob) return fail();
        setNote(offer(blob, `fraudwatch-${fig.id}.png`) ? null : BLOCKED);
      }, 'image/png');
    };
    img.onerror = fail;
    img.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
  };

  return (
    <div className="fw-stack" data-testid="architecture-figures">
      <div className="fw-row fw-row-wrap">
        <SourceBadge kind="config" />
        <span className="fw-small fw-muted">Drawn from the deployed component set. These figures carry no measured numbers.</span>
      </div>
      <div className="fw-row fw-row-wrap">
        {figures.map((f) => (
          <button
            key={f.id}
            type="button"
            aria-pressed={f.id === active}
            className={`fw-btn fw-btn-sm ${f.id === active ? 'fw-btn-primary' : 'fw-btn-secondary'}`}
            onClick={() => {
              setActive(f.id);
              setNote(null);
            }}
          >
            {f.title}
          </button>
        ))}
      </div>
      <figure className="fw-figure">
        <div className="fw-figure-canvas">
          <FigureSvg fig={fig} svgRef={ref} />
        </div>
        <figcaption className="fw-small fw-muted">{fig.caption}</figcaption>
      </figure>
      <div className="fw-row fw-row-wrap">
        <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={downloadSvg}>
          <Download size={14} aria-hidden="true" /> Download SVG
        </button>
        <button type="button" className="fw-btn fw-btn-secondary fw-btn-sm" onClick={downloadPng}>
          <Download size={14} aria-hidden="true" /> Download PNG
        </button>
        <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" onClick={openTab}>
          <ExternalLink size={14} aria-hidden="true" /> Open in a new tab
        </button>
      </div>
      <p className="fw-small fw-muted">
        Each figure is plain SVG at a fixed aspect ratio, so it prints on A4 without redrawing. Embedded viewers sometimes block downloads started by a
        page; the new-tab option works there.
      </p>
      {note && (
        <p className="fw-small" role="status">
          {note}
        </p>
      )}
    </div>
  );
}
