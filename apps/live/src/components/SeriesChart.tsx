import type { SeriesPoint } from '@fraudwatch/contracts';
import { EmptyState, formatInt, formatTimestamp } from '@fraudwatch/ui';
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

/** Scored vs flagged counts per interval, drawn only from GET /sessions/{id}/overview series points. */
export function SeriesChart({ series }: { series: SeriesPoint[] }) {
  if (!series.length) return <EmptyState title="No activity yet">The chart fills in once the engine persists scored transactions.</EmptyState>;
  const last = series[series.length - 1]!;
  return (
    <figure style={{ margin: 0 }}>
      <div className="fw-chart" aria-hidden="true">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={series} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="t" tickFormatter={(t: number) => formatTimestamp(t).slice(0, 8)} minTickGap={40} />
            <YAxis allowDecimals={false} width={44} />
            <Tooltip labelFormatter={(t) => formatTimestamp(Number(t))} formatter={(v) => formatInt(Number(v))} />
            <Legend />
            <Line type="monotone" dataKey="scored" name="Scored" stroke="var(--chart-4)" strokeWidth={2} dot={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="flagged" name="Flagged (alert + review)" stroke="var(--chart-2)" strokeWidth={2} dot={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <figcaption className="fw-sr-only">
        Line chart of scored and flagged transactions over {series.length} intervals. Latest interval: {formatInt(last.scored)} scored, {formatInt(last.flagged)} flagged.
      </figcaption>
    </figure>
  );
}
