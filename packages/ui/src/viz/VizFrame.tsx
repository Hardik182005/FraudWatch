import { Download, Table2, BarChart3 } from 'lucide-react';
import { useId, useState, type ReactNode } from 'react';
import { formatInt, formatNumber } from '../format';
import { availability, toCsv } from './math';

export type SourceKind = 'live' | 'saved' | 'heldout' | 'validation' | 'illustration' | 'config';

export const SOURCE_LABEL: Record<SourceKind, string> = {
  live: 'Live measurements',
  saved: 'Saved run',
  heldout: 'Held-out evaluation',
  validation: 'Validation exploration',
  illustration: 'Illustration',
  config: 'Configuration',
};

export function SourceBadge({ kind, children }: { kind: SourceKind; children?: ReactNode }) {
  return (
    <span className="fw-source" data-kind={kind} data-testid="viz-source">
      <span className="fw-source-dot" aria-hidden="true" />
      {children ?? SOURCE_LABEL[kind]}
    </span>
  );
}

/** Saves a Blob as a file without placing credentials in a URL. */
export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.rel = 'noopener';
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

export function downloadCsv(rows: Array<Record<string, unknown>>, filename: string, columns?: string[]) {
  downloadBlob(new Blob([toCsv(rows, columns)], { type: 'text/csv;charset=utf-8' }), filename);
}

export interface VizTable {
  columns: Array<{ key: string; label: string; align?: 'left' | 'right'; format?: (v: unknown, row: Record<string, unknown>) => ReactNode }>;
  rows: Array<Record<string, unknown>>;
}

export interface VizFrameProps {
  title: ReactNode;
  source: SourceKind;
  sourceText?: ReactNode;
  /** Units, time range, sample size, aggregation interval, model/run reference. */
  meta?: ReactNode;
  /** Screen-reader summary of what the chart shows. */
  summary: string;
  table?: VizTable | null;
  csvName?: string;
  actions?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  testId?: string;
  /** Render without the outer card border. */
  bare?: boolean;
}

/**
 * Wrapper for every quantitative visual: short title, source label, metadata line, chart/table toggle,
 * CSV export and a screen-reader summary.
 */
export function VizFrame({ title, source, sourceText, meta, summary, table, csvName, actions, children, footer, testId, bare }: VizFrameProps) {
  const [view, setView] = useState<'chart' | 'table'>('chart');
  const id = useId();
  const hasTable = !!table && table.rows.length > 0;
  return (
    <figure className="fw-viz" data-bare={bare || undefined} data-testid={testId} aria-labelledby={`${id}-t`}>
      <div className="fw-viz-head">
        <div className="fw-viz-titles">
          <h3 id={`${id}-t`} className="fw-viz-title">
            {title}
          </h3>
          <div className="fw-viz-meta">
            <SourceBadge kind={source}>{sourceText}</SourceBadge>
            {meta && <span className="fw-small fw-muted">{meta}</span>}
          </div>
        </div>
        <div className="fw-viz-actions">
          {actions}
          {hasTable && (
            <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm" aria-pressed={view === 'table'} onClick={() => setView((v) => (v === 'chart' ? 'table' : 'chart'))}>
              {view === 'chart' ? <Table2 size={14} aria-hidden="true" /> : <BarChart3 size={14} aria-hidden="true" />}
              {view === 'chart' ? 'Table' : 'Chart'}
            </button>
          )}
          {hasTable && csvName && (
            <button
              type="button"
              className="fw-btn fw-btn-ghost fw-btn-sm"
              onClick={() => downloadCsv(table!.rows, csvName, table!.columns.map((c) => c.key))}
              aria-label={`Download ${typeof title === 'string' ? title : 'chart data'} as CSV`}
            >
              <Download size={14} aria-hidden="true" />
              CSV
            </button>
          )}
        </div>
      </div>
      {view === 'table' && hasTable ? <VizDataTable table={table!} caption={summary} /> : <div className="fw-viz-body">{children}</div>}
      <figcaption className="fw-sr-only">{summary}</figcaption>
      {footer && <div className="fw-viz-foot fw-small fw-muted">{footer}</div>}
    </figure>
  );
}

export function VizDataTable({ table, caption }: { table: VizTable; caption: string }) {
  return (
    <div className="fw-table-wrap" tabIndex={0} role="region" aria-label="Chart data table">
      <table className="fw-table fw-table-compact">
        <caption className="fw-sr-only">{caption}</caption>
        <thead>
          <tr>
            {table.columns.map((c) => (
              <th key={c.key} scope="col" style={{ textAlign: c.align ?? 'left' }}>
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.slice(0, 500).map((r, i) => (
            <tr key={i}>
              {table.columns.map((c) => (
                <td key={c.key} className={c.align === 'right' ? 'fw-num' : undefined} style={{ textAlign: c.align ?? 'left' }}>
                  {c.format ? c.format(r[c.key], r) : r[c.key] === null || r[c.key] === undefined ? '—' : String(r[c.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {table.rows.length > 500 && <p className="fw-small fw-muted">Showing the first 500 of {formatInt(table.rows.length)} rows. The CSV contains all rows.</p>}
    </div>
  );
}

/**
 * A single measured quantity that never turns missing data into 0:
 * value → formatted; 0 → "0"; null → "Not recorded"; "not_instrumented" → "Not instrumented".
 */
export function Measure({ value, format, unavailable = 'Not recorded' }: { value: unknown; format?: (n: number) => string; unavailable?: string }) {
  const a = availability(value);
  if (a === 'not_instrumented') return <span className="fw-measure-na" data-state="not_instrumented">Not instrumented</span>;
  if (a === 'unavailable') return <span className="fw-measure-na" data-state="unavailable">{unavailable}</span>;
  if (typeof value !== 'number') return <span>{String(value)}</span>;
  return <span className="fw-num">{format ? format(value) : Number.isInteger(value) ? formatInt(value) : formatNumber(value)}</span>;
}

/** Empty chart state that says why and what to do next. */
export function VizEmpty({ kind = 'unavailable', title, children, action }: { kind?: 'no_data' | 'unavailable' | 'not_instrumented' | 'stale' | 'not_applicable'; title: ReactNode; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="fw-viz-empty" data-state={kind} data-testid="viz-empty">
      <strong>{title}</strong>
      {children && <p className="fw-small fw-muted">{children}</p>}
      {action}
    </div>
  );
}
