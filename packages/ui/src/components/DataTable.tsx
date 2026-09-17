import { useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import { cx } from './basics';

export interface Column<T> {
  key: string;
  header: ReactNode;
  render: (row: T) => ReactNode;
  align?: 'left' | 'right';
  className?: string;
}

export interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  caption: string;
  onRowActivate?: (row: T) => void;
  selectedKey?: string | null;
  empty?: ReactNode;
  compact?: boolean;
  rowLabel?: (row: T) => string;
}

/**
 * Accessible data table. When rows are activatable they use a roving tabindex:
 * Tab enters the table once, ArrowUp/ArrowDown/Home/End move between rows, Enter/Space opens.
 */
export function DataTable<T>({ columns, rows, rowKey, caption, onRowActivate, selectedKey, empty, compact, rowLabel }: DataTableProps<T>) {
  const [focusIndex, setFocusIndex] = useState(0);
  const bodyRef = useRef<HTMLTableSectionElement>(null);
  const activatable = !!onRowActivate;
  const safeFocus = Math.min(focusIndex, Math.max(0, rows.length - 1));

  const focusRow = (i: number) => {
    const clamped = Math.max(0, Math.min(rows.length - 1, i));
    setFocusIndex(clamped);
    const el = bodyRef.current?.querySelectorAll<HTMLTableRowElement>('tr[data-row]')[clamped];
    el?.focus();
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTableRowElement>, i: number, row: T) => {
    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        focusRow(i + 1);
        break;
      case 'ArrowUp':
        e.preventDefault();
        focusRow(i - 1);
        break;
      case 'Home':
        e.preventDefault();
        focusRow(0);
        break;
      case 'End':
        e.preventDefault();
        focusRow(rows.length - 1);
        break;
      case 'Enter':
      case ' ':
        e.preventDefault();
        onRowActivate?.(row);
        break;
    }
  };

  if (!rows.length && empty) return <>{empty}</>;

  return (
    <div className="fw-table-wrap">
      <table className={cx('fw-table', compact && 'fw-table-compact')}>
        <caption className="fw-sr-only">
          {caption}
          {activatable ? '. Use arrow keys to move between rows and Enter to open a row.' : ''}
        </caption>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col" className={cx(c.align === 'right' && 'fw-right', c.className)}>
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody ref={bodyRef}>
          {rows.map((row, i) => {
            const key = rowKey(row);
            return (
              <tr
                key={key}
                data-row
                data-clickable={activatable || undefined}
                tabIndex={activatable ? (i === safeFocus ? 0 : -1) : undefined}
                aria-selected={selectedKey !== undefined ? selectedKey === key : undefined}
                aria-label={activatable && rowLabel ? rowLabel(row) : undefined}
                onClick={activatable ? () => onRowActivate?.(row) : undefined}
                onFocus={activatable ? () => setFocusIndex(i) : undefined}
                onKeyDown={activatable ? (e) => onKeyDown(e, i, row) : undefined}
              >
                {columns.map((c) => (
                  <td key={c.key} className={cx(c.align === 'right' && 'fw-right', c.className)}>
                    {c.render(row)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
