import { forwardRef, type ButtonHTMLAttributes, type HTMLAttributes, type ReactNode } from 'react';
import { formatValue, humanize } from '../format';

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger';
type Size = 'sm' | 'md' | 'lg';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: Size;
  icon?: ReactNode;
  iconOnly?: boolean;
  loading?: boolean;
}

export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = 'secondary', size = 'md', icon, iconOnly, loading, className, children, disabled, type = 'button', ...rest },
  ref,
) {
  return (
    <button
      ref={ref}
      type={type}
      className={cx('fw-btn', `fw-btn-${variant}`, size !== 'md' && `fw-btn-${size}`, iconOnly && 'fw-icon-btn', className)}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      {...rest}
    >
      {icon}
      {children}
    </button>
  );
});

export function buttonClass(variant: Variant = 'secondary', size: Size = 'md', extra?: string) {
  return cx('fw-btn', `fw-btn-${variant}`, size !== 'md' && `fw-btn-${size}`, extra);
}

export interface CardProps extends Omit<HTMLAttributes<HTMLElement>, 'title'> {
  title?: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  flush?: boolean;
  as?: 'section' | 'div' | 'article';
  headingLevel?: 2 | 3;
}

export function Card({ title, description, actions, flush, children, className, as = 'section', headingLevel = 2, ...rest }: CardProps) {
  const Tag = as;
  const H = headingLevel === 2 ? 'h2' : 'h3';
  return (
    <Tag className={cx('fw-card', className)} {...rest}>
      {(title || actions) && (
        <div className="fw-card-head">
          <div>
            {title && <H>{title}</H>}
            {description && <div className="fw-card-desc">{description}</div>}
          </div>
          {actions && <div className="fw-row">{actions}</div>}
        </div>
      )}
      <div className={flush ? 'fw-card-body-flush' : 'fw-card-body'}>{children}</div>
    </Tag>
  );
}

export function Stat({ label, value, note, icon }: { label: ReactNode; value: ReactNode; note?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="fw-card fw-stat">
      <div className="fw-stat-label">
        {icon}
        {label}
      </div>
      <div className="fw-stat-value">{value}</div>
      {note && <div className="fw-stat-note">{note}</div>}
    </div>
  );
}

export interface KeyValueItem {
  key: string;
  label?: ReactNode;
  value: ReactNode;
}

export function KeyValue({ items, className }: { items: KeyValueItem[]; className?: string }) {
  return (
    <dl className={cx('fw-kv', className)}>
      {items.map((it) => (
        <div key={it.key} style={{ display: 'contents' }}>
          <dt>{it.label ?? humanize(it.key)}</dt>
          <dd>{it.value}</dd>
        </div>
      ))}
    </dl>
  );
}

/** KeyValue from a plain object (unknown shapes from the API). */
export function ObjectKeyValue({ data, empty = 'No values reported' }: { data: Record<string, unknown> | null | undefined; empty?: string }) {
  const entries = data ? Object.entries(data) : [];
  if (!entries.length) return <p className="fw-muted fw-small">{empty}</p>;
  return (
    <KeyValue
      items={entries.map(([k, v]) => ({
        key: k,
        value: v !== null && typeof v === 'object' ? <code className="fw-small">{formatValue(v)}</code> : formatValue(v),
      }))}
    />
  );
}

export function Field({ label, help, htmlFor, children }: { label: ReactNode; help?: ReactNode; htmlFor: string; children: ReactNode }) {
  return (
    <div className="fw-field">
      <label className="fw-label" htmlFor={htmlFor}>
        {label}
      </label>
      {children}
      {help && (
        <div className="fw-help" id={`${htmlFor}-help`}>
          {help}
        </div>
      )}
    </div>
  );
}

export function Meter({ value, max, label }: { value: number; max: number; label: string }) {
  const pct = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;
  return (
    <div
      className={cx('fw-meter', pct >= 100 ? 'fw-meter-full' : pct >= 80 && 'fw-meter-warn')}
      role="meter"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={max}
      aria-valuenow={value}
    >
      <span style={{ width: `${pct}%` }} />
    </div>
  );
}

export function Disclosure({ summary, children, defaultOpen }: { summary: ReactNode; children: ReactNode; defaultOpen?: boolean }) {
  return (
    <details className="fw-details" open={defaultOpen}>
      <summary>{summary}</summary>
      <div className="fw-details-body">{children}</div>
    </details>
  );
}
