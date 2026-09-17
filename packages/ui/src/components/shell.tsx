import type { ReactNode } from 'react';
import { describeError, type Health } from '@fraudwatch/contracts';
import { HealthDot } from './status';
import { formatRelativeTime } from '../format';

/**
 * Page chrome shared by both websites: skip link, sticky header with brand/nav/actions, main landmark, footer.
 * Navigation links are passed in so the shell stays router-agnostic.
 */
export function AppShell({
  brand,
  nav,
  actions,
  banner,
  footer,
  children,
}: {
  brand: ReactNode;
  nav: ReactNode;
  actions?: ReactNode;
  banner?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="fw-shell">
      <a className="fw-skip-link" href="#main">
        Skip to content
      </a>
      <header className="fw-header">
        <div className="fw-header-inner">
          {brand}
          <nav className="fw-nav" aria-label="Main">
            {nav}
          </nav>
          {actions && <div className="fw-header-actions">{actions}</div>}
        </div>
      </header>
      {banner}
      <main id="main" className="fw-main" tabIndex={-1}>
        {children}
      </main>
      {footer && (
        <footer className="fw-footer">
          <div className="fw-footer-inner">{footer}</div>
        </footer>
      )}
    </div>
  );
}

export function BrandMark({ product }: { product: 'Live' | 'Lab' }) {
  return (
    <>
      <span className="fw-brand-mark" aria-hidden="true">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
          <path d="M1.5 9h3l2-5 3 9 2-4h3" />
        </svg>
      </span>
      <span>
        FraudWatch <span className="fw-brand-sub">{product}</span>
      </span>
    </>
  );
}

/**
 * Overall system status from a real GET /health response. When the request fails the API is shown as
 * unreachable; there is never a fixed "connected" badge.
 */
export function HealthSummary({ health, error, loading }: { health: Health | undefined | null; error: unknown; loading: boolean }) {
  if (!health && loading) return <HealthDot status="unknown" label="System" />;
  if (!health && error) {
    return (
      <span title={describeError(error)}>
        <HealthDot status="down" label="API unreachable" />
      </span>
    );
  }
  if (!health) return <HealthDot status="unknown" label="System" />;
  const stale = !!error;
  return (
    <span title={`Checked ${formatRelativeTime(health.as_of)}${stale ? ' (latest check failed)' : ''}`} data-testid="health-summary">
      <HealthDot status={health.overall} label={stale ? 'System (last known)' : 'System'} />
    </span>
  );
}
