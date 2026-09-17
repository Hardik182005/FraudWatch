import { CircleAlert, Inbox, Info, RefreshCw, TriangleAlert, WifiOff } from 'lucide-react';
import type { ReactNode } from 'react';
import { describeError } from '@fraudwatch/contracts';
import { formatRelativeTime, formatMs } from '../format';
import { Button, cx } from './basics';

export function EmptyState({ title, children, action, icon }: { title: ReactNode; children?: ReactNode; action?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="fw-state" role="status">
      <div className="fw-state-icon" aria-hidden="true">
        {icon ?? <Inbox size={18} />}
      </div>
      <h3>{title}</h3>
      {children && <div>{children}</div>}
      {action}
    </div>
  );
}

export function ErrorState({ error, title = 'Could not load this data', onRetry }: { error: unknown; title?: ReactNode; onRetry?: () => void }) {
  return (
    <div className="fw-state fw-state-error" role="alert">
      <div className="fw-state-icon" aria-hidden="true">
        <CircleAlert size={18} />
      </div>
      <h3>{title}</h3>
      <p>{describeError(error)}</p>
      {onRetry && (
        <Button size="sm" icon={<RefreshCw size={16} aria-hidden="true" />} onClick={onRetry}>
          Retry
        </Button>
      )}
    </div>
  );
}

export function LoadingSkeleton({ lines = 3, label = 'Loading' }: { lines?: number; label?: string }) {
  return (
    <div className="fw-stack" style={{ gap: 10, padding: 16 }} role="status" aria-live="polite">
      <span className="fw-sr-only">{label}…</span>
      {Array.from({ length: lines }, (_, i) => (
        <span key={i} className="fw-skeleton" style={{ width: `${90 - i * 12}%` }} aria-hidden="true" />
      ))}
    </div>
  );
}

export function Banner({ tone = 'info', children, icon, className }: { tone?: 'info' | 'warn' | 'error' | 'neutral'; children: ReactNode; icon?: ReactNode; className?: string }) {
  const Icon = tone === 'error' ? CircleAlert : tone === 'warn' ? TriangleAlert : Info;
  return (
    <div className={cx('fw-banner', tone !== 'neutral' && `fw-banner-${tone}`, className)} role={tone === 'error' ? 'alert' : 'note'}>
      {icon ?? <Icon size={18} aria-hidden="true" />}
      <div>{children}</div>
    </div>
  );
}

export type ConnectionState = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'stale' | 'closed';

/** Honest stream status: shows when data may be out of date and when the next retry happens. */
export function StaleBanner({ status, lastMessageAt, nextRetryMs, onReconnect }: { status: ConnectionState; lastMessageAt: number | null; nextRetryMs?: number | null; onReconnect?: () => void }) {
  if (status === 'open' || status === 'idle') return null;
  const connecting = status === 'connecting';
  return (
    <div className={cx('fw-banner', connecting ? 'fw-banner-info' : 'fw-banner-warn')} role="status" aria-live="polite">
      {connecting ? <RefreshCw size={18} aria-hidden="true" /> : <WifiOff size={18} aria-hidden="true" />}
      <div style={{ flex: 1 }}>
        {connecting && 'Connecting to the live stream…'}
        {status === 'reconnecting' && (
          <>
            Live stream interrupted — data may be stale. Reconnecting{nextRetryMs ? ` in ${formatMs(nextRetryMs)}` : ''} and resuming from the last cursor.
          </>
        )}
        {status === 'stale' && 'No updates received for over 30 seconds — data may be stale. Reconnecting…'}
        {status === 'closed' && 'Live stream closed.'}
        {lastMessageAt && <span className="fw-muted"> Last update {formatRelativeTime(lastMessageAt)}.</span>}
      </div>
      {onReconnect && status === 'closed' && (
        <Button size="sm" onClick={onReconnect}>
          Reconnect
        </Button>
      )}
    </div>
  );
}
