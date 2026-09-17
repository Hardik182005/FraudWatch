import { CircleCheck, CircleDashed, Clock, Eye, OctagonAlert, type LucideIcon } from 'lucide-react';
import type { Action, AlertStatus, ComponentStatus } from '@fraudwatch/contracts';
import { cx } from './basics';

const ACTIONS: Record<Action, { label: string; icon: LucideIcon; cls: string }> = {
  alert: { label: 'Alert', icon: OctagonAlert, cls: 'fw-pill-alert' },
  review: { label: 'Review', icon: Eye, cls: 'fw-pill-review' },
  allow: { label: 'Allow', icon: CircleCheck, cls: 'fw-pill-allow' },
  unscored: { label: 'Unscored', icon: CircleDashed, cls: '' },
};

/** Policy action with colour + icon + text (never colour alone). */
export function ActionBadge({ action, className }: { action: Action | string | null | undefined; className?: string }) {
  const a = ACTIONS[(action as Action) ?? 'unscored'] ?? ACTIONS.unscored;
  const Icon = a.icon;
  return (
    <span className={cx('fw-pill', a.cls, className)} data-action={action ?? 'unscored'}>
      <Icon aria-hidden="true" />
      {a.label}
    </span>
  );
}

export function LateBadge({ late }: { late: boolean | null | undefined }) {
  if (!late) return null;
  return (
    <span className="fw-pill fw-pill-late" title="Event time was behind the watermark when processed">
      <Clock aria-hidden="true" />
      Late
    </span>
  );
}

const ALERT_STATUS: Record<AlertStatus, { label: string; cls: string }> = {
  open: { label: 'Open', cls: 'fw-pill-neutral' },
  acknowledged: { label: 'Acknowledged', cls: 'fw-pill-info' },
  escalated: { label: 'Escalated', cls: 'fw-pill-review' },
  dismissed: { label: 'Dismissed', cls: '' },
  confirmed_fraud_demo: { label: 'Confirmed fraud (demo)', cls: 'fw-pill-alert' },
};

export function AlertStatusPill({ status }: { status: AlertStatus | string }) {
  const s = ALERT_STATUS[status as AlertStatus] ?? { label: status, cls: '' };
  return <span className={cx('fw-pill', s.cls)}>{s.label}</span>;
}

export function StatusPill({ tone = 'neutral', children, icon }: { tone?: 'alert' | 'review' | 'allow' | 'late' | 'info' | 'neutral' | 'muted'; children: React.ReactNode; icon?: React.ReactNode }) {
  return (
    <span className={cx('fw-pill', tone !== 'muted' && `fw-pill-${tone}`)}>
      {icon}
      {children}
    </span>
  );
}

const HEALTH_LABEL: Record<ComponentStatus, string> = {
  ok: 'OK',
  degraded: 'Degraded',
  down: 'Down',
  idle: 'Idle',
  unknown: 'Unknown',
};

/** Health status: dot shape differs per state (filled / ring / dashed) and always has text. */
export function HealthDot({ status, label, className }: { status: ComponentStatus | string | null | undefined; label?: React.ReactNode; className?: string }) {
  const s = (status && status in HEALTH_LABEL ? status : 'unknown') as ComponentStatus;
  return (
    <span className={cx('fw-health', `fw-health-${s}`, className)} data-status={s}>
      <span className="fw-health-dot" aria-hidden="true" />
      {label ? (
        <span>
          {label}
          <span className="fw-sr-only">: </span>
          <span className="fw-muted"> {HEALTH_LABEL[s]}</span>
        </span>
      ) : (
        <span>{HEALTH_LABEL[s]}</span>
      )}
    </span>
  );
}
