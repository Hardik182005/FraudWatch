import { getRuntimeConfig } from '@fraudwatch/contracts';
import { SystemIntelligence, type SystemTab } from '@fraudwatch/ui';
import { Link, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { useSession } from '../session';

const TABS: SystemTab[] = ['topology', 'logs', 'trace', 'quality', 'operations', 'lifecycle'];

/** Topology, logs, per-event trace, data quality, operations and cloud lifecycle in one place. */
export function SystemPage() {
  const { token, sessionId, runId } = useSession();
  const [params, setParams] = useSearchParams();
  const raw = params.get('tab');
  const tab = TABS.includes(raw as SystemTab) ? (raw as SystemTab) : undefined;
  const eventId = params.get('event');
  return (
    <div className="fw-stack">
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">Operate</div>
          <h1>System intelligence</h1>
        </div>
      </div>
      <SystemIntelligence
        client={api}
        token={token}
        sessionId={sessionId}
        runId={runId}
        controlUrl={getRuntimeConfig().controlUrl}
        tab={tab ?? (eventId ? 'trace' : 'topology')}
        initialEventId={eventId}
        onTabChange={(t) => {
          const next = new URLSearchParams(params);
          next.set('tab', t);
          setParams(next, { replace: true });
        }}
        renderLink={(kind, id, label) => {
          const to = kind === 'tx' ? `/tx/${encodeURIComponent(id)}` : kind === 'alert' ? '/alerts' : kind === 'experiment' ? '/experiments' : '/console';
          return <Link to={to}>{label}</Link>;
        }}
      />
    </div>
  );
}
