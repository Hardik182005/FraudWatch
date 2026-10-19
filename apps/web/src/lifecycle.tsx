import { getRuntimeConfig } from '@fraudwatch/contracts';
import { notifyBackendReady } from '@fraudwatch/contracts/react';
import { useActivityPings, useLifecycle, type LifecycleView, type WakePhase } from '@fraudwatch/ui';
import { createContext, useContext, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { readStored } from './api';

const Ctx = createContext<LifecycleView | null>(null);

export function useCloud(): LifecycleView {
  const v = useContext(Ctx);
  if (!v) throw new Error('useCloud must be used inside <CloudProvider>');
  return v;
}

/**
 * Wakes the cloud backend on every visit (the VM sleeps after the idle timeout), polls its progress, keeps
 * it awake while the visitor interacts, and reloads every data view once the wake completes.
 * Locally (no controller configured) this is inert.
 */
export function CloudProvider({ children }: { children: ReactNode }) {
  const sessionId = readStored<{ id: string }>('fraudwatch.live.session')?.id ?? readStored<{ sessionId: string }>('fraudwatch.live.view')?.sessionId ?? null;
  const view = useLifecycle({ controlUrl: getRuntimeConfig().controlUrl, sessionId, onReady: notifyBackendReady });
  useActivityPings(view.client, sessionId);
  return <Ctx.Provider value={view}>{children}</Ctx.Provider>;
}

const CHIP: Record<WakePhase, { label: string; tone: 'ok' | 'warn' | 'down' | 'muted' }> = {
  local: { label: 'Local stack', tone: 'muted' },
  checking: { label: 'Checking cloud', tone: 'muted' },
  sleeping: { label: 'Cloud asleep', tone: 'warn' },
  starting_vm: { label: 'Starting VM', tone: 'warn' },
  starting_services: { label: 'Starting services', tone: 'warn' },
  restoring: { label: 'Restoring', tone: 'warn' },
  ready: { label: 'Cloud ready', tone: 'ok' },
  stopping: { label: 'Going to sleep', tone: 'warn' },
  failed: { label: 'Wake failed', tone: 'down' },
  unreachable: { label: 'Controller unreachable', tone: 'down' },
};

/** Compact cloud state in the header; links to the lifecycle timeline. */
export function CloudChip() {
  const { phase, managed } = useCloud();
  if (!managed) return null;
  const c = CHIP[phase];
  const busy = phase === 'starting_vm' || phase === 'starting_services' || phase === 'restoring' || phase === 'checking';
  return (
    <Link to="/system?tab=lifecycle" className="fw-cloud-chip" data-tone={c.tone} data-busy={busy || undefined} data-testid="cloud-chip" title="Cloud lifecycle: the VM wakes on a visit and sleeps after inactivity">
      <span className="fw-cloud-dot" aria-hidden="true" />
      {c.label}
    </Link>
  );
}
