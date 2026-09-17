import { getRuntimeConfig, type ShareLink } from '@fraudwatch/contracts';
import { Banner, Button, Dialog, ErrorState, formatRelativeTime } from '@fraudwatch/ui';
import { ExternalLink, FlaskConical } from 'lucide-react';
import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { useSession } from '../session';

export function buildLabLink(base: string, p: { session: string; vt: string; run: string | null; engine: string; action: string | null }): string {
  const url = new URL(base.includes('://') ? base : `${window.location.origin}${base.startsWith('/') ? '' : '/'}${base}`);
  url.searchParams.set('session', p.session);
  url.searchParams.set('vt', p.vt);
  if (p.run) url.searchParams.set('run', p.run);
  url.searchParams.set('engine', p.engine);
  if (p.action) url.searchParams.set('action', p.action);
  return url.toString();
}

/**
 * "Open in Lab": creates a read-only share link (POST /sessions/{id}/share) and carries the current run,
 * engine and action filter across to FraudWatch Lab.
 */
export function OpenInLab() {
  const { mode, sessionId, view, runId, engine, config } = useSession();
  const [params] = useSearchParams();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [share, setShare] = useState<ShareLink | null>(null);

  const labBase = share?.lab_url ?? config.data?.lab_url ?? getRuntimeConfig().labUrl;
  const action = params.get('action') ?? view?.action ?? null;

  if (mode === 'none' || !sessionId) {
    return labBase ? (
      <a className="fw-btn fw-btn-ghost fw-btn-sm" href={labBase} target="_blank" rel="noopener noreferrer">
        <FlaskConical size={16} aria-hidden="true" />
        Lab
      </a>
    ) : null;
  }

  const create = async () => {
    setOpen(true);
    if (mode === 'view') return;
    setBusy(true);
    setError(null);
    try {
      setShare(await api.shareSession(sessionId));
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  };

  const vt = mode === 'view' ? view?.viewToken ?? null : share?.view_token ?? null;
  const link = labBase && vt ? buildLabLink(labBase, { session: sessionId, vt, run: runId, engine, action }) : null;

  return (
    <>
      <Button size="sm" variant="secondary" icon={<FlaskConical size={16} aria-hidden="true" />} onClick={() => void create()}>
        Open in Lab
      </Button>
      <Dialog open={open} onClose={() => setOpen(false)} title="Open this session in FraudWatch Lab" description="A read-only link that keeps the current run, engine and filters.">
        <div className="fw-stack">
          {busy && <p>Creating a read-only link…</p>}
          {error ? <ErrorState error={error} title="Could not create a share link" onRetry={() => void create()} /> : null}
          {!busy && !error && !labBase && <Banner tone="warn">The Lab website address is not configured for this deployment (LAB_APP_URL), so a link cannot be built.</Banner>}
          {link && (
            <>
              <p className="fw-small fw-muted">
                Anyone with this link can view (not change) this demo session{share ? ` until it expires ${formatRelativeTime(share.expires_at)}` : ''}. Run: {runId ?? 'all runs'} · Engine: {engine}
                {action ? ` · Action: ${action}` : ''}.
              </p>
              <a className="fw-btn fw-btn-primary" href={link} target="_blank" rel="noopener noreferrer" data-autofocus data-testid="open-lab-link">
                <ExternalLink size={16} aria-hidden="true" />
                Open FraudWatch Lab
              </a>
            </>
          )}
        </div>
      </Dialog>
    </>
  );
}
