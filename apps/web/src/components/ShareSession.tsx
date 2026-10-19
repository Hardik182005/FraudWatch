import type { ShareLink } from '@fraudwatch/contracts';
import { Button, Dialog, ErrorState, formatRelativeTime, useToast } from '@fraudwatch/ui';
import { Copy, Share2 } from 'lucide-react';
import { useState } from 'react';
import { useLocation, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { useSession } from '../session';

export function buildShareLink(path: string, p: { session: string; vt: string; run: string | null; engine: string; action: string | null }): string {
  const url = new URL(path, window.location.origin);
  url.searchParams.set('session', p.session);
  url.searchParams.set('vt', p.vt);
  if (p.run) url.searchParams.set('run', p.run);
  url.searchParams.set('engine', p.engine);
  if (p.action) url.searchParams.set('action', p.action);
  return url.toString();
}

/** View-only link to the current page with this session, run, engine and filter (POST /sessions/{id}/share). */
export function ShareSession() {
  const { mode, sessionId, view, runId, engine } = useSession();
  const location = useLocation();
  const [params] = useSearchParams();
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [share, setShare] = useState<ShareLink | null>(null);

  if (mode === 'none' || !sessionId) return null;
  const action = params.get('action') ?? view?.action ?? null;

  const create = async () => {
    setOpen(true);
    if (mode === 'view' || share) return;
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
  const link = vt ? buildShareLink(location.pathname, { session: sessionId, vt, run: runId, engine, action }) : null;
  const copy = async () => {
    if (!link) return;
    try {
      await navigator.clipboard.writeText(link);
      toast({ kind: 'success', title: 'Link copied' });
    } catch {
      toast({ kind: 'error', title: 'Copy failed', message: 'Select the link and copy it manually.' });
    }
  };

  return (
    <>
      <Button size="sm" variant="secondary" icon={<Share2 size={16} aria-hidden="true" />} onClick={() => void create()} data-testid="share-open">
        Share
      </Button>
      <Dialog open={open} onClose={() => setOpen(false)} title="Share a view-only link" description="Opens this page with your run, engine and filters. Read-only.">
        <div className="fw-stack">
          {busy && <p>Creating link…</p>}
          {error ? <ErrorState error={error} title="Could not create a share link" onRetry={() => { setShare(null); void create(); }} /> : null}
          {link && (
            <>
              <input className="fw-input" readOnly value={link} onFocus={(e) => e.currentTarget.select()} aria-label="Share link" data-testid="share-link" />
              <div className="fw-row">
                <Button variant="primary" icon={<Copy size={16} aria-hidden="true" />} onClick={() => void copy()} data-autofocus>
                  Copy link
                </Button>
                <span className="fw-small fw-muted">{share ? `Expires ${formatRelativeTime(share.expires_at)}` : 'Shared view'}</span>
              </div>
            </>
          )}
        </div>
      </Dialog>
    </>
  );
}
