import { nextPreference, useTheme, useToast } from '@fraudwatch/ui';
import { Command, CornerDownLeft, Search } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router-dom';
import { useCloud } from '../lifecycle';
import { useSession } from '../session';

interface Item {
  id: string;
  label: string;
  group: 'Operate' | 'Study' | 'Actions';
  hint?: string;
  run: () => void;
}

export const ROUTES: Array<{ to: string; label: string; group: 'Operate' | 'Study'; hint?: string }> = [
  { to: '/', label: 'Home', group: 'Operate' },
  { to: '/results', label: 'Results', group: 'Operate', hint: 'Plain-English outcomes, history' },
  { to: '/console', label: 'Overview', group: 'Operate', hint: 'Live stream, KPIs' },
  { to: '/alerts', label: 'Alerts', group: 'Operate', hint: 'Review queue' },
  { to: '/scenario', label: 'Scenario', group: 'Operate', hint: 'Fraud mix, rate' },
  { to: '/pipeline', label: 'Pipeline', group: 'Operate', hint: 'Engines, health' },
  { to: '/system', label: 'System', group: 'Operate', hint: 'Logs, trace, quality' },
  { to: '/system?tab=lifecycle', label: 'Cloud lifecycle', group: 'Operate', hint: 'Wake / sleep' },
  { to: '/study', label: 'Study index', group: 'Study' },
  { to: '/ca1', label: 'CA1 · Fraud model', group: 'Study', hint: 'LightGBM, PR-AUC' },
  { to: '/ca2', label: 'CA2 · Flink vs Spark', group: 'Study', hint: 'Latency, parity' },
  { to: '/ca2#beyond', label: 'Stream labs', group: 'Study', hint: 'Watermarks, windows, skew' },
  { to: '/experiments', label: 'Experiments', group: 'Study', hint: 'Your run' },
  { to: '/architecture', label: 'Architecture', group: 'Study', hint: '2D / 3D' },
  { to: '/evidence', label: 'Evidence', group: 'Study', hint: 'Downloads, Q&A' },
];

function isTyping(t: EventTarget | null) {
  const el = t as HTMLElement | null;
  return !!el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName));
}

/** Ctrl/Cmd+K (or "/") jump-to-anything palette. */
export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState('');
  const [sel, setSel] = useState(0);
  const navigate = useNavigate();
  const { startDemo, mode } = useSession();
  const cloud = useCloud();
  const { preference, setPreference } = useTheme();
  const toast = useToast();
  const inputRef = useRef<HTMLInputElement>(null);
  const close = useCallback(() => setOpen(false), []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setOpen((o) => !o);
      } else if (e.key === '/' && !isTyping(e.target) && !open) {
        e.preventDefault();
        setOpen(true);
      }
    };
    const onOpen = () => setOpen(true);
    window.addEventListener('keydown', onKey);
    window.addEventListener('fraudwatch:palette', onOpen);
    return () => {
      window.removeEventListener('keydown', onKey);
      window.removeEventListener('fraudwatch:palette', onOpen);
    };
  }, [open]);

  useEffect(() => {
    if (open) {
      setQ('');
      setSel(0);
      requestAnimationFrame(() => inputRef.current?.focus());
    }
  }, [open]);

  const items = useMemo<Item[]>(() => {
    const go = (to: string) => () => navigate(to);
    const list: Item[] = ROUTES.map((r) => ({ id: r.to, label: r.label, group: r.group, hint: r.hint, run: go(r.to) }));
    if (mode !== 'owner') list.push({ id: 'demo', label: 'Start a demo session', group: 'Actions', run: () => { void startDemo(); navigate('/console'); } });
    if (cloud.managed) list.push({ id: 'wake', label: cloud.phase === 'ready' ? 'Refresh cloud status' : 'Wake the cloud backend', group: 'Actions', hint: cloud.phase, run: () => (cloud.phase === 'ready' ? cloud.refresh() : cloud.retry()) });
    list.push({ id: 'theme', label: `Theme: switch to ${nextPreference(preference)}`, group: 'Actions', run: () => setPreference(nextPreference(preference)) });
    list.push({
      id: 'copy',
      label: 'Copy page link',
      group: 'Actions',
      run: () => {
        navigator.clipboard?.writeText(window.location.href).then(
          () => toast({ kind: 'success', title: 'Link copied' }),
          () => toast({ kind: 'error', title: 'Copy failed' }),
        );
      },
    });
    return list;
  }, [navigate, mode, startDemo, cloud, preference, setPreference, toast]);

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return items;
    return items.filter((i) => `${i.label} ${i.hint ?? ''} ${i.group}`.toLowerCase().includes(needle));
  }, [items, q]);

  useEffect(() => setSel(0), [q]);

  if (!open) return null;

  const choose = (i: Item | undefined) => {
    if (!i) return;
    close();
    i.run();
  };

  return createPortal(
    <div className="fw-cmdk-backdrop" onMouseDown={close}>
      <div className="fw-cmdk" role="dialog" aria-modal="true" aria-label="Command palette" onMouseDown={(e) => e.stopPropagation()} data-testid="command-palette">
        <div className="fw-cmdk-search">
          <Search size={16} aria-hidden="true" />
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Jump to a page or run an action…"
            aria-label="Search commands"
            role="combobox"
            aria-expanded="true"
            aria-controls="fw-cmdk-list"
            aria-activedescendant={filtered[sel] ? `cmdk-${filtered[sel]!.id}` : undefined}
            onKeyDown={(e) => {
              if (e.key === 'ArrowDown') { e.preventDefault(); setSel((s) => Math.min(s + 1, filtered.length - 1)); }
              else if (e.key === 'ArrowUp') { e.preventDefault(); setSel((s) => Math.max(s - 1, 0)); }
              else if (e.key === 'Enter') { e.preventDefault(); choose(filtered[sel]); }
              else if (e.key === 'Escape') { e.preventDefault(); close(); }
            }}
          />
          <kbd>Esc</kbd>
        </div>
        <ul id="fw-cmdk-list" className="fw-cmdk-list" role="listbox">
          {filtered.length === 0 && <li className="fw-cmdk-empty">No matches</li>}
          {filtered.map((i, idx) => (
            <li
              key={i.id}
              id={`cmdk-${i.id}`}
              role="option"
              aria-selected={idx === sel}
              className="fw-cmdk-item"
              data-active={idx === sel || undefined}
              onMouseEnter={() => setSel(idx)}
              onClick={() => choose(i)}
            >
              <span className="fw-cmdk-group">{i.group}</span>
              <span className="fw-cmdk-label">{i.label}</span>
              {i.hint && <span className="fw-cmdk-hint">{i.hint}</span>}
              {idx === sel && <CornerDownLeft size={14} aria-hidden="true" />}
            </li>
          ))}
        </ul>
      </div>
    </div>,
    document.body,
  );
}

export function PaletteButton() {
  const mac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform);
  return (
    <button type="button" className="fw-cmdk-trigger" onClick={() => window.dispatchEvent(new Event('fraudwatch:palette'))} aria-label="Open command palette" data-testid="palette-open">
      <Command size={14} aria-hidden="true" />
      <span className="fw-cmdk-trigger-label">Search</span>
      <kbd>{mac ? '⌘K' : 'Ctrl K'}</kbd>
    </button>
  );
}
