import { X } from 'lucide-react';
import { Component, createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState, type ErrorInfo, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { CircleAlert, CircleCheck, Info, Monitor, Moon, Sun } from 'lucide-react';
import { Button, cx } from './basics';
import { Segmented } from './Tabs';
import { useTheme, type ThemePreference } from '../theme';

const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

function useFocusTrap(open: boolean, onClose: () => void) {
  const ref = useRef<HTMLDivElement>(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    if (!open) return;
    const previouslyFocused = document.activeElement as HTMLElement | null;
    const node = ref.current;
    const first = node?.querySelector<HTMLElement>('[data-autofocus]') ?? node?.querySelector<HTMLElement>(FOCUSABLE);
    (first ?? node)?.focus();
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        closeRef.current();
        return;
      }
      if (e.key !== 'Tab' || !node) return;
      const items = Array.from(node.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((el) => el.offsetParent !== null || el === document.activeElement);
      if (!items.length) {
        e.preventDefault();
        return;
      }
      const firstEl = items[0]!;
      const lastEl = items[items.length - 1]!;
      if (e.shiftKey && (document.activeElement === firstEl || !node.contains(document.activeElement))) {
        e.preventDefault();
        lastEl.focus();
      } else if (!e.shiftKey && document.activeElement === lastEl) {
        e.preventDefault();
        firstEl.focus();
      }
    };
    document.addEventListener('keydown', onKey, true);
    return () => {
      document.removeEventListener('keydown', onKey, true);
      document.body.style.overflow = prevOverflow;
      previouslyFocused?.focus?.();
    };
  }, [open]);
  return ref;
}

interface OverlayProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  description?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
}

function Overlay({ open, onClose, title, description, children, footer, kind }: OverlayProps & { kind: 'dialog' | 'drawer' }) {
  const ref = useFocusTrap(open, onClose);
  const titleId = useId();
  if (!open || typeof document === 'undefined') return null;
  return createPortal(
    <div
      className="fw-overlay"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div ref={ref} className={kind === 'dialog' ? 'fw-dialog' : 'fw-drawer'} role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}>
        <div className="fw-overlay-head">
          <div>
            <h2 id={titleId} style={{ fontSize: '1.2rem' }}>
              {title}
            </h2>
            {description && <div className="fw-muted fw-small" style={{ marginTop: 4 }}>{description}</div>}
          </div>
          <Button variant="ghost" iconOnly aria-label="Close" onClick={onClose} icon={<X size={20} aria-hidden="true" />} />
        </div>
        <div className="fw-overlay-body">{children}</div>
        {footer && <div className="fw-overlay-head" style={{ borderTop: '1px solid var(--border)', borderBottom: 0 }}>{footer}</div>}
      </div>
    </div>,
    document.body,
  );
}

export function Dialog(props: OverlayProps) {
  return <Overlay {...props} kind="dialog" />;
}

export function Drawer(props: OverlayProps) {
  return <Overlay {...props} kind="drawer" />;
}

/* ---------------- Toasts ---------------- */

export interface ToastInput {
  kind?: 'info' | 'success' | 'error';
  title: string;
  message?: string;
}
interface ToastItem extends ToastInput {
  id: number;
}

const ToastCtx = createContext<(t: ToastInput) => void>(() => undefined);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([]);
  const seq = useRef(0);
  const dismiss = useCallback((id: number) => setItems((xs) => xs.filter((x) => x.id !== id)), []);
  const push = useCallback(
    (t: ToastInput) => {
      const id = ++seq.current;
      setItems((xs) => [...xs.slice(-3), { ...t, id }]);
      setTimeout(() => dismiss(id), t.kind === 'error' ? 9000 : 5000);
    },
    [dismiss],
  );
  return (
    <ToastCtx.Provider value={push}>
      {children}
      <div className="fw-toasts" aria-live="polite" aria-relevant="additions">
        {items.map((t) => {
          const Icon = t.kind === 'error' ? CircleAlert : t.kind === 'success' ? CircleCheck : Info;
          return (
            <div key={t.id} className={cx('fw-toast', `fw-toast-${t.kind ?? 'info'}`)} role={t.kind === 'error' ? 'alert' : 'status'}>
              <Icon size={18} aria-hidden="true" style={{ marginTop: 3, flex: 'none' }} />
              <div style={{ flex: 1 }}>
                <strong>{t.title}</strong>
                {t.message && <div className="fw-small fw-muted">{t.message}</div>}
              </div>
              <Button variant="ghost" size="sm" iconOnly aria-label="Dismiss notification" onClick={() => dismiss(t.id)} icon={<X size={16} aria-hidden="true" />} />
            </div>
          );
        })}
      </div>
    </ToastCtx.Provider>
  );
}

export function useToast() {
  return useContext(ToastCtx);
}

/* ---------------- Theme toggle ---------------- */

export function ThemeToggle({ compact }: { compact?: boolean }) {
  const { preference, setPreference } = useTheme();
  const options = useMemo(
    () =>
      (
        [
          ['light', 'Light', Sun],
          ['dark', 'Dark', Moon],
          ['system', 'System', Monitor],
        ] as const
      ).map(([value, label, Icon]) => ({
        value: value as ThemePreference,
        title: `${label} theme`,
        label: (
          <>
            <Icon size={16} aria-hidden="true" />
            <span className={compact ? 'fw-sr-only' : undefined}>{label}</span>
          </>
        ),
      })),
    [compact],
  );
  return <Segmented label="Colour theme" options={options} value={preference} onChange={setPreference} />;
}

/* ---------------- Error boundary ---------------- */

export class ErrorBoundary extends Component<{ fallback: (error: Error, reset: () => void) => ReactNode; children: ReactNode; onError?: (e: Error) => void }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  componentDidCatch(error: Error, _info: ErrorInfo) {
    this.props.onError?.(error);
  }
  reset = () => this.setState({ error: null });
  render() {
    if (this.state.error) return this.props.fallback(this.state.error, this.reset);
    return this.props.children;
  }
}
