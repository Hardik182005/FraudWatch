import { Menu, X } from 'lucide-react';
import { useEffect, useState, type ReactNode } from 'react';
import { ThemeToggle } from '../components/overlay';

export const TEAM_LINE = 'Group 4: Hardik Hinduja, Avinash Gehi, Siddharth Dey, Sahil Deshmukh';

/** Router-agnostic landing header. `links` are pre-rendered anchors/Links; `cta` is the primary action. */
export function LandingHeader({ brand, links, cta }: { brand: ReactNode; links: ReactNode[]; cta?: ReactNode }) {
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && setOpen(false);
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open]);
  return (
    <header className="fw-lheader">
      <a className="fw-skip-link" href="#main">
        Skip to content
      </a>
      <div className="fw-lheader-inner">
        {brand}
        <nav className="fw-lnav" aria-label="Main" data-open={open || undefined} id="fw-lnav" onClick={(e) => (e.target as HTMLElement).closest('a') && setOpen(false)}>
          {links}
        </nav>
        <div className="fw-lheader-actions">
          <ThemeToggle compact />
          {cta}
          <button type="button" className="fw-btn fw-btn-ghost fw-btn-sm fw-lmenu" aria-expanded={open} aria-controls="fw-lnav" aria-label={open ? 'Close menu' : 'Open menu'} onClick={() => setOpen((o) => !o)}>
            {open ? <X size={18} aria-hidden="true" /> : <Menu size={18} aria-hidden="true" />}
          </button>
        </div>
      </div>
    </header>
  );
}

export function LandingHero({ eyebrow, title, lead, actions, scene, meta }: { eyebrow?: ReactNode; title: [string, string]; lead: ReactNode; actions: ReactNode; scene: ReactNode; meta?: ReactNode }) {
  return (
    <section className="fw-hero" aria-labelledby="fw-hero-title">
      <div className="fw-hero-copy">
        {eyebrow && <div className="fw-hero-eyebrow">{eyebrow}</div>}
        <h1 id="fw-hero-title" className="fw-hero-title">
          <span>{title[0]}</span>
          <span>{title[1]}</span>
        </h1>
        <p className="fw-hero-lead">{lead}</p>
        <div className="fw-hero-actions">{actions}</div>
        {meta && <div className="fw-hero-meta">{meta}</div>}
      </div>
      <div className="fw-hero-scene">{scene}</div>
    </section>
  );
}

export function LandingSection({ id, eyebrow, title, lead, children }: { id?: string; eyebrow?: ReactNode; title: ReactNode; lead?: ReactNode; children: ReactNode }) {
  const headingId = id ? `${id}-title` : undefined;
  return (
    <section className="fw-lsection" id={id} aria-labelledby={headingId}>
      <div className="fw-lsection-head">
        {eyebrow && <div className="fw-eyebrow">{eyebrow}</div>}
        <h2 id={headingId}>{title}</h2>
        {lead && <p className="fw-muted">{lead}</p>}
      </div>
      {children}
    </section>
  );
}

export function LandingPage({ header, children, footer }: { header: ReactNode; children: ReactNode; footer: ReactNode }) {
  return (
    <div className="fw-landing">
      {header}
      <main id="main" tabIndex={-1} className="fw-landing-main">
        {children}
      </main>
      <footer className="fw-lfooter">
        <div className="fw-lfooter-inner">{footer}</div>
      </footer>
    </div>
  );
}

export function StepCard({ n, title, children }: { n: number; title: ReactNode; children: ReactNode }) {
  return (
    <article className="fw-step">
      <span className="fw-step-n" aria-hidden="true">
        {String(n).padStart(2, '0')}
      </span>
      <h3>{title}</h3>
      <div className="fw-small fw-muted">{children}</div>
    </article>
  );
}
