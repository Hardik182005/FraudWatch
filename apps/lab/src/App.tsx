import { AppShell, Banner, BrandMark, Button, HealthSummary, ThemeToggle } from '@fraudwatch/ui';
import { BookOpen, FlaskConical, Network, ShieldCheck, TestTube2 } from 'lucide-react';
import { useEffect, type ReactNode } from 'react';
import { Link, NavLink, Navigate, Route, Routes, useLocation } from 'react-router-dom';
import { useLab } from './lab';
import { LabLandingPage } from './landing';
import { ArchitecturePage, Ca1Page, Ca2Page, EvidencePage, ExperimentsPage, HomePage, NotFoundPage } from './pages';
import './lab.css';

const NAV: Array<{ to: string; label: string; icon: ReactNode; end?: boolean }> = [
  { to: '/study', label: 'Study', icon: <BookOpen size={16} aria-hidden="true" />, end: true },
  { to: '/ca1', label: 'CA1', icon: <ShieldCheck size={16} aria-hidden="true" /> },
  { to: '/ca2', label: 'CA2', icon: <TestTube2 size={16} aria-hidden="true" /> },
  { to: '/experiments', label: 'Experiments', icon: <FlaskConical size={16} aria-hidden="true" /> },
  { to: '/architecture', label: 'Architecture', icon: <Network size={16} aria-hidden="true" /> },
];

const TITLES: Record<string, string> = {
  '/study': 'Study lab', '/ca1': 'CA1', '/ca2': 'CA2', '/experiments': 'Experiment lab',
  '/architecture': 'Architecture explorer', '/evidence': 'Evidence',
};

export function App() {
  const { health, view, clearView, liveLink } = useLab();
  const location = useLocation();
  const landing = location.pathname === '/';
  useEffect(() => {
    document.title = landing ? 'FraudWatch Lab · Streaming fraud detection, studied' : `${TITLES[location.pathname] ?? 'Not found'} · FraudWatch Lab`;
  }, [location.pathname, landing]);

  if (landing) return <LabLandingPage />;

  return (
    <AppShell
      brand={<Link to="/study" className="fw-brand" aria-label="FraudWatch Lab study home"><BrandMark product="Lab" /></Link>}
      nav={NAV.map((n) => <NavLink key={n.to} to={n.to} end={n.end}>{n.icon}{n.label}</NavLink>)}
      actions={
        <>
          <HealthSummary health={health.data} error={health.error} loading={health.status === 'loading'} />
          {liveLink('/') && <a className="fw-btn fw-btn-secondary fw-btn-sm" href={liveLink('/')!}>Open Live</a>}
          <ThemeToggle compact />
        </>
      }
      banner={view ? (
        <div className="fw-shell-banner">
          <Banner tone="info">
            <div className="fw-lab-context">
              <span><strong>Shared Live context:</strong> session <code>{view.sessionId || 'unknown'}</code>{view.run && <> · run <code>{view.run}</code></>}.</span>
              <Button size="sm" onClick={clearView}>Clear context</Button>
            </div>
          </Banner>
        </div>
      ) : null}
      footer={<><span>Academic companion for FraudWatch · measured evidence is labelled separately from explanation.</span><Link to="/evidence">Evidence & status</Link></>}
    >
      <Routes>
        <Route path="/study" element={<HomePage />} />
        <Route path="/ca1" element={<Ca1Page />} />
        <Route path="/ca2" element={<Ca2Page />} />
        <Route path="/experiments" element={<ExperimentsPage />} />
        <Route path="/architecture" element={<ArchitecturePage />} />
        <Route path="/viva" element={<Navigate to="/" replace />} />
        <Route path="/evidence" element={<EvidencePage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </AppShell>
  );
}
