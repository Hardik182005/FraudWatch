import { AppShell, Banner, BrandMark, Button, HealthSummary, LoadingSkeleton, ThemeToggle, WakeBanner } from '@fraudwatch/ui';
import { Activity, Bell, PieChart, BookOpen, Boxes, Brain, Cpu, FileCheck2, FlaskConical, GitCompareArrows, Network, SlidersHorizontal } from 'lucide-react';
import { lazy, Suspense, useEffect, type ReactNode } from 'react';
import { Link, Navigate, NavLink, Route, Routes, useLocation } from 'react-router-dom';
import { CommandPalette, PaletteButton } from './components/CommandPalette';
import { ShareSession } from './components/ShareSession';
import { CloudChip, useCloud } from './lifecycle';
import { AlertsPage } from './pages/Alerts';
import { InspectorPage } from './pages/Inspector';
import { HomeLanding } from './pages/Landing';
import { NotFound } from './pages/NotFound';
import { OverviewPage } from './pages/Overview';
import { PipelinePage } from './pages/Pipeline';
import { ScenarioPage } from './pages/Scenario';
import { useSession } from './session';
import './lab/lab.css';
import './app.css';

const lab = () => import('./lab/pages');
const StudyPage = lazy(() => lab().then((m) => ({ default: m.HomePage })));
const Ca1Page = lazy(() => lab().then((m) => ({ default: m.Ca1Page })));
const Ca2Page = lazy(() => lab().then((m) => ({ default: m.Ca2Page })));
const ExperimentsPage = lazy(() => lab().then((m) => ({ default: m.ExperimentsPage })));
const ArchitecturePage = lazy(() => lab().then((m) => ({ default: m.ArchitecturePage })));
const EvidencePage = lazy(() => lab().then((m) => ({ default: m.EvidencePage })));
const ResultsPage = lazy(() => import('./pages/Results').then((m) => ({ default: m.ResultsPage })));
const SystemPage = lazy(() => import('./pages/System').then((m) => ({ default: m.SystemPage })));

type NavItem = { to: string; label: string; icon: ReactNode; end?: boolean };
const i = (C: typeof Activity) => <C size={16} aria-hidden="true" />;

const NAV: Array<{ group: string; items: NavItem[] }> = [
  {
    group: 'Operate',
    items: [
      { to: '/results', label: 'Results', icon: i(PieChart) },
      { to: '/console', label: 'Overview', icon: i(Activity), end: true },
      { to: '/alerts', label: 'Alerts', icon: i(Bell) },
      { to: '/scenario', label: 'Scenario', icon: i(SlidersHorizontal) },
      { to: '/pipeline', label: 'Pipeline', icon: i(Network) },
      { to: '/system', label: 'System', icon: i(Cpu) },
    ],
  },
  {
    group: 'Study',
    items: [
      { to: '/study', label: 'Index', icon: i(BookOpen) },
      { to: '/ca1', label: 'CA1', icon: i(Brain) },
      { to: '/ca2', label: 'CA2', icon: i(GitCompareArrows) },
      { to: '/experiments', label: 'Experiments', icon: i(FlaskConical) },
      { to: '/architecture', label: 'Architecture', icon: i(Boxes) },
      { to: '/evidence', label: 'Evidence', icon: i(FileCheck2) },
    ],
  },
];

const TITLES: Record<string, string> = {
  '/results': 'Results',
  '/console': 'Overview',
  '/alerts': 'Alerts',
  '/scenario': 'Scenario',
  '/pipeline': 'Pipeline',
  '/system': 'System',
  '/study': 'Study',
  '/ca1': 'CA1 · Fraud model',
  '/ca2': 'CA2 · Flink vs Spark',
  '/experiments': 'Experiments',
  '/architecture': 'Architecture',
  '/evidence': 'Evidence',
};

function CloudBanner() {
  const cloud = useCloud();
  if (!cloud.managed || cloud.phase === 'ready' || cloud.phase === 'checking') return null;
  return (
    <div className="fw-shell-banner">
      <WakeBanner view={cloud} />
    </div>
  );
}

export function App() {
  const { health, mode, exitView, view, startDemo, starting } = useSession();
  const location = useLocation();
  const landing = location.pathname === '/';

  useEffect(() => {
    if (landing) {
      document.title = 'FraudWatch · Real-time fraud detection, CA1 + CA2';
      return;
    }
    const page = location.pathname.startsWith('/tx/') ? 'Transaction' : TITLES[location.pathname] ?? 'Not found';
    document.title = `${page} · FraudWatch`;
  }, [location.pathname, landing]);

  if (landing) {
    return (
      <>
        <HomeLanding />
        <CommandPalette />
      </>
    );
  }

  return (
    <>
      <AppShell
        brand={
          <Link to="/" className="fw-brand" aria-label="FraudWatch home">
            <BrandMark />
          </Link>
        }
        nav={NAV.map((g) => (
          <span key={g.group} className="fw-nav-group" role="group" aria-label={g.group}>
            <span className="fw-nav-group-label" aria-hidden="true">{g.group}</span>
            {g.items.map((n) => (
              <NavLink key={n.to} to={n.to} end={n.end}>
                {n.icon}
                {n.label}
              </NavLink>
            ))}
          </span>
        ))}
        actions={
          <>
            <PaletteButton />
            <CloudChip />
            <HealthSummary health={health.data} error={health.error} loading={health.status === 'loading'} />
            <ShareSession />
            <ThemeToggle compact />
          </>
        }
        banner={
          <>
            <CloudBanner />
            {mode === 'view' && view ? (
              <div className="fw-shell-banner">
                <Banner tone="info">
                  <div className="fw-row" style={{ justifyContent: 'space-between', width: '100%' }}>
                    <span data-testid="view-only-banner">
                      <strong>View-only link.</strong> Controls and decisions are disabled.
                    </span>
                    <span className="fw-row">
                      <Button size="sm" onClick={exitView}>
                        Leave
                      </Button>
                      <Button size="sm" variant="primary" loading={starting} onClick={() => void startDemo()}>
                        Start my own demo
                      </Button>
                    </span>
                  </div>
                </Banner>
              </div>
            ) : null}
          </>
        }
        footer={
          <>
            <span>Educational project. Replayed IEEE-CIS records; no real payments.</span>
            <span>FraudWatch · Group 4</span>
          </>
        }
      >
        <Suspense fallback={<LoadingSkeleton lines={6} label="Loading page" />}>
        <Routes>
          <Route path="/results" element={<ResultsPage />} />
          <Route path="/console" element={<OverviewPage />} />
          <Route path="/tx/:eventId" element={<InspectorPage />} />
          <Route path="/alerts" element={<AlertsPage />} />
          <Route path="/scenario" element={<ScenarioPage />} />
          <Route path="/pipeline" element={<PipelinePage />} />
          <Route path="/system" element={<SystemPage />} />
          <Route path="/study" element={<StudyPage />} />
          <Route path="/ca1" element={<Ca1Page />} />
          <Route path="/ca2" element={<Ca2Page />} />
          <Route path="/experiments" element={<ExperimentsPage />} />
          <Route path="/architecture" element={<ArchitecturePage />} />
          <Route path="/evidence" element={<EvidencePage />} />
          <Route path="/lab" element={<Navigate to="/study" replace />} />
          <Route path="/viva" element={<Navigate to="/evidence" replace />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
        </Suspense>
      </AppShell>
      <CommandPalette />
    </>
  );
}
