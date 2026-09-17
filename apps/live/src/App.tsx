import { AppShell, Banner, BrandMark, Button, HealthSummary, ThemeToggle } from '@fraudwatch/ui';
import { Activity, Bell, Network, SlidersHorizontal } from 'lucide-react';
import { useEffect, type ReactNode } from 'react';
import { Link, NavLink, Route, Routes, useLocation } from 'react-router-dom';
import { OpenInLab } from './components/OpenInLab';
import { AlertsPage } from './pages/Alerts';
import { InspectorPage } from './pages/Inspector';
import { LiveLandingPage } from './pages/Landing';
import { NotFound } from './pages/NotFound';
import { OverviewPage } from './pages/Overview';
import { PipelinePage } from './pages/Pipeline';
import { ScenarioPage } from './pages/Scenario';
import { useSession } from './session';

const NAV: Array<{ to: string; label: string; icon: ReactNode; end?: boolean }> = [
  { to: '/console', label: 'Overview', icon: <Activity size={16} aria-hidden="true" />, end: true },
  { to: '/alerts', label: 'Alerts', icon: <Bell size={16} aria-hidden="true" /> },
  { to: '/scenario', label: 'Scenario', icon: <SlidersHorizontal size={16} aria-hidden="true" /> },
  { to: '/pipeline', label: 'Pipeline', icon: <Network size={16} aria-hidden="true" /> },
];

const TITLES: Record<string, string> = {
  '/console': 'Overview',
  '/alerts': 'Alerts',
  '/scenario': 'Scenario controls',
  '/pipeline': 'Pipeline',
};

export function App() {
  const { health, mode, exitView, view, startDemo, starting } = useSession();
  const location = useLocation();
  const landing = location.pathname === '/';

  useEffect(() => {
    if (landing) {
      document.title = 'FraudWatch Live · Real-time fraud detection on a replayed stream';
      return;
    }
    const page = location.pathname.startsWith('/tx/') ? 'Transaction inspector' : TITLES[location.pathname] ?? 'Not found';
    document.title = `${page} · FraudWatch Live`;
  }, [location.pathname, landing]);

  if (landing) return <LiveLandingPage />;

  return (
    <AppShell
      brand={
        <Link to="/console" className="fw-brand" aria-label="FraudWatch Live console home">
          <BrandMark product="Live" />
        </Link>
      }
      nav={NAV.map((n) => (
        <NavLink key={n.to} to={n.to} end={n.end}>
          {n.icon}
          {n.label}
        </NavLink>
      ))}
      actions={
        <>
          <HealthSummary health={health.data} error={health.error} loading={health.status === 'loading'} />
          <OpenInLab />
          <ThemeToggle compact />
        </>
      }
      banner={
        mode === 'view' && view ? (
          <div className="fw-shell-banner">
            <Banner tone="info">
              <div className="fw-row" style={{ justifyContent: 'space-between', width: '100%' }}>
                <span data-testid="view-only-banner">
                  <strong>View-only link.</strong> You are viewing a shared demo session; scenario controls and alert decisions are disabled.
                </span>
                <span className="fw-row">
                  <Button size="sm" onClick={exitView}>
                    Leave shared view
                  </Button>
                  <Button
                    size="sm"
                    variant="primary"
                    loading={starting}
                    onClick={() => {
                      void startDemo();
                    }}
                  >
                    Start my own demo
                  </Button>
                </span>
              </div>
            </Banner>
          </div>
        ) : null
      }
      footer={
        <>
          <span>
            Educational project, not a banking integration. Transactions are replayed records from the IEEE-CIS Fraud Detection dataset (anonymized Vesta competition data), the account id is a pseudonymous key derived from card1, addr1 and P_emaildomain, and no real payments are processed.
          </span>
          <span>FraudWatch · Group 4</span>
        </>
      }
    >
      <Routes>
        <Route path="/console" element={<OverviewPage />} />
        <Route path="/tx/:eventId" element={<InspectorPage />} />
        <Route path="/alerts" element={<AlertsPage />} />
        <Route path="/scenario" element={<ScenarioPage />} />
        <Route path="/pipeline" element={<PipelinePage />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </AppShell>
  );
}
