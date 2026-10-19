import type { AppConfig, Engine, Health, OperatorLogin } from '@fraudwatch/contracts';
import type { QueryState } from '@fraudwatch/contracts/react';
import { useCallback } from 'react';
import { useSession } from '../session';

export interface LabView {
  sessionId: string;
  viewToken: string | null;
  run: string | null;
  engine: Engine | null;
}

interface LabContextValue {
  config: QueryState<AppConfig> & { refetch: () => void };
  health: QueryState<Health> & { refetch: () => void };
  /** The demo session (owned or shared) whose run the study pages inspect. */
  view: LabView | null;
  operator: OperatorLogin | null;
  login: (passphrase: string) => Promise<void>;
  logout: () => void;
  /** In-site path to the operational console; kept as a function so pages read the same as before the merge. */
  liveLink: (path?: string) => string;
}

/** The study pages' view of the one shared session: Live and Lab are now the same website. */
export function useLab(): LabContextValue {
  const { config, health, sessionId, token, runId, engine, operator, login, logout } = useSession();
  const liveLink = useCallback((path = '/console') => path, []);
  const view: LabView | null = sessionId ? { sessionId, viewToken: token, run: runId, engine } : null;
  return { config, health, view, operator, login, logout, liveLink };
}
