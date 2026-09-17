import type {
  Alert,
  AlertPage,
  AlertStatus,
  AppConfig,
  Architecture,
  AssistRequest,
  AssistResponse,
  AuditEntry,
  CreateRunRequest,
  Engine,
  EvidenceItem,
  Experiment,
  ExperimentKind,
  Health,
  InjectResult,
  ModelMetadata,
  ModelResultsResponse,
  OperatorLogin,
  Overview,
  Page,
  Pattern,
  Run,
  RunComparison,
  RunLate,
  Session,
  SessionWithToken,
  ShareLink,
  TransactionDetailResponse,
  TxnSummary,
  WhatIfRequest,
  WhatIfResponse,
  HealthComponent,
} from './types';

export type ApiErrorKind = 'http' | 'network' | 'timeout' | 'aborted' | 'parse';

/** Structured error for every failed API call. */
export class ApiError extends Error {
  readonly kind: ApiErrorKind;
  readonly status: number | null;
  readonly code: string;
  readonly retryAfterS: number | null;

  constructor(opts: { kind: ApiErrorKind; message: string; status?: number | null; code?: string; retryAfterS?: number | null }) {
    super(opts.message);
    this.name = 'ApiError';
    this.kind = opts.kind;
    this.status = opts.status ?? null;
    this.code = opts.code ?? opts.kind;
    this.retryAfterS = opts.retryAfterS ?? null;
  }

  get isRateLimited(): boolean {
    return this.status === 429;
  }
  get isUnauthorized(): boolean {
    return this.status === 401 || this.status === 403;
  }
  get isNotFound(): boolean {
    return this.status === 404;
  }
  get isDependencyDown(): boolean {
    return this.status === 503;
  }
  /** Worth retrying automatically for idempotent requests. */
  get isTransient(): boolean {
    return this.kind === 'network' || this.kind === 'timeout' || this.status === 502 || this.status === 503 || this.status === 504;
  }
}

export function describeError(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.kind === 'network') return 'The FraudWatch API could not be reached. Check your connection or the API status.';
    if (err.kind === 'timeout') return 'The FraudWatch API did not respond in time.';
    if (err.status === 429) return `Demo limit reached: ${err.message}`;
    if (err.status === 503) return `A backend dependency is unavailable: ${err.message}`;
    return err.message;
  }
  if (err instanceof Error) return err.message;
  return 'Unexpected error';
}

export interface ClientOptions {
  baseUrl: string;
  getToken?: () => string | null;
  getOperatorToken?: () => string | null;
  timeoutMs?: number;
  retries?: number;
  retryBaseDelayMs?: number;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
}

export interface RequestOptions {
  method?: 'GET' | 'POST' | 'PATCH' | 'DELETE';
  body?: unknown;
  query?: Record<string, string | number | boolean | null | undefined>;
  signal?: AbortSignal;
  /** Override bearer token (e.g. a view token). */
  token?: string | null;
  timeoutMs?: number;
}

export function buildQuery(query?: RequestOptions['query']): string {
  if (!query) return '';
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query)) {
    if (v === undefined || v === null || v === '') continue;
    params.set(k, String(v));
  }
  const s = params.toString();
  return s ? `?${s}` : '';
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class FraudWatchClient {
  readonly baseUrl: string;
  private readonly opts: ClientOptions;

  constructor(opts: ClientOptions) {
    this.opts = opts;
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
  }

  url(path: string, query?: RequestOptions['query']): string {
    return `${this.baseUrl}${path.startsWith('/') ? path : `/${path}`}${buildQuery(query)}`;
  }

  async request<T>(path: string, ro: RequestOptions = {}): Promise<T> {
    const method = ro.method ?? 'GET';
    const idempotent = method === 'GET';
    const maxAttempts = idempotent ? 1 + (this.opts.retries ?? 2) : 1;
    let lastErr: ApiError | null = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await this.once<T>(path, ro, method);
      } catch (e) {
        const err = e instanceof ApiError ? e : new ApiError({ kind: 'network', message: String(e) });
        lastErr = err;
        if (!idempotent || !err.isTransient || attempt === maxAttempts || ro.signal?.aborted) throw err;
        const delay = (this.opts.retryBaseDelayMs ?? 400) * 2 ** (attempt - 1);
        await (this.opts.sleep ?? defaultSleep)(delay);
      }
    }
    throw lastErr ?? new ApiError({ kind: 'network', message: 'Request failed' });
  }

  private async once<T>(path: string, ro: RequestOptions, method: string): Promise<T> {
    const fetchImpl = this.opts.fetchImpl ?? globalThis.fetch.bind(globalThis);
    const controller = new AbortController();
    const timeoutMs = ro.timeoutMs ?? this.opts.timeoutMs ?? 15000;
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
    const onAbort = () => controller.abort();
    ro.signal?.addEventListener('abort', onAbort);

    const headers: Record<string, string> = { Accept: 'application/json' };
    const token = ro.token !== undefined ? ro.token : this.opts.getToken?.() ?? null;
    if (token) headers.Authorization = `Bearer ${token}`;
    const op = this.opts.getOperatorToken?.();
    if (op) headers['X-Operator-Token'] = op;
    let body: string | undefined;
    if (ro.body !== undefined) {
      headers['Content-Type'] = 'application/json';
      body = JSON.stringify(ro.body);
    }

    let res: Response;
    try {
      res = await fetchImpl(this.url(path, ro.query), { method, headers, body, signal: controller.signal });
    } catch (e) {
      if (timedOut) throw new ApiError({ kind: 'timeout', message: `Request timed out after ${timeoutMs} ms` });
      if (ro.signal?.aborted) throw new ApiError({ kind: 'aborted', message: 'Request cancelled' });
      throw new ApiError({ kind: 'network', message: e instanceof Error ? e.message : 'Network error' });
    } finally {
      clearTimeout(timer);
      ro.signal?.removeEventListener('abort', onAbort);
    }

    const text = await res.text().catch(() => '');
    if (!res.ok) {
      let code = `http_${res.status}`;
      let message = res.statusText || `HTTP ${res.status}`;
      try {
        const parsed = JSON.parse(text) as { error?: { code?: string; message?: string }; detail?: unknown };
        if (parsed?.error) {
          code = parsed.error.code ?? code;
          message = parsed.error.message ?? message;
        } else if (typeof parsed?.detail === 'string') {
          message = parsed.detail;
        }
      } catch {
        /* non-JSON error body */
      }
      const ra = res.headers.get('Retry-After');
      throw new ApiError({ kind: 'http', status: res.status, code, message, retryAfterS: ra && !Number.isNaN(Number(ra)) ? Number(ra) : null });
    }
    if (!text) return undefined as T;
    try {
      return JSON.parse(text) as T;
    } catch {
      throw new ApiError({ kind: 'parse', status: res.status, message: 'The API returned an unreadable response' });
    }
  }

  /* ----- Config & sessions ----- */
  getConfig = () => this.request<AppConfig>('/config');
  createSession = (label?: string) => this.request<SessionWithToken>('/sessions', { method: 'POST', body: label ? { label } : {} });
  getCurrentSession = () => this.request<Session>('/sessions/current');
  getSession = (id: string, token?: string | null) => this.request<Session>(`/sessions/${encodeURIComponent(id)}`, { token });
  resetSession = () => this.request<SessionWithToken>('/sessions/current/reset', { method: 'POST', body: {} });
  shareSession = (id: string) => this.request<ShareLink>(`/sessions/${encodeURIComponent(id)}/share`, { method: 'POST', body: {} });
  operatorLogin = (passphrase: string) => this.request<OperatorLogin>('/operator/login', { method: 'POST', body: { passphrase } });

  /* ----- Runs ----- */
  createRun = (req: CreateRunRequest) => this.request<Run>('/runs', { method: 'POST', body: req });
  listRuns = (sessionId: string) => this.request<Run[]>(`/sessions/${encodeURIComponent(sessionId)}/runs`);
  getRun = (id: string) => this.request<Run>(`/runs/${encodeURIComponent(id)}`);
  runControl = (id: string, action: 'pause' | 'resume' | 'stop') => this.request<Run>(`/runs/${encodeURIComponent(id)}/${action}`, { method: 'POST', body: {} });
  setRunRate = (id: string, rate_eps: number) => this.request<Run>(`/runs/${encodeURIComponent(id)}/rate`, { method: 'POST', body: { rate_eps } });
  inject = (id: string, pattern: Pattern) => this.request<InjectResult>(`/runs/${encodeURIComponent(id)}/inject`, { method: 'POST', body: { pattern } });
  replayRun = (id: string) => this.request<Run>(`/runs/${encodeURIComponent(id)}/replay`, { method: 'POST', body: {} });

  /* ----- Live data ----- */
  getOverview = (sessionId: string, q: { engine?: Engine; run_id?: string | null }, signal?: AbortSignal) =>
    this.request<Overview>(`/sessions/${encodeURIComponent(sessionId)}/overview`, { query: q, signal });
  listTransactions = (
    sessionId: string,
    q: { engine?: Engine; run_id?: string | null; action?: string | null; late?: boolean | null; pattern?: string | null; limit?: number; cursor?: string | null },
    signal?: AbortSignal,
  ) => this.request<Page<TxnSummary>>(`/sessions/${encodeURIComponent(sessionId)}/transactions`, { query: q, signal });
  getTransaction = (eventId: string, signal?: AbortSignal) => this.request<TransactionDetailResponse>(`/transactions/${encodeURIComponent(eventId)}`, { signal });
  streamUrl = (sessionId: string, token: string, cursor?: string | null) =>
    this.url(`/sessions/${encodeURIComponent(sessionId)}/stream`, { token, cursor });

  /* ----- Alerts ----- */
  listAlerts = (
    sessionId: string,
    q: { status?: string | null; action?: string | null; engine?: string | null; min_score?: number | null; limit?: number; cursor?: string | null },
    signal?: AbortSignal,
  ) => this.request<AlertPage>(`/sessions/${encodeURIComponent(sessionId)}/alerts`, { query: q, signal });
  updateAlert = (id: string, status: AlertStatus, note: string) => this.request<Alert>(`/alerts/${encodeURIComponent(id)}`, { method: 'PATCH', body: { status, note } });
  getAudit = (id: string) => this.request<AuditEntry[]>(`/alerts/${encodeURIComponent(id)}/audit`);

  /* ----- Model ----- */
  getModel = () => this.request<ModelMetadata>('/model');
  getModelResults = () => this.request<ModelResultsResponse>('/model/results');
  whatIf = (req: WhatIfRequest) => this.request<WhatIfResponse>('/model/what-if', { method: 'POST', body: req });

  /* ----- Health & architecture ----- */
  getHealth = (signal?: AbortSignal) => this.request<Health>('/health', { signal, timeoutMs: 10000 });
  getArchitecture = () => this.request<Architecture>('/architecture');

  /* ----- CA2 ----- */
  getComparison = (runId: string) => this.request<RunComparison>(`/runs/${encodeURIComponent(runId)}/comparison`);
  getLate = (runId: string) => this.request<RunLate>(`/runs/${encodeURIComponent(runId)}/late`);
  listExperiments = () => this.request<Experiment[]>('/experiments');
  getExperiment = (id: string) => this.request<Experiment>(`/experiments/${encodeURIComponent(id)}`);
  experimentRawCsvUrl = (id: string) => this.url(`/experiments/${encodeURIComponent(id)}/raw.csv`);
  experimentEvidenceUrl = (id: string) => this.url(`/experiments/${encodeURIComponent(id)}/evidence.json`);
  createExperiment = (kind: ExperimentKind, params: Record<string, unknown>) => this.request<Experiment>('/experiments', { method: 'POST', body: { kind, params } });
  cancelExperiment = (id: string) => this.request<Experiment>(`/experiments/${encodeURIComponent(id)}/cancel`, { method: 'POST', body: {} });
  sparkStart = (body: { session_id?: string; run_id?: string; mode: 'live' | 'replay' }) => this.request<HealthComponent>('/ops/spark/start', { method: 'POST', body });
  sparkStop = () => this.request<HealthComponent>('/ops/spark/stop', { method: 'POST', body: {} });

  /* ----- Evidence & AI ----- */
  listEvidence = () => this.request<EvidenceItem[]>('/evidence');
  assist = (req: AssistRequest) => this.request<AssistResponse>('/assist', { method: 'POST', body: req, timeoutMs: 45000 });
}
