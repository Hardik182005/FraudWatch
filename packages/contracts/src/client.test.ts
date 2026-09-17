import { describe, expect, it, vi } from 'vitest';
import { ApiError, FraudWatchClient, buildQuery, describeError } from './client';
import { resolveRuntimeConfig, DEFAULT_API_BASE_URL } from './config';

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
  return new Response(body === undefined ? '' : JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', ...headers } });
}

const noSleep = () => Promise.resolve();

async function failure(p: Promise<unknown>): Promise<ApiError> {
  try {
    await p;
  } catch (e) {
    return e as ApiError;
  }
  throw new Error('expected the request to fail');
}

describe('FraudWatchClient', () => {
  it('sends bearer and operator headers and parses JSON', async () => {
    const fetchImpl = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => jsonResponse(200, { ok: true }));
    const c = new FraudWatchClient({ baseUrl: 'http://api/api/v1/', getToken: () => 'sess', getOperatorToken: () => 'op', fetchImpl });
    const out = await c.request<{ ok: boolean }>('/config');
    expect(out.ok).toBe(true);
    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe('http://api/api/v1/config');
    const headers = init!.headers as Record<string, string>;
    expect(headers.Authorization).toBe('Bearer sess');
    expect(headers['X-Operator-Token']).toBe('op');
  });

  it('maps the contract error body into ApiError', async () => {
    const fetchImpl = vi.fn(async () => jsonResponse(429, { error: { code: 'event_cap', message: 'Session event cap reached' } }, { 'Retry-After': '12' }));
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl });
    const err = await failure(c.request('/runs', { method: 'POST', body: {} }));
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(429);
    expect(err.code).toBe('event_cap');
    expect(err.message).toBe('Session event cap reached');
    expect(err.retryAfterS).toBe(12);
    expect(err.isRateLimited).toBe(true);
    expect(describeError(err)).toContain('Demo limit reached');
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('retries idempotent GETs on 503 then succeeds', async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(503, { error: { code: 'kafka_down', message: 'Kafka unavailable' } }))
      .mockResolvedValueOnce(jsonResponse(200, { overall: 'ok' }));
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, sleep: noSleep, retries: 2 });
    const out = await c.request<{ overall: string }>('/health');
    expect(out.overall).toBe('ok');
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('does not retry non-idempotent requests', async () => {
    const fetchImpl = vi.fn(async () => jsonResponse(503, { error: { code: 'down', message: 'x' } }));
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, sleep: noSleep, retries: 3 });
    await expect(c.request('/sessions', { method: 'POST' })).rejects.toBeInstanceOf(ApiError);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('does not retry 404s', async () => {
    const fetchImpl = vi.fn(async () => jsonResponse(404, { error: { code: 'not_found', message: 'nope' } }));
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, sleep: noSleep });
    const err = await failure(c.request('/transactions/x'));
    expect(err.isNotFound).toBe(true);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('classifies network failures and gives up after retries', async () => {
    const fetchImpl = vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    });
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, sleep: noSleep, retries: 2 });
    const err = await failure(c.request('/health'));
    expect(err.kind).toBe('network');
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it('times out slow requests', async () => {
    const fetchImpl = vi.fn(
      (_u: RequestInfo | URL, init?: RequestInit) =>
        new Promise<Response>((_res, rej) => {
          init?.signal?.addEventListener('abort', () => rej(new DOMException('aborted', 'AbortError')));
        }),
    );
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, timeoutMs: 20, retries: 0 });
    const err = await failure(c.request('/health'));
    expect(err.kind).toBe('timeout');
  });

  it('handles non-JSON error bodies', async () => {
    const fetchImpl = vi.fn(async () => new Response('<html>bad gateway</html>', { status: 502, statusText: 'Bad Gateway' }));
    const c = new FraudWatchClient({ baseUrl: 'http://api', fetchImpl, retries: 0 });
    const err = await failure(c.request('/health'));
    expect(err.status).toBe(502);
    expect(err.code).toBe('http_502');
  });

  it('allows a per-request token override (view token)', async () => {
    const fetchImpl = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => jsonResponse(200, {}));
    const c = new FraudWatchClient({ baseUrl: 'http://api', getToken: () => 'sess', fetchImpl });
    await c.getSession('s1', 'view-token');
    expect((fetchImpl.mock.calls[0]![1]!.headers as Record<string, string>).Authorization).toBe('Bearer view-token');
  });

  it('builds query strings skipping empty values', () => {
    expect(buildQuery({ engine: 'flink', run_id: null, action: '', limit: 50 })).toBe('?engine=flink&limit=50');
    expect(buildQuery({})).toBe('');
  });
});

describe('resolveRuntimeConfig', () => {
  it('prefers injected config, trims trailing slashes', () => {
    expect(resolveRuntimeConfig({ apiBaseUrl: 'https://api.example/api/v1/' }, { VITE_API_BASE_URL: 'http://x' }).apiBaseUrl).toBe('https://api.example/api/v1');
  });
  it('ignores unsubstituted placeholders and falls back', () => {
    expect(resolveRuntimeConfig({ apiBaseUrl: '${API_BASE_URL}' }, {}).apiBaseUrl).toBe(DEFAULT_API_BASE_URL);
    expect(resolveRuntimeConfig(undefined, { VITE_API_BASE_URL: 'http://dev:9/api/v1' }).apiBaseUrl).toBe('http://dev:9/api/v1');
  });
});
