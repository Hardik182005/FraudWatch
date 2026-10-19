import { ApiError, type ModelMetadata, type ModelResultsResponse } from '@fraudwatch/contracts';
import { useApi } from '@fraudwatch/contracts/react';
import { useEffect, useState } from 'react';
import { api } from '../api';

export interface ModelSnapshot {
  model: ModelMetadata;
  results: ModelResultsResponse;
}

let pending: Promise<ModelSnapshot> | null = null;

/**
 * The exact files the API serves for /model and /model/results (the scoring service returns metadata.json
 * verbatim; the API returns the two results files verbatim), bundled at build time as a lazy chunk.
 * Shown only while the API cannot answer, and always labelled as the bundled artifact.
 */
export function loadModelSnapshot(): Promise<ModelSnapshot> {
  pending ??= Promise.all([
    import('../../../../ml/artifacts/fw-ieee-stream-1.1.0/metadata.json'),
    import('../../../../ml/results/stream_model_results.json'),
    import('../../../../ml/results/reference_model_results.json'),
  ]).then(([meta, stream, reference]) => ({
    model: meta.default as unknown as ModelMetadata,
    results: { stream: stream.default, reference: reference.default } as unknown as ModelResultsResponse,
  }));
  return pending;
}

export type ModelSource = 'api' | 'snapshot' | 'loading' | 'error';

/**
 * Model metadata + evaluation results: from the API when it answers, otherwise from the bundled artifact
 * snapshot so the CA1 evidence is readable while the cloud backend sleeps or wakes. Switches to the API
 * automatically once it answers (useApi refetches when the wake completes).
 */
export function useModelEvidence() {
  const model = useApi((signal) => api.request<ModelMetadata>('/model', { signal }), []);
  const results = useApi((signal) => api.request<ModelResultsResponse>('/model/results', { signal }), []);
  const [snap, setSnap] = useState<ModelSnapshot | null>(null);
  const failed = model.status === 'error' || results.status === 'error';
  useEffect(() => {
    if (failed && !snap) void loadModelSnapshot().then(setSnap).catch(() => undefined);
  }, [failed, snap]);

  const live = model.data && results.data?.stream ? { model: model.data, results: results.data } : null;
  const data = live ?? (failed ? snap : null);
  const source: ModelSource = live ? 'api' : data ? 'snapshot' : failed ? 'error' : 'loading';
  const error = model.error ?? results.error;
  const waking = error instanceof ApiError && error.isPipelineWaking;
  const refetch = () => {
    model.refetch();
    results.refetch();
  };
  return { data, source, error, waking, refetch };
}
