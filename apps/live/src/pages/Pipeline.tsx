import { useApi } from '@fraudwatch/contracts/react';
import { Banner, Button, Card, ErrorState, PipelineDiagram, formatINR, shortId } from '@fraudwatch/ui';
import { X } from 'lucide-react';
import { Link, useSearchParams } from 'react-router-dom';
import { api } from '../api';
import { useSession } from '../session';

export function PipelinePage() {
  const { health } = useSession();
  const [params, setParams] = useSearchParams();
  const traceId = params.get('trace');
  const architecture = useApi(() => api.getArchitecture(), [], { refreshMs: 30000 });
  const trace = useApi((signal) => api.getTransaction(traceId!, signal), [traceId], { enabled: !!traceId });

  const clearTrace = () => {
    const next = new URLSearchParams(params);
    next.delete('trace');
    setParams(next, { replace: true });
  };

  return (
    <div className="fw-stack" style={{ gap: 16 }}>
      <div className="fw-page-head">
        <div>
          <div className="fw-eyebrow">How it fits together</div>
          <h1>Pipeline</h1>
          <p>Transactions flow through Kafka into Apache Flink (and Spark for comparisons), get scored, and land in alerts and storage that both websites read. Select a component for its role, configuration and live status.</p>
        </div>
      </div>

      {traceId && (
        <Banner tone="info">
          <span style={{ flex: 1 }}>
            {trace.data ? (
              <>
                Highlighting the path of {formatINR(trace.data.event.amount_inr)} event <code>{shortId(traceId, 12)}</code>.{' '}
                <Link to={`/tx/${encodeURIComponent(traceId)}`}>Open in inspector</Link>
              </>
            ) : trace.status === 'error' ? (
              <>Could not load the trace for event {shortId(traceId, 12)}.</>
            ) : (
              <>Loading the trace for event {shortId(traceId, 12)}…</>
            )}
          </span>
          <Button size="sm" variant="ghost" icon={<X size={16} aria-hidden="true" />} onClick={clearTrace}>
            Clear trace
          </Button>
        </Banner>
      )}
      {traceId && trace.status === 'error' && !trace.data ? <ErrorState error={trace.error} title="Trace unavailable" onRetry={trace.refetch} /> : null}

      <Card flush>
        <PipelineDiagram
          architecture={architecture.data}
          health={health.data}
          loading={architecture.status === 'loading' && !architecture.data}
          error={architecture.status === 'error' && !architecture.data ? architecture.error : undefined}
          onRetry={architecture.refetch}
          trace={trace.data?.trace ?? null}
          traceLabel={traceId ? `Event ${shortId(traceId, 12)}` : undefined}
        />
      </Card>
    </div>
  );
}
