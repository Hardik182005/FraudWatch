import type { TxnSummary } from '@fraudwatch/contracts';
import { ActionBadge, DataTable, LateBadge, formatAmount, formatMs, formatScore, formatTimestamp, shortId, type Column } from '@fraudwatch/ui';
import { useNavigate } from 'react-router-dom';

export function TxnTable({ rows, caption, empty }: { rows: TxnSummary[]; caption: string; empty?: React.ReactNode }) {
  const navigate = useNavigate();
  const columns: Column<TxnSummary>[] = [
    { key: 'time', header: 'Event time', render: (r) => <span className="fw-num">{formatTimestamp(r.event_time)}</span> },
    { key: 'account', header: 'Account', render: (r) => <code className="fw-small">{shortId(r.account_id, 10)}</code> },
    { key: 'source', header: 'Source record', render: (r) => r.source_ref ?? <span className="fw-muted">Not provided</span> },
    { key: 'amount', header: 'Amount', align: 'right', render: (r) => <span className="fw-num">{formatAmount(r.amount)}</span> },
    { key: 'score', header: 'Score', align: 'right', render: (r) => <span className="fw-num">{formatScore(r.score)}</span> },
    {
      key: 'action',
      header: 'Action',
      render: (r) => (
        <span className="fw-row" style={{ gap: 4 }}>
          <ActionBadge action={r.action} />
          {r.late && <LateBadge late />}
        </span>
      ),
    },
    { key: 'latency', header: 'End-to-end', align: 'right', render: (r) => <span className="fw-num">{formatMs(r.e2e_ms)}</span> },
  ];
  return (
    <DataTable
      columns={columns}
      rows={rows}
      rowKey={(r) => `${r.engine}:${r.event_id}`}
      caption={caption}
      compact
      empty={empty}
      onRowActivate={(r) => navigate(`/tx/${encodeURIComponent(r.event_id)}?engine=${r.engine}`)}
      rowLabel={(r) => `${r.action} transaction of ${formatAmount(r.amount)} for account ${r.account_id}, score ${formatScore(r.score)}. Open inspector.`}
    />
  );
}
