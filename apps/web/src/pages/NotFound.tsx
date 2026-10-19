import { EmptyState } from '@fraudwatch/ui';
import { Link } from 'react-router-dom';

export function NotFound() {
  return (
    <EmptyState title="Page not found" action={<Link className="fw-btn fw-btn-secondary" to="/">Home</Link>}>
      This address does not match any FraudWatch page.
    </EmptyState>
  );
}
