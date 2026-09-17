#!/usr/bin/env bash
# Refuses to continue unless the project is linked to the expected Free Trial billing account.
# Trial credit is consumed; nothing is charged unless the account is manually upgraded (never do that).
set -euo pipefail
source "$(dirname "$0")/env.sh"
acct=$(G billing projects describe "$GCP_PROJECT_ID" --format='value(billingAccountName)')
acct="${acct##*/}"
if [ "$acct" != "$BILLING_ACCOUNT_EXPECTED" ]; then
  echo "billing account is '$acct', expected $BILLING_ACCOUNT_EXPECTED - stopping" >&2
  exit 1
fi
echo "project $GCP_PROJECT_ID -> billing account $acct (Free Trial; check Console > Billing > Credits)"
