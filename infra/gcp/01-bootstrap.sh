#!/usr/bin/env bash
# One-time project setup: APIs, Firestore, Cloud Tasks queue, least-privilege service accounts,
# Secret Manager entries and the private firewall rule. Idempotent; secret values are never printed.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
./00-verify-billing.sh
ROOT="$(cd ../.. && pwd)"

G services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com \
  cloudtasks.googleapis.com firestore.googleapis.com secretmanager.googleapis.com iam.googleapis.com compute.googleapis.com

if ! G firestore databases describe --database='(default)' >/dev/null 2>&1; then
  G firestore databases create --database='(default)' --location="$GCP_REGION" --type=firestore-native
fi
# TTL keeps the per-IP rate-limit documents from accumulating
G firestore fields ttls update expires_at --collection-group=ratelimits --enable-ttl --async >/dev/null 2>&1 || true

if ! G tasks queues describe "$CONTROL_TASK_QUEUE" --location="$GCP_REGION" >/dev/null 2>&1; then
  G tasks queues create "$CONTROL_TASK_QUEUE" --location="$GCP_REGION" \
    --max-dispatches-per-second=5 --max-concurrent-dispatches=5 --max-attempts=20 --min-backoff=5s --max-backoff=120s
fi

ensure_sa() {
  G iam service-accounts describe "$1@${GCP_PROJECT_ID}.iam.gserviceaccount.com" >/dev/null 2>&1 || \
    G iam service-accounts create "$1" --display-name="$2"
}
ensure_sa "$CONTROLLER_SA_NAME" "FraudWatch lifecycle controller"
ensure_sa "$VM_SA_NAME" "FraudWatch pipeline VM"

if ! G iam roles describe fraudwatchVmLifecycle >/dev/null 2>&1; then
  G iam roles create fraudwatchVmLifecycle --title="FraudWatch VM lifecycle" --stage=GA \
    --permissions=compute.instances.get,compute.instances.start,compute.instances.stop,compute.zoneOperations.get
fi
bind() { G projects add-iam-policy-binding "$GCP_PROJECT_ID" --member="serviceAccount:$1" --role="$2" --condition=None >/dev/null; }
bind "$CONTROLLER_SA" "projects/$GCP_PROJECT_ID/roles/fraudwatchVmLifecycle"
bind "$CONTROLLER_SA" roles/datastore.user
bind "$CONTROLLER_SA" roles/cloudtasks.enqueuer
bind "$VM_SA" roles/logging.logWriter
# Cloud Tasks mints OIDC tokens as the controller identity
G iam service-accounts add-iam-policy-binding "$CONTROLLER_SA" --member="serviceAccount:$CONTROLLER_SA" \
  --role=roles/iam.serviceAccountUser >/dev/null

for name in INTERNAL_API_TOKEN OPERATOR_AUTH_SECRET; do
  value=$(grep -E "^${name}=" "$ROOT/.env" | head -n1 | cut -d= -f2- | tr -d '\r' | sed -e 's/^"//' -e 's/"$//')
  if [ -z "$value" ]; then echo "$name missing in .env" >&2; exit 1; fi
  secret="fraudwatch-$(echo "$name" | tr 'A-Z_' 'a-z-')"
  if ! G secrets describe "$secret" >/dev/null 2>&1; then
    printf '%s' "$value" | G secrets create "$secret" --replication-policy=user-managed --locations="$GCP_REGION" --data-file=- >/dev/null
  elif [ "$(G secrets versions access latest --secret="$secret")" != "$value" ]; then
    printf '%s' "$value" | G secrets versions add "$secret" --data-file=- >/dev/null
  fi
  G secrets add-iam-policy-binding "$secret" --member="serviceAccount:$CONTROLLER_SA" \
    --role=roles/secretmanager.secretAccessor >/dev/null
  echo "secret $secret ready"
done
unset value

if ! G compute firewall-rules describe fraudwatch-api-from-cloudrun >/dev/null 2>&1; then
  G compute firewall-rules create fraudwatch-api-from-cloudrun --network=default --direction=INGRESS \
    --action=ALLOW --rules=tcp:8000 --source-ranges="$EGRESS_SUBNET_RANGE" --target-tags=fraudwatch-pipeline \
    --description="FraudWatch API reachable only from Cloud Run Direct VPC egress (private)"
fi
echo "bootstrap complete"
