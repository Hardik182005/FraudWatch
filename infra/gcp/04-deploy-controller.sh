#!/usr/bin/env bash
# Deploys the lifecycle controller + API proxy to Cloud Run (min instances 0, Direct VPC egress to the VM).
# Usage: 04-deploy-controller.sh "https://live-url,https://lab-url"   (origins allowed to wake the pipeline)
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
./00-verify-billing.sh
ROOT="$(cd ../.. && pwd)"
ORIGINS="${1:-}"
IDLE="${PIPELINE_IDLE_TIMEOUT_MINUTES:-45}"
MIN_UPTIME="${PIPELINE_MIN_UPTIME_MINUTES:-10}"

NUM=$(G projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')
URL="https://fraudwatch-control-${NUM}.${GCP_REGION}.run.app"

G run deploy fraudwatch-control --source="$ROOT/services/controller" --region="$GCP_REGION" \
  --service-account="$CONTROLLER_SA" --allow-unauthenticated \
  --min-instances=0 --max-instances=3 --cpu=1 --memory=512Mi --timeout=3600 --concurrency=80 \
  --network=default --subnet=default --vpc-egress=private-ranges-only \
  --set-secrets="INTERNAL_API_TOKEN=fraudwatch-internal-api-token:latest,OPERATOR_AUTH_SECRET=fraudwatch-operator-auth-secret:latest" \
  --set-env-vars="^|^GCP_PROJECT_ID=$GCP_PROJECT_ID|GCP_REGION=$GCP_REGION|PIPELINE_VM_NAME=$PIPELINE_VM_NAME|PIPELINE_VM_ZONE=$PIPELINE_VM_ZONE|CONTROL_STATE_COLLECTION=$CONTROL_STATE_COLLECTION|CONTROL_TASK_QUEUE=$CONTROL_TASK_QUEUE|CONTROLLER_URL=$URL|TASKS_SERVICE_ACCOUNT=$CONTROLLER_SA|ALLOWED_ORIGINS=$ORIGINS|AUTO_WAKE_ENABLED=${AUTO_WAKE_ENABLED:-true}|PIPELINE_IDLE_TIMEOUT_MINUTES=$IDLE|PIPELINE_MIN_UPTIME_MINUTES=$MIN_UPTIME|PIPELINE_STARTUP_TIMEOUT_SECONDS=${PIPELINE_STARTUP_TIMEOUT_SECONDS:-600}|PIPELINE_DRAIN_TIMEOUT_SECONDS=${PIPELINE_DRAIN_TIMEOUT_SECONDS:-180}|APP_ENV=production"

actual=$(G run services describe fraudwatch-control --region="$GCP_REGION" --format='value(metadata.annotations."run.googleapis.com/urls")')
echo "controller: $URL"
echo "reported urls: $actual"
curl -fsS "$URL/health" && echo
