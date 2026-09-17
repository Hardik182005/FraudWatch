#!/usr/bin/env bash
# Builds both Vite apps locally and deploys each as a static nginx service on Cloud Run (min instances 0).
# The browser reaches the API through the controller's /api/v1 proxy, which wakes and fronts the private VM.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
./00-verify-billing.sh
ROOT="$(cd ../.. && pwd)"
NUM=$(G projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')
run_url() { echo "https://$1-${NUM}.${GCP_REGION}.run.app"; }
CONTROL=$(run_url fraudwatch-control)
LIVE=$(run_url fraudwatch-live)
LAB=$(run_url fraudwatch-lab)

(cd "$ROOT" && npm run build -w apps/live && npm run build -w apps/lab)

for app in live lab; do
  ctx="$(mktemp -d)"
  cp web/Dockerfile web/nginx.conf web/40-fraudwatch-config.sh "$ctx/"
  cp -r "$ROOT/apps/$app/dist" "$ctx/dist"
  rm -f "$ctx/dist/config.js"
  G run deploy "fraudwatch-$app" --source="$ctx" --region="$GCP_REGION" --allow-unauthenticated \
    --min-instances=0 --max-instances=2 --cpu=1 --memory=256Mi --concurrency=200 \
    --set-env-vars="^|^API_BASE_URL=$CONTROL/api/v1|LIVE_APP_URL=$LIVE|LAB_APP_URL=$LAB|CONTROL_URL=$CONTROL"
  rm -rf "$ctx"
done
echo "live:    $LIVE"
echo "lab:     $LAB"
echo "control: $CONTROL"
echo "allowed origins for the controller and VM API: $LIVE,$LAB"
