#!/usr/bin/env bash
# Copies the backend source, model artifacts and a VM-specific .env to the pipeline VM, builds the images there
# (no registry storage needed) and starts the stack. Usage: 03-sync-and-build.sh "https://live-url,https://lab-url"
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
ROOT="$(cd ../.. && pwd)"
ORIGINS="${1:-}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Source bundle: backend, streaming jobs, model artifacts and replay pack; no frontend, datasets, venvs or git data.
tar -C "$ROOT" -czf "$WORK/src.tgz" \
  --exclude='node_modules' --exclude='.venv' --exclude='__pycache__' --exclude='target' --exclude='.pytest_cache' \
  --exclude='ml/data/raw' --exclude='ml/data/*.pq' --exclude='*.zip' \
  infra services streaming packages/fwcore tests/streaming ml/artifacts $( [ -d "$ROOT/ml/replay" ] && echo ml/replay ) \
  $( [ -d "$ROOT/ml/results" ] && echo ml/results ) $( [ -d "$ROOT/experiments" ] && echo experiments )

# VM .env: the local .env with cloud overrides. Written with 0600 permissions and never printed.
grep -vE '^(API_BIND|API_HOST_PORT|TRUST_PROXY|ALLOWED_ORIGINS|APP_ENV)=' "$ROOT/.env" | tr -d '\r' > "$WORK/vm.env"
{
  echo "API_BIND=0.0.0.0"
  echo "API_HOST_PORT=8000"
  echo "TRUST_PROXY=true"
  echo "APP_ENV=production"
  [ -n "$ORIGINS" ] && echo "ALLOWED_ORIGINS=$ORIGINS"
} >> "$WORK/vm.env"
chmod 600 "$WORK/vm.env"

G compute scp --zone="$PIPELINE_VM_ZONE" "$WORK/src.tgz" "$WORK/vm.env" "$PIPELINE_VM_NAME":/tmp/
G compute ssh "$PIPELINE_VM_NAME" --zone="$PIPELINE_VM_ZONE" --command='
set -euo pipefail
for i in $(seq 1 60); do command -v docker >/dev/null && sudo docker info >/dev/null 2>&1 && break; sleep 10; done
sudo mkdir -p /opt/fraudwatch
sudo tar -xzf /tmp/src.tgz -C /opt/fraudwatch
sudo install -m 600 /tmp/vm.env /opt/fraudwatch/.env
rm -f /tmp/vm.env /tmp/src.tgz
cd /opt/fraudwatch
DC="sudo docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml -f infra/gcp/vm/compose.vm.yaml"
$DC --profile spark build
sudo touch .images-built
$DC up -d --remove-orphans
$DC ps --format "table {{.Service}}\t{{.State}}\t{{.Status}}"
'
