#!/usr/bin/env bash
# GCE startup-script: install Docker once, then (re)start the FraudWatch stack on every boot.
set -euo pipefail
exec > >(logger -t fraudwatch-startup) 2>&1
if ! command -v docker >/dev/null; then
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
fi
APP=/opt/fraudwatch
if [ -f "$APP/.env" ] && [ -f "$APP/.images-built" ]; then
  cd "$APP"
  docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml \
    -f infra/gcp/vm/compose.vm.yaml up -d --remove-orphans
fi
