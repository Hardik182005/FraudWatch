#!/usr/bin/env bash
# GCE shutdown-script: stop producers first, then processors, then storage, so Kafka, Postgres and the
# retained Flink checkpoints are flushed to the persistent disk before the VM powers off.
exec > >(logger -t fraudwatch-shutdown) 2>&1
APP=/opt/fraudwatch
[ -f "$APP/.env" ] || exit 0
cd "$APP"
dc() { docker compose --env-file .env -p fraudwatch -f infra/compose/compose.yaml -f infra/compose/compose.streaming.yaml -f infra/gcp/vm/compose.vm.yaml "$@"; }
timeout 25 dc stop -t 10 generator flink-submitter spark || true
timeout 30 dc stop -t 20 flink-taskmanager flink-jobmanager result-consumer api scoring || true
timeout 30 dc stop -t 20 kafka postgres || true
