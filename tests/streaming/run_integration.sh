#!/usr/bin/env bash
# Streaming integration test harness (isolated Compose project "fwstreamtest"; never touches "fraudwatch").
# Run from anywhere; images fraudwatch/flink:1.0.0, fraudwatch/spark:1.0.0 and fraudwatch/scoring:1.0.0
# must exist (build: docker compose -p fwstreamtest -f infra/compose/compose.streaming.yaml build).
#
#   tests/streaming/run_integration.sh up            # start kafka, scoring, flink, spark (test host ports)
#   tests/streaming/run_integration.sh parity        # replay fixture, compare engines -> out/parity_report.*
#   tests/streaming/run_integration.sh failure       # stop scoring, expect status=failed attempts=3, restart scoring
#   tests/streaming/run_integration.sh restore-flink # cancel job mid-stream, re-run submitter (restores -s chk)
#   tests/streaming/run_integration.sh restore-spark # restart spark container mid-stream (checkpoint restore)
#   tests/streaming/run_integration.sh logs          # save engine logs to out/
#   tests/streaming/run_integration.sh down          # docker compose -p fwstreamtest down -v
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && { pwd -W 2>/dev/null || pwd; })"   # pwd -W: Windows path under Git Bash
OUT="$REPO/tests/streaming/out"
mkdir -p "$OUT"
export FLINK_UI_HOST_PORT="${FLINK_UI_HOST_PORT:-18081}"
export SPARK_UI_HOST_PORT="${SPARK_UI_HOST_PORT:-14040}"
export MSYS_NO_PATHCONV=1
DC=(docker compose -p fwstreamtest -f "$REPO/infra/compose/compose.streaming.yaml" -f "$REPO/tests/streaming/compose.test.yaml" --profile spark)

py() {
  docker run --rm --network fwstreamtest_default --mount "type=bind,source=$REPO,target=/work" -w /work \
    --entrypoint python3 fraudwatch/spark:1.0.0 -u tests/streaming/parity_integration.py "$@"
}

flink_jid() {
  docker run --rm --network fwstreamtest_default --entrypoint bash fraudwatch/flink:1.0.0 -c \
    "curl -fsS http://flink-jobmanager:8081/jobs/overview | jq -r '[.jobs[]|select(.name==\"fraudwatch-flink\" and .state==\"RUNNING\")][0].jid // empty'"
}

case "${1:-}" in
  up)
    "${DC[@]}" up -d
    "${DC[@]}" ps
    ;;
  parity)
    py parity "${@:2}" 2>&1 | tee "$OUT/parity_run.log"
    ;;
  failure)
    "${DC[@]}" stop scoring
    set +e
    py failure "${@:2}" 2>&1 | tee "$OUT/failure_run.log"
    rc=${PIPESTATUS[0]}
    set -e
    "${DC[@]}" start scoring
    exit "$rc"
    ;;
  restore-flink)
    py restore produce --count 600 --seconds 60 > "$OUT/restore_flink_produce.log" 2>&1 &
    prod=$!
    sleep 25
    jid="$(flink_jid)"
    echo "cancelling Flink job $jid"
    docker run --rm --network fwstreamtest_default --entrypoint curl fraudwatch/flink:1.0.0 -fsS -X PATCH \
      "http://flink-jobmanager:8081/jobs/$jid?mode=cancel"
    sleep 10
    "${DC[@]}" up -d --force-recreate --no-deps flink-submitter
    wait "$prod"
    "${DC[@]}" logs --no-color flink-submitter > "$OUT/flink_submitter_restore.log" 2>&1
    py restore check 2>&1 | tee "$OUT/restore_flink_check.log"
    cp "$OUT/restore_report.json" "$OUT/restore_flink_report.json"
    ;;
  restore-spark)
    py restore produce --count 600 --seconds 60 > "$OUT/restore_spark_produce.log" 2>&1 &
    prod=$!
    sleep 25
    "${DC[@]}" restart spark
    wait "$prod"
    py restore check 2>&1 | tee "$OUT/restore_spark_check.log"
    cp "$OUT/restore_report.json" "$OUT/restore_spark_report.json"
    ;;
  logs)
    for s in flink-jobmanager flink-taskmanager flink-submitter spark scoring; do
      "${DC[@]}" logs --no-color "$s" > "$OUT/log_$s.txt" 2>&1 || true
    done
    ;;
  down)
    docker compose -p fwstreamtest down -v
    ;;
  *)
    sed -n '2,13p' "$0"; exit 2 ;;
esac
