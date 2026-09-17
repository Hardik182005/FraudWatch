#!/usr/bin/env bash
# FraudWatch Flink job submitter (idempotent).
#
# 1. waits for the JobManager REST API and enough free task slots
# 2. if a job named fraudwatch-flink is already active (RUNNING/RESTARTING/CREATED/INITIALIZING/
#    RECONCILING/CANCELLING/FAILING) it does nothing
# 3. otherwise submits the jar, restoring from FLINK_RESTORE_PATH if set, else from the newest
#    *complete* retained checkpoint (a chk-N directory containing _metadata) in the shared volume
# 4. waits until the job is RUNNING and exits 0
#
# FLINK_SUBMITTER_MODE=watch repeats the check every FLINK_SUBMITTER_INTERVAL_S seconds instead of
# exiting (re-submits after a JobManager restart or after the restart strategy is exhausted).
set -euo pipefail

JM_URL="${FLINK_JOBMANAGER_URL:-http://flink-jobmanager:8081}"
JM_HOSTPORT="${JM_URL#http://}"
JOB_NAME="${FLINK_JOB_NAME:-fraudwatch-flink}"
JAR="${FLINK_JOB_JAR:-/opt/flink/usrlib/fraudwatch-flink.jar}"
CKPT_ROOT="${FLINK_CHECKPOINTS_PATH:-/opt/flink/checkpoints}"
PARALLELISM="${FLINK_PARALLELISM:-2}"
WAIT_S="${FLINK_SUBMITTER_WAIT_S:-300}"
KEEP_DIRS="${FLINK_KEEP_CHECKPOINT_JOB_DIRS:-3}"
MODE="${FLINK_SUBMITTER_MODE:-once}"
INTERVAL_S="${FLINK_SUBMITTER_INTERVAL_S:-30}"

log() { echo "[submitter $(date -u +%FT%TZ)] $*"; }

active_job_id() {
  curl -fsS "$JM_URL/jobs/overview" | jq -r --arg n "$JOB_NAME" \
    '[.jobs[] | select(.name == $n and (.state | IN("RUNNING","RESTARTING","CREATED","INITIALIZING","RECONCILING","CANCELLING","FAILING")))][0].jid // empty'
}

wait_for_cluster() {
  local deadline=$(( $(date +%s) + WAIT_S ))
  while true; do
    if ov=$(curl -fsS "$JM_URL/overview" 2>/dev/null); then
      tms=$(echo "$ov" | jq -r '.taskmanagers')
      slots=$(echo "$ov" | jq -r '."slots-total"')
      if [ "${tms:-0}" -ge 1 ] && [ "${slots:-0}" -ge "$PARALLELISM" ]; then
        log "cluster ready: taskmanagers=$tms slots-total=$slots flink-version=$(echo "$ov" | jq -r '."flink-version"')"
        return 0
      fi
      log "waiting for task slots (taskmanagers=$tms slots-total=$slots, need $PARALLELISM)"
    else
      log "waiting for JobManager REST at $JM_URL"
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "timed out after ${WAIT_S}s"
      return 1
    fi
    sleep 3
  done
}

latest_complete_checkpoint() {
  # newest _metadata by modification time; prints the chk-N directory
  find "$CKPT_ROOT" -mindepth 3 -maxdepth 3 -type f -name _metadata -path '*/chk-*/_metadata' -printf '%T@ %h\n' 2>/dev/null \
    | sort -rn | head -n1 | cut -d' ' -f2-
}

prune_old_job_dirs() {
  local keep_path="$1"
  local keep_job=""
  [ -n "$keep_path" ] && keep_job="$(basename "$(dirname "$keep_path")")"
  local n=0
  # job directories newest first (32 hex chars)
  while read -r _ d; do
    n=$((n + 1))
    if [ "$n" -gt "$KEEP_DIRS" ] && [ "$(basename "$d")" != "$keep_job" ]; then
      log "pruning old checkpoint directory $d"
      rm -rf -- "$d" || log "could not prune $d"
    fi
  done < <(find "$CKPT_ROOT" -mindepth 1 -maxdepth 1 -type d -regextype posix-extended -regex '.*/[0-9a-f]{32}' -printf '%T@ %p\n' 2>/dev/null | sort -rn)
}

submit_if_needed() {
  wait_for_cluster
  local jid
  jid="$(active_job_id)"
  if [ -n "$jid" ]; then
    log "job '$JOB_NAME' already active: $jid (nothing to do)"
    return 0
  fi

  local restore="${FLINK_RESTORE_PATH:-}"
  if [ -z "$restore" ]; then
    restore="$(latest_complete_checkpoint || true)"
  fi
  local args=(run -d -m "$JM_HOSTPORT" -p "$PARALLELISM")
  if [ -n "$restore" ]; then
    log "restoring from retained checkpoint/savepoint: $restore"
    args+=(-s "file://$restore")
    prune_old_job_dirs "$restore"
  else
    log "no complete retained checkpoint found under $CKPT_ROOT: starting fresh (committed offsets, else latest)"
  fi
  args+=("$JAR")

  log "flink ${args[*]}"
  /opt/flink/bin/flink "${args[@]}"

  local deadline=$(( $(date +%s) + 120 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state=$(curl -fsS "$JM_URL/jobs/overview" | jq -r --arg n "$JOB_NAME" '[.jobs[] | select(.name == $n)] | sort_by(.["start-time"]) | last | "\(.jid) \(.state)"')
    log "job status: $state"
    case "$state" in
      *" RUNNING") log "submitted OK: jobId=${state%% *}"; return 0 ;;
      *" FAILED"|*" CANCELED") log "job did not start"; return 1 ;;
    esac
    sleep 2
  done
  log "job did not reach RUNNING within 120s"
  return 1
}

if [ "$MODE" = "watch" ]; then
  while true; do
    submit_if_needed || log "submission attempt failed; retrying in ${INTERVAL_S}s"
    sleep "$INTERVAL_S"
  done
else
  submit_if_needed
fi
