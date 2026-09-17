#!/usr/bin/env bash
# Creates the single Apache pipeline VM (Kafka, Flink, Spark on demand, scoring, generator, consumer, API, Postgres).
# The VM is stopped whenever it is idle; while stopped only its persistent disk consumes trial credit.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
./00-verify-billing.sh
if G compute instances describe "$PIPELINE_VM_NAME" --zone="$PIPELINE_VM_ZONE" >/dev/null 2>&1; then
  echo "VM $PIPELINE_VM_NAME already exists"
else
  G compute instances create "$PIPELINE_VM_NAME" --zone="$PIPELINE_VM_ZONE" \
    --machine-type="$PIPELINE_VM_TYPE" \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size="${PIPELINE_DISK_GB}GB" --boot-disk-type=pd-balanced \
    --service-account="$VM_SA" --scopes=logging-write \
    --tags=fraudwatch-pipeline \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata-from-file=startup-script=vm/startup.sh,shutdown-script=vm/shutdown.sh \
    --labels=app=fraudwatch,role=pipeline
fi
G compute instances describe "$PIPELINE_VM_NAME" --zone="$PIPELINE_VM_ZONE" \
  --format='table(name,status,machineType.basename(),networkInterfaces[0].networkIP)'
