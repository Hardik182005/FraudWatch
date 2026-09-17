#!/usr/bin/env bash
# Shared settings for the FraudWatch GCP deployment. Source this file; it contains no secrets.
export GCP_ACCOUNT="${GCP_ACCOUNT:-sahildeshmukh1616@gmail.com}"
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-fraudwatch-508912}"
export GCP_REGION="${GCP_REGION:-asia-south1}"
export PIPELINE_VM_ZONE="${PIPELINE_VM_ZONE:-asia-south1-a}"
export PIPELINE_VM_NAME="${PIPELINE_VM_NAME:-fraudwatch-pipeline}"
export PIPELINE_VM_TYPE="${PIPELINE_VM_TYPE:-e2-standard-4}"
export PIPELINE_DISK_GB="${PIPELINE_DISK_GB:-60}"
export CONTROL_STATE_COLLECTION="${CONTROL_STATE_COLLECTION:-fraudwatch-runtime}"
export CONTROL_TASK_QUEUE="${CONTROL_TASK_QUEUE:-fraudwatch-lifecycle}"
export CONTROLLER_SA_NAME="fraudwatch-controller"
export VM_SA_NAME="fraudwatch-pipeline-vm"
export CONTROLLER_SA="${CONTROLLER_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
export VM_SA="${VM_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
# default subnet in asia-south1; Cloud Run Direct VPC egress takes addresses from it
export EGRESS_SUBNET_RANGE="${EGRESS_SUBNET_RANGE:-10.160.0.0/20}"
export BILLING_ACCOUNT_EXPECTED="${BILLING_ACCOUNT_EXPECTED:-015A1C-E5B9BD-7AC38E}"
G() { gcloud --account="$GCP_ACCOUNT" --project="$GCP_PROJECT_ID" "$@"; }
