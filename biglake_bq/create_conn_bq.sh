#!/usr/bin/env bash
set -euo pipefail

source .env

gcloud config set project "$GCP_PROJECT_ID"

bq mk --connection \
  --location="$GCP_LOCATION" \
  --project_id="$GCP_PROJECT_ID" \
  --connection_type=CLOUD_RESOURCE \
  "$BQ_CONNECTION_ID"

echo "Connection created: ${GCP_PROJECT_ID}.${GCP_LOCATION}.${BQ_CONNECTION_ID}"