#!/usr/bin/env bash
set -euo pipefail

source .env

SERVICE_ACCOUNT=$(bq show --connection \
  --location="$GCP_LOCATION" \
  --format=json \
  "${GCP_PROJECT_ID}.${GCP_LOCATION}.${BQ_CONNECTION_ID}" \
  | jq -r '.cloudResource.serviceAccountId')

gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET_NAME}" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"

echo "Access granted to GCS"