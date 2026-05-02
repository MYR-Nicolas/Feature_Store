source .env

bq mk \
  --dataset \
  --location="$GCP_LOCATION" \
  "${GCP_PROJECT_ID}:${BQ_DATASET}"
  