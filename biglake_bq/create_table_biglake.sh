source .env && \
bq query --location="$GCP_LOCATION" --use_legacy_sql=false "
CREATE OR REPLACE EXTERNAL TABLE \`${GCP_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE_NAME}\`
(
  open_time TIMESTAMP,
  close_time TIMESTAMP,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  volume FLOAT64,
  symbol STRING,
  interval_value STRING,
  source STRING,
  extracted_at TIMESTAMP
)
WITH CONNECTION \`${GCP_PROJECT_ID}.${GCP_LOCATION}.${BQ_CONNECTION_ID}\`
OPTIONS (
  format = 'PARQUET',
  uris = ['${GCS_URI}']
);
"