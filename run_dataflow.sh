#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
PROJECT_ID="your-project-id" # <-- IMPORTANT: SET YOUR GCP PROJECT ID HERE
REGION="us-central1" # <-- Change to your preferred region
GCS_BUCKET="gs://your-bucket-name" # <-- IMPORTANT: SET YOUR GCS BUCKET HERE
BIGQUERY_DATASET="dataflow_demo"
BIGQUERY_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.taxirides_realtime"
BIGQUERY_ICEBERG_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.taxirides_realtime_iceberg"
# BIGQUERY_AGG_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.taxirides_hourly_agg"
# BIGQUERY_AGG_VIEW="taxirides_hourly_latest"
ICEBERG_STORAGE_URI="${GCS_BUCKET}/${BIGQUERY_DATASET}/taxirides_realtime_iceberg"
PUBSUB_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
SUBSCRIPTION_NAME="taxirides-dataflow-sub"
FULL_SUBSCRIPTION_NAME="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"
JOB_NAME="pubsub-to-bq-$(date +%Y%m%d-%H%M%S)"

# --- Main Script ---

echo "=== Dataflow Deployment Script ==="
echo "Project ID: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Job Name: ${JOB_NAME}"
echo "PubSub Subscription: ${FULL_SUBSCRIPTION_NAME}"
echo "BigQuery Table: ${BIGQUERY_TABLE}"
echo ""

# 1. Create GCS bucket if it doesn't exist
echo "Creating GCS bucket if it doesn't exist..."
gsutil ls ${GCS_BUCKET} 2>/dev/null || gsutil mb -p ${PROJECT_ID} ${GCS_BUCKET}

# 2. Create BigQuery dataset if it doesn't exist
echo "Creating BigQuery dataset if it doesn't exist..."
bq show --dataset ${PROJECT_ID}:${BIGQUERY_DATASET} || bq mk --dataset ${PROJECT_ID}:${BIGQUERY_DATASET}

# 3. Create Iceberg table if it doesn't exist
echo "Creating Iceberg table if it doesn't exist..."
uv run python schemas/create_iceberg_tables.py \
    --project_id=${PROJECT_ID} \
    --dataset_id=${BIGQUERY_DATASET} \
    --table_id=taxirides_realtime_iceberg \
    --storage_uri=${ICEBERG_STORAGE_URI}

# # 4. Create BigQuery view for aggregations (disabled)
# echo "Creating BigQuery view for aggregations..."
# uv run python schemas/create_agg_view.py \
#     --project_id=${PROJECT_ID} \
#     --dataset_id=${BIGQUERY_DATASET} \
#     --view_id=${BIGQUERY_AGG_VIEW} \
#     --source_table_id=taxirides_hourly_agg

# 5. Create PubSub subscription if it doesn't exist
echo "Creating PubSub subscription if it doesn't exist..."
gcloud pubsub subscriptions describe ${FULL_SUBSCRIPTION_NAME} >/dev/null 2>&1 || \
    gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} --topic=${PUBSUB_TOPIC}

# 6. Install dependencies with UV
echo "Installing dependencies with UV..."
uv sync

# 7. Run the pipeline on Dataflow (disable agg branch)
echo "Submitting pipeline to Dataflow..."
uv run python pubsub_to_bigquery.py \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --job_name=${JOB_NAME} \
    --temp_location=${GCS_BUCKET}/temp \
    --staging_location=${GCS_BUCKET}/staging \
    --output_table=${BIGQUERY_TABLE} \
    --output_iceberg_table=${BIGQUERY_ICEBERG_TABLE} \
    --output_agg_table=${BIGQUERY_AGG_TABLE} \
    --pubsub_subscription=${FULL_SUBSCRIPTION_NAME} \
    --max_num_workers=3

echo ""
echo "=== Job Submitted Successfully ==="
echo "The pipeline has been submitted to Dataflow and is starting up..."
