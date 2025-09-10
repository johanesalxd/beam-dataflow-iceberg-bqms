#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
PROJECT_ID="your-project-id" # <-- IMPORTANT: SET YOUR GCP PROJECT ID HERE
BIGQUERY_DATASET="dataflow_demo_local"
BIGQUERY_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.taxirides_realtime"
GCS_BUCKET="your-bucket-name"
BIGQUERY_ICEBERG_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.taxirides_realtime_iceberg"
ICEBERG_STORAGE_URI="${GCS_BUCKET}/${BIGQUERY_DATASET}/taxirides_realtime_iceberg"
PUBSUB_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
SUBSCRIPTION_NAME="taxirides-local-sub"
FULL_SUBSCRIPTION_NAME="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}"

# --- Main Script ---

echo "=== Local Pipeline Execution Script ==="
echo "Project ID: ${PROJECT_ID}"
echo "PubSub Subscription: ${FULL_SUBSCRIPTION_NAME}"
echo "BigQuery Table: ${BIGQUERY_TABLE}"
echo ""

# 1. Create BigQuery dataset if it doesn't exist
echo "Creating BigQuery dataset if it doesn't exist..."
bq show --dataset ${PROJECT_ID}:${BIGQUERY_DATASET} || bq mk --dataset ${PROJECT_ID}:${BIGQUERY_DATASET}

# 2. Create Iceberg table if it doesn't exist
echo "Creating Iceberg table if it doesn't exist..."
uv run python schemas/create_iceberg_tables.py \
    --project_id=${PROJECT_ID} \
    --dataset_id=${BIGQUERY_DATASET} \
    --table_id=taxirides_realtime_iceberg \
    --storage_uri=${ICEBERG_STORAGE_URI}

# 3. Create PubSub subscription if it doesn't exist
echo "Creating PubSub subscription if it doesn't exist..."
gcloud pubsub subscriptions describe ${FULL_SUBSCRIPTION_NAME} >/dev/null 2>&1 || \
    gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} --topic=${PUBSUB_TOPIC}

# 4. Install dependencies with UV
echo "Installing dependencies with UV..."
uv sync

# 5. Run the pipeline
echo "Running the pipeline locally..."
uv run python pubsub_to_bigquery.py \
    --runner=DirectRunner \
    --project=${PROJECT_ID} \
    --output_table=${BIGQUERY_TABLE} \
    --output_iceberg_table=${BIGQUERY_ICEBERG_TABLE} \
    --pubsub_subscription=${FULL_SUBSCRIPTION_NAME}

echo ""
echo "=== Pipeline Started ==="
echo "The pipeline is now running locally and will process messages in real-time."
echo "Press Ctrl+C to stop the pipeline."
