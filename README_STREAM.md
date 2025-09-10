# PubSub to BigQuery Pipeline

A Python Apache Beam pipeline that reads taxi ride data from Google Cloud PubSub and writes it to BigQuery with the full JSON payload preserved.

## Overview

This pipeline:
- Reads real-time taxi ride data from a PubSub subscription tied to the public topic `projects/pubsub-public-data/topics/taxirides-realtime`
- Processes JSON messages containing taxi ride information
- Stores the data in BigQuery with the original JSON payload preserved in a JSON column
- **Optionally**, writes the same data to a BigQuery managed Iceberg table.
- Can run locally using DirectRunner or on Google Cloud Dataflow

## Data Schema

The pipeline reads JSON messages with the following structure:
```json
{
  "ride_id": "7da878a4-37ce-4aa6-a899-07d4054afe96",
  "point_idx": 267,
  "latitude": 40.76561,
  "longitude": -73.96572,
  "timestamp": "2023-07-10T19:00:33.56833-04:00",
  "meter_reading": 11.62449,
  "meter_increment": 0.043537416,
  "ride_status": "enroute",
  "passenger_count": 1
}
```

And creates BigQuery records with this schema:
- `event_time` (TIMESTAMP): Extracted from the message timestamp field
- `processing_time` (TIMESTAMP): When the pipeline processed the message
- `payload` (JSON): The complete original JSON message, stored as a JSON string

## Schema Configuration

The pipeline uses a schema defined in `schemas/generic_table.json`. This file should contain:
```json
[
  {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "payload", "type": "JSON", "mode": "REQUIRED"}
]
```

## Prerequisites

1. **UV Package Manager**: This project uses [UV](https://github.com/astral-sh/uv) for dependency management
2. **Google Cloud SDK**: Install and configure `gcloud`, `bq`, and `gsutil`
3. **Google Cloud Project**: With the following APIs enabled:
   - Dataflow API
   - BigQuery API
   - Cloud Pub/Sub API
4. **Authentication**: Set up Application Default Credentials

### Installing UV

If you don't have UV installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```


### Google Cloud Setup

1. Install Google Cloud SDK:
```bash
# Follow instructions at: https://cloud.google.com/sdk/docs/install
```

2. Authenticate:
```bash
gcloud auth login
gcloud auth application-default login
```

3. Set your project:
```bash
gcloud config set project YOUR_PROJECT_ID
```

4. Enable required APIs:
```bash
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
```

## Configuration

Before running the pipeline, update the configuration variables in the shell scripts:

### For Local Execution (`run_local.sh`)
```bash
PROJECT_ID="your-project-id"  # Your GCP project ID
```

### For Dataflow Execution (`run_dataflow.sh`)
```bash
PROJECT_ID="your-project-id"     # Your GCP project ID
REGION="us-central1"             # Your preferred GCP region
TEMP_BUCKET="gs://your-bucket"   # Your GCS bucket for temp files
```

## Usage

The pipeline can now write to a standard BigQuery table and an optional BigQuery Iceberg table.

### Pre-computation Step: Create Iceberg Table

Since BigQuery-managed Iceberg tables do not support dynamic creation, you must create them manually before running the pipeline. The provided run scripts (`run_local.sh` and `run_dataflow.sh`) handle this automatically.

The script `create_iceberg_tables.py` is called by the run scripts to create the table with the following options:
- `table_format = 'ICEBERG'`
- `file_format = 'PARQUET'`
- A specified `storage_uri` on Google Cloud Storage.

**Note on Iceberg Limitations**: As of this writing, BigQuery-managed Iceberg tables have some limitations:
- They do not support the `JSON` data type, so the `payload` is stored as a `STRING`. JSON functions can still be used on this string at query time.
- They do not support partitioning. The table will be unpartitioned.

### Running Locally

1. Make the scripts executable:
```bash
chmod +x run_local.sh create_iceberg_tables.py
```

2. Run the pipeline:
```bash
./run_local.sh
```

This will:
- Create the BigQuery dataset `dataflow_demo_local` if it doesn't exist.
- Create the BigQuery Iceberg table `taxirides_realtime_iceberg` if it doesn't exist.
- Create the PubSub subscription.
- Install dependencies using UV.
- Run the pipeline locally, writing to both the standard and Iceberg tables.
- Process messages in real-time (press Ctrl+C to stop).

### Running on Dataflow

1. Create a GCS bucket for temporary files if you haven't already:
```bash
gsutil mb gs://your-bucket-name
```

2. Make the scripts executable:
```bash
chmod +x run_dataflow.sh create_iceberg_tables.py
```

3. Run the pipeline:
```bash
./run_dataflow.sh
```

This will:
- Create the GCS bucket if it doesn't exist.
- Create the BigQuery dataset `dataflow_demo` if it doesn't exist.
- Create the BigQuery Iceberg table `taxirides_realtime_iceberg` if it doesn't exist.
- Create the PubSub subscription.
- Install dependencies using UV.
- Submit the job to Dataflow, writing to both the standard and Iceberg tables.
- Provide links to monitor the job and data.

## BigQuery Table Features

The pipeline automatically creates a partitioned table:
- **Partitioning**: Daily partitions based on the `event_time` field
- **Benefits**: Improved query performance and cost optimization when filtering by date

## Dataflow Optimizations

When running on Dataflow, the pipeline automatically enables:
- **Streaming Engine**: Offloads state and timer processing to Google's backend
- **Autoscaling**: Uses `THROUGHPUT_BASED` algorithm for dynamic worker scaling
- **Max Workers**: Default limit of 3 workers (configurable)

### Manual Execution

You can also run the pipeline manually with custom parameters:

```bash
# Install dependencies
uv sync

# Run locally (standard table only)
uv run python pubsub_to_bigquery.py \
    --runner=DirectRunner \
    --project=your-project-id \
    --output_table=your-project-id:dataset.table \
    --streaming

# Run locally (both standard and Iceberg tables)
uv run python pubsub_to_bigquery.py \
    --runner=DirectRunner \
    --project=your-project-id \
    --output_table=your-project-id:dataset.table \
    --output_iceberg_table=your-project-id:dataset.table_iceberg \
    --streaming

# Run on Dataflow (both standard and Iceberg tables)
uv run python pubsub_to_bigquery.py \
    --runner=DataflowRunner \
    --project=your-project-id \
    --region=us-central1 \
    --temp_location=gs://your-bucket/temp \
    --staging_location=gs://your-bucket/staging \
    --output_table=your-project-id:dataset.table \
    --output_iceberg_table=your-project-id:dataset.table_iceberg \
    --streaming \
    --max_num_workers=3
```

## Pipeline Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `--project` | GCP project ID | - | Yes |
| `--output_table` | BigQuery table (project:dataset.table) | - | Yes |
| `--output_iceberg_table` | Optional Iceberg table (project:dataset.table) | - | No |
| `--pubsub_subscription` | PubSub subscription to read from | - | Yes |
| `--runner` | Pipeline runner | `DirectRunner` | No |
| `--region` | GCP region for Dataflow | `us-central1` | No |
| `--temp_location` | GCS temp location | - | For Dataflow |
| `--staging_location` | GCS staging location | - | For Dataflow |
| `--streaming` | Enable streaming mode | False | No |
| `--max_num_workers` | Max Dataflow workers | 3 | No |

## Monitoring

### Local Execution
- Monitor logs in your terminal
- Check BigQuery table: `https://console.cloud.google.com/bigquery`

### Dataflow Execution
- Monitor job: `https://console.cloud.google.com/dataflow`
- Check BigQuery table: `https://console.cloud.google.com/bigquery`

## Querying the Data

Once data is flowing, you can query it in BigQuery:

```sql
-- View recent records
SELECT
  event_time,
  processing_time,
  JSON_EXTRACT_SCALAR(payload, '$.ride_id') as ride_id,
  JSON_EXTRACT_SCALAR(payload, '$.ride_status') as ride_status,
  JSON_EXTRACT_SCALAR(payload, '$.latitude') as latitude,
  JSON_EXTRACT_SCALAR(payload, '$.longitude') as longitude
FROM `your-project.dataflow_demo.taxirides_realtime`
ORDER BY processing_time DESC
LIMIT 10;

-- Count rides by status
SELECT
  JSON_EXTRACT_SCALAR(payload, '$.ride_status') as ride_status,
  COUNT(*) as count
FROM `your-project.dataflow_demo.taxirides_realtime`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY ride_status
ORDER BY count DESC;
```

## Troubleshooting

### Error Handling

The pipeline handles errors gracefully:
- **Malformed JSON**: Messages that can't be parsed are logged and skipped
- **Missing Timestamps**: If a message lacks a timestamp field, the processing time is used
- **Unicode Errors**: Non-UTF8 messages are logged and skipped
- All errors are logged for debugging without stopping the pipeline

### Logs

- **Local**: Logs appear in your terminal
- **Dataflow**: View logs in the Dataflow console

## License

This project is provided as-is for educational and demonstration purposes.
