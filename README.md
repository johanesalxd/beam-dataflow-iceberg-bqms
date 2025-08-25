# Apache Beam Iceberg Demo with BigLake Metastore

A demonstration of using Apache Beam with Dataflow Managed I/O to read from and write to Apache Iceberg tables using Google Cloud BigLake Metastore.

## What This Demo Does

- **Write Pipeline**: Creates sample employee data and writes it to an Iceberg table
- **Read Pipeline**: Reads all data from the Iceberg table
- **Filtered Read Pipeline**: Demonstrates reading with SQL-like filters

## Prerequisites

1. **GCP Project Setup**
   - Enable BigQuery, Cloud Storage, and Dataflow APIs
   - Set up Application Default Credentials: `gcloud auth application-default login`

2. **Required Resources**
   - BigQuery dataset (acts as Iceberg catalog)
   - Cloud Storage bucket for Iceberg data files

## Quick Start

1. **Install Dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure Settings**
   Update `config.py` with your GCP project details:
   - `GCP_PROJECT`: Your Google Cloud project ID
   - `BQ_DATASET`: BigQuery dataset name
   - `GCS_WAREHOUSE_PATH`: Cloud Storage path for data files

3. **Run the Demo**
   ```bash
   python beam_iceberg_demo.py
   ```

## Sample Data

The demo uses employee records with the following schema:
- `id` (int), `name` (str), `age` (int), `city` (str)
- `salary` (float), `is_active` (bool), `department` (str), `created_at` (str)

## Key Features Demonstrated

- **Managed I/O**: Uses Apache Beam's Dataflow Managed I/O for Iceberg
- **Schema-aware Processing**: Typed data structures with NamedTuple
- **Partitioning**: Tables partitioned by department for better performance
- **Filtering**: SQL-like predicates for selective data reading

## Expected Output

```
============================================================
APACHE BEAM ICEBERG DEMO WITH BIGLAKE METASTORE
============================================================

1. Writing sample data to Iceberg table...
2. Reading all data from Iceberg table...
3. Reading filtered data (active Engineering employees, age > 30)...

DEMO COMPLETED SUCCESSFULLY!
============================================================
```

## Troubleshooting

- **Authentication**: Ensure `gcloud auth application-default login` is run
- **Permissions**: Need BigQuery Data Editor, Storage Object Admin, Dataflow Developer roles
- **Resources**: Verify BigQuery dataset and Cloud Storage bucket exist

## Files

- `beam_iceberg_demo.py`: Main demo script with three pipeline functions
- `config.py`: Configuration settings (update with your project details)
- `requirements.txt`: Python dependencies

## Resources

- [Dataflow Managed I/O for Iceberg](https://cloud.google.com/dataflow/docs/guides/managed-io-iceberg)
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
