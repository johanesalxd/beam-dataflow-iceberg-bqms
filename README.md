# Apache Beam BigQuery Demo

A demonstration of using Apache Beam with standard BigQueryIO to read from and write to BigQuery tables. This serves as a foundation for future extensions to include Managed I/O for both BigQuery and Iceberg tables.

## What This Demo Does

- **Write Pipeline**: Creates sample employee data and writes it to a BigQuery table
- **Read Pipeline**: Reads all data from the BigQuery table
- **Filtered Read Pipeline**: Demonstrates reading with SQL filters

## Prerequisites

1. **GCP Project Setup**
   - Enable BigQuery API
   - Set up Application Default Credentials: `gcloud auth application-default login`

2. **Required Resources**
   - BigQuery dataset for storing the table

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
   - `REGION`: GCP region for processing

3. **Run the Demo**
   ```bash
   python beam_iceberg_demo.py
   ```

## Sample Data

The demo uses employee records with the following schema:
- `id` (int), `name` (str), `age` (int), `city` (str)
- `salary` (float), `is_active` (bool), `department` (str), `created_at` (timestamp)

## Key Features Demonstrated

- **Standard BigQueryIO**: Uses Apache Beam's proven BigQuery connectors
- **Simple Data Processing**: Direct dictionary handling without schema complications
- **Table Management**: Automatic table creation with explicit schema definition
- **SQL Filtering**: Standard SQL queries for selective data reading

## Expected Output

```
============================================================
APACHE BEAM BIGQUERY DEMO
============================================================

1. Writing sample data to BigQuery table...
2. Reading all data from BigQuery table...
3. Reading filtered data (active Engineering employees, age > 30)...

DEMO COMPLETED SUCCESSFULLY!
============================================================
```

## Future Extensions

This implementation provides a solid foundation for adding:
- **Managed I/O for BigQuery**: Using `managed.BIGQUERY`
- **Managed I/O for Iceberg**: Using `managed.ICEBERG` with BigLake Metastore
- **Dual Writing**: Writing to both BigQuery and Iceberg tables simultaneously

## Troubleshooting

- **Authentication**: Ensure `gcloud auth application-default login` is run
- **Permissions**: Need BigQuery Data Editor role
- **Dataset**: Verify BigQuery dataset exists in your project

## Files

- `beam_iceberg_demo.py`: Main demo script with three pipeline functions
- `config.py`: Configuration settings (update with your project details)
- `requirements.txt`: Python dependencies

## Resources

- [Apache Beam BigQuery I/O](https://beam.apache.org/documentation/io/built-in/google-bigquery/)
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Dataflow Managed I/O for Iceberg](https://cloud.google.com/dataflow/docs/guides/managed-io-iceberg) (for future reference)
