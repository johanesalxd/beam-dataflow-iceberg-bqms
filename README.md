# Apache Beam BigQuery and Managed I/O Demo

A comprehensive demonstration of Apache Beam with both standard BigQueryIO and Managed I/O for BigQuery operations. This demo showcases 6 different pipeline patterns for reading, writing, and copying data.

## What This Demo Does

The demo runs 6 sequential pipelines:

### Standard BigQueryIO Operations
1. **Write Pipeline**: Creates sample employee data and writes it to a BigQuery table
2. **Read Pipeline**: Reads all data from the BigQuery table
3. **Filtered Read Pipeline**: Demonstrates reading with SQL filters (active Engineering employees, age > 30)
4. **Copy Table to BigQuery Iceberg Pipeline**: Copies data to another BigQuery table using standard BigQueryIO

### Managed I/O Operations
5. **Managed Copy Pipeline**: Copies data using Managed I/O (automatic schema handling)
6. **Managed Filtered Read**: Reads filtered data using Managed I/O with timestamp casting

## Prerequisites

1. **GCP Project Setup**
   - Enable BigQuery API
   - Set up Application Default Credentials: `gcloud auth application-default login`

2. **Required Resources**
   - BigQuery dataset for storing the tables
   - GCS bucket for temporary files (configured in `config.py`)

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
   - `GCS_BUCKET`: GCS bucket for temporary files
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

### Standard BigQueryIO
- **Explicit Schema Definition**: Manual schema specification for table creation
- **Direct Table Operations**: Reading from and writing to specific tables
- **SQL Query Support**: Custom SQL queries for filtered reads
- **Disposition Control**: CREATE_IF_NEEDED and WRITE_TRUNCATE options

### Managed I/O
- **Automatic Schema Handling**: No need to define schemas manually
- **Simplified Configuration**: Streamlined config-based approach
- **Query-based Reading**: SQL queries with automatic type handling
- **Timestamp Workaround**: CAST operations for timestamp compatibility

## Expected Output

```
============================================================
APACHE BEAM BIGQUERY + MANAGED I/O DEMO
============================================================

1. Writing sample data to BigQuery table (BigQueryIO)...
2. Reading all data from BigQuery table (BigQueryIO)...
3. Reading filtered data (BigQueryIO, active Engineering employees, age > 30)...
4. Copying table data using Managed I/O...
5. Reading filtered data using Managed I/O...
6. Copying table data to Managed Iceberg Table...

============================================================
DEMO COMPLETED SUCCESSFULLY!
All BigQueryIO and Managed I/O operations completed!
============================================================
```

## Tables Created

The demo creates three BigQuery tables:
- **Standard Table** (`BQ_TABLE_NAME`): Original data written with BigQueryIO
- **Managed Table** (`BQ_MANAGED_TABLE_NAME`): Copy created with Managed I/O
- **Iceberg Table** (`BQ_ICEBERG_TABLE_NAME`): Copy created with BigQueryIO

## BigQueryIO vs Managed I/O

| Feature | Standard BigQueryIO | Managed I/O |
|---------|-------------------|-------------|
| Schema Definition | Manual schema required | Automatic schema inference |
| Configuration | Detailed parameters | Simplified config object |
| Timestamp Handling | Native support | Requires CAST workaround |
| Table Operations | Full control | Streamlined operations |
| Use Case | Production workloads | Rapid prototyping |

## Known Issues

- **Managed I/O Timestamps**: The `created_at` field must be cast as STRING in Managed I/O queries to avoid `TypeError: int() argument must be a string, a bytes-like object or a real number, not 'NoneType'`

## Troubleshooting

- **Authentication**: Ensure `gcloud auth application-default login` is run
- **Permissions**: Need BigQuery Data Editor role
- **Dataset**: Verify BigQuery dataset exists in your project
- **GCS Bucket**: Ensure the GCS bucket exists and is accessible

## Files

- `beam_iceberg_demo.py`: Main demo script with 6 pipeline functions
- `config.py`: Configuration settings (update with your project details)
- `requirements.txt`: Python dependencies

## Resources

- [Apache Beam BigQuery I/O](https://beam.apache.org/documentation/io/built-in/google-bigquery/)
- [Apache Beam Managed I/O](https://beam.apache.org/documentation/io/managed/)
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Dataflow Managed I/O for Iceberg](https://cloud.google.com/dataflow/docs/guides/managed-io-iceberg)
