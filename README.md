# Apache Beam BigQuery and Managed I/O Demo

A demonstration of Apache Beam with standard BigQueryIO and Managed I/O for BigQuery operations. This demo showcases 8 different pipeline patterns for reading, writing, and copying data.

## What This Demo Does

The demo runs 8 sequential pipelines:

### Standard BigQueryIO Operations
1. **Write Pipeline**: Creates sample employee data and writes it to a BigQuery table
2. **Read Pipeline**: Reads all data from the BigQuery table
3. **Filtered Read Pipeline**: Reads with SQL filters (active Engineering employees, age > 30)
4. **Copy to BigQuery Iceberg**: Copies data to BigQuery Iceberg table using standard BigQueryIO

### Managed I/O Operations
5. **Managed Copy Pipeline**: Copies data using Managed I/O (automatic schema handling)
6. **Managed Filtered Read**: Reads filtered data using Managed I/O with timestamp casting
7. **Copy to BigLake Iceberg**: Copies data to BigLake Iceberg table using Managed I/O
8. **Read from BigLake Iceberg**: Reads filtered data from BigLake Iceberg table using Managed I/O

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

## Tables Created

The demo creates four tables:
- **Standard Table** (`BQ_TABLE_NAME`): Original data written with BigQueryIO
- **Managed Table** (`BQ_MANAGEDIO_TABLE_NAME`): Copy created with Managed I/O
- **BigQuery Iceberg Table** (`BQ_ICEBERG_TABLE_NAME`): Copy created with BigQueryIO
- **BigLake Iceberg Table** (`BQ_ICEBERG_MANAGEDIO_TABLE_NAME`): Copy created with Managed I/O

### Catalog Configuration
Uses BigQuery Metastore Catalog with these settings:
- **Warehouse**: GCS bucket path for Iceberg data storage
- **Catalog Implementation**: `org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog`
- **Project**: GCP project for BigQuery operations
- **Location**: GCP region for processing

### Key Features
- **Automatic Schema Handling**: No manual schema definition required
- **Filter Support**: Native filter expressions for reading data
- **Simplified Configuration**: Config-based approach for table operations

## BigQuery Iceberg Table Limitations

The demo includes a special implementation for copying data to BigQuery Iceberg tables due to specific limitations:

### Limitations
- **No TRUNCATE Support**: Iceberg tables don't support TRUNCATE operations
- **Limited Write Dispositions**: Cannot use `CREATE_IF_NEEDED` or `WRITE_TRUNCATE` with BigQueryIO when writing to Iceberg tables
- **Manual Table Management**: Must use SQL DDL statements to create and manage Iceberg tables

### Solution Implemented
The `copy_table_iceberg()` function works around these limitations by:

1. **Manual Table Creation**: Uses `CREATE TABLE IF NOT EXISTS` with Iceberg-specific options:
   ```sql
   CREATE TABLE IF NOT EXISTS `table_name` (...)
   WITH CONNECTION DEFAULT
   OPTIONS (
       file_format = 'PARQUET',
       table_format = 'ICEBERG',
       storage_uri = 'gs://bucket/path'
   )
   ```

2. **Data Clearing**: Executes `DELETE FROM table WHERE TRUE` to clear existing data (equivalent to TRUNCATE)

3. **Safe Writing**: Uses `WRITE_APPEND` disposition since the table is freshly cleared

This approach ensures the Iceberg table behaves like a standard table with TRUNCATE functionality while respecting BigQuery Iceberg limitations.

## Known Issues

- **Managed I/O Timestamps**: The `created_at` field must be cast as STRING in Managed I/O queries to avoid TypeError when reading directly from tables
- **Iceberg Table Dependencies**: Requires `google-cloud-bigquery` client library for SQL execution

## Troubleshooting

- **Authentication**: Ensure `gcloud auth application-default login` is run
- **Permissions**: Need BigQuery Data Editor role
- **Dataset**: Verify BigQuery dataset exists in your project
- **GCS Bucket**: Ensure the GCS bucket exists and is accessible

## Files

- `beam_iceberg_demo.py`: Main demo script with 8 pipeline functions
- `config.py`: Configuration settings (update with your project details)
- `requirements.txt`: Python dependencies

## Resources

- [Apache Beam BigQuery I/O](https://beam.apache.org/documentation/io/built-in/google-bigquery/)
- [Apache Beam Managed I/O](https://beam.apache.org/documentation/io/managed/)
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Dataflow Managed I/O for Iceberg](https://cloud.google.com/dataflow/docs/guides/managed-io-iceberg)
