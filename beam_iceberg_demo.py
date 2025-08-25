"""
Apache Beam BigQuery Demo (with future Iceberg support)
======================================================

This demo shows how to:
1. Write data to a BigQuery table using standard BigQueryIO
2. Read data from the BigQuery table
3. Apply filters when reading data

Future extensions will include:
- Managed I/O for BigQuery
- Managed I/O for Iceberg tables
- Dual writing capability

Requirements:
- GCP project with BigQuery enabled
- Application Default Credentials configured
- Required Python packages installed (see requirements.txt)
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

# Import configuration
from config import BQ_TABLE_NAME
from config import GCP_PROJECT
from config import REGION
from config import SAMPLE_DATA

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_to_bigquery():
    """
    Pipeline to write sample data to BigQuery table.
    Creates the table if it doesn't exist.
    """
    logger.info("Starting write pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        '--temp_location=gs://johanesa-playground-326616-dataflow-bucket/temp',
    ])

    # Define BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'age', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'city', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'salary', 'type': 'FLOAT', 'mode': 'REQUIRED'},
            {'name': 'is_active', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
            {'name': 'department', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Writing {len(SAMPLE_DATA)} records to {BQ_TABLE_NAME}")

        # Create input data - use dictionaries directly (no schema complications)
        input_data = pipeline | 'CreateSampleData' >> beam.Create(SAMPLE_DATA)

        # Write to BigQuery using standard BigQueryIO
        input_data | 'WriteToBigQuery' >> bigquery.WriteToBigQuery(
            table=BQ_TABLE_NAME,
            schema=table_schema,
            create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.BigQueryDisposition.WRITE_TRUNCATE
        )

        logger.info("Write pipeline completed successfully!")


def read_from_bigquery():
    """
    Pipeline to read all data from BigQuery table.
    """
    logger.info("Starting read pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        '--temp_location=gs://johanesa-playground-326616-dataflow-bucket/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {BQ_TABLE_NAME}")

        # Read from BigQuery using standard BigQueryIO
        read_data = pipeline | 'ReadFromBigQuery' >> bigquery.ReadFromBigQuery(
            table=BQ_TABLE_NAME,
            use_standard_sql=True
        )

        # Print each record
        read_data | 'PrintRecords' >> beam.Map(
            lambda record: logger.info(f"Record: {record}")
        )

        logger.info("Read pipeline completed successfully!")


def read_with_filter():
    """
    Pipeline to read filtered data from BigQuery table.
    Example: Read only active employees in Engineering department.
    """
    logger.info("Starting filtered read pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        '--temp_location=gs://johanesa-playground-326616-dataflow-bucket/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading filtered data from {BQ_TABLE_NAME}")

        # Read from BigQuery with SQL filter
        query = f"""
        SELECT *
        FROM `{BQ_TABLE_NAME}`
        WHERE is_active = true
        AND department = 'Engineering'
        AND age > 30
        """

        filtered_data = pipeline | 'ReadFilteredData' >> bigquery.ReadFromBigQuery(
            query=query,
            use_standard_sql=True
        )

        # Print filtered records
        filtered_data | 'PrintFilteredRecords' >> beam.Map(
            lambda record: logger.info(f"Filtered Record: {record}")
        )

        logger.info("Filtered read pipeline completed successfully!")


def run_demo():
    """
    Run the complete demo: write, read, and filtered read.
    """
    try:
        logger.info("=" * 60)
        logger.info("APACHE BEAM BIGQUERY DEMO")
        logger.info("=" * 60)

        # Step 1: Write data to BigQuery
        logger.info("\n1. Writing sample data to BigQuery table...")
        write_to_bigquery()

        # Step 2: Read all data
        logger.info("\n2. Reading all data from BigQuery table...")
        read_from_bigquery()

        # Step 3: Read with filter
        logger.info(
            "\n3. Reading filtered data (active Engineering employees, age > 30)...")
        read_with_filter()

        logger.info("\n" + "=" * 60)
        logger.info("DEMO COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed with error: {str(e)}")
        raise


if __name__ == '__main__':
    run_demo()
