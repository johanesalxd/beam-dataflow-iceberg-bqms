"""
Apache Beam BigQuery and Managed I/O Demo
==========================================

This demo demonstrates 8 different Apache Beam pipelines:

Standard BigQueryIO operations:
1. Write sample data to BigQuery table
2. Read all data from BigQuery table
3. Read filtered data with SQL queries
4. Copy table data to BigQuery Iceberg table

Managed I/O operations:
5. Copy table data using Managed I/O
6. Read filtered data using Managed I/O
7. Copy table data to Iceberg using Managed I/O
8. Read filtered data from Iceberg using Managed I/O

Requirements:
- GCP project with BigQuery enabled
- Application Default Credentials configured
- Required Python packages installed (see requirements.txt)
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import managed
from google.cloud import bigquery as bq_client

# Import configuration
from config import BQ_DATASET
from config import BQ_ICEBERG_MANAGEDIO_TABLE_NAME
from config import BQ_ICEBERG_TABLE_NAME
from config import BQ_MANAGEDIO_TABLE_NAME
from config import BQ_TABLE_NAME
from config import GCP_PROJECT
from config import GCS_BUCKET
from config import REGION
from config import SAMPLE_DATA

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_to_bigquery():
    """Write sample data to BigQuery table."""
    logger.info("Starting write pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Writing {len(SAMPLE_DATA)} records to {BQ_TABLE_NAME}")

        input_data = pipeline | 'CreateSampleData' >> beam.Create(SAMPLE_DATA)

        input_data | 'WriteToBigQuery' >> bigquery.WriteToBigQuery(
            table=BQ_TABLE_NAME,
            create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.BigQueryDisposition.WRITE_TRUNCATE
        )

        logger.info("Write pipeline completed successfully!")


def read_from_bigquery():
    """Read all data from BigQuery table."""
    logger.info("Starting read pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {BQ_TABLE_NAME}")

        read_data = pipeline | 'ReadFromBigQuery' >> bigquery.ReadFromBigQuery(
            table=BQ_TABLE_NAME,
            use_standard_sql=True
        )

        read_data | 'PrintRecords' >> beam.Map(
            lambda record: logger.info(f"Record: {record}")
        )

        logger.info("Read pipeline completed successfully!")


def read_with_filter():
    """Read filtered data from BigQuery table (active Engineering employees, age > 30)."""
    logger.info("Starting filtered read pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading filtered data from {BQ_TABLE_NAME}")

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

        filtered_data | 'PrintFilteredRecords' >> beam.Map(
            lambda record: logger.info(f"Filtered Record: {record}")
        )

        logger.info("Filtered read pipeline completed successfully!")


def copy_table_iceberg():
    """Copy data from BigQuery table to BigQuery Iceberg table."""
    logger.info("Starting copy table to Iceberg pipeline...")

    # Initialize BigQuery client for table management
    client = bq_client.Client(project=GCP_PROJECT)

    # Create Iceberg table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_ICEBERG_TABLE_NAME}` (
        id INTEGER,
        name STRING,
        age INTEGER,
        city STRING,
        salary FLOAT64,
        is_active BOOLEAN,
        department STRING,
        created_at TIMESTAMP
    )
    WITH CONNECTION DEFAULT
    OPTIONS (
        file_format = 'PARQUET',
        table_format = 'ICEBERG',
        storage_uri = '{GCS_BUCKET}/managed_iceberg_on_bq'
    )
    """

    logger.info(
        f"Creating Iceberg table if not exists: {BQ_ICEBERG_TABLE_NAME}")
    client.query(create_table_sql).result()

    # Delete existing data from Iceberg table
    delete_sql = f"DELETE FROM `{BQ_ICEBERG_TABLE_NAME}` WHERE TRUE"
    logger.info(f"Clearing existing data from {BQ_ICEBERG_TABLE_NAME}")
    client.query(delete_sql).result()

    # Run Beam pipeline to copy data
    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {BQ_TABLE_NAME} using BigQueryIO")

        read_data = pipeline | 'ReadFromBigQuery' >> bigquery.ReadFromBigQuery(
            table=BQ_TABLE_NAME,
            use_standard_sql=True
        )

        read_data | 'WriteToIcebergTable' >> bigquery.WriteToBigQuery(
            table=BQ_ICEBERG_TABLE_NAME,
            write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND
        )

        logger.info("Copy table to Iceberg pipeline completed successfully!")


def copy_table_with_managed_io():
    """Copy data from BigQuery table to another BigQuery table using Managed I/O."""
    logger.info("Starting copy table with Managed I/O pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {BQ_TABLE_NAME} using Managed I/O")

        read_data = pipeline | 'ReadWithManagedIO' >> managed.Read(
            managed.BIGQUERY,
            config={
                'table': BQ_TABLE_NAME,
            }
        )

        # Managed I/O handles schema conversion automatically
        read_data | 'WriteWithManagedIO' >> managed.Write(
            managed.BIGQUERY,
            config={
                'table': BQ_MANAGEDIO_TABLE_NAME,
                'create_disposition': 'CREATE_IF_NEEDED',
                'write_disposition': 'WRITE_TRUNCATE'
            }
        )

        logger.info(
            "Copy table with Managed I/O pipeline completed successfully!")


def read_filtered_with_managed_io():
    """Read filtered data from BigQuery table using Managed I/O."""
    logger.info("Starting filtered read with Managed I/O pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(
            f"Reading filtered data from {BQ_MANAGEDIO_TABLE_NAME} using Managed I/O")

        # Note: created_at is cast as STRING to avoid TypeError with Managed I/O
        filtered_data = pipeline | 'ReadFilteredWithManagedIO' >> managed.Read(
            managed.BIGQUERY,
            config={
                'query': f"""
                SELECT
                    id,
                    name,
                    age,
                    city,
                    salary,
                    is_active,
                    department,
                    CAST(created_at AS STRING) as created_at
                FROM `{BQ_MANAGEDIO_TABLE_NAME}`
                WHERE is_active = true
                AND department = 'Engineering'
                AND age > 30
                """
            }
        )

        filtered_data | 'PrintFilteredManagedRecords' >> beam.Map(
            lambda record: logger.info(
                f"Filtered Managed I/O Record: {record}")
        )

        logger.info(
            "Filtered read with Managed I/O pipeline completed successfully!")


def copy_table_to_iceberg_with_managed_io():
    """Copy data from BigQuery table to Iceberg table using Managed I/O."""
    logger.info("Starting copy table to Iceberg with Managed I/O pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {BQ_TABLE_NAME} using Managed I/O")

        read_data = pipeline | 'ReadFromBigQueryManagedIO' >> managed.Read(
            managed.BIGQUERY,
            config={
                'table': BQ_TABLE_NAME,
            }
        )

        # Simple catalog configuration
        catalog_config = {
            'warehouse': f'{GCS_BUCKET}/iceberg_on_bq_managedio'
        }

        # Managed I/O handles table creation and schema conversion
        read_data | 'WriteToIcebergManagedIO' >> managed.Write(
            managed.ICEBERG,
            config={
                'table': BQ_ICEBERG_MANAGEDIO_TABLE_NAME,
                'catalog_name': BQ_DATASET,
                'catalog_properties': catalog_config
            }
        )

        logger.info(
            "Copy table to Iceberg with Managed I/O pipeline completed successfully!")


def read_from_iceberg_with_managed_io():
    """Read filtered data from Iceberg table using Managed I/O."""
    logger.info("Starting read from Iceberg with Managed I/O pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        f'--temp_location={GCS_BUCKET}/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(
            f"Reading filtered data from {BQ_ICEBERG_MANAGEDIO_TABLE_NAME} using Managed I/O")

        # Simple catalog configuration
        catalog_config = {
            'warehouse': f'{GCS_BUCKET}/iceberg_on_bq_managedio'
        }

        filtered_data = pipeline | 'ReadFromIcebergManagedIO' >> managed.Read(
            managed.ICEBERG,
            config={
                'table': BQ_ICEBERG_MANAGEDIO_TABLE_NAME,
                'catalog_name': BQ_DATASET,
                'catalog_properties': catalog_config,
                'filter': "is_active = true AND department = 'Engineering' AND age > 30"
            }
        )

        filtered_data | 'PrintIcebergManagedRecords' >> beam.Map(
            lambda record: logger.info(f"Iceberg Managed I/O Record: {record}")
        )

        logger.info(
            "Read from Iceberg with Managed I/O pipeline completed successfully!")


def run_demo():
    """Run the complete demo: BigQueryIO + Managed I/O demonstrations."""
    try:
        logger.info("=" * 60)
        logger.info("APACHE BEAM BIGQUERY + MANAGED I/O DEMO")
        logger.info("=" * 60)

        logger.info(
            "\n1. Writing sample data to BigQuery table (BigQueryIO)...")
        write_to_bigquery()

        logger.info("\n2. Reading all data from BigQuery table (BigQueryIO)...")
        read_from_bigquery()

        logger.info(
            "\n3. Reading filtered data (BigQueryIO, active Engineering employees, age > 30)...")
        read_with_filter()

        logger.info("\n4. Copying table data using Managed I/O...")
        copy_table_with_managed_io()

        logger.info("\n5. Reading filtered data using Managed I/O...")
        read_filtered_with_managed_io()

        logger.info("\n6. Copying table data to Managed Iceberg Table...")
        copy_table_iceberg()

        logger.info("\n7. Copying table to Iceberg using Managed I/O...")
        copy_table_to_iceberg_with_managed_io()

        logger.info(
            "\n8. Reading filtered data from Iceberg using Managed I/O...")
        read_from_iceberg_with_managed_io()

        logger.info("\n" + "=" * 60)
        logger.info("DEMO COMPLETED SUCCESSFULLY!")
        logger.info("All BigQueryIO and Managed I/O operations completed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed with error: {str(e)}")
        raise


if __name__ == '__main__':
    run_demo()
