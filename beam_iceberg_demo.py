"""
Apache Beam Iceberg Demo with BigLake Metastore
===============================================

This demo shows how to:
1. Write data to an Iceberg table using BigLake metastore
2. Read data from the Iceberg table
3. Apply filters when reading data

Requirements:
- GCP project with BigQuery and Cloud Storage enabled
- Application Default Credentials configured
- Required Python packages installed (see requirements.txt)
"""

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import managed
from typing import NamedTuple

# Import configuration
from config import (
    GCP_PROJECT, TABLE_NAME, CATALOG_NAME, CATALOG_PROPERTIES,
    SAMPLE_DATA, REGION
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define a typed schema using NamedTuple
class EmployeeRecord(NamedTuple):
    id: int
    name: str
    age: int
    city: str
    salary: float
    is_active: bool
    department: str
    created_at: str


def dict_to_employee_record(data_dict):
    """Convert dictionary to typed EmployeeRecord."""
    return EmployeeRecord(
        id=data_dict['id'],
        name=data_dict['name'],
        age=data_dict['age'],
        city=data_dict['city'],
        salary=data_dict['salary'],
        is_active=data_dict['is_active'],
        department=data_dict['department'],
        created_at=data_dict['created_at']
    )


def write_to_iceberg():
    """
    Pipeline to write sample data to Iceberg table.
    Creates the table if it doesn't exist.
    """
    logger.info("Starting write pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        '--temp_location=gs://johanesa-playground-326616-dataflow-bucket/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Writing {len(SAMPLE_DATA)} records to {TABLE_NAME}")

        # Create input data and convert to typed records with explicit schema
        input_data = (pipeline
                     | 'CreateSampleData' >> beam.Create(SAMPLE_DATA)
                     | 'ConvertToTypedRecords' >> beam.Map(dict_to_employee_record).with_output_types(EmployeeRecord))

        # Write to Iceberg using Managed I/O
        _ = input_data | 'WriteToIceberg' >> managed.Write(
            managed.ICEBERG,
            config={
                'table': TABLE_NAME,
                'catalog_name': CATALOG_NAME,
                'catalog_properties': CATALOG_PROPERTIES,
                # Partition by department for better query performance
                'partition_fields': ['department']
            }
        )

        logger.info("Write pipeline completed successfully!")


def read_from_iceberg():
    """
    Pipeline to read all data from Iceberg table.
    """
    logger.info("Starting read pipeline...")

    pipeline_options = PipelineOptions([
        f'--project={GCP_PROJECT}',
        f'--region={REGION}',
        '--runner=DirectRunner',
        '--temp_location=gs://johanesa-playground-326616-dataflow-bucket/temp',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info(f"Reading data from {TABLE_NAME}")

        # Read from Iceberg using Managed I/O
        read_data = pipeline | 'ReadFromIceberg' >> managed.Read(
            managed.ICEBERG,
            config={
                'table': TABLE_NAME,
                'catalog_name': CATALOG_NAME,
                'catalog_properties': CATALOG_PROPERTIES
            }
        )

        # Print each record
        read_data | 'PrintRecords' >> beam.Map(
            lambda record: logger.info(f"Record: {record}")
        )

        logger.info("Read pipeline completed successfully!")


def read_with_filter():
    """
    Pipeline to read filtered data from Iceberg table.
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
        logger.info(f"Reading filtered data from {TABLE_NAME}")

        # Read from Iceberg with filter using Managed I/O
        filtered_data = pipeline | 'ReadFilteredData' >> managed.Read(
            managed.ICEBERG,
            config={
                'table': TABLE_NAME,
                'catalog_name': CATALOG_NAME,
                'catalog_properties': CATALOG_PROPERTIES,
                # Filter for active Engineering employees with age > 30
                'filter': "is_active = true AND department = 'Engineering' AND age > 30"
            }
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
        logger.info("APACHE BEAM ICEBERG DEMO WITH BIGLAKE METASTORE")
        logger.info("=" * 60)

        # Step 1: Write data to Iceberg
        logger.info("\n1. Writing sample data to Iceberg table...")
        write_to_iceberg()

        # Step 2: Read all data
        logger.info("\n2. Reading all data from Iceberg table...")
        read_from_iceberg()

        # Step 3: Read with filter
        logger.info("\n3. Reading filtered data (active Engineering employees, age > 30)...")
        read_with_filter()

        logger.info("\n" + "=" * 60)
        logger.info("DEMO COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed with error: {str(e)}")
        raise


if __name__ == '__main__':
    run_demo()
