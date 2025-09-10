#!/usr/bin/env python3
"""
Utility script to create BigQuery Iceberg tables for the streaming demo.

This script creates the necessary Iceberg tables for both local and Dataflow
environments, as dynamic table creation is not supported for Iceberg tables.
"""

import argparse
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_iceberg_table(project_id, dataset_id, table_id, storage_uri):
    """
    Creates a BigQuery Iceberg table if it doesn't already exist.

    Args:
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID for the Iceberg table.
        storage_uri (str): GCS URI for the Iceberg table's storage.
    """
    client = bigquery.Client(project=project_id)
    full_table_name = f"{project_id}.{dataset_id}.{table_id}"

    try:
        client.get_table(full_table_name)
        logger.info("Iceberg table %s already exists.", full_table_name)
        return
    except NotFound:
        logger.info("Iceberg table %s not found. Creating...", full_table_name)

    # Define the SQL for table creation
    create_table_sql = f"""
    CREATE TABLE `{full_table_name}` (
        event_time TIMESTAMP,
        processing_time TIMESTAMP,
        payload STRING
    )
    WITH CONNECTION DEFAULT
    OPTIONS (
        file_format = 'PARQUET',
        table_format = 'ICEBERG',
        storage_uri = '{storage_uri}'
    )
    """

    try:
        # Execute the query
        query_job = client.query(create_table_sql)
        query_job.result()  # Wait for the job to complete
        logger.info("Successfully created Iceberg table: %s", full_table_name)
    except Exception as e:
        logger.error("Failed to create Iceberg table %s: %s",
                     full_table_name, e)
        raise


def main():
    """Main function to parse arguments and create tables."""
    parser = argparse.ArgumentParser(
        description="Create BigQuery Iceberg tables.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--dataset_id", required=True,
                        help="BigQuery dataset ID.")
    parser.add_argument("--table_id", required=True, help="BigQuery table ID.")
    parser.add_argument("--storage_uri", required=True,
                        help="GCS storage URI for Iceberg data.")

    args = parser.parse_args()

    create_iceberg_table(
        args.project_id,
        args.dataset_id,
        args.table_id,
        args.storage_uri
    )


if __name__ == "__main__":
    main()
