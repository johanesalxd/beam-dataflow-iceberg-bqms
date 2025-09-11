#!/usr/bin/env python3
"""
Utility script to create the BigQuery view for the aggregated results.
"""

import argparse
import json
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def table_exists(client, table_ref):
    """Check if a BigQuery table exists."""
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def create_empty_table(client, table_ref, schema_path):
    """Create an empty BigQuery table from a schema file."""
    logger.info("Creating empty table: %s", table_ref)
    with open(schema_path, 'r') as f:
        schema_fields = [bigquery.SchemaField.from_api_repr(
            field) for field in json.load(f)]

    table = bigquery.Table(table_ref, schema=schema_fields)
    # Set time partitioning
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.HOUR,
        field="window_start",
    )
    client.create_table(table)
    logger.info("Successfully created table: %s", table_ref)


def create_agg_view(project_id, dataset_id, view_id, source_table_id):
    """
    Creates or replaces a BigQuery view for the aggregated data.
    """
    client = bigquery.Client(project=project_id)
    full_view_name = f"{project_id}.{dataset_id}.{view_id}"
    full_source_table_name = f"{project_id}.{dataset_id}.{source_table_id}"
    table_ref = client.dataset(dataset_id).table(source_table_id)

    # Create the source table if it doesn't exist
    if not table_exists(client, table_ref):
        logger.warning("Source table %s not found. Creating it.",
                       full_source_table_name)
        create_empty_table(client, table_ref, 'schemas/agg_table.json')

    logger.info("Creating or replacing view: %s", full_view_name)

    # Load SQL from file and format with table names
    with open('schemas/agg_table_view.sql', 'r') as f:
        view_sql = f.read().format(
            full_view_name=full_view_name,
            full_source_table_name=full_source_table_name
        )

    try:
        query_job = client.query(view_sql)
        query_job.result()  # Wait for the job to complete
        logger.info("Successfully created or replaced view: %s",
                    full_view_name)
    except Exception as e:
        logger.error("Failed to create view %s: %s", full_view_name, e)
        raise


def main():
    """Main function to parse arguments and create the view."""
    parser = argparse.ArgumentParser(
        description="Create BigQuery view for aggregations.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--dataset_id", required=True,
                        help="BigQuery dataset ID.")
    parser.add_argument("--view_id", required=True,
                        help="The ID for the view.")
    parser.add_argument("--source_table_id", required=True,
                        help="The source table for the view.")

    args = parser.parse_args()

    create_agg_view(
        args.project_id,
        args.dataset_id,
        args.view_id,
        args.source_table_id
    )


if __name__ == "__main__":
    main()
