#!/usr/bin/env python3
"""
Apache Beam pipeline to read from PubSub and write to BigQuery.

This pipeline reads taxi ride data from the public PubSub topic
'projects/pubsub-public-data/topics/taxirides-realtime' and writes
it to BigQuery with the full JSON payload preserved.
"""

import logging

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from utils.pipeline_options import log_pipeline_info
from utils.pipeline_options import parse_arguments
from utils.pipeline_options import setup_pipeline_options
from utils.transforms import get_bigquery_schema
from utils.transforms import ParsePubSubMessage


def run_pipeline(argv=None):
    """Run the Apache Beam pipeline."""

    # Parse arguments and setup pipeline options
    known_args, pipeline_args = parse_arguments(argv)
    pipeline_options = setup_pipeline_options(known_args, pipeline_args)

    # Log pipeline configuration
    log_pipeline_info(known_args)

    # Create and run pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        # Read from PubSub
        messages = (
            pipeline
            | 'ReadFromPubSub' >> ReadFromPubSub(subscription=known_args.pubsub_subscription)
        )

        # Parse and transform messages
        parsed_messages = (
            messages
            | 'ParseMessages' >> beam.ParDo(ParsePubSubMessage())
        )

        # Branch 1: Write to standard BigQuery table (required)
        partitioning = {'timePartitioning': {
            'type': 'DAY', 'field': 'event_time'}}
        _ = (
            parsed_messages
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=get_bigquery_schema(),
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters=partitioning
            )
        )

        # Branch 2: Write to Iceberg table (optional)
        if known_args.output_iceberg_table:
            _ = (
                parsed_messages
                | 'WriteToIcebergTable' >> WriteToBigQuery(
                    table=known_args.output_iceberg_table,
                    schema=get_bigquery_schema(),
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_NEVER  # Iceberg tables must exist
                )
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
