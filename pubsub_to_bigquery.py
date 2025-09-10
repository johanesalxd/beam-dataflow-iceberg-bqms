#!/usr/bin/env python3
"""
Apache Beam pipeline to read from PubSub and write to BigQuery.

This pipeline reads taxi ride data from the public PubSub topic
'projects/pubsub-public-data/topics/taxirides-realtime' and writes
it to BigQuery with the full JSON payload preserved.
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions
)
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition


class ParsePubSubMessage(beam.DoFn):
    """Parse PubSub message and create BigQuery row."""

    def process(self, element):
        """
        Process a PubSub message and convert to BigQuery format.

        Args:
            element: PubSub message (bytes)

        Yields:
            Dict: BigQuery row with event_time, processing_time, and payload
        """
        try:
            # Decode the message
            message_str = element.decode('utf-8')

            # Parse JSON
            message_json = json.loads(message_str)

            # Extract timestamp from the message
            event_time = message_json.get('timestamp')
            processing_time = datetime.now(timezone.utc)

            if event_time is None:
                logging.warning("Timestamp not found in message. Defaulting to processing time: %s", processing_time.isoformat())
                event_time = processing_time.isoformat()

            # Create BigQuery row
            row = {
                'event_time': event_time,
                'processing_time': processing_time.isoformat(),
                'payload': json.dumps(message_json)  # Convert to JSON string for BigQuery JSON type
            }

            yield row

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error("Error parsing message: %s. Message content (truncated): %s", e, element[:1000])
            # Skip malformed messages
            pass
        except Exception as e:
            logging.error("Unexpected error processing message: %s", e)
            pass


def get_bigquery_schema():
    """Load BigQuery table schema from a JSON file."""
    with open('schemas/generic_table.json', 'r') as f:
        fields = json.load(f)
    return {'fields': fields}


def run_pipeline(argv=None):
    """Run the Apache Beam pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        required=True,
        help='GCP project ID'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery output table in format project:dataset.table'
    )
    parser.add_argument(
        '--pubsub_subscription',
        required=True,
        help='PubSub subscription to read from'
    )
    parser.add_argument(
        '--temp_location',
        help='GCS temp location for Dataflow'
    )
    parser.add_argument(
        '--staging_location',
        help='GCS staging location for Dataflow'
    )
    parser.add_argument(
        '--region',
        default='us-central1',
        help='GCP region for Dataflow'
    )
    parser.add_argument(
        '--job_name',
        help='Dataflow job name'
    )
    parser.add_argument(
        '--runner',
        default='DirectRunner',
        help='Pipeline runner (DirectRunner or DataflowRunner)'
    )
    parser.add_argument(
        '--max_num_workers',
        type=int,
        default=3,
        help='Maximum number of workers for Dataflow'
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = known_args.runner

    # For PubSub, always enable streaming
    pipeline_options.view_as(StandardOptions).streaming = True

    if known_args.runner == 'DataflowRunner':
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        if known_args.temp_location:
            google_cloud_options.temp_location = known_args.temp_location
        if known_args.staging_location:
            google_cloud_options.staging_location = known_args.staging_location
        if known_args.job_name:
            google_cloud_options.job_name = known_args.job_name

        worker_options = pipeline_options.view_as(WorkerOptions)
        worker_options.max_num_workers = known_args.max_num_workers
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'

        google_cloud_options.enable_streaming_engine = True

    logging.info("Starting pipeline with runner: %s", known_args.runner)
    logging.info("Reading from PubSub subscription: %s", known_args.pubsub_subscription)
    logging.info("Writing to BigQuery table: %s", known_args.output_table)

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

        # Write to BigQuery
        # Define table partitioning
        partitioning = {'timePartitioning': {'type': 'DAY', 'field': 'event_time'}}

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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
