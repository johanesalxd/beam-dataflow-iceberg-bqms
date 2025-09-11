#!/usr/bin/env python3
"""
Pipeline options and argument parsing utilities for Apache Beam pipelines.
"""

import argparse
import logging

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions


def parse_arguments(argv=None):
    """Parse command line arguments for the pipeline."""
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
        '--output_iceberg_table',
        help='Optional BigQuery Iceberg table in the format "PROJECT:DATASET.TABLE"'
    )
    parser.add_argument(
        '--output_agg_table',
        help='Optional BigQuery table for streaming aggregations in the format "PROJECT:DATASET.TABLE"'
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

    return parser.parse_known_args(argv)


def setup_pipeline_options(known_args, pipeline_args):
    """Set up pipeline options based on parsed arguments."""
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).runner = known_args.runner

    # For PubSub, always enable streaming
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

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

    return pipeline_options


def log_pipeline_info(known_args):
    """Log pipeline configuration information."""
    logging.info("Starting pipeline with runner: %s", known_args.runner)
    logging.info("Reading from PubSub subscription: %s",
                 known_args.pubsub_subscription)
    logging.info("Writing to BigQuery table: %s", known_args.output_table)
    if known_args.output_iceberg_table:
        logging.info("Also writing to BigQuery Iceberg table: %s",
                     known_args.output_iceberg_table)
    if known_args.output_agg_table:
        logging.info("Writing aggregations to BigQuery table: %s",
                     known_args.output_agg_table)
