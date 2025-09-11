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

# from apache_beam.transforms import trigger
# from apache_beam.transforms.sql import SqlTransform
# from apache_beam.transforms.window import FixedWindows
# from utils.transforms import get_sql_transform
# from utils.transforms import JsonToRow
# from utils.transforms import AddWindowInfo
# from utils.transforms import get_agg_bigquery_schema
# from utils.transforms import StatefulDeduplication


def run_pipeline(argv=None):
    """Run the Apache Beam pipeline."""

    # Parse arguments and setup pipeline options
    known_args, pipeline_args = parse_arguments(argv)
    pipeline_args.append('--experiments=use_runner_v2')
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
        (
            parsed_messages
            | 'SelectFieldsForStandardTable' >> beam.Map(lambda x: {
                'event_time': x['event_time'],
                'processing_time': x['processing_time'],
                'payload': x['payload']
            })
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
            (
                parsed_messages
                | 'SelectFieldsForIcebergTable' >> beam.Map(lambda x: {
                    'event_time': x['event_time'],
                    'processing_time': x['processing_time'],
                    'payload': x['payload']
                })
                | 'WriteToIcebergTable' >> WriteToBigQuery(
                    table=known_args.output_iceberg_table,
                    schema=get_bigquery_schema(),
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_NEVER
                )
            )

        # # Branch 3: Perform streaming aggregation (disabled)
        # if known_args.output_agg_table:
        #     agg_partitioning = {'timePartitioning': {
        #         'type': 'HOUR', 'field': 'window_start'}}
        #     (
        #         parsed_messages
        #         # Apply windowing for both deduplication and aggregation
        #         | 'ApplyWindow' >> beam.WindowInto(
        #             FixedWindows(3600),  # 1-hour windows based on event time
        #             trigger=trigger.Repeatedly(
        #                 trigger.AfterProcessingTime(300)),
        #             # For progressive aggregation
        #             accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        #             allowed_lateness=600  # 10 minutes for late data
        #         )
        #         # Stateful deduplication - maintains state across triggers within windows
        #         | 'ExtractRideIdAsKey' >> beam.Map(lambda x: (x['ride_id'], x))
        #         | 'StatefulDeduplication' >> beam.ParDo(StatefulDeduplication())
        #         | 'ConvertToRow' >> beam.ParDo(JsonToRow())
        #         | 'SqlAggregation' >> SqlTransform(get_sql_transform())
        #         | 'AddWindowInfo' >> beam.ParDo(AddWindowInfo())
        #         | 'WriteAggregationToBigQuery' >> WriteToBigQuery(
        #             table=known_args.output_agg_table,
        #             schema=get_agg_bigquery_schema(),
        #             write_disposition=BigQueryDisposition.WRITE_APPEND,
        #             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        #             additional_bq_parameters=agg_partitioning
        #         )
        #     )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
