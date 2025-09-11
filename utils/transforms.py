#!/usr/bin/env python3
"""
Apache Beam transforms and utilities for PubSub to BigQuery pipeline.
"""

from datetime import datetime
from datetime import timezone
import json
import logging

import apache_beam as beam


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
                logging.warning(
                    "Timestamp not found in message. Defaulting to processing time: %s", processing_time.isoformat())
                event_time = processing_time.isoformat()

            # Create BigQuery row
            row = {
                'event_time': event_time,
                'processing_time': processing_time.isoformat(),
                # Convert to JSON string for BigQuery JSON type
                'payload': json.dumps(message_json)
            }

            yield row

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error(
                "Error parsing message: %s. Message content (truncated): %s", e, element[:1000])
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
