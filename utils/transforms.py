#!/usr/bin/env python3
"""
Apache Beam transforms and utilities for PubSub to BigQuery pipeline.
"""

from datetime import datetime
from datetime import timezone
import json
import logging

from apache_beam import typehints
import apache_beam as beam
from apache_beam.transforms.userstate import BagStateSpec


@typehints.with_input_types(dict)
class JsonToRow(beam.DoFn):
    """
    Converts a dictionary into a beam.Row for SQL compatibility.
    """

    def process(self, element):
        # Assuming element is a dictionary
        try:
            # Keep timestamps as strings for SQL compatibility
            event_time = element['event_time']
            processing_time = element['processing_time']

            # Ensure passenger_count is an integer
            passenger_count = int(element.get('passenger_count', 0))

            # Return a beam.Row
            yield beam.Row(
                event_time=str(event_time),
                processing_time=str(processing_time),
                ride_id=str(element.get('ride_id', '')),
                ride_status=str(element.get('ride_status', '')),
                passenger_count=passenger_count,
                payload=str(element.get('payload', ''))
            )
        except (TypeError, KeyError, ValueError) as e:
            logging.warning("Could not convert element to Row: %s", e)


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

            # Create a dictionary with all the fields
            row = {
                'event_time': event_time,
                'processing_time': processing_time.isoformat(),
                'ride_id': message_json.get('ride_id'),
                'ride_status': message_json.get('ride_status'),
                'passenger_count': message_json.get('passenger_count'),
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


def get_agg_bigquery_schema():
    """Load BigQuery table schema from a JSON file."""
    with open('schemas/agg_table.json', 'r') as f:
        fields = json.load(f)
    return {'fields': fields}


def get_sql_transform():
    """Load SQL transform from a file."""
    with open('schemas/agg_table.sql', 'r') as f:
        return f.read()


class AddWindowInfo(beam.DoFn):
    """A DoFn that adds the window start and end times to an element."""

    def process(self, element, window=beam.DoFn.WindowParam):
        # The element is the dictionary from the SqlTransform output
        element['window_start'] = window.start.to_rfc3339()
        element['window_end'] = window.end.to_rfc3339()
        yield element


class StatefulDeduplication(beam.DoFn):
    """
    Stateful deduplication that maintains state across triggers within windows.
    Relies on window expiration for state cleanup.
    """
    SEEN_IDS_STATE = BagStateSpec(
        'seen_ids', beam.coders.StrUtf8Coder())

    def process(
            self,
            element,
            seen_ids=beam.DoFn.StateParam(SEEN_IDS_STATE)):
        """Process elements and deduplicate based on ride_id."""
        ride_id, message = element
        current_seen = set(seen_ids.read())

        if ride_id not in current_seen:
            seen_ids.add(ride_id)
            yield message
            logging.debug("New ride_id %s processed in window", ride_id)
        else:
            logging.debug("Duplicate ride_id %s ignored in window", ride_id)
