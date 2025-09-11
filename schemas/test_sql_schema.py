#!/usr/bin/env python3
"""
Test script to verify SqlTransform works with our Row schema.
"""

import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform

from utils.transforms import JsonToRow


def test_sql_transform():
    """Test that SqlTransform works with our Row schema."""

    # Create a simple pipeline
    with beam.Pipeline() as pipeline:

        # Create test data
        test_data = [
            {
                'event_time': '2023-01-01T10:00:00',
                'processing_time': '2023-01-01T10:00:01',
                'ride_id': 'ride1',
                'ride_status': 'pickup',
                'passenger_count': 2,
                'payload': '{"test": "data"}'
            },
            {
                'event_time': '2023-01-01T10:30:00',
                'processing_time': '2023-01-01T10:30:01',
                'ride_id': 'ride2',
                'ride_status': 'dropoff',
                'passenger_count': 1,
                'payload': '{"test": "data2"}'
            }
        ]

        # Create PCollection and convert to Rows
        rows = (
            pipeline
            | 'CreateTestData' >> beam.Create(test_data)
            | 'ConvertToRow' >> beam.ParDo(JsonToRow())
        )

        # Simple SQL query for testing (without windowing functions)
        sql_query = """
        SELECT
            ride_status,
            SUM(passenger_count) as total_passengers,
            COUNT(*) as ride_count
        FROM PCOLLECTION
        GROUP BY ride_status
        """

        # Apply SQL transform
        result = (
            rows
            | 'SqlAggregation' >> SqlTransform(sql_query)
            | 'PrintResults' >> beam.Map(print)
        )


if __name__ == '__main__':
    print("Testing SqlTransform with Row schema...")
    test_sql_transform()
    print("Test completed successfully!")
