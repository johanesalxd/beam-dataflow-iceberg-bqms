"""
Configuration file for the Beam Iceberg demo.
Configured for johanesa-playground-326616 project.
"""

# GCP Project Configuration
GCP_PROJECT = 'johanesa-playground-326616'
BQ_DATASET = 'my_iceberg_metastore'
GCS_BUCKET = 'gs://johanesa-playground-326616-dataflow-bucket'
# GCS_WAREHOUSE_PATH = f'{GCS_BUCKET}/iceberg-warehouse'
REGION = 'us-central1'

# Table Configuration
# BigQuery table (standard)
BQ_TABLE_NAME = f'{GCP_PROJECT}.{BQ_DATASET}.employee_demo_table'
# BigQuery table for Managed I/O
BQ_MANAGEDIO_TABLE_NAME = f'{GCP_PROJECT}.{BQ_DATASET}.employee_demo_table_managedio'
# BigQuery Iceberg table (standard)
BQ_ICEBERG_TABLE_NAME = f'{GCP_PROJECT}.{BQ_DATASET}.employee_demo_table_iceberg'
# BigQuery table for Managed I/O Iceberg
BQ_ICEBERG_MANAGEDIO_TABLE_NAME = f'{BQ_DATASET}.employee_demo_table_iceberg_managedio'

# Rich sample data for demonstration
SAMPLE_DATA = [
    {
        'id': 1,
        'name': 'Alice Johnson',
        'age': 30,
        'city': 'New York',
        'salary': 75000.50,
        'is_active': True,
        'department': 'Engineering',
        'created_at': '2024-01-15T10:30:00Z'
    },
    {
        'id': 2,
        'name': 'Bob Smith',
        'age': 25,
        'city': 'San Francisco',
        'salary': 68000.00,
        'is_active': True,
        'department': 'Product',
        'created_at': '2024-02-20T14:15:00Z'
    },
    {
        'id': 3,
        'name': 'Charlie Brown',
        'age': 35,
        'city': 'Chicago',
        'salary': 82000.75,
        'is_active': False,
        'department': 'Engineering',
        'created_at': '2024-01-10T09:00:00Z'
    },
    {
        'id': 4,
        'name': 'Diana Prince',
        'age': 28,
        'city': 'Seattle',
        'salary': 71500.25,
        'is_active': True,
        'department': 'Marketing',
        'created_at': '2024-03-05T11:45:00Z'
    },
    {
        'id': 5,
        'name': 'Eve Wilson',
        'age': 32,
        'city': 'Boston',
        'salary': 79000.00,
        'is_active': True,
        'department': 'Engineering',
        'created_at': '2024-02-28T16:20:00Z'
    },
    {
        'id': 6,
        'name': 'Frank Miller',
        'age': 42,
        'city': 'Austin',
        'salary': 95000.00,
        'is_active': True,
        'department': 'Management',
        'created_at': '2024-01-05T08:30:00Z'
    }
]
