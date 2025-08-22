#!/usr/bin/env python3
"""
Check BigQuery data for the completed job
"""

from google.cloud import bigquery
import os

# Initialize client
project_id = "happyweb-340014"
client = bigquery.Client(project=project_id)

# Dataset from the job
dataset_name = "shopify_test_sorio_v2"

# Check if dataset exists
dataset_id = f"{project_id}.{dataset_name}"
try:
    dataset = client.get_dataset(dataset_id)
    print(f"✓ Dataset '{dataset_name}' exists")
    
    # List all tables in the dataset
    tables = list(client.list_tables(dataset))
    print(f"\nFound {len(tables)} tables in dataset:")
    
    for table in tables:
        table_id = f"{dataset_id}.{table.table_id}"
        table_ref = client.get_table(table_id)
        
        # Get row count
        query = f"SELECT COUNT(*) as count FROM `{table_id}`"
        result = list(client.query(query))
        row_count = result[0].count if result else 0
        
        print(f"  - {table.table_id}: {row_count:,} rows")
        
    # Check specific tables that should exist
    expected_tables = ['orders', 'customers', 'products', 'order_items']
    print(f"\nChecking expected tables:")
    
    for table_name in expected_tables:
        table_id = f"{dataset_id}.{table_name}"
        try:
            table = client.get_table(table_id)
            query = f"SELECT COUNT(*) as count FROM `{table_id}`"
            result = list(client.query(query))
            row_count = result[0].count if result else 0
            print(f"  ✓ {table_name}: {row_count:,} rows")
        except Exception as e:
            print(f"  ✗ {table_name}: Not found or error: {str(e)}")
            
except Exception as e:
    print(f"✗ Dataset '{dataset_name}' not found or error: {str(e)}")

# Also check the job logs for more details
print("\n\nChecking pipeline logs for job bc5ae183-3cec-43c4-8102-adcb9220c204:")
logs_query = f"""
SELECT timestamp, log_level, message, component
FROM `{project_id}.shopify_logs.pipeline_logs`
WHERE job_id = 'bc5ae183-3cec-43c4-8102-adcb9220c204'
AND (log_level = 'ERROR' OR message LIKE '%error%' OR message LIKE '%fail%')
ORDER BY timestamp DESC
LIMIT 20
"""

try:
    logs = list(client.query(logs_query))
    if logs:
        print(f"\nFound {len(logs)} error/failure related logs:")
        for log in logs:
            print(f"  [{log.log_level}] {log.message[:100]}...")
    else:
        print("\nNo error logs found")
except Exception as e:
    print(f"\nCould not check logs: {e}")