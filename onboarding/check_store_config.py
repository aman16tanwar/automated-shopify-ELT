#!/usr/bin/env python3
"""
Check store configuration in BigQuery
"""

from google.cloud import bigquery
import os

# Initialize client
project_id = "happyweb-340014"
client = bigquery.Client(project=project_id)

# Check store configuration
query = f"""
SELECT *
FROM `{project_id}.shopify_logs.store_config`
WHERE store_url = 'sorio-romstar.myshopify.com'
"""

try:
    configs = list(client.query(query))
    if configs:
        for config in configs:
            print("Store Configuration:")
            print(f"  Store URL: {config.store_url}")
            print(f"  Dataset Name: {config.dataset_name}")
            print(f"  Is Active: {config.is_active}")
            print(f"  Created At: {config.created_at}")
            print(f"  Updated At: {config.updated_at}")
            print(f"  Metadata: {config.metadata}")
    else:
        print("No configuration found for sorio-romstar.myshopify.com")
except Exception as e:
    print(f"Error: {e}")

# Also check the actual dataset where data was written
print("\n\nChecking the actual dataset where data was written:")
actual_dataset = "shopify_sorio_romstar"
dataset_id = f"{project_id}.{actual_dataset}"

try:
    dataset = client.get_dataset(dataset_id)
    print(f"✓ Dataset '{actual_dataset}' exists")
    
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
        
except Exception as e:
    print(f"Error checking actual dataset: {e}")