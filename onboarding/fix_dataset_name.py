#!/usr/bin/env python3
"""
Fix the dataset name in store configuration
"""

from google.cloud import bigquery
import os

# Initialize client
project_id = "happyweb-340014"
client = bigquery.Client(project=project_id)

# First, let's check the schema
table_id = f"{project_id}.shopify_logs.store_config"
table = client.get_table(table_id)

print("Store config table schema:")
for field in table.schema:
    print(f"  - {field.name}: {field.field_type}")

# Now let's check all store configurations
print("\nAll store configurations:")
query = f"""
SELECT *
FROM `{project_id}.shopify_logs.store_config`
"""

configs = list(client.query(query))
for config in configs:
    print(f"\nStore: {config.merchant}")
    print(f"  Dataset: {config.bigquery_dataset}")
    print(f"  Active: {config.is_active}")
    
# Update the dataset name for sorio-romstar
print("\n\nUpdating dataset name for sorio-romstar...")
update_query = f"""
UPDATE `{project_id}.shopify_logs.store_config`
SET bigquery_dataset = 'shopify_test_sorio_v2'
WHERE merchant = 'sorio-romstar.myshopify.com'
"""

try:
    query_job = client.query(update_query)
    query_job.result()
    print("✓ Dataset name updated successfully")
except Exception as e:
    print(f"Error updating: {e}")