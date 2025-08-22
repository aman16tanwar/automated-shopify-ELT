#!/usr/bin/env python3
"""
Check detailed job logs to understand why no data was written
"""

from google.cloud import bigquery
import os

# Initialize client
project_id = "happyweb-340014"
client = bigquery.Client(project=project_id)

job_id = "bc5ae183-3cec-43c4-8102-adcb9220c204"

# Get all logs for this job
logs_query = f"""
SELECT timestamp, log_level, message, component
FROM `{project_id}.shopify_logs.pipeline_logs`
WHERE job_id = '{job_id}'
  AND (
    message LIKE '%Fetched%'
    OR message LIKE '%records%' 
    OR message LIKE '%No %'
    OR message LIKE '%WARNING%'
    OR message LIKE '%ERROR%'
    OR message LIKE '%Failed%'
    OR message LIKE '%Uploaded%'
    OR message LIKE '%success%'
  )
ORDER BY timestamp ASC
"""

print(f"Checking logs for job {job_id}:")
print("=" * 80)

try:
    logs = list(client.query(logs_query))
    print(f"\nFound {len(logs)} relevant log entries:\n")
    
    for log in logs:
        # Format timestamp
        ts = log.timestamp.strftime("%H:%M:%S")
        # Truncate long messages
        msg = log.message[:150] + "..." if len(log.message) > 150 else log.message
        print(f"[{ts}] [{log.log_level}] [{log.component}] {msg}")
        
    # Also check for any data fetch patterns
    print("\n\nChecking for data fetch indicators:")
    fetch_query = f"""
    SELECT timestamp, message
    FROM `{project_id}.shopify_logs.pipeline_logs`
    WHERE job_id = '{job_id}'
      AND (
        message LIKE '%[FETCHING]%'
        OR message LIKE '%[SUCCESS]%'
        OR message LIKE '%[WARNING] No%'
        OR message LIKE '%Uploaded to BigQuery%'
      )
    ORDER BY timestamp ASC
    """
    
    fetch_logs = list(client.query(fetch_query))
    print(f"\nFound {len(fetch_logs)} fetch-related entries:")
    for log in fetch_logs:
        ts = log.timestamp.strftime("%H:%M:%S")
        print(f"[{ts}] {log.message}")
        
except Exception as e:
    print(f"Error checking logs: {e}")