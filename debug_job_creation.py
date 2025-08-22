#!/usr/bin/env python3
"""
Debug script to check Cloud Run Job creation
"""

import os
import sys
from google.cloud import bigquery

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from onboarding.job_manager import JobManager
from onboarding.cloud_run_job_manager import CloudRunJobManager

def check_recent_job_logs():
    """Check recent job logs for Cloud Run Job creation attempts"""
    project_id = "happyweb-340014"
    
    # Initialize job manager
    job_manager = JobManager(project_id=project_id)
    
    # Get recent jobs
    recent_jobs = job_manager.get_recent_jobs(limit=5)
    
    print("\n=== Recent Jobs ===")
    for job in recent_jobs:
        print(f"\nJob ID: {job.job_id}")
        print(f"Store: {job.store_url}")
        print(f"Status: {job.status}")
        print(f"Started: {job.started_at}")
        
        # Get logs for this job
        logs = job_manager.get_job_logs(job.job_id, limit=20)
        
        print("\nLogs:")
        for log in logs:
            if "Cloud Run" in log.message or "ERROR" in log.log_level:
                print(f"  [{log.log_level}] {log.message}")
    
    # Check if docker image exists
    print("\n=== Checking Docker Image ===")
    cr_manager = CloudRunJobManager(project_id=project_id)
    image_path = f"us-central1-docker.pkg.dev/{project_id}/cloud-run-source-deploy/shopify-pipeline:latest"
    print(f"Expected image: {image_path}")
    
    # Try to create a test job
    print("\n=== Testing Cloud Run Job Creation ===")
    test_config = {
        "MERCHANT": "test-store.myshopify.com",
        "TOKEN": "test-token",
        "GCP_PROJECT_ID": project_id,
        "BIGQUERY_DATASET": "shopify_test"
    }
    
    try:
        result = cr_manager.create_historical_job(test_config, "test-job-123")
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_recent_job_logs()