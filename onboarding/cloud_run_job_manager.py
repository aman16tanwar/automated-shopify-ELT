#!/usr/bin/env python3
"""
Cloud Run Job Manager for Shopify Historical Pipelines
Creates and manages Cloud Run Jobs for each store
"""

import os
import re
import json
from google.cloud import run_v2
from google.api_core import exceptions
import time

class CloudRunJobManager:
    def __init__(self, project_id=None, region="us-central1"):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID") or "happyweb-340014"
        self.region = region
        self.client = run_v2.JobsClient()
        # Use the provided service account
        self.service_account = "elt-pipelines@happyweb-340014.iam.gserviceaccount.com"
        
    def sanitize_job_name(self, store_name):
        """Convert store name to valid Cloud Run job name"""
        # Remove .myshopify.com if present
        store_name = store_name.replace('.myshopify.com', '')
        # Replace invalid characters with hyphens
        job_name = re.sub(r'[^a-z0-9-]', '-', store_name.lower())
        # Remove consecutive hyphens
        job_name = re.sub(r'-+', '-', job_name)
        # Trim hyphens from start/end
        job_name = job_name.strip('-')
        # Ensure it starts with 'shopify-'
        if not job_name.startswith('shopify-'):
            job_name = f'shopify-{job_name}'
        return job_name
    
    def get_unique_job_name(self, base_name):
        """Get unique job name by checking existing jobs"""
        parent = f"projects/{self.project_id}/locations/{self.region}"
        
        # Try the base name first
        job_name = base_name
        version = 1
        
        while True:
            full_name = f"{parent}/jobs/{job_name}"
            try:
                # Check if job exists
                self.client.get_job(name=full_name)
                # If we get here, job exists, try next version
                version += 1
                job_name = f"{base_name}-v{version}"
            except exceptions.NotFound:
                # Job doesn't exist, we can use this name
                return job_name
            except Exception as e:
                print(f"Error checking job existence: {e}")
                # If we can't check, append timestamp to ensure uniqueness
                return f"{base_name}-{int(time.time())}"
    
    def create_historical_job(self, store_config, job_id):
        """Create a Cloud Run Job for historical pipeline"""
        # Generate job name
        base_job_name = self.sanitize_job_name(store_config['MERCHANT'])
        job_name = self.get_unique_job_name(base_job_name)
        
        # Full resource name
        parent = f"projects/{self.project_id}/locations/{self.region}"
        full_job_name = f"{parent}/jobs/{job_name}"
        
        # Job configuration
        job = {
            "name": full_job_name,
            "template": {
                "template": {
                    "containers": [{
                        "image": f"us-central1-docker.pkg.dev/{self.project_id}/cloud-run-source-deploy/shopify-pipeline:latest",
                        "resources": {
                            "limits": {
                                "cpu": "2",
                                "memory": "8Gi"
                            }
                        },
                        "env": [
                            {"name": "PIPELINE_JOB_ID", "value": job_id},
                            {"name": "TARGET_STORE", "value": store_config['MERCHANT']},
                            {"name": "PIPELINE_TYPE", "value": "historical"},
                            {"name": "GCP_PROJECT_ID", "value": self.project_id},
                            # Pass store config as JSON
                            {"name": "STORE_CONFIG_JSON", "value": json.dumps(store_config)}
                        ]
                    }],
                    "service_account": self.service_account,
                    "timeout": "72000s",  # 20 hours
                    "max_retries": 3
                },
                "parallelism": 1  # Run one task at a time
            }
        }
        
        try:
            # Create the job
            operation = self.client.create_job(
                parent=parent,
                job=job,
                job_id=job_name
            )
            
            print(f"Created Cloud Run Job: {job_name}")
            
            # Wait for job creation to complete
            operation.result()
            
            # Execute the job immediately
            execution_response = self.execute_job(full_job_name)
            
            return {
                "success": True,
                "job_name": job_name,
                "full_name": full_job_name,
                "execution_name": execution_response.name if execution_response else None
            }
            
        except exceptions.AlreadyExists:
            # Job already exists, just execute it
            print(f"Job {job_name} already exists, executing it...")
            execution_response = self.execute_job(full_job_name)
            
            return {
                "success": True,
                "job_name": job_name,
                "full_name": full_job_name,
                "execution_name": execution_response.name if execution_response else None,
                "existing": True
            }
            
        except Exception as e:
            print(f"Error creating Cloud Run Job: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def execute_job(self, job_name):
        """Execute an existing Cloud Run Job"""
        try:
            # Run the job
            operation = self.client.run_job(name=job_name)
            
            # Get the execution name
            execution = operation.metadata
            print(f"Started job execution: {execution.name}")
            
            return execution
            
        except Exception as e:
            print(f"Error executing job: {e}")
            return None
    
    def get_job_executions(self, job_name):
        """Get list of executions for a job"""
        try:
            parent = f"projects/{self.project_id}/locations/{self.region}/jobs/{job_name}"
            executions = self.client.list_executions(parent=parent)
            return list(executions)
        except Exception as e:
            print(f"Error listing executions: {e}")
            return []
    
    def get_job_status(self, job_name):
        """Get the current status of a Cloud Run Job"""
        try:
            # Get the latest execution
            executions = self.get_job_executions(job_name)
            if not executions:
                return "No executions found"
            
            # Get the most recent execution
            latest_execution = executions[0]
            
            # Map Cloud Run execution states to simple status
            state_mapping = {
                "EXECUTION_STATE_UNSPECIFIED": "Unknown",
                "ACTIVE": "Running",
                "SUCCEEDED": "Completed",
                "FAILED": "Failed",
                "CANCELLED": "Cancelled",
                "PENDING": "Pending"
            }
            
            # Get the state name from the enum
            state_name = latest_execution.state.name if hasattr(latest_execution.state, 'name') else str(latest_execution.state)
            
            return state_mapping.get(state_name, state_name)
            
        except Exception as e:
            print(f"Error getting job status: {e}")
            return "Error checking status"
    
    def delete_job(self, job_name):
        """Delete a Cloud Run Job"""
        try:
            full_name = f"projects/{self.project_id}/locations/{self.region}/jobs/{job_name}"
            operation = self.client.delete_job(name=full_name)
            operation.result()
            print(f"Deleted job: {job_name}")
            return True
        except Exception as e:
            print(f"Error deleting job: {e}")
            return False

# Example usage
if __name__ == "__main__":
    # Test job creation
    manager = CloudRunJobManager()
    
    test_config = {
        "MERCHANT": "test-store.myshopify.com",
        "TOKEN": "test-token",
        "GCP_PROJECT_ID": "happyweb-340014",
        "BIGQUERY_DATASET": "shopify_test_store"
    }
    
    result = manager.create_historical_job(test_config, "test-job-123")
    print(json.dumps(result, indent=2))