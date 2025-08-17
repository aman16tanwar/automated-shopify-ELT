"""
Health check endpoints for Cloud Run monitoring
"""

import os
from datetime import datetime
from google.cloud import bigquery
from google.cloud import secretmanager

def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "shopify-pipeline-onboarding",
        "version": os.getenv("VERSION", "1.0.0")
    }

def readiness_check():
    """Readiness check - verify all dependencies are accessible"""
    checks = {
        "bigquery": False,
        "secret_manager": False,
        "environment": False
    }
    
    try:
        # Check BigQuery connectivity
        client = bigquery.Client()
        list(client.list_datasets(max_results=1))
        checks["bigquery"] = True
    except Exception as e:
        checks["bigquery_error"] = str(e)
    
    try:
        # Check Secret Manager connectivity
        sm_client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT_ID", "")
        if project_id:
            # Try to list secrets (doesn't need to succeed, just not fail catastrophically)
            parent = f"projects/{project_id}"
            list(sm_client.list_secrets(request={"parent": parent, "page_size": 1}))
        checks["secret_manager"] = True
    except Exception as e:
        checks["secret_manager_error"] = str(e)
    
    # Check required environment variables
    required_env = ["GCP_PROJECT_ID"]
    checks["environment"] = all(os.getenv(var) for var in required_env)
    
    # Overall readiness
    is_ready = all(checks.get(k, False) for k in ["bigquery", "secret_manager", "environment"])
    
    return {
        "ready": is_ready,
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }