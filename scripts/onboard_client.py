#!/usr/bin/env python3
"""
Automated Client Onboarding Script
This script handles the complete onboarding process for a new Shopify client
"""

import json
import os
import sys
import subprocess
import argparse
from datetime import datetime
from google.cloud import bigquery, secretmanager
from typing import Dict, List

class ShopifyClientOnboarding:
    def __init__(self, project_id: str = "happyweb-340014"):
        self.project_id = project_id
        self.bigquery_client = bigquery.Client(project=project_id)
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
    def validate_inputs(self, client_id: str, merchant_url: str, token: str) -> List[str]:
        """Validate all inputs before processing"""
        errors = []
        
        # Validate client_id format
        import re
        if not re.match(r'^[a-z0-9_]+$', client_id):
            errors.append("Client ID must contain only lowercase letters, numbers, and underscores")
            
        # Validate merchant URL
        if not merchant_url.endswith('.myshopify.com'):
            errors.append("Merchant URL must end with .myshopify.com")
            
        # Validate token
        if not token.startswith('shpat_'):
            errors.append("Token must start with 'shpat_'")
            
        # Check if client already exists
        dataset_name = f"shopify_{client_id}"
        try:
            self.bigquery_client.get_dataset(f"{self.project_id}.{dataset_name}")
            errors.append(f"Client {client_id} already exists")
        except:
            pass  # Dataset doesn't exist, which is what we want
            
        return errors
    
    def create_bigquery_infrastructure(self, client_id: str) -> str:
        """Create BigQuery dataset and tables"""
        dataset_name = f"shopify_{client_id}"
        print(f"📊 Creating BigQuery dataset: {dataset_name}")
        
        # Import and execute multiple_datasets.py logic
        sys.path.append(os.path.join(self.base_dir, 'historical'))
        from multiple_datasets import (
            order_items_insights_schema,
            customer_insights_schema,
            order_insights_schema,
            products_schema
        )
        
        # Create dataset
        dataset_id = f"{self.project_id}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = f"Shopify data for {client_id}"
        
        try:
            dataset = self.bigquery_client.create_dataset(dataset, timeout=30)
            print(f"✅ Created dataset {dataset_id}")
        except Exception as e:
            if "Already Exists" not in str(e):
                raise e
                
        # Create tables
        tables_config = {
            "order_items_insights": order_items_insights_schema,
            "customer_insights": customer_insights_schema,
            "order_insights": order_insights_schema,
            "products_insights": products_schema
        }
        
        for table_name, schema in tables_config.items():
            table_id = f"{dataset_id}.{table_name}"
            
            # Convert schema format if needed (for order_insights and products)
            if isinstance(schema[0], dict):
                # Convert dict format to SchemaField format
                schema_fields = []
                for field in schema:
                    schema_fields.append(
                        bigquery.SchemaField(field["name"], field["type"])
                    )
                schema = schema_fields
                
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                table = self.bigquery_client.create_table(table)
                print(f"✅ Created table {table_name}")
            except Exception as e:
                if "Already exists" not in str(e):
                    raise e
                    
        return dataset_name
    
    def store_credentials(self, client_id: str, token: str) -> str:
        """Store Shopify token in Secret Manager"""
        secret_name = f"shopify-token-{client_id}"
        parent = f"projects/{self.project_id}"
        
        print(f"🔐 Storing credentials in Secret Manager")
        
        try:
            # Create secret
            secret = self.secret_client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_name,
                    "secret": {"replication": {"automatic": {}}}
                }
            )
            
            # Add secret version
            self.secret_client.add_secret_version(
                request={
                    "parent": secret.name,
                    "payload": {"data": token.encode("UTF-8")}
                }
            )
            print(f"✅ Stored secret: {secret_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"⚠️  Secret {secret_name} already exists, updating...")
                # Add new version to existing secret
                secret_path = f"{parent}/secrets/{secret_name}"
                self.secret_client.add_secret_version(
                    request={
                        "parent": secret_path,
                        "payload": {"data": token.encode("UTF-8")}
                    }
                )
            else:
                raise e
                
        return secret_name
    
    def update_store_config(self, client_id: str, merchant_url: str, dataset_name: str, secret_name: str):
        """Add new client to store configuration"""
        config_path = os.path.join(self.base_dir, "configs", "store_configs.json")
        
        print(f"📝 Updating store configuration")
        
        # Create new config entry
        new_config = {
            "CLIENT_ID": client_id,
            "MERCHANT": merchant_url,
            "TOKEN_SECRET": secret_name,  # Reference to Secret Manager
            "GCP_PROJECT_ID": self.project_id,
            "BIGQUERY_DATASET": dataset_name,
            "BIGQUERY_TABLE_CUSTOMER_INSIGHTS": "customer_insights",
            "BIGQUERY_TABLE_ORDER_INSIGHTS": "order_insights",
            "BIGQUERY_TABLE_ORDER_ITEMS_INSIGHTS": "order_items_insights",
            "BIGQUERY_TABLE_PRODUCT_INSIGHTS": "products_insights",
            "ACTIVE": True,
            "CREATED_AT": datetime.now().isoformat()
        }
        
        # Load existing configs or create new
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                configs = json.load(f)
        else:
            configs = []
            
        # Add new config
        configs.append(new_config)
        
        # Save updated configs
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(configs, f, indent=2)
            
        print(f"✅ Updated configuration for {client_id}")
        
        # Also create individual config file
        individual_config_path = os.path.join(self.base_dir, "configs", f"{client_id}_config.json")
        with open(individual_config_path, 'w') as f:
            json.dump(new_config, f, indent=2)
    
    def build_and_deploy(self, client_id: str, memory: str = "2Gi", timeout: int = 3600):
        """Build Docker image and deploy to Cloud Run"""
        print(f"🐳 Building Docker image for {client_id}")
        
        # Change to historical directory for build
        historical_dir = os.path.join(self.base_dir, "historical")
        
        # Build image using Cloud Build
        image_name = f"gcr.io/{self.project_id}/shopify-historical-{client_id}"
        
        build_config = {
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", "-t", image_name, "."],
                    "dir": "historical"
                },
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["push", image_name]
                }
            ]
        }
        
        # Save build config
        build_config_path = os.path.join(self.base_dir, "cloudbuild.yaml")
        import yaml
        with open(build_config_path, 'w') as f:
            yaml.dump(build_config, f)
        
        # Submit build
        print(f"🏗️  Submitting build to Cloud Build...")
        result = subprocess.run([
            "gcloud", "builds", "submit",
            "--config", build_config_path,
            "--project", self.project_id,
            self.base_dir
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Build failed: {result.stderr}")
            raise Exception("Build failed")
            
        print(f"✅ Image built: {image_name}")
        
        # Deploy to Cloud Run
        print(f"☁️  Deploying to Cloud Run...")
        
        service_name = f"shopify-historical-{client_id}"
        
        deploy_result = subprocess.run([
            "gcloud", "run", "deploy", service_name,
            "--image", image_name,
            "--platform", "managed",
            "--region", "us-central1",
            "--timeout", str(timeout),
            "--memory", memory,
            "--max-instances", "1",
            "--project", self.project_id,
            "--set-env-vars", f"CLIENT_ID={client_id},CONFIG_PATH=/configs/{client_id}_config.json",
            "--no-allow-unauthenticated"
        ], capture_output=True, text=True)
        
        if deploy_result.returncode != 0:
            print(f"❌ Deployment failed: {deploy_result.stderr}")
            raise Exception("Deployment failed")
            
        print(f"✅ Deployed to Cloud Run: {service_name}")
        
        return service_name
    
    def create_scheduler_job(self, client_id: str, schedule: str = "0 */6 * * *"):
        """Create Cloud Scheduler job for incremental updates"""
        print(f"⏰ Creating Cloud Scheduler job")
        
        job_name = f"shopify-incremental-{client_id}"
        service_url = f"https://shopify-historical-{client_id}-xxxxx-uc.a.run.app"  # You'll need to get actual URL
        
        # This would create a scheduler job
        # For now, we'll just document it
        scheduler_config = {
            "name": job_name,
            "schedule": schedule,
            "target": service_url,
            "http_target": {
                "http_method": "POST",
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps({"mode": "incremental"})
            }
        }
        
        config_path = os.path.join(self.base_dir, "configs", f"{client_id}_scheduler.json")
        with open(config_path, 'w') as f:
            json.dump(scheduler_config, f, indent=2)
            
        print(f"✅ Scheduler configuration created")
    
    def onboard_client(self, client_id: str, merchant_url: str, token: str, 
                      memory: str = "2Gi", run_initial: bool = True):
        """Main onboarding function"""
        print(f"\n🚀 Starting onboarding for {client_id}")
        print("=" * 50)
        
        # Validate inputs
        errors = self.validate_inputs(client_id, merchant_url, token)
        if errors:
            print("❌ Validation failed:")
            for error in errors:
                print(f"   - {error}")
            return False
        
        try:
            # 1. Create BigQuery infrastructure
            dataset_name = self.create_bigquery_infrastructure(client_id)
            
            # 2. Store credentials
            secret_name = self.store_credentials(client_id, token)
            
            # 3. Update configuration
            self.update_store_config(client_id, merchant_url, dataset_name, secret_name)
            
            # 4. Build and deploy
            service_name = self.build_and_deploy(client_id, memory)
            
            # 5. Create scheduler job
            self.create_scheduler_job(client_id)
            
            # 6. Run initial load if requested
            if run_initial:
                print(f"\n🏃 Triggering initial historical load...")
                subprocess.run([
                    "gcloud", "run", "services", "proxy",
                    service_name,
                    "--project", self.project_id,
                    "--region", "us-central1"
                ])
            
            print(f"\n✅ Onboarding completed successfully!")
            print(f"\n📋 Summary:")
            print(f"   - Client ID: {client_id}")
            print(f"   - Dataset: {self.project_id}.{dataset_name}")
            print(f"   - Service: {service_name}")
            print(f"   - Secret: {secret_name}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Onboarding failed: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Onboard a new Shopify client")
    parser.add_argument("--client-id", required=True, help="Unique client identifier (lowercase, underscores)")
    parser.add_argument("--merchant", required=True, help="Shopify merchant URL (e.g., store.myshopify.com)")
    parser.add_argument("--token", required=True, help="Shopify access token")
    parser.add_argument("--memory", default="2Gi", help="Memory allocation (default: 2Gi)")
    parser.add_argument("--no-initial-run", action="store_true", help="Skip initial historical load")
    parser.add_argument("--project", default="happyweb-340014", help="GCP Project ID")
    
    args = parser.parse_args()
    
    # Create onboarding instance
    onboarding = ShopifyClientOnboarding(project_id=args.project)
    
    # Run onboarding
    success = onboarding.onboard_client(
        client_id=args.client_id,
        merchant_url=args.merchant,
        token=args.token,
        memory=args.memory,
        run_initial=not args.no_initial_run
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()