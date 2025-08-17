#!/usr/bin/env python3
"""
First-time setup script for Shopify Automated Pipelines
Run this once to set up your environment
"""

import os
import subprocess
import sys

def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"\n📌 {description}")
    print(f"   Running: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}")
        return False
    else:
        print(f"✅ Success")
        return True

def main():
    print("🚀 Shopify Automated Pipelines - Initial Setup")
    print("=" * 50)
    
    # Get project ID
    project_id = input("Enter your GCP Project ID [happyweb-340014]: ").strip() or "happyweb-340014"
    
    print(f"\nSetting up project: {project_id}")
    
    # Step 1: Set project
    if not run_command(f"gcloud config set project {project_id}", "Setting GCP project"):
        return
    
    # Step 2: Enable APIs
    apis = [
        "bigquery.googleapis.com",
        "cloudbuild.googleapis.com",
        "run.googleapis.com",
        "secretmanager.googleapis.com",
        "cloudscheduler.googleapis.com"
    ]
    
    print("\n📌 Enabling required APIs...")
    for api in apis:
        run_command(f"gcloud services enable {api}", f"Enabling {api}")
    
    # Step 3: Create service account
    sa_name = "shopify-pipeline-sa"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    
    print(f"\n📌 Creating service account: {sa_name}")
    run_command(
        f'gcloud iam service-accounts create {sa_name} --display-name="Shopify Pipeline Service Account"',
        "Creating service account"
    )
    
    # Step 4: Grant permissions
    roles = [
        "roles/bigquery.admin",
        "roles/secretmanager.admin",
        "roles/cloudbuild.builds.editor",
        "roles/run.admin",
        "roles/cloudscheduler.admin"
    ]
    
    print("\n📌 Granting permissions...")
    for role in roles:
        run_command(
            f'gcloud projects add-iam-policy-binding {project_id} '
            f'--member="serviceAccount:{sa_email}" --role="{role}"',
            f"Granting {role}"
        )
    
    # Step 5: Create service account key
    print("\n📌 Creating service account key...")
    key_path = "historical/bigquery.json"
    os.makedirs("historical", exist_ok=True)
    
    run_command(
        f"gcloud iam service-accounts keys create {key_path} --iam-account={sa_email}",
        "Creating service account key"
    )
    
    # Step 6: Create initial config directory
    print("\n📌 Creating configuration directory...")
    os.makedirs("configs", exist_ok=True)
    
    # Step 7: Install Python dependencies
    print("\n📌 Installing Python dependencies...")
    run_command(
        f"{sys.executable} -m pip install -r requirements.txt",
        "Installing Python packages"
    )
    
    print("\n✅ Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Deploy the onboarding service:")
    print(f"   gcloud builds submit --config deploy.yaml")
    print("\n2. Get the service URL and start onboarding clients!")
    print("\n3. For local testing, you can run:")
    print("   python onboard.py --web")

if __name__ == "__main__":
    main()