# Cloud Run Job Implementation for Shopify Historical Pipelines

## Overview
This implementation creates separate Cloud Run Jobs for each store's historical pipeline, preventing the issue where pipelines stop when you close your laptop.

## Components Added

### 1. `cloud_run_job_manager.py`
- Creates Cloud Run Jobs with naming convention: `shopify-{store-name}`
- Handles duplicate names by appending `-v2`, `-v3`, etc.
- Configures jobs with specified settings:
  - Memory: 8GB
  - CPU: 2
  - Timeout: 20 hours
  - Retries: 3
  - Service account: elt-pipelines@happyweb-340014.iam.gserviceaccount.com

### 2. Updated `job_manager.py`
- Modified `run_historical_load_async()` to:
  - Detect Cloud Run environment (`K_SERVICE` env var)
  - Create and execute Cloud Run Job when in Cloud Run
  - Fall back to subprocess for local development

### 3. Updated `historical/main.py`
- Added support for `STORE_CONFIG_JSON` environment variable
- Cloud Run Jobs pass store config via environment
- Maintains backward compatibility with BigQuery/JSON file loading

### 4. Updated `requirements.txt`
- Added `google-cloud-run==0.10.0` for Cloud Run v2 API

## How It Works

1. **User adds store in UI**
   - Store config saved to BigQuery
   - JobManager creates a job record

2. **Cloud Run Job Creation**
   - UI calls `run_historical_load_async()`
   - Detects Cloud Run environment
   - Creates new Cloud Run Job named `shopify-{store-name}`
   - Passes store config and job ID as environment variables

3. **Job Execution**
   - Cloud Run Job starts independently
   - Reads store config from `STORE_CONFIG_JSON` env var
   - Runs historical pipeline
   - Updates job status in BigQuery
   - Continues running even if browser closes

4. **Monitoring**
   - Progress tracked in `pipeline_jobs` table
   - Logs stored in `pipeline_logs` table
   - UI can show job status without maintaining connection

## Deployment Steps

1. **Build and deploy the updated image:**
   ```bash
   cd shopify-automated-pipelines
   gcloud builds submit --config cloudbuild.yaml
   ```

2. **Deploy the UI service:**
   ```bash
   gcloud run deploy shopify-pipeline \
     --image gcr.io/happyweb-340014/shopify-pipeline:latest \
     --region us-central1 \
     --service-account elt-pipelines@happyweb-340014.iam.gserviceaccount.com
   ```

3. **Grant necessary permissions:**
   ```bash
   # Allow service account to create/run Cloud Run Jobs
   gcloud projects add-iam-policy-binding happyweb-340014 \
     --member="serviceAccount:elt-pipelines@happyweb-340014.iam.gserviceaccount.com" \
     --role="roles/run.admin"
   ```

## Usage

1. Access the UI at https://shopify-pipeline-357379797589.us-central1.run.app/
2. Add store configuration
3. Click "Start Historical Data Load"
4. UI will show: "Cloud Run Job created: shopify-{store-name}"
5. You can now close your laptop - the job continues running
6. Check progress in the "Pipeline Jobs" tab

## Benefits

- **No timeout issues**: Jobs can run for up to 20 hours
- **Independent execution**: Closing browser doesn't affect pipeline
- **Better resource management**: Each store gets its own job
- **Retry capability**: Failed jobs automatically retry up to 3 times
- **Easy monitoring**: All logs centralized in BigQuery

## Notes

- Each store gets its own Cloud Run Job
- Jobs are reusable - subsequent runs execute the existing job
- Failed executions are automatically retried
- All job metadata and logs stored in BigQuery for monitoring