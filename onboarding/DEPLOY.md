# Deploying Shopify Onboarding App to Cloud Run

## Prerequisites
- Google Cloud Project with billing enabled
- Cloud Run API enabled
- Cloud Build API enabled
- Container Registry API enabled
- BigQuery API enabled

## Quick Deploy

### Option 1: Using Cloud Build (Recommended)
```bash
# From the shopify-automated-pipelines directory
gcloud builds submit --config=onboarding/cloudbuild.yaml
```

### Option 2: Manual Deploy
```bash
# 1. Build the Docker image
cd onboarding
docker build -t gcr.io/happyweb-340014/shopify-onboarding:latest .

# 2. Push to Container Registry
docker push gcr.io/happyweb-340014/shopify-onboarding:latest

# 3. Deploy to Cloud Run
gcloud run deploy shopify-onboarding \
  --image gcr.io/happyweb-340014/shopify-onboarding:latest \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080 \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 10 \
  --set-env-vars GCP_PROJECT_ID=happyweb-340014
```

## Important Notes

1. **Authentication**: The app uses Application Default Credentials (ADC). Make sure the Cloud Run service account has:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Viewer (if using GCS)

2. **Historical Pipeline**: The Dockerfile copies the historical pipeline code because the app uses subprocess to run it. Ensure both directories are present:
   ```
   shopify-automated-pipelines/
   ├── onboarding/
   │   ├── app.py
   │   ├── job_manager.py
   │   ├── Dockerfile
   │   └── ...
   └── historical/
       ├── main.py
       ├── customer_queue.py
       └── ...
   ```

3. **Environment Variables**: Set these in Cloud Run:
   - `GCP_PROJECT_ID`: Your Google Cloud project ID
   - `GOOGLE_APPLICATION_CREDENTIALS`: (Optional) Path to service account key

4. **Logs**: View logs in Cloud Console or:
   ```bash
   gcloud logs read --service shopify-onboarding
   ```

## Updating the App

After making changes:
```bash
# Rebuild and deploy
gcloud builds submit --config=onboarding/cloudbuild.yaml

# Or manually
docker build -t gcr.io/happyweb-340014/shopify-onboarding:v2 .
docker push gcr.io/happyweb-340014/shopify-onboarding:v2
gcloud run deploy shopify-onboarding --image gcr.io/happyweb-340014/shopify-onboarding:v2
```

## Troubleshooting

1. **Port Issues**: Ensure the app listens on port 8080 (set by PORT env var)
2. **Memory Issues**: Increase memory if processing large datasets
3. **Timeout Issues**: Cloud Run has a max timeout of 60 minutes
4. **Permissions**: Check service account permissions in IAM