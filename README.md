# Shopify Automated Pipelines

Automated onboarding and data pipeline system for Shopify clients.

## 🚀 Quick Start

### Prerequisites
- Google Cloud Project with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.11+

### Initial Setup (One Time)

1. **Clone and navigate to the project**
```bash
cd Shopify-automated-pipelines
```

2. **Set up GCP**
```bash
# Set your project
gcloud config set project happyweb-340014

# Enable APIs
gcloud services enable bigquery.googleapis.com cloudbuild.googleapis.com run.googleapis.com secretmanager.googleapis.com cloudscheduler.googleapis.com
```

3. **Create service account**
```bash
# Create service account
gcloud iam service-accounts create shopify-pipeline-sa \
    --display-name="Shopify Pipeline Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding happyweb-340014 \
    --member="serviceAccount:shopify-pipeline-sa@happyweb-340014.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding happyweb-340014 \
    --member="serviceAccount:shopify-pipeline-sa@happyweb-340014.iam.gserviceaccount.com" \
    --role="roles/secretmanager.admin"

gcloud projects add-iam-policy-binding happyweb-340014 \
    --member="serviceAccount:shopify-pipeline-sa@happyweb-340014.iam.gserviceaccount.com" \
    --role="roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding happyweb-340014 \
    --member="serviceAccount:shopify-pipeline-sa@happyweb-340014.iam.gserviceaccount.com" \
    --role="roles/run.admin"

# Download key
gcloud iam service-accounts keys create historical/bigquery.json \
    --iam-account=shopify-pipeline-sa@happyweb-340014.iam.gserviceaccount.com
```

4. **Deploy the onboarding service**
```bash
gcloud builds submit --config deploy.yaml
```

## 📋 Onboarding a New Client

### Option 1: Using the Web Interface

1. Open the deployed Cloud Run URL
2. Fill in the client details
3. Click "Start Onboarding"

### Option 2: Using the API

```bash
curl -X POST https://shopify-onboarding-api-xxxxx-uc.a.run.app/onboard \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "ninja_kitchen_au",
    "client_name": "Ninja Kitchen AU",
    "merchant_url": "ninjaau.myshopify.com",
    "access_token": "shpat_xxxxxxxxxxxxx",
    "contact_email": "client@example.com"
  }'
```

### Option 3: Using the Python Script (Local)

```bash
python scripts/onboard_client.py \
    --client-id ninja_kitchen_au \
    --merchant ninjaau.myshopify.com \
    --token shpat_xxxxxxxxxxxxx
```

## 📁 Project Structure

```
Shopify-automated-pipelines/
├── historical/          # Historical data pipeline code
│   ├── main.py         # Main entry point
│   ├── customer_queue.py
│   ├── order_queue.py
│   ├── order_items_queue.py
│   ├── products_queue.py
│   └── Dockerfile
├── incremental/        # Incremental update pipeline
├── onboarding/         # Onboarding service
│   ├── main.py        # FastAPI service
│   ├── app.py         # Streamlit UI
│   └── Dockerfile
├── scripts/           # Automation scripts
│   └── onboard_client.py
├── configs/           # Configuration files
└── deploy.yaml        # Cloud Build deployment
```

## 🔧 Configuration

Each client has:
- BigQuery dataset: `shopify_{client_id}`
- Cloud Run service: `shopify-historical-{client_id}`
- Secret Manager entry: `shopify-token-{client_id}`

## 📊 Monitoring

Check logs:
```bash
# Onboarding service logs
gcloud run logs read --service shopify-onboarding-api

# Client pipeline logs
gcloud run logs read --service shopify-historical-{client_id}
```

Query data:
```sql
SELECT COUNT(*) 
FROM `happyweb-340014.shopify_{client_id}.order_insights`
WHERE DATE(created_at) = CURRENT_DATE()
```

## 🛠️ Troubleshooting

1. **Authentication issues**: Ensure service account has proper permissions
2. **Build failures**: Check Cloud Build logs
3. **Data not loading**: Verify Shopify token and permissions
4. **BigQuery errors**: Check dataset and table exist

## 🚨 Support

For issues, check:
1. Cloud Run logs
2. Cloud Build history
3. BigQuery job history