# Production Deployment Checklist for Shopify ELT Pipeline

## 🚨 CRITICAL - Must Fix Before Deployment

### 1. Security Issues
- [ ] **REMOVE hardcoded credentials from store_config.json**
- [ ] **REMOVE bigquery.json service account key**
- [ ] **Rotate ALL exposed Shopify API tokens**
- [ ] **Rotate GCP service account key**
- [ ] Implement Secret Manager for all credentials
- [ ] Add .gitignore file (already created)

### 2. Code Fixes Required

#### A. Error Handling
- [ ] Add try-catch blocks in shopifyclient.py
- [ ] Add retry logic with exponential backoff
- [ ] Add proper error messages for users
- [ ] Add connection validation

#### B. Resource Management
- [ ] Add connection pooling for BigQuery
- [ ] Implement proper session cleanup
- [ ] Add memory limits for large datasets
- [ ] Implement streaming for large data

#### C. Configuration
- [ ] Replace hardcoded project IDs
- [ ] Add environment-based configuration
- [ ] Remove hardcoded file paths
- [ ] Add health check endpoints

### 3. Deployment Configuration

#### Update cloudbuild.yaml:
```yaml
steps:
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/shopify-onboarding:$COMMIT_SHA', '.']
    dir: 'onboarding'
    
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: gcloud
    args:
      - 'run'
      - 'deploy'
      - 'shopify-onboarding'
      - '--image=gcr.io/$PROJECT_ID/shopify-onboarding:$COMMIT_SHA'
      - '--region=us-central1'
      - '--platform=managed'
      - '--service-account=shopify-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com'
      - '--memory=1Gi'
      - '--cpu=1'
      - '--min-instances=1'
      - '--max-instances=10'
      - '--set-env-vars=ENVIRONMENT=production,GCP_PROJECT_ID=$PROJECT_ID'
```

### 4. Quick Fixes to Apply

#### Fix 1: Update main.py to use environment variables
```python
# Replace this:
project_id = "happyweb-340014"

# With this:
project_id = os.getenv("GCP_PROJECT_ID", "happyweb-340014")
```

#### Fix 2: Add error handling to shopifyclient.py
```python
def shopify_client(merchant, token):
    try:
        if not merchant or not token:
            raise ValueError("Merchant and token are required")
        
        # Validate merchant format
        if not merchant.endswith('.myshopify.com'):
            merchant = f"{merchant}.myshopify.com"
            
        session = shopify.Session(merchant, '2025-01', token)
        shopify.ShopifyResource.activate_session(session)
        return shopify.GraphQL()
    except Exception as e:
        print(f"[ERROR] Failed to create Shopify client: {e}")
        raise
```

#### Fix 3: Add connection pooling
```python
# Create a singleton BigQuery client
_bigquery_client = None

def get_bigquery_client():
    global _bigquery_client
    if _bigquery_client is None:
        _bigquery_client = bigquery.Client()
    return _bigquery_client
```

### 5. Testing Before Deployment

1. **Security Scan**
   ```bash
   # Install and run truffleHog
   pip install truffleHog
   trufflehog filesystem . --exclude-paths .gitignore
   ```

2. **Local Testing**
   ```bash
   # Test with environment variables
   export GCP_PROJECT_ID=your-project
   export ENVIRONMENT=development
   python onboarding/app.py
   ```

3. **Load Testing**
   - Test with a store that has >10,000 products
   - Test with multiple concurrent users
   - Monitor memory usage

### 6. Monitoring Setup

1. **Enable Cloud Logging**
2. **Set up alerts for:**
   - Failed pipelines
   - High error rates
   - Memory usage > 80%
   - API rate limits

3. **Create dashboard for:**
   - Pipeline success rate
   - Average processing time
   - Records processed per hour
   - Error trends

### 7. Cost Controls

1. **Set BigQuery quotas:**
   - Maximum bytes processed per day
   - Maximum queries per user

2. **Set Cloud Run limits:**
   - Maximum instances: 10
   - CPU throttling enabled
   - Memory limit: 1Gi

3. **Enable budget alerts**

## Post-Deployment

1. **Monitor for 24 hours**
2. **Check logs for errors**
3. **Verify all stores can connect**
4. **Test rollback procedure**
5. **Document runbooks**

## Summary

**DO NOT DEPLOY** until you have:
1. Removed ALL hardcoded credentials
2. Implemented proper error handling
3. Added health checks
4. Set up monitoring
5. Tested with large datasets

This will prevent data breaches, reduce failures, and ensure a smooth production deployment.