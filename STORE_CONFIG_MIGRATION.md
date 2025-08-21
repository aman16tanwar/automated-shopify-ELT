# Store Configuration Migration Guide

## Overview

We've migrated from JSON-based store configuration to BigQuery-based configuration for better security, scalability, and cloud-native operation.

## Benefits of BigQuery Configuration

1. **Security**: Tokens are stored in BigQuery, not in Docker images
2. **Dynamic Updates**: Add/update stores without redeploying
3. **Audit Trail**: Track who added/modified stores and when
4. **Cloud-Native**: Works seamlessly in Cloud Run
5. **Centralized**: All pipeline management in one dataset (`shopify_logs`)

## Migration Steps

### 1. Run the Migration Script

```bash
cd onboarding
python migrate_stores_to_bigquery.py
```

This will:
- Read your existing `store_config.json`
- Create the `shopify_logs.store_config` table
- Migrate all stores to BigQuery
- Show a summary of migrated stores

### 2. Verify Migration

Check that stores were migrated successfully:

```bash
python manage_stores.py list
```

### 3. Update Cloud Build

The cloudbuild.yaml has been updated to build from the parent directory:

```yaml
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', 'gcr.io/$PROJECT_ID/shopify-onboarding:$COMMIT_SHA', '-f', 'onboarding/Dockerfile', '.']
```

### 4. Remove Old Config File

After verifying everything works, you can remove the old config file:

```bash
rm historical/store_config.json
```

## Managing Stores

### Using the CLI Tool

```bash
# List all active stores
python manage_stores.py list

# List all stores (including inactive)
python manage_stores.py list --all

# Add a new store interactively
python manage_stores.py add

# Update a store's token or settings
python manage_stores.py update example.myshopify.com

# Deactivate a store (soft delete)
python manage_stores.py deactivate example.myshopify.com
```

### Using the Web UI

The Streamlit app automatically uses BigQuery for store configurations. You can:
- View connected stores in the "Connected Stores" tab
- Add new stores through the onboarding flow
- Update store configurations
- Restart historical loads

### Using SQL (BigQuery Console)

```sql
-- View all active stores
SELECT * FROM `happyweb-340014.shopify_logs.store_config`
WHERE is_active = TRUE
ORDER BY merchant;

-- Add a new store
INSERT INTO `happyweb-340014.shopify_logs.store_config`
(merchant, token, gcp_project_id, bigquery_dataset, backfill_start_date, is_active, created_at, updated_at, created_by)
VALUES
('example.myshopify.com', 'shpat_xxxxx', 'happyweb-340014', 'shopify_example', '2024-01-01', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'manual');

-- Update a store's token
UPDATE `happyweb-340014.shopify_logs.store_config`
SET token = 'shpat_new_token', 
    updated_at = CURRENT_TIMESTAMP(),
    updated_by = 'manual'
WHERE merchant = 'example.myshopify.com';

-- Deactivate a store
UPDATE `happyweb-340014.shopify_logs.store_config`
SET is_active = FALSE,
    updated_at = CURRENT_TIMESTAMP()
WHERE merchant = 'example.myshopify.com';
```

## Table Schema

The `shopify_logs.store_config` table has the following schema:

| Column | Type | Description |
|--------|------|-------------|
| merchant | STRING | Store URL (primary key) |
| token | STRING | Shopify access token |
| gcp_project_id | STRING | GCP project for BigQuery |
| bigquery_dataset | STRING | Target dataset name |
| backfill_start_date | DATE | Historical data start date |
| is_active | BOOLEAN | Whether store is active |
| created_at | TIMESTAMP | When record was created |
| updated_at | TIMESTAMP | When record was last updated |
| created_by | STRING | Who created the record |
| updated_by | STRING | Who last updated the record |
| metadata | JSON | Additional configuration |

## Fallback Behavior

Both the web app and historical pipeline have fallback logic:
1. Try to load from BigQuery
2. If that fails, fall back to `store_config.json`
3. Offer to migrate JSON configs to BigQuery

This ensures zero downtime during migration.

## Security Notes

1. The `.dockerignore` file ensures `store_config.json` is never included in Docker builds
2. Access to the `shopify_logs.store_config` table should be restricted to authorized personnel
3. Consider using BigQuery column-level security to protect the token column
4. Use service accounts with minimal required permissions