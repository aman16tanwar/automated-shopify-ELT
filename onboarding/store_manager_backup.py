# store_manager.py
"""
Store configuration management using BigQuery
"""

import os
from datetime import datetime, timezone
from google.cloud import bigquery
import uuid

class StoreManager:
    def __init__(self, project_id=None):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID")
        self.client = bigquery.Client(project=self.project_id)
        self.dataset = "shopify_logs"
        self.table = "store_config"
        
        # Initialize table
        self._ensure_store_config_table()
    
    def _ensure_store_config_table(self):
        """Create store config table if it doesn't exist"""
        # Create dataset if needed
        dataset_id = f"{self.project_id}.{self.dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        
        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
        except Exception as e:
            print(f"Dataset might already exist: {e}")
        
        # Create store config table
        table_id = f"{self.project_id}.{self.dataset}.{self.table}"
        schema = [
            bigquery.SchemaField("merchant", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("token", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("gcp_project_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bigquery_dataset", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("backfill_start_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("created_by", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("updated_by", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table, exists_ok=True)
    
    def get_store_configs(self, active_only=True):
        """Get all store configurations"""
        query = f"""
        SELECT 
            merchant,
            token,
            gcp_project_id,
            bigquery_dataset,
            backfill_start_date,
            is_active,
            created_at,
            updated_at,
            metadata
        FROM `{self.project_id}.{self.dataset}.{self.table}`
        {"WHERE is_active = TRUE" if active_only else ""}
        ORDER BY merchant
        """
        
        try:
            results = list(self.client.query(query))
            configs = []
            for row in results:
                config = {
                    "MERCHANT": row.merchant,
                    "TOKEN": row.token,
                    "GCP_PROJECT_ID": row.gcp_project_id,
                    "BIGQUERY_DATASET": row.bigquery_dataset,
                    "BACKFILL_START_DATE": row.backfill_start_date.isoformat() if row.backfill_start_date else None,
                    "last_updated": row.updated_at.isoformat() if row.updated_at else None,
                    "is_active": row.is_active,
                    # Add standard table names
                    "BIGQUERY_TABLE_CUSTOMER_INSIGHTS": "customer_insights",
                    "BIGQUERY_TABLE_ORDER_INSIGHTS": "order_insights",
                    "BIGQUERY_TABLE_ORDER_ITEMS_INSIGHTS": "order_items_insights",
                    "BIGQUERY_TABLE_PRODUCT_INSIGHTS": "products_insights",
                }
                # Add metadata fields if present
                if row.metadata:
                    config.update(row.metadata)
                configs.append(config)
            return configs
        except Exception as e:
            print(f"[ERROR] Failed to get store configs: {e}")
            # Return empty list if table doesn't exist yet
            return []
    
    def upsert_store_config(self, config, user=None):
        """Insert or update a store configuration"""
        # Check if store exists
        check_query = f"""
        SELECT merchant FROM `{self.project_id}.{self.dataset}.{self.table}`
        WHERE merchant = @merchant
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("merchant", "STRING", config["MERCHANT"]),
            ]
        )
        
        try:
            exists = len(list(self.client.query(check_query, job_config=job_config))) > 0
        except:
            exists = False
        
        if exists:
            # Update existing using MERGE to handle streaming buffer
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset}.{self.table}` AS target
            USING (
                SELECT 
                    @merchant as merchant,
                    @token as token,
                    @gcp_project_id as gcp_project_id,
                    @bigquery_dataset as bigquery_dataset,
                    @backfill_start_date as backfill_start_date,
                    @is_active as is_active,
                    CURRENT_TIMESTAMP() as updated_at,
                    @user as updated_by
            ) AS source
            ON target.merchant = source.merchant
            WHEN MATCHED THEN
                UPDATE SET
                    token = source.token,
                    gcp_project_id = source.gcp_project_id,
                    bigquery_dataset = source.bigquery_dataset,
                    backfill_start_date = source.backfill_start_date,
                    is_active = source.is_active,
                    updated_at = source.updated_at,
                    updated_by = source.updated_by
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("merchant", "STRING", config["MERCHANT"]),
                    bigquery.ScalarQueryParameter("token", "STRING", config["TOKEN"]),
                    bigquery.ScalarQueryParameter("gcp_project_id", "STRING", config.get("GCP_PROJECT_ID", self.project_id)),
                    bigquery.ScalarQueryParameter("bigquery_dataset", "STRING", config["BIGQUERY_DATASET"]),
                    bigquery.ScalarQueryParameter("backfill_start_date", "DATE", config.get("BACKFILL_START_DATE", "2024-01-01")),
                    bigquery.ScalarQueryParameter("is_active", "BOOL", config.get("is_active", True)),
                    bigquery.ScalarQueryParameter("user", "STRING", user or "system"),
                ]
            )
            
            self.client.query(merge_query, job_config=job_config).result()
        else:
            # Insert new
            insert_data = {
                "merchant": config["MERCHANT"],
                "token": config["TOKEN"],
                "gcp_project_id": config.get("GCP_PROJECT_ID", self.project_id),
                "bigquery_dataset": config["BIGQUERY_DATASET"],
                "backfill_start_date": config.get("BACKFILL_START_DATE", "2024-01-01"),
                "is_active": config.get("is_active", True),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "created_by": user or "system",
                "updated_by": user or "system",
                "metadata": {}
            }
            
            table_id = f"{self.project_id}.{self.dataset}.{self.table}"
            errors = self.client.insert_rows_json(table_id, [insert_data])
            
            if errors:
                raise Exception(f"Failed to insert store config: {errors}")
    
    def delete_store_config(self, merchant):
        """Soft delete a store by marking as inactive"""
        update_query = f"""
        UPDATE `{self.project_id}.{self.dataset}.{self.table}`
        SET 
            is_active = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE merchant = @merchant
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("merchant", "STRING", merchant),
            ]
        )
        
        self.client.query(update_query, job_config=job_config).result()
    
    def migrate_from_json(self, json_configs):
        """Migrate configurations from JSON file to BigQuery"""
        migrated = 0
        for config in json_configs:
            try:
                self.upsert_store_config(config, user="migration")
                migrated += 1
                print(f"Migrated store: {config['MERCHANT']}")
            except Exception as e:
                print(f"Failed to migrate {config['MERCHANT']}: {e}")
        return migrated