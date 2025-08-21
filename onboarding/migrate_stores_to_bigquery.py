#!/usr/bin/env python3
"""
Migration script to move store configurations from JSON file to BigQuery
"""

import json
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from onboarding.store_manager import StoreManager

def migrate_stores():
    """Migrate store configurations from JSON to BigQuery"""
    
    # Initialize store manager
    store_manager = StoreManager()
    
    # Find JSON file
    json_paths = [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "historical", "store_config.json"),
        "store_config.json",
        "../historical/store_config.json"
    ]
    
    json_file = None
    for path in json_paths:
        if os.path.exists(path):
            json_file = path
            break
    
    if not json_file:
        print("[ERROR] No store_config.json file found")
        return
    
    print(f"[INFO] Found config file at: {json_file}")
    
    # Load JSON configurations
    with open(json_file, 'r') as f:
        json_configs = json.load(f)
    
    print(f"[INFO] Found {len(json_configs)} stores to migrate")
    
    # Check existing stores in BigQuery
    existing_stores = store_manager.get_store_configs(active_only=False)
    existing_merchants = {s['MERCHANT'] for s in existing_stores}
    
    if existing_merchants:
        print(f"[INFO] Found {len(existing_merchants)} existing stores in BigQuery")
    
    # Migrate each store
    migrated = 0
    skipped = 0
    failed = 0
    
    for config in json_configs:
        merchant = config.get('MERCHANT')
        
        if merchant in existing_merchants:
            print(f"[SKIP] {merchant} already exists in BigQuery")
            skipped += 1
            continue
        
        try:
            # Ensure required fields
            if 'BACKFILL_START_DATE' not in config:
                config['BACKFILL_START_DATE'] = '2024-01-01'
            
            # Add active flag
            config['is_active'] = True
            
            # Save to BigQuery
            store_manager.upsert_store_config(config, user='migration')
            print(f"[SUCCESS] Migrated {merchant}")
            migrated += 1
            
        except Exception as e:
            print(f"[ERROR] Failed to migrate {merchant}: {e}")
            failed += 1
    
    # Summary
    print("\n" + "="*50)
    print("MIGRATION SUMMARY")
    print("="*50)
    print(f"Total stores in JSON: {len(json_configs)}")
    print(f"Successfully migrated: {migrated}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Failed: {failed}")
    
    if migrated > 0:
        print(f"\n[INFO] Successfully migrated {migrated} stores to BigQuery!")
        print("[INFO] The app will now use BigQuery for store configurations.")
        print("[INFO] You can safely remove the store_config.json file after verifying.")
    
    # Verify migration
    print("\n[INFO] Verifying migration...")
    final_stores = store_manager.get_store_configs(active_only=True)
    print(f"[INFO] Total active stores in BigQuery: {len(final_stores)}")
    
    for store in final_stores:
        print(f"  - {store['MERCHANT']} (dataset: {store['BIGQUERY_DATASET']})")

if __name__ == "__main__":
    migrate_stores()