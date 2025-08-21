#!/usr/bin/env python3
"""
CLI tool for managing Shopify store configurations in BigQuery
"""

import argparse
import json
import sys
import os
from datetime import datetime
from getpass import getpass

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from onboarding.store_manager import StoreManager

def list_stores(store_manager, active_only=True):
    """List all stores"""
    stores = store_manager.get_store_configs(active_only=active_only)
    
    if not stores:
        print("No stores found.")
        return
    
    print(f"\n{'Active' if active_only else 'All'} Stores ({len(stores)} total):")
    print("-" * 80)
    print(f"{'Merchant':<30} {'Dataset':<25} {'Active':<8} {'Updated'}")
    print("-" * 80)
    
    for store in stores:
        active = "Yes" if store.get('is_active', True) else "No"
        updated = store.get('last_updated', 'Never')
        if updated != 'Never':
            try:
                # Format timestamp
                dt = datetime.fromisoformat(updated.replace('Z', '+00:00'))
                updated = dt.strftime("%Y-%m-%d %H:%M")
            except:
                pass
        
        print(f"{store['MERCHANT']:<30} {store['BIGQUERY_DATASET']:<25} {active:<8} {updated}")

def add_store(store_manager):
    """Add a new store interactively"""
    print("\nAdd New Store")
    print("-" * 40)
    
    # Get store details
    merchant = input("Shopify Store URL (e.g., example.myshopify.com): ").strip()
    if not merchant.endswith('.myshopify.com'):
        print("[ERROR] Store URL must end with .myshopify.com")
        return
    
    token = getpass("Access Token (hidden): ").strip()
    if not token:
        print("[ERROR] Access token is required")
        return
    
    dataset = input("BigQuery Dataset Name (e.g., shopify_example): ").strip()
    if not dataset.startswith('shopify_'):
        print("[WARNING] Dataset name should start with 'shopify_'")
        confirm = input("Continue anyway? (y/n): ")
        if confirm.lower() != 'y':
            return
    
    project_id = input(f"GCP Project ID (default: {os.environ.get('GCP_PROJECT_ID', 'happyweb-340014')}): ").strip()
    if not project_id:
        project_id = os.environ.get('GCP_PROJECT_ID', 'happyweb-340014')
    
    backfill_date = input("Backfill Start Date (YYYY-MM-DD, default: 2024-01-01): ").strip()
    if not backfill_date:
        backfill_date = "2024-01-01"
    
    # Confirm
    print("\nStore Configuration:")
    print(f"  Merchant: {merchant}")
    print(f"  Token: {token[:10]}...{token[-5:]}")
    print(f"  Dataset: {dataset}")
    print(f"  Project: {project_id}")
    print(f"  Backfill: {backfill_date}")
    
    confirm = input("\nAdd this store? (y/n): ")
    if confirm.lower() != 'y':
        print("Cancelled.")
        return
    
    # Create config
    config = {
        "MERCHANT": merchant,
        "TOKEN": token,
        "GCP_PROJECT_ID": project_id,
        "BIGQUERY_DATASET": dataset,
        "BACKFILL_START_DATE": backfill_date,
        "is_active": True
    }
    
    try:
        store_manager.upsert_store_config(config, user="cli")
        print(f"\n[SUCCESS] Added store: {merchant}")
    except Exception as e:
        print(f"\n[ERROR] Failed to add store: {e}")

def update_store(store_manager, merchant):
    """Update an existing store"""
    # Get current config
    stores = store_manager.get_store_configs(active_only=False)
    current = None
    for store in stores:
        if store['MERCHANT'] == merchant:
            current = store
            break
    
    if not current:
        print(f"[ERROR] Store not found: {merchant}")
        return
    
    print(f"\nUpdating Store: {merchant}")
    print("-" * 40)
    
    # Get new values (empty = keep current)
    new_token = getpass(f"New Access Token (empty to keep current): ").strip()
    new_backfill = input(f"New Backfill Date (current: {current['BACKFILL_START_DATE']}, empty to keep): ").strip()
    
    # Update config
    if new_token:
        current['TOKEN'] = new_token
    if new_backfill:
        current['BACKFILL_START_DATE'] = new_backfill
    
    try:
        store_manager.upsert_store_config(current, user="cli")
        print(f"\n[SUCCESS] Updated store: {merchant}")
    except Exception as e:
        print(f"\n[ERROR] Failed to update store: {e}")

def deactivate_store(store_manager, merchant):
    """Deactivate a store"""
    confirm = input(f"Are you sure you want to deactivate {merchant}? (y/n): ")
    if confirm.lower() != 'y':
        print("Cancelled.")
        return
    
    try:
        store_manager.delete_store_config(merchant)
        print(f"\n[SUCCESS] Deactivated store: {merchant}")
    except Exception as e:
        print(f"\n[ERROR] Failed to deactivate store: {e}")

def main():
    parser = argparse.ArgumentParser(description="Manage Shopify store configurations")
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List stores')
    list_parser.add_argument('--all', action='store_true', help='Show all stores including inactive')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new store')
    
    # Update command
    update_parser = subparsers.add_parser('update', help='Update a store')
    update_parser.add_argument('merchant', help='Store URL (e.g., example.myshopify.com)')
    
    # Deactivate command
    deactivate_parser = subparsers.add_parser('deactivate', help='Deactivate a store')
    deactivate_parser.add_argument('merchant', help='Store URL to deactivate')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize store manager
    try:
        store_manager = StoreManager()
    except Exception as e:
        print(f"[ERROR] Failed to initialize store manager: {e}")
        print("Make sure you have BigQuery credentials configured.")
        return
    
    # Execute command
    if args.command == 'list':
        list_stores(store_manager, active_only=not args.all)
    elif args.command == 'add':
        add_store(store_manager)
    elif args.command == 'update':
        update_store(store_manager, args.merchant)
    elif args.command == 'deactivate':
        deactivate_store(store_manager, args.merchant)

if __name__ == "__main__":
    main()