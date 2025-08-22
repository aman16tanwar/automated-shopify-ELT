import json
import os
import sys
import traceback
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from dotenv import load_dotenv
from customer_queue import run_customer_insights  # assumes you modularized the customer logic
from order_queue import run_order_insights        # assumes you modularized the order logic
from order_items_queue import run_order_items_insights
  # assumes you modularized the order items logic
from products_queue import run_product_insights
from job_logger import JobLogger

# Load env and credentials
load_dotenv()

# Use Application Default Credentials in Cloud Run
if os.getenv("K_SERVICE"):  # This env var is set in Cloud Run
    # Running in Cloud Run - use default credentials
    credentials = None  # Will use Application Default Credentials
else:
    # Running locally - use service account file if it exists
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, "bigquery.json")
    if os.path.exists(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        credentials = service_account.Credentials.from_service_account_file(creds_path)
    else:
        credentials = None  # Will use Application Default Credentials

# Initialize job logger
logger = JobLogger()

# Log startup
print(f"[INFO] Starting historical pipeline. Job ID: {os.environ.get('PIPELINE_JOB_ID', 'None')}", flush=True)
logger.info(f"Historical pipeline started. Job ID: {os.environ.get('PIPELINE_JOB_ID', 'None')}")

# Read store config from BigQuery or fallback to JSON file
# First check if we have store config passed via environment (Cloud Run Job)
store_config_json = os.environ.get("STORE_CONFIG_JSON")
if store_config_json:
    try:
        # Parse the JSON config from environment
        store_config = json.loads(store_config_json)
        stores = [store_config]
        print(f"[INFO] Loaded store config from environment: {store_config.get('MERCHANT', 'unknown')}")
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse STORE_CONFIG_JSON: {e}")
        exit(1)
else:
    # Try to load from BigQuery or JSON file
    try:
        # Try to import and use StoreManager
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.insert(0, parent_dir)
        from onboarding.store_manager import StoreManager
        
        store_manager = StoreManager()
        stores = store_manager.get_store_configs(active_only=True)
        print(f"[INFO] Loaded {len(stores)} active stores from BigQuery")
    except Exception as e:
        print(f"[WARNING] Could not load from BigQuery: {e}. Falling back to JSON file.")
        # Fallback to JSON file
        config_path = "/secrets/store_config.json" if os.path.exists("/secrets/store_config.json") else "store_config.json"
        with open(config_path) as f:
            stores = json.load(f)
        print(f"[INFO] Loaded {len(stores)} stores from {config_path}")

# Filter by TARGET_STORE if specified (for single store runs)
target_store = os.environ.get("TARGET_STORE")
if target_store:
    stores = [s for s in stores if s.get("MERCHANT") == target_store]
    if not stores:
        logger.error(f"Target store {target_store} not found in configuration")
        if logger.job_id:
            logger.update_job_status("failed", f"Store {target_store} not found")
        exit(1)

# Track total records and failures
total_records = 0
failed_components = []
error_details = []

try:
    # Loop over each store
    for store in stores:
        logger.info(f"Processing store: {store['MERCHANT']}", store['MERCHANT'], "main")
        print(f"\n[PROCESSING] Store: {store['MERCHANT']}")

        # Set per-store env vars
        os.environ["MERCHANT"] = store["MERCHANT"]
        os.environ["TOKEN"] = store["TOKEN"]
        os.environ["GCP_PROJECT_ID"] = store["GCP_PROJECT_ID"]
        os.environ["BIGQUERY_DATASET"] = store["BIGQUERY_DATASET"]

        # # Set customer table and run customer insights
        os.environ["BIGQUERY_TABLE"] = store["BIGQUERY_TABLE_CUSTOMER_INSIGHTS"]
        try:
            logger.info(f"Starting customer insights", store['MERCHANT'], "customers")
            customer_records = run_customer_insights(store)
            total_records += customer_records if customer_records else 0
            logger.info(f"Customer insights completed successfully", store['MERCHANT'], "customers")
            print(f"[SUCCESS] Customer insights completed for {store['MERCHANT']}")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.error(f"Failed to run customer insights: {str(e)}\n{error_trace}", store['MERCHANT'], "customers")
            print(f"[ERROR] Failed to run customer insights for {store['MERCHANT']}: {e}")
            print(f"[TRACE] {error_trace}")
            failed_components.append("customers")
            error_details.append(f"Customers: {str(e)}")

    


        # # Set order table and run order insights
        os.environ["BIGQUERY_TABLE"] = store["BIGQUERY_TABLE_ORDER_INSIGHTS"]
        try:
            logger.info(f"Starting order insights", store['MERCHANT'], "orders")
            order_records = run_order_insights(store)
            total_records += order_records if order_records else 0
            logger.info(f"Order insights completed successfully", store['MERCHANT'], "orders")
            print(f"[SUCCESS] Order insights completed for {store['MERCHANT']}")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.error(f"Failed to run order insights: {str(e)}\n{error_trace}", store['MERCHANT'], "orders")
            print(f"[ERROR] Failed to run order insights for {store['MERCHANT']}: {e}")
            print(f"[TRACE] {error_trace}")
            failed_components.append("orders")
            error_details.append(f"Orders: {str(e)}")


        #  Set order table and run order items insights

        os.environ["BIGQUERY_TABLE"] = store["BIGQUERY_TABLE_ORDER_ITEMS_INSIGHTS"]
        try:
            logger.info(f"Starting order items insights", store['MERCHANT'], "order_items")
            order_items_records = run_order_items_insights(store)
            total_records += order_items_records if order_items_records else 0
            logger.info(f"Order items insights completed successfully", store['MERCHANT'], "order_items")
            print(f"[SUCCESS] Order items insights completed for {store['MERCHANT']}")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.error(f"Failed to run order items insights: {str(e)}\n{error_trace}", store['MERCHANT'], "order_items")
            print(f"[ERROR] Failed to run order items insights for {store['MERCHANT']}: {e}")
            print(f"[TRACE] {error_trace}")
            failed_components.append("order_items")
            error_details.append(f"Order Items: {str(e)}")   

        # # Set products table and run products insights
        os.environ["BIGQUERY_TABLE"] = store["BIGQUERY_TABLE_PRODUCT_INSIGHTS"]
        try:
            logger.info(f"Starting product insights", store['MERCHANT'], "products")
            product_records = run_product_insights(store)
            total_records += product_records if product_records else 0
            logger.info(f"Product insights completed successfully", store['MERCHANT'], "products")
            print(f"[SUCCESS] Products insights completed for {store['MERCHANT']}")
        except Exception as e:
            error_trace = traceback.format_exc()
            logger.error(f"Failed to run product insights: {str(e)}\n{error_trace}", store['MERCHANT'], "products")
            print(f"[ERROR] Failed to run Products insights for {store['MERCHANT']}: {e}")
            print(f"[TRACE] {error_trace}")
            failed_components.append("products")
            error_details.append(f"Products: {str(e)}")
    
    print(f"[DEBUG] Finished processing all stores. Moving to finally block...")

except Exception as e:
    # Catch any unexpected errors
    print(f"[ERROR] Unexpected error in pipeline: {str(e)}")
    print(f"[TRACE] {traceback.format_exc()}")
    failed_components.append("pipeline")
    error_details.append(f"Pipeline: {str(e)}")
finally:
    # Always update job status
    print(f"[DEBUG] Entering finally block. Job ID: {logger.job_id}, Total records: {total_records}")
    print(f"[DEBUG] Failed components: {failed_components}")
    sys.stdout.flush()  # Force output
    
    # Determine final status
    if failed_components:
        status = "failed"
        error_msg = f"Failed components: {', '.join(failed_components)}. " + " | ".join(error_details[:3])
        logger.error(f"Pipeline completed with errors. Total records: {total_records}. {error_msg}", None, "main")
        print(f"\n[FAILED] Pipeline completed with errors: {', '.join(failed_components)}")
    else:
        status = "completed"
        error_msg = None
        logger.info(f"All stores processed successfully. Total records: {total_records}", None, "main")
        print("\n[COMPLETED] All stores processed successfully!")

    # Update job status if we have a job_id
    if logger.job_id:
        print(f"[INFO] Updating job status to: {status}")
        try:
            logger.update_job_status(status, error_message=error_msg, records_processed=total_records)
            print(f"[INFO] Job status updated successfully")
        except Exception as e:
            print(f"[ERROR] Failed to update job status: {str(e)}")
    else:
        print("[WARNING] No job_id found, status update skipped")
    
    print(f"[DEBUG] Pipeline script completed. Exiting now...")
    sys.stdout.flush()  # Force all output
    
    # Explicit exit with appropriate code
    exit_code = 1 if failed_components else 0
    sys.exit(exit_code)
