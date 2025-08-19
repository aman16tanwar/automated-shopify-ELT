import json
import pandas as pd
import time
import os
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import pandas_gbq
import shopify


def run_customer_insights(config):
    # Use Application Default Credentials in Cloud Run
    if os.getenv("K_SERVICE"):  # This env var is set in Cloud Run
        # Running in Cloud Run - use default credentials
        credentials = None
    else:
        # Running locally - use service account file if it exists
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, "bigquery.json")
        if os.path.exists(creds_path):
            credentials = service_account.Credentials.from_service_account_file(creds_path)
            pandas_gbq.context.credentials = credentials
        else:
            credentials = None
    
    pandas_gbq.context.project = config["GCP_PROJECT_ID"]

    api_session = shopify.Session(config["MERCHANT"], '2025-01', config["TOKEN"])
    shopify.ShopifyResource.activate_session(api_session)
    client = shopify.GraphQL()

    def build_query(include_pii=True, cursor=None):
        after_clause = f', after: "{cursor}"' if cursor else ''
        # Use date from config, default to 2015-01-01 if not specified
        start_date = config.get('BACKFILL_START_DATE', '2015-01-01')
        pii_fields = """
            email
            firstName
            displayName
            phone
            note
            tags
            defaultAddress {
              id
              firstName
              lastName
              company
              address1
              address2
              city
              province
              country
              zip
              phone
              name
            }
        """ if include_pii else ""

        return f'''
        query {{
          customers(first: 250, query: "created_at:>={start_date}"{after_clause}) {{
            edges {{
              cursor
              node {{
                id
                lastOrder {{ id name }}
                numberOfOrders
                amountSpent {{ amount currencyCode }}
                createdAt
                updatedAt
                {pii_fields}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        '''

    def fetch_all_customers():
        all_customers = []
        cursor = None
        page = 1
        include_pii = True

        while True:
            print(f"[FETCHING] Page {page}... (PII: {include_pii})")
            query = build_query(include_pii=include_pii, cursor=cursor)

            try:
                response = json.loads(client.execute(query))
            except Exception as e:
                print(f"[ERROR] Shopify API error: {e}")
                break

            if 'errors' in response:
                throttled = any("Throttled" in err["message"] for err in response["errors"])
                pii_error = any("not approved to access the Customer object" in err["message"] for err in response["errors"])

                if throttled:
                    print("[INFO] Throttled by Shopify. Sleeping 5 seconds before retry...")
                    time.sleep(5)
                    continue

                if pii_error and include_pii:
                    print("[WARNING] PII access denied. Retrying with limited fields...")
                    include_pii = False
                    continue

                print("[ERROR] GraphQL errors:")
                print(json.dumps(response["errors"], indent=2))
                break

            if "data" not in response or "customers" not in response["data"]:
                print("[ERROR] 'data.customers' missing.")
                break

            edges = response["data"]["customers"]["edges"]
            page_info = response["data"]["customers"]["pageInfo"]

            print(f"[SUCCESS] Page {page}: {len(edges)} customers")
            all_customers.extend([edge["node"] for edge in edges])

            if not page_info["hasNextPage"]:
                break

            cursor = page_info["endCursor"]
            page += 1

        return all_customers

    def parse_customer_data(customer_list):
        parsed = []
        for cust in customer_list:
            last_order = cust.get("lastOrder")
            default_address = cust.get("defaultAddress") or {}

            parsed.append({
                "store_name": config["MERCHANT"],
                "created_at": pd.to_datetime(cust.get("createdAt", None), errors="coerce"),
                "updated_at": pd.to_datetime(cust.get("updatedAt", None), errors="coerce"),
                "id": cust["id"].split("/")[-1],
                "email": cust.get("email", ""),
                "first_name": cust.get("firstName", ""),
                "display_name": cust.get("displayName", ""),
                "total_spent": float(cust.get("amountSpent", {}).get("amount", 0.0)),
                "last_order_id": last_order.get("id").split("/")[-1] if last_order else None,
                "last_order_name": last_order.get("name") if last_order else None,
                "orders_count": int(cust.get("numberOfOrders", 0) or 0),
                "currency_code": cust.get("amountSpent", {}).get("currencyCode"),
                "phone": cust.get("phone"),
                "note": cust.get("note", ""),
                "tags": cust.get("tags", ""),
                "default_address_id": default_address.get("id"),
                "default_address_first_name": default_address.get("firstName"),
                "default_address_last_name": default_address.get("lastName"),
                "default_address_company": default_address.get("company"),
                "default_address_address1": default_address.get("address1"),
                "default_address_address2": default_address.get("address2"),
                "default_address_city": default_address.get("city"),
                "default_address_province": default_address.get("province"),
                "default_address_country": default_address.get("country"),
                "default_address_zip": default_address.get("zip"),
                "default_address_phone": default_address.get("phone"),
                "default_address_name": default_address.get("name")
            })
        return parsed

    print("[FETCHING] Customers...")
    raw_customers = fetch_all_customers()
    if not raw_customers:
        print("[WARNING] No customers fetched, skipping this store.")
        return 0

    df = pd.DataFrame(parse_customer_data(raw_customers))
    
    df["total_spent"] = df["total_spent"].astype(float)
    df["store_name"] = df["store_name"].astype(str)
    # Convert orders_count to int64 to avoid issues
    df["orders_count"] = pd.to_numeric(df["orders_count"], errors='coerce').fillna(0).astype('int64')
    
    record_count = len(df)

    table_id = f"{config['GCP_PROJECT_ID']}.{config['BIGQUERY_DATASET']}.{config['BIGQUERY_TABLE_CUSTOMER_INSIGHTS']}"
    
    # Debug: Check data types before upload
    print("[DEBUG] DataFrame info before upload:")
    print(df.info())
    print("\n[DEBUG] Sample data:")
    print(df.head(2))
    
    # Check for any infinity or NaN values that might cause issues
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        if df[col].isin([float('inf'), float('-inf')]).any():
            print(f"[WARNING] Column '{col}' contains infinity values")
            df[col] = df[col].replace([float('inf'), float('-inf')], 0)
        if df[col].isna().any():
            print(f"[WARNING] Column '{col}' contains NaN values")
            df[col] = df[col].fillna(0)
    
    try:
        # Try to identify data type issues before upload
        print("[DEBUG] Checking for data type issues...")
        
        # Ensure all object columns are strings
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('').astype(str)
        
        # Ensure datetime columns are properly formatted
        datetime_cols = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            # Ensure timezone awareness is handled
            if df[col].dt.tz is None:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        # Pass credentials only if not None (for local auth)
        gbq_kwargs = {"destination_table": table_id, "project_id": config['GCP_PROJECT_ID'], "if_exists": "replace"}
        if credentials is not None:
            gbq_kwargs["credentials"] = credentials
        
        print(f"[DEBUG] Starting upload to BigQuery table: {table_id}")
        to_gbq(df, **gbq_kwargs)
        print(f"[SUCCESS] Uploaded to BigQuery: {table_id} - {record_count} records")
    except Exception as e:
        print(f"[ERROR] Failed to upload to BigQuery: {str(e)}")
        print(f"[ERROR] Full error: {type(e).__name__}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        
        # Try to identify the problematic column
        print("\n[DEBUG] Checking each column for issues...")
        for col in df.columns:
            try:
                print(f"[DEBUG] Column '{col}': dtype={df[col].dtype}, nulls={df[col].isna().sum()}, unique={df[col].nunique()}")
                if df[col].dtype == 'object':
                    # Check string lengths
                    max_len = df[col].astype(str).str.len().max()
                    print(f"  - Max string length: {max_len}")
                    # Check for any special characters
                    try:
                        sample = df[col].dropna().head(3).tolist()
                        # Safely convert to string representation
                        sample_str = repr(sample)[:200]  # Limit length
                        print(f"  - Sample values: {sample_str}")
                    except Exception as sample_err:
                        print(f"  - Error getting sample: {type(sample_err).__name__}")
            except Exception as col_err:
                print(f"[ERROR] Failed to analyze column '{col}': {type(col_err).__name__}: {str(col_err)[:100]}")
        raise
    
    return record_count


