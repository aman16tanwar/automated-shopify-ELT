import json
import pandas as pd
import time
import os
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import pandas_gbq
import shopify


def run_customer_insights(config):
    import google.auth
    
    # Handle credentials for both Cloud Run and local environments
    if os.getenv("K_SERVICE"):  # This env var is set in Cloud Run
        # Running in Cloud Run - use ADC explicitly
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
        pandas_gbq.context.credentials = credentials
    else:
        # Running locally - use service account file if it exists
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, "bigquery.json")
        if os.path.exists(creds_path):
            credentials = service_account.Credentials.from_service_account_file(creds_path)
            pandas_gbq.context.credentials = credentials
        else:
            # Fallback to ADC if no service account file
            credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
            pandas_gbq.context.credentials = credentials
    
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
                "tags": cust.get("tags", []),
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
    
    # Handle numeric fields properly for BigQuery FLOAT type
    # First convert to numeric, handling any conversion errors
    df["total_spent"] = pd.to_numeric(df["total_spent"], errors='coerce')
    # Fill NaN values with 0 for numeric fields
    df["total_spent"] = df["total_spent"].fillna(0.0)
    # Convert to float64 explicitly to ensure proper type for FLOAT in BigQuery
    df["total_spent"] = df["total_spent"].astype('float64')
    
    # Handle orders_count
    df["orders_count"] = pd.to_numeric(df["orders_count"], errors='coerce')
    df["orders_count"] = df["orders_count"].fillna(0)
    df["orders_count"] = df["orders_count"].astype('int64')
    
    # Standardize timestamps to UTC-naive for BigQuery
    for c in ["created_at", "updated_at"]:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").dt.tz_localize(None)
    
    # Ensure string fields are properly typed and handle list/dict fields
    df["store_name"] = df["store_name"].astype(str)
    
    # Handle tags as a REPEATED field (array) for BigQuery
    # Shopify returns tags as a list already, so we just need to ensure it's a list
    def process_tags(tags_value):
        if isinstance(tags_value, list):
            return tags_value
        elif tags_value is None or tags_value == '':
            return []
        else:
            # Fallback: if it's any other type, convert to string and wrap in list
            return [str(tags_value)]
    
    df["tags"] = df["tags"].apply(process_tags)
    
    # JSON serialization function for list/dict fields (except tags which is REPEATED)
    def to_jsonish(x):
        return json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
    
    # Convert all other fields, applying JSON serialization where needed
    string_columns = [col for col in df.columns if col not in ['total_spent', 'orders_count', 'created_at', 'updated_at', 'tags']]
    for col in string_columns:
        # Apply JSON serialization for potential list/dict values
        df[col] = df[col].apply(to_jsonish).fillna('').astype(str)
    
    record_count = len(df)

    table_id = f"{config['GCP_PROJECT_ID']}.{config['BIGQUERY_DATASET']}.{config['BIGQUERY_TABLE_CUSTOMER_INSIGHTS']}"
    
    try:
        # Debug: Check the tags column specifically
        print(f"[DEBUG] Tags column type: {type(df['tags'].iloc[0]) if len(df) > 0 else 'No data'}")
        print(f"[DEBUG] Tags sample values: {df['tags'].head(3).tolist() if len(df) > 0 else 'No data'}")
        
        # Upload to BigQuery using pandas_gbq
        # Note: credentials are already set in pandas_gbq.context above
        to_gbq(
            df, 
            destination_table=table_id, 
            project_id=config['GCP_PROJECT_ID'], 
            if_exists="replace"
        )
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


