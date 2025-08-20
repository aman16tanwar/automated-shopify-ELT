import json
import os
import pandas as pd
import time
import sys
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import pandas_gbq
import shopify


def run_product_insights(config):
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

    # BigQuery table path
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BIGQUERY_DATASET']}.{config['BIGQUERY_TABLE_PRODUCT_INSIGHTS']}"

    # Setup Shopify session
    session = shopify.Session(config["MERCHANT"], '2025-01', config["TOKEN"])
    shopify.ShopifyResource.activate_session(session)
    client = shopify.GraphQL()
    store_name = config["MERCHANT"]

    def build_product_query(cursor=None):
        after_clause = f', after: "{cursor}"' if cursor else ''
        return f'''
        query {{
          products(first: 50{after_clause}) {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              cursor
              node {{
                createdAt
                updatedAt
                id
                title
                productType
                handle
                status
                publishedAt
                tags
                vendor
                media(first: 1, sortKey: POSITION) {{
                  nodes {{
                    mediaContentType
                    ... on MediaImage {{
                      image {{
                        url
                        altText
                      }}
                    }}
                  }}
                }}
                variants(first: 50) {{
                  edges {{
                    node {{
                      id
                      sku
                      title
                      price
                      compareAtPrice
                      createdAt
                      inventoryItem {{
                        id
                      }}
                      inventoryQuantity
                      image {{
                        url
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        '''

    def list_products():
        all_products = []
        cursor = None
        page = 0

        while True:
            try:
                query = build_product_query(cursor)
                response = json.loads(client.execute(query))

                if "data" not in response or "products" not in response["data"]:
                    print(f"[WARNING] Invalid response from Shopify API. Response: {json.dumps(response, indent=2)}")
                    # Try to continue with what we have
                    if all_products:
                        print(f"[INFO] Continuing with {len(all_products)} products collected so far")
                        break
                    else:
                        raise Exception("No products collected and API returned invalid response")

                edges = response["data"]["products"]["edges"]
                all_products.extend(edges)
                page += 1
                print(f"[FETCHING] Page {page} fetched, products collected: {len(edges)}, total: {len(all_products)}")

                page_info = response["data"]["products"]["pageInfo"]
                if not page_info["hasNextPage"]:
                    break
                cursor = page_info["endCursor"]
                
                # Add small delay to avoid rate limiting
                if page % 10 == 0:
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"[ERROR] Error fetching products page {page + 1}: {str(e)}")
                if "Throttled" in str(e) or "rate" in str(e).lower():
                    print("[INFO] Rate limited, waiting 2 seconds...")
                    time.sleep(2)
                    continue
                elif all_products:
                    print(f"[WARNING] Error occurred but continuing with {len(all_products)} products")
                    break
                else:
                    raise e

        return all_products

    def extract_gid(gid):
        if gid and isinstance(gid, str) and "gid://" in gid:
            return gid.split("/")[-1]
        return gid

    def parse_products():
        rows = []

        for product in list_products():
            node = product.get("node")
            if node is None:
                print("[WARNING] Skipping product with null node:", json.dumps(product, indent=2))
                continue

            base_data = {
                "store_name": store_name,
                "created_at": node.get("createdAt"),
                "updated_at": node.get("updatedAt"),
                "id": extract_gid(node.get("id")),
                "title": node.get("title"),
                "product_type": node.get("productType"),
                "handle": node.get("handle"),
                "status": node.get("status"),
                "published_at": node.get("publishedAt"),
                "tags": ", ".join(node.get("tags", [])),
                "vendor": node.get("vendor"),
            }

            # Extract product-level fallback image (from media nodes)
            product_media_nodes = node.get("media", {}).get("nodes", [])
            product_image_url = ""
            for media in product_media_nodes:
                image_data = media.get("image")
                if image_data and image_data.get("url"):
                    product_image_url = image_data["url"]
                    break


            variants = node.get("variants", {}).get("edges", [])
            for variant in variants:
                v = variant.get("node")
                if v is None:
                    print("[WARNING] Skipping variant with null node:", json.dumps(variant, indent=2))
                    continue

                row = base_data.copy()
                row["variant_id"] = extract_gid(v.get("id"))
                row["variant_sku"] = v.get("sku")
                row["variant_title"] = v.get("title")
                row["variant_price"] = v.get("price")
                row["variant_compareAtPrice"] = v.get("compareAtPrice")
                row["variant_inventoryItem_id"] = extract_gid(
                    v.get("inventoryItem", {}).get("id")
                ) if v.get("inventoryItem") else ""
                row["variant_inventoryQuantity"] = v.get("inventoryQuantity")
                variant_image_url = v.get("image", {}).get("url") if v.get("image") else ""
                
                row["variant_image_url"] = variant_image_url or product_image_url

                rows.append(row)

        return rows

    # Extract and transform
    products_data = parse_products()
    if not products_data:
        print("[WARNING] No products found. Skipping.")
        return

    df = pd.DataFrame(products_data)

    # Type casting and cleaning
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["published_at"] = pd.to_datetime(df["published_at"])

    df["id"] = df["id"].astype(str)
    df["variant_id"] = df["variant_id"].astype(str)
    df["variant_inventoryItem_id"] = df["variant_inventoryItem_id"].astype(str)
    df["variant_inventoryQuantity"] = pd.to_numeric(df["variant_inventoryQuantity"], errors="coerce").fillna(0).astype(int)

    str_cols = [
        "store_name", "title", "product_type", "handle", "status",
        "tags", "vendor", "variant_sku", "variant_title", "variant_price",
        "variant_compareAtPrice", "variant_image_url"
    ]

    for col in str_cols:
        df[col] = df[col].astype(str).fillna("")

    # Check for any problematic values
    print("[INFO] Checking data types and values...")
    for col in df.columns:
        if df[col].isna().any():
            na_count = df[col].isna().sum()
            print(f"[WARNING] Column '{col}' has {na_count} NaN values")
    
    # Load to BigQuery
    print(df.info())
    print(df.head())  # Show sample data
    record_count = len(df)
    
    print(f"[INFO] Starting BigQuery upload for {record_count} product variants...")
    print(f"[INFO] Table: {table_id}, Project: {config['GCP_PROJECT_ID']}")
    sys.stdout.flush()  # Force output
    
    # Check if dataframe is empty
    if df.empty:
        print("[WARNING] DataFrame is empty, skipping BigQuery upload")
        return 0
        
    try:
        # For large datasets, use chunking
        if record_count > 10000:
            print(f"[INFO] Large dataset detected, uploading in chunks...")
            chunk_size = 5000
            for i in range(0, record_count, chunk_size):
                chunk_end = min(i + chunk_size, record_count)
                print(f"[INFO] Uploading chunk {i//chunk_size + 1} ({i+1}-{chunk_end} of {record_count})...")
                sys.stdout.flush()
                # Note: credentials are already set in pandas_gbq.context above
                to_gbq(
                    dataframe=df.iloc[i:chunk_end],
                    destination_table=table_id,
                    project_id=config["GCP_PROJECT_ID"],
                    if_exists="append" if i > 0 else "replace",
                    progress_bar=False
                )
                print(f"[INFO] Chunk {i//chunk_size + 1} uploaded successfully")
                sys.stdout.flush()
        else:
            print(f"[INFO] Starting upload of {record_count} records...")
            sys.stdout.flush()
            
            # Set a reasonable timeout and retry configuration
            from google.cloud.bigquery import LoadJobConfig, TimePartitioning
            
            # Create a dataframe copy to avoid issues
            df_copy = df.copy()
            
            # Upload with explicit configuration
            # Note: credentials are already set in pandas_gbq.context above
            to_gbq(
                dataframe=df_copy,
                destination_table=table_id,
                project_id=config["GCP_PROJECT_ID"],
                if_exists="replace",
                progress_bar=True,  # Enable progress bar for visibility
                chunksize=500  # Process in smaller chunks even for small datasets
            )
        print(f"[SUCCESS] Uploaded {record_count} product variants to BigQuery -> {table_id}")
        sys.stdout.flush()
    except Exception as e:
        print(f"[ERROR] Failed to upload products to BigQuery: {str(e)}")
        print(f"[ERROR] Error type: {type(e).__name__}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        raise e
    
    return record_count


