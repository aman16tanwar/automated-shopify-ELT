import json
import os
import pandas as pd
from shopifyclient import shopify_client
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import time
import pytz
import shopify


def get_shopify_timezone(config):
    api_session = shopify.Session(config["MERCHANT"], '2025-01', config["TOKEN"])
    shopify.ShopifyResource.activate_session(api_session)
    try:
        shop = shopify.Shop.current()
        if shop and shop.iana_timezone:
            return pytz.timezone(shop.iana_timezone)
        else:
            print(f"[WARNING] Could not retrieve timezone for {config['MERCHANT']}. Using UTC.")
            return pytz.utc
    except Exception as e:
        print(f"[ERROR] Error fetching timezone: {e}")
        return pytz.utc


def run_order_items_insights(config):
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
    table_id = f"{config['GCP_PROJECT_ID']}.{config['BIGQUERY_DATASET']}.{config['BIGQUERY_TABLE_ORDER_ITEMS_INSIGHTS']}"

    client = shopify_client(config["MERCHANT"], config["TOKEN"])
    shopify_timezone = get_shopify_timezone(config)

    def list_orders():
        all_orders = []
        cursor = None
        pages = 0
        print(f"[INFO] Fetching orders from {config.get('BACKFILL_START_DATE', '2015-01-01')}...")
        while True:
            time.sleep(0.5)
            after_clause = f', after: "{cursor}"' if cursor else ''
            # Use date from config, default to 2015-01-01 if not specified
            start_date = config.get('BACKFILL_START_DATE', '2015-01-01')
            query = f'''
            query {{
              orders(first: 250,query: "processed_at:>={start_date}"{after_clause}) {{
                edges {{
                  cursor
                  node {{
                    createdAt
                    processedAt
                    email
                    displayFinancialStatus
                    name
                    updatedAt
                    paymentGatewayNames
                    currencyCode
                    totalDiscountsSet {{ shopMoney {{ amount }} }}
                    totalPriceSet {{ shopMoney {{ amount }} }}
                    discountCode
                    shippingLines(first: 1) {{
                      edges {{
                        node {{
                          code
                          discountedPriceSet {{ shopMoney {{ amount }} }}
                        }}
                      }}
                    }}
                    refunds {{
                      id
                      refundLineItems(first: 10) {{
                        edges {{
                          node {{
                            restockType
                            subtotalSet {{ shopMoney {{ amount }} }}
                            lineItem {{ id }}
                          }}
                        }}
                      }}
                    }}
                    # In your query string inside list_orders():
                    lineItems(first: 250) {{
                      edges {{
                        node {{
                          id
                          currentQuantity
                          quantity
                          title
                          sku
                          vendor
                          product {{ id }}
                          variant {{ id }}
                          originalTotalSet {{ shopMoney {{ amount }} }}
                          discountedTotalSet {{ shopMoney {{ amount }} }}
                          originalUnitPriceSet {{ shopMoney {{ amount }} }}
                          discountedUnitPriceSet {{ shopMoney {{ amount }} }}
                          discountAllocations {{
                            allocatedAmountSet {{ shopMoney {{ amount }} }}
                          }}
                          taxLines {{
                            priceSet {{ shopMoney {{ amount }} }}
                            rate
                          }}
                          variantTitle
                        }}
                      }}
                    }}

                  }}
                }}
                pageInfo {{
                  hasNextPage
                  endCursor
                }}
              }}
            }}
            '''

            response = json.loads(client.execute(query))
            if 'errors' in response and any(e.get("message") == "Throttled" for e in response['errors']):
                print("[WARNING] Rate limited. Backing off...")
                time.sleep(5)
                continue
            if "data" not in response or "orders" not in response["data"]:
                break
            edges = response["data"]["orders"]["edges"]
            all_orders.extend(edges)
            pages += 1
            print(f"[INFO] Page {pages}, Orders: {len(edges)}")
            page_info = response["data"]["orders"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
        return all_orders

    def parse_order_items():
      rows = []
      for order in list_orders():
          o = order["node"]
          created_at = pd.to_datetime(o.get("createdAt"), utc=True)
          updated_at = pd.to_datetime(o.get("updatedAt"), utc=True)
          processed_at = pd.to_datetime(o.get("processedAt"), utc=True)
          processed_at_store = processed_at.tz_convert(shopify_timezone)

          shipping_lines = o.get("shippingLines", {}).get("edges", [])
          shipping_node = shipping_lines[0]["node"] if shipping_lines else {}

          for item in o["lineItems"]["edges"]:
              i = item["node"]
              tax_lines = i.get("taxLines", [])
              tax_price = float(tax_lines[0]["priceSet"]["shopMoney"]["amount"]) if tax_lines else None
              pre_tax_price = float(i.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount", 0))
              discounted_price = float(i.get("discountedUnitPriceSet", {}).get("shopMoney", {}).get("amount", 0))
              # NEW: sum all discount allocations for this line item
              discount_allocations = i.get("discountAllocations", [])
              line_item_discount = sum(
                  float(da.get("allocatedAmountSet", {}).get("shopMoney", {}).get("amount", 0))
                  for da in discount_allocations
              ) if discount_allocations else 0.0

              restock_type, subtotal_amount = None, None
              refunds = o.get("refunds", [])
              for refund in refunds:
                  refund_items = refund.get("refundLineItems", {}).get("edges", [])
                  for refund_item in refund_items:
                      refund_node = refund_item["node"]
                      if refund_node.get("lineItem", {}).get("id") == i["id"]:
                          restock_type = refund_node.get("restockType")
                          subtotal_amount = float(refund_node.get("subtotalSet", {}).get("shopMoney", {}).get("amount", 0))
                          break
                  if restock_type:
                      break

              rows.append({
                  "processed_at_store_date": processed_at_store.date(),
                  "created_at": created_at,
                  "updated_at": updated_at,
                  "currency_code": o["currencyCode"],
                  "email": o["email"],
                  "display_financial_status": o["displayFinancialStatus"],
                  "name": o["name"],
                  "payment_gateway_names": ", ".join(o["paymentGatewayNames"]) if o.get("paymentGatewayNames") else None,
                  "total_discounts": float(o["totalDiscountsSet"]["shopMoney"]["amount"]),
                  "total_price": float(o["totalPriceSet"]["shopMoney"]["amount"]),
                  "line_item_id": i["id"],
                  "line_item_current_quantity": i["currentQuantity"],
                  "line_item_pre_tax_price": pre_tax_price,
                  "line_item_price": discounted_price,
                  "line_item_discount": line_item_discount,
                  "line_item_product_id": i["product"]["id"] if i.get("product") else None,
                  "line_item_quantity": i["quantity"],
                  "line_item_sku": i["sku"],
                  "line_item_title": i["title"],
                  "line_item_variant_id": i["variant"]["id"] if i.get("variant") else None,
                  "line_item_variant_title": i.get("variantTitle"),
                  "line_item_tax_rate": i["taxLines"][0].get("rate") if i.get("taxLines") and len(i["taxLines"]) > 0 else None,
                  "line_item_vendor": i["vendor"],
                  "line_item_tax_price": tax_price,
                  "shipping_line_code": shipping_node.get("code"),
                  "shipping_line_discounted_price": float(shipping_node.get("discountedPriceSet", {}).get("shopMoney", {}).get("amount", 0)),
                  "discount_code": o.get("discountCode"),
                  "refund_restock_type": restock_type,
                  "refund_subtotal": subtotal_amount
              })
      return rows


    print("[FETCHING] Order item rows...")
    order_items_data = parse_order_items()
    print(f"[INFO] Found {len(order_items_data)} order items")
    
    if not order_items_data:
        print("[WARNING] No order items found. This could mean no orders exist in the date range.")
        return
        
    df = pd.DataFrame(order_items_data)
    df["processed_at_store_date"] = pd.to_datetime(df["processed_at_store_date"]).dt.date
    df["created_at"] = pd.to_datetime(df["created_at"], errors='coerce')
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors='coerce')
    df["line_item_id"] = df["line_item_id"].str.extract(r'(\d+)$')
    df["line_item_variant_id"] = df["line_item_variant_id"].str.extract(r'(\d+)$')
    df["line_item_product_id"] = df["line_item_product_id"].str.extract(r'(\d+)$')

    # Convert to float and fill NaN values
    df["line_item_tax_price"] = df["line_item_tax_price"].fillna(0.0).astype(float)
    df["refund_subtotal"] = df["refund_subtotal"].fillna(0.0).astype(float)
    df["line_item_discount"] = df["line_item_discount"].fillna(0.0).astype(float)
    df["shipping_line_discounted_price"] = df["shipping_line_discounted_price"].fillna(0.0).astype(float)
    df["total_discounts"] = df["total_discounts"].fillna(0.0).astype(float)
    df["total_price"] = df["total_price"].fillna(0.0).astype(float)
    
    # Round all monetary values to 6 decimal places to avoid BigQuery precision issues
    monetary_columns = ["line_item_tax_price", "refund_subtotal", "line_item_discount", 
                       "shipping_line_discounted_price", "total_discounts", "total_price",
                       "line_item_price", "line_item_discount_amount", "line_item_pre_tax_price"]
    for col in monetary_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).round(6)

    print(df.info())
    print(df.head())

    record_count = len(df)
    
    try:
        # Try to convert numeric columns to proper types
        numeric_columns = ['line_item_current_quantity', 'line_item_quantity']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        
        # Upload to BigQuery
        to_gbq(df, destination_table=table_id, project_id=config["GCP_PROJECT_ID"], 
               credentials=credentials, if_exists="replace", progress_bar=False)
        print(f"[SUCCESS] Uploaded to BigQuery: {table_id} - {record_count} records")
        
    except Exception as e:
        print(f"[ERROR] Failed to upload to BigQuery: {str(e)}")
        # Try to identify problematic columns
        print("[DEBUG] Checking data types...")
        for col in df.columns:
            try:
                # Try to convert each column to see which one fails
                if df[col].dtype == 'object':
                    test_df = pd.DataFrame({col: df[col]})
                    test_df.to_parquet('test.parquet')
                    os.remove('test.parquet')
            except Exception as col_err:
                print(f"[DEBUG] Column '{col}' might be problematic: {col_err}")
                print(f"[DEBUG] Sample values: {df[col].head()}")
                print(f"[DEBUG] Unique values count: {df[col].nunique()}")
        raise e
    
    return record_count



