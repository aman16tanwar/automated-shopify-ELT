import json
import pandas as pd
import time
import os
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import pandas_gbq
import shopify
from dateutil.relativedelta import relativedelta
import pytz  # Import the pytz library for timezone handling

def get_shopify_timezone(config):
    """
    Fetches the Shopify store's timezone.

    Args:
        config (dict): Configuration dictionary containing MERCHANT and TOKEN.

    Returns:
        pytz.timezone: A pytz timezone object, or pytz.utc if the timezone cannot be retrieved.
    """
    api_session = shopify.Session(config["MERCHANT"], '2025-01', config["TOKEN"])
    shopify.ShopifyResource.activate_session(api_session)
    try:
        shop = shopify.Shop.current()
        if shop and shop.iana_timezone:
            return pytz.timezone(shop.iana_timezone)
        else:
            print(f"Warning: Could not retrieve valid timezone for {config['MERCHANT']}. Using UTC as default.")
            return pytz.utc
    except Exception as e:
        print(f"Error fetching Shopify timezone for {config['MERCHANT']}: {e}. Using UTC as default.")
        return pytz.utc

def run_order_insights(config):
    """
    Extracts order data from Shopify, transforms it, and loads it into BigQuery.

    Args:
        config (dict): Configuration dictionary containing GCP_PROJECT_ID, BIGQUERY_DATASET,
            BIGQUERY_TABLE_ORDER_INSIGHTS, MERCHANT, and TOKEN.
    """
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

    table_id = f"{config['GCP_PROJECT_ID']}.{config['BIGQUERY_DATASET']}.{config['BIGQUERY_TABLE_ORDER_INSIGHTS']}"
    store_name = config["MERCHANT"]

    api_session = shopify.Session(config["MERCHANT"], '2025-01', config["TOKEN"])
    shopify.ShopifyResource.activate_session(api_session)
    client = shopify.GraphQL()

    shopify_timezone = get_shopify_timezone(config)
    print(f"Shopify store timezone for {config['MERCHANT']}: {shopify_timezone.zone}")

    # Build GraphQL query with date range
    def build_order_query(start_date, end_date, cursor=None):
        after_clause = f', after: "{cursor}"' if cursor else ''
        return f'''
        query {{
          orders(first: 250, query: "processed_at:>={start_date} AND processed_at:<={end_date}"{after_clause}) {{
            edges {{
              cursor
              node {{
                createdAt
                updatedAt
                processedAt
                currencyCode
                discountCodes
                email
                displayFinancialStatus
                name
                paymentGatewayNames
                shippingLine {{ title price taxLines {{ rate priceSet {{ shopMoney {{ amount }} }} }} }}
                totalDiscountsSet {{ shopMoney {{ amount }} }}
                totalPriceSet {{ shopMoney {{ amount }} }}
                cancelledAt
                confirmationNumber
                displayFulfillmentStatus
                landingPageUrl
                note
                tags
                totalRefundedSet {{ shopMoney {{ amount }} }}
                currentTotalDutiesSet {{ shopMoney {{ amount }} }}
                currentTotalAdditionalFeesSet {{ shopMoney {{ amount }} }}
                taxLines {{ priceSet {{ shopMoney {{ amount }} }} rate }}
                totalTipReceived {{ amount }}
                customer {{ id defaultAddress {{ country }} }}
                lineItems(first: 10) {{ edges {{ node {{ name vendor }} }} }}
              }}
            }}
            pageInfo {{ hasNextPage endCursor }}
          }}
        }}
        '''

    # Generator for months
    def generate_months(start_date, end_date):
        current = start_date
        while current <= end_date:
            yield current, (current + relativedelta(months=1) - pd.Timedelta(days=1))
            current += relativedelta(months=1)

    # Main list orders function (per month)
    def list_orders():
        all_orders = []
        # Use date from config, default to 2015-01-01 if not specified
        backfill_start = config.get('BACKFILL_START_DATE', '2015-01-01')
        start_date = pd.Timestamp(backfill_start)
        end_date = pd.Timestamp.today()

        for start, end in generate_months(start_date, end_date):
            start_str = start.strftime('%Y-%m-%d')
            end_str = end.strftime('%Y-%m-%d')
            print(f"[FETCHING] Orders from {start_str} to {end_str}...")

            cursor = None
            pages_fetched = 0
            while True:
                query = build_order_query(start_str, end_str, cursor)
                response = json.loads(client.execute(query))
                if "data" not in response or "orders" not in response["data"]: break
                edges = response["data"]["orders"]["edges"]
                if not edges: break
                all_orders.extend(edges)
                pages_fetched += 1
                print(f"  [INFO] Month {start_str}: Page {pages_fetched}, Orders fetched: {len(edges)}")
                page_info = response["data"]["orders"]["pageInfo"]
                if not page_info["hasNextPage"]: break
                cursor = page_info["endCursor"]
                time.sleep(0.5)

        return all_orders

    def parse_orders():
        orders = []
        for order in list_orders():
            node = order.get("node", {})
            shipping_line = node.get("shippingLine") or {}
            shipping_tax_lines = shipping_line.get("taxLines") or []
            shipping_tax_line = shipping_tax_lines[0] if shipping_tax_lines else {}
            order_tax_lines = node.get("taxLines") or []
            customer = node.get("customer") or {}
            default_address = customer.get("defaultAddress") or {}

            created_at_utc = pd.to_datetime(node.get("createdAt").replace('Z', '+00:00'), utc=True)
            updated_at_utc = pd.to_datetime(node.get("updatedAt").replace('Z', '+00:00'), utc=True)
            processed_at_utc = pd.to_datetime(node.get("processedAt").replace('Z', '+00:00'), utc=True)
            processed_at_shopify_tz = processed_at_utc.astimezone(shopify_timezone)

            orders.append({
                "store_name": store_name,
                "created_at": created_at_utc,
                "updated_at": updated_at_utc,
                "processed_at": processed_at_utc,
                "processed_at_shopify_timezone": processed_at_shopify_tz,
                "currency_code": node.get("currencyCode"),
                "discount_codes": node.get("discountCodes"),
                "email": node.get("email"),
                "display_financial_status": node.get("displayFinancialStatus"),
                "name": node.get("name"),
                "payment_gateway_names": node.get("paymentGatewayNames"),
                "shipping_line_title": shipping_line.get("title"),
                "shipping_line_price": float(shipping_line.get("price") or 0),
                "shipping_line_tax_rate": shipping_tax_line.get("rate"),
                "shipping_line_tax_amount": float(shipping_tax_line.get("priceSet", {}).get("shopMoney", {}).get("amount") or 0),
                "total_refunded": float((node.get("totalRefundedSet") or {}).get("shopMoney", {}).get("amount") or 0),
                "duties": float((node.get("currentTotalDutiesSet") or {}).get("shopMoney", {}).get("amount") or 0),
                "additional_fees": float((node.get("currentTotalAdditionalFeesSet") or {}).get("shopMoney", {}).get("amount") or 0),
                "order_level_tax_amount": sum(float(line.get("priceSet", {}).get("shopMoney", {}).get("amount", 0) or 0) for line in order_tax_lines),
                "total_discounts": float(node.get("totalDiscountsSet", {}).get("shopMoney", {}).get("amount") or 0),
                "total_price": float(node.get("totalPriceSet", {}).get("shopMoney", {}).get("amount") or 0),
                "cancelled_at": node.get("cancelledAt"),
                "confirmation_number": node.get("confirmationNumber"),
                "display_fulfillment_status": node.get("displayFulfillmentStatus"),
                "landing_page_url": node.get("landingPageUrl"),
                "note": node.get("note"),
                "tags": node.get("tags"),
                "total_tip_received": float(node.get("totalTipReceived", {}).get("amount") or 0),
                "customer_id": customer.get("id"),
                "customer_country": default_address.get("country"),
                "line_items": [item["node"].get("name") for item in node.get("lineItems", {}).get("edges", [])],
                "vendor": [item["node"].get("vendor") for item in node.get("lineItems", {}).get("edges", [])],
            })
        return orders

    orders_data = parse_orders()
    if not orders_data:
        print("[WARNING] No orders fetched, exiting.")
        return

    df = pd.DataFrame(orders_data)

    # Data cleaning
    df["store_name"] = df["store_name"].astype(str)
    for col in ["total_discounts", "total_price", "total_tip_received", "shipping_line_price", "shipping_line_tax_amount", "shipping_line_tax_rate", "order_level_tax_amount", "duties", "additional_fees", "total_refunded"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # Round to 6 decimal places to avoid precision issues with BigQuery
        df[col] = df[col].round(6)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)  # Ensure UTC
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)  # Ensure UTC
    df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce", utc=True)  # Ensure UTC

    # Directly assign the timezone-aware Series with the dynamic timezone
    df["processed_at_shopify_timezone"] = pd.Series(
        [order['processed_at_shopify_timezone'] for order in orders_data],
        dtype=f'datetime64[ns, {shopify_timezone.zone}]'
    )
    df["processed_at_store_date"] = df["processed_at_shopify_timezone"].dt.date


    df["customer_id"] = df["customer_id"].astype(str).str.extract(r'(\d+)$')
    for col in ["vendor", "line_items", "discount_codes", "payment_gateway_names", "display_financial_status", "email", "name", "currency_code", "cancelled_at", "confirmation_number", "display_fulfillment_status", "landing_page_url", "note", "tags", "customer_id", "customer_country", "shipping_line_title"]:
        df[col] = df[col].fillna("").astype(str)

    print(df.info())

    # Upload to BigQuery
    record_count = len(df)
    to_gbq(df, destination_table=table_id, project_id=config["GCP_PROJECT_ID"], credentials=credentials, if_exists="replace")
    print(f"[SUCCESS] All data uploaded to BigQuery: {table_id} - {record_count} records")
    
    return record_count


