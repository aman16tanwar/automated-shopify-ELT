from google.cloud import bigquery
from google.oauth2 import service_account

# Auth setup
credentials = service_account.Credentials.from_service_account_file("bigquery.json")
client = bigquery.Client(credentials=credentials, project="happyweb-340014")

# List of datasets (stores)
stores = [
    # "shopify_pixie_wing", "shopify_ajjaya","shopify_aqua_life_force","shopify_juvabun","shopify_natura_force","shopify_radotech","shopify_vounot"
"shopify_ninja_kitchen_nz"
]

# Schema for order_items_insights
order_items_insights_schema = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("processed_at", "TIMESTAMP"),
    bigquery.SchemaField("processed_at_store_date", "DATE"),

    bigquery.SchemaField("orders_currency", "STRING"),
    bigquery.SchemaField("orders_email", "STRING"),
    bigquery.SchemaField("orders_financial_status", "STRING"),
    bigquery.SchemaField("orders_name", "STRING"),
    bigquery.SchemaField("orders_payment_gateway_names", "STRING"),
    bigquery.SchemaField("orders_total_discounts", "FLOAT"),
    bigquery.SchemaField("orders_total_price", "FLOAT"),
    bigquery.SchemaField("orders_line_items_id", "STRING"),
    bigquery.SchemaField("orders_line_items_current_quantity", "INTEGER"),
    bigquery.SchemaField("orders_line_items_pre_tax_price", "FLOAT"),
    bigquery.SchemaField("orders_line_items_price", "FLOAT"),
    bigquery.SchemaField("orders_line_items_product_id", "STRING"),
    bigquery.SchemaField("orders_line_items_quantity", "INTEGER"),
    bigquery.SchemaField("orders_line_items_sku", "STRING"),
    bigquery.SchemaField("orders_line_items_title", "STRING"),
    bigquery.SchemaField("orders_line_items_variant_id", "STRING"),
    bigquery.SchemaField("orders_line_items_variant_title", "STRING"),
    bigquery.SchemaField("orders_line_items_tax_lines_rate", "FLOAT"),
    bigquery.SchemaField("orders_line_items_vendor", "STRING"),
    bigquery.SchemaField("orders_line_items_tax_lines_price", "FLOAT"),
    bigquery.SchemaField("orders_shipping_lines_code", "STRING"),
    bigquery.SchemaField("orders_shipping_lines_discounted_price", "FLOAT"),
    bigquery.SchemaField("orders_discount_codes_code", "STRING"),
    bigquery.SchemaField("orders_refunds_refund_line_items_restock_type", "STRING"),
    bigquery.SchemaField("orders_refunds_refund_line_items_subtotal", "FLOAT"),
]




customer_insights_schema = [
    bigquery.SchemaField("store_name", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("display_name", "STRING"),
    bigquery.SchemaField("total_spent", "FLOAT"),
    bigquery.SchemaField("last_order_id", "STRING"),
    bigquery.SchemaField("last_order_name", "STRING"),
    bigquery.SchemaField("orders_count", "INTEGER"),
    bigquery.SchemaField("currency_code", "STRING"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("tags", "STRING"),
    bigquery.SchemaField("default_address_id", "STRING"),
    bigquery.SchemaField("default_address_first_name", "STRING"),
    bigquery.SchemaField("default_address_last_name", "STRING"),
    bigquery.SchemaField("default_address_company", "STRING"),
    bigquery.SchemaField("default_address_address1", "STRING"),
    bigquery.SchemaField("default_address_address2", "STRING"),
    bigquery.SchemaField("default_address_city", "STRING"),
    bigquery.SchemaField("default_address_province", "STRING"),
    bigquery.SchemaField("default_address_country", "STRING"),
    bigquery.SchemaField("default_address_zip", "STRING"),
    bigquery.SchemaField("default_address_phone", "STRING"),
    bigquery.SchemaField("default_address_name", "STRING"),
    bigquery.SchemaField("default_address_country_name", "STRING"),
]


order_insights_schema = [
    {"name": "store_name", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP"},
    {"name": "processed_at", "type": "TIMESTAMP"},
    {"name": "processed_at_shopify_timezone", "type": "TIMESTAMP"},
    {"name": "processed_at_store_date", "type": "DATE"},
    {"name": "currency_code", "type": "STRING"},
    {"name": "discount_codes", "type": "STRING"},       
    {"name": "email", "type": "STRING"},
    {"name": "display_financial_status", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "payment_gateway_names", "type": "STRING"},
    {"name": "total_refunded", "type": "FLOAT"},
    {"name": "shipping_line_title", "type": "STRING"},
    {"name": "shipping_line_price", "type": "FLOAT"},
    {"name": "shipping_line_tax_rate", "type": "FLOAT"},
    {"name": "shipping_line_tax_amount", "type": "FLOAT"},
    {"name": "total_discounts", "type": "FLOAT"},
    {"name": "total_price", "type": "FLOAT"},
    {"name": "cancelled_at", "type": "STRING"},
    {"name": "confirmation_number", "type": "STRING"},
    {"name": "display_fulfillment_status", "type": "STRING"},
    {"name": "landing_page_url", "type": "STRING"},
    {"name": "note", "type": "STRING"},
    {"name": "tags", "type": "STRING"},
    {"name": "total_tip_received", "type": "FLOAT"},
    {"name": "customer_id", "type": "STRING"},
    {"name": "customer_country", "type": "STRING"},
    {"name": "line_items", "type": "STRING"},
    {"name": "vendor", "type": "STRING"},
    {"name": "order_level_tax_amount", "type": "FLOAT"},
    {"name": "duties", "type": "FLOAT"},
    {"name": "additional_fees", "type": "FLOAT"}

    
    
]

products_schema = [
    {"name": "store_name", "type": "STRING"},
    {"name": "created_at", "type": "TIMESTAMP"},
    {"name": "updated_at", "type": "TIMESTAMP"},
    {"name": "id", "type": "STRING"},
    {"name": "title", "type": "STRING"},
    {"name": "product_type", "type": "STRING"},
    {"name": "handle", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "published_at", "type": "TIMESTAMP"},
    {"name": "tags", "type": "STRING"},
    {"name": "vendor", "type": "STRING"},

    # Serialized JSON Arrays (List of variant values as stringified JSON)
    {"name": "variant_id", "type": "STRING"},
    {"name": "variant_sku", "type": "STRING"},
    {"name": "variant_title", "type": "STRING"},
    {"name": "variant_price", "type": "STRING"},
    {"name": "variant_compareAtPrice", "type": "STRING"},
    {"name": "variant_inventoryItem_id", "type": "STRING"},
    {"name": "variant_inventoryQuantity", "type": "STRING"},
    {"name": "variant_image_url", "type": "STRING"},
]



# Loop to create the order_items_insights table
for store in stores:
    dataset_id = f"{client.project}.{store}"

    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset already exists: {dataset_id}")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"🆕 Created dataset: {dataset_id}")

    order_items_table_id = f"{dataset_id}.order_items_insights"
    try:
        client.get_table(order_items_table_id)
        print(f"✅ Table exists: {order_items_table_id}")
    except:
        table = bigquery.Table(order_items_table_id, schema=order_items_insights_schema)
        client.create_table(table)
        print(f"🆕 Created table: {order_items_table_id}")


# Loop to create the order_insights table
for store in stores:
    dataset_id = f"{client.project}.{store}"

    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset already exists: {dataset_id}")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"🆕 Created dataset: {dataset_id}")

    order_items_table_id = f"{dataset_id}.order_insights"
    try:
        client.get_table(order_items_table_id)
        print(f"✅ Table exists: {order_items_table_id}")
    except:
        table = bigquery.Table(order_items_table_id, schema=order_insights_schema)
        client.create_table(table)
        print(f"🆕 Created table: {order_items_table_id}")        

# Loop to create the customer_insights_schema table
for store in stores:
    dataset_id = f"{client.project}.{store}"

    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset already exists: {dataset_id}")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"🆕 Created dataset: {dataset_id}")

    order_items_table_id = f"{dataset_id}.customer_insights"
    try:
        client.get_table(order_items_table_id)
        print(f"✅ Table exists: {order_items_table_id}")
    except:
        table = bigquery.Table(order_items_table_id, schema=customer_insights_schema)
        client.create_table(table)
        print(f"🆕 Created table: {order_items_table_id}")  
#
# 
# # Loop to create the products_insights table
for store in stores:
    dataset_id = f"{client.project}.{store}"

    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset already exists: {dataset_id}")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"🆕 Created dataset: {dataset_id}")

    products_table_id = f"{dataset_id}.products_insights"
    try:
        client.get_table(products_table_id)
        print(f"✅ Table exists: {products_table_id}")
    except:
        table = bigquery.Table(products_table_id, schema=products_schema)
        client.create_table(table)
        print(f"🆕 Created table: {products_table_id}")      
