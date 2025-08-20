# app.py
"""
HappyWeb Shopify Data Pipeline - Client Onboarding Portal
"""

import streamlit as st
import subprocess
import json
import os
import sys
from datetime import datetime, timedelta, timezone
import pandas as pd
import re
import base64
import time
import html

# -------------------------
# Optional: Job Manager import (safe)
# -------------------------
try:
    from job_manager import JobManager
except Exception:
    JobManager = None  # handled later

# -------------------------
# Auth / environment setup
# -------------------------
app_dir = os.path.dirname(os.path.abspath(__file__))

# Prefer ADC; if not set, fall back to project-local JSON for dev
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    credentials_path = os.path.join(os.path.dirname(app_dir), "historical", "bigquery.json")
    if os.path.exists(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# -------------------------
# Page config
# -------------------------
st.set_page_config(
    page_title="HappyWeb - Shopify Data Pipeline",
    page_icon="🌟",
    layout="wide"
)

# -------------------------
# Load / embed logo
# -------------------------
logo_path = os.path.join(app_dir, "happyweb.png")
if os.path.exists(logo_path):
    with open(logo_path, "rb") as f:
        logo_data = base64.b64encode(f.read()).decode()
    logo_html = f'<img src="data:image/png;base64,{logo_data}" height="50">'
else:
    logo_html = ""

# -------------------------
# Styles
# -------------------------
st.markdown("""
<style>
    :root {
        --happyweb-primary: #17D4BE;
        --happyweb-primary-dark: #12B0A0;
        --happyweb-secondary: #002C5F;
        --happyweb-light: #E8FFFE;
        --happyweb-accent: #1DDCC5;
    }
    .main > div { padding-top: 1rem; max-width: 1200px; margin: 0 auto; }
    .block-container { padding: 2rem 1rem; }
    h1, h2, h3 { color: var(--happyweb-secondary); font-weight: 600; }
    div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stVerticalBlock"]) {
        background-color: #ffffff; padding: 1.5rem; border-radius: 12px;
        box-shadow: 0 2px 8px rgba(23,212,190,0.1); margin-bottom: 1.5rem;
        border: 1px solid rgba(23,212,190,0.2);
    }
    .stTextInput > div > div > input {
        background-color: #f8fffe; border: 2px solid #e0f5f3; border-radius: 8px;
        padding: 0.75rem; font-size: 0.95rem; transition: all 0.2s ease;
    }
    .stTextInput > div > div > input:focus {
        background-color: #ffffff; border-color: var(--happyweb-primary);
        box-shadow: 0 0 0 3px rgba(23,212,190,0.2);
    }
    .stButton > button {
        background-color: var(--happyweb-primary); color: white; font-weight: 600;
        padding: 0.75rem 2rem; border-radius: 25px; border: none; transition: all 0.2s ease;
        box-shadow: 0 2px 8px rgba(23,212,190,0.3);
    }
    .stButton > button:hover {
        background-color: var(--happyweb-primary-dark);
        box-shadow: 0 4px 12px rgba(23,212,190,0.4); transform: translateY(-2px);
    }
    .stButton > button[kind="secondary"] {
        background-color: #ffffff; color: var(--happyweb-primary);
        border: 2px solid var(--happyweb-primary);
    }
    .stAlert > div { border-radius: 8px; padding: 1rem 1.25rem; font-size: 0.9rem; border: none; }
    div[data-testid="stInfo"] > div { background-color: var(--happyweb-light); color: var(--happyweb-secondary); border-left: 4px solid var(--happyweb-primary); }
    div[data-testid="stSuccess"] > div { background-color: #e8f5e9; color: #1b5e20; border-left: 4px solid #4caf50; }
    div[data-testid="stWarning"] > div { background-color: #fff3e0; color: #e65100; border-left: 4px solid #ff9800; }
    div[data-testid="stError"] > div { background-color: #ffebee; color: #b71c1c; border-left: 4px solid #f44336; }
    .stProgress > div > div > div > div { background-color: var(--happyweb-primary); }
    div[data-testid="metric-container"] {
        background-color: var(--happyweb-light); padding: 1.25rem; border-radius: 8px;
        border: 1px solid rgba(23,212,190,0.2); text-align: center;
    }
    div[data-testid="metric-container"] label {
        color: var(--happyweb-secondary); font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px;
    }
    div[data-testid="metric-container"] div[data-testid="metric-container-value"] {
        font-size: 2rem; font-weight: 700; color: var(--happyweb-primary);
    }
    .streamlit-expanderHeader { background-color: var(--happyweb-light); border-radius: 8px; font-weight: 600; color: var(--happyweb-secondary); }
    .stDateInput > div > div > input { background-color: #f8fffe; border: 2px solid #e0f5f3; border-radius: 8px; padding: 0.75rem; }
    .header-container {
        background: #ffffff;
        padding: 2rem; border-radius: 12px; margin-bottom: 2rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border: 1px solid #f0f0f0;
    }
    .logo-header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .tagline { color: var(--happyweb-secondary); font-size: 1.1rem; margin-bottom: 2rem; text-align: center; font-weight: 500; }
    .feature-card { background-color: #ffffff; border: 2px solid rgba(23,212,190,0.2); border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; transition: all 0.2s ease; }
    .feature-card:hover { box-shadow: 0 4px 16px rgba(23,212,190,0.2); transform: translateY(-2px); border-color: var(--happyweb-primary); }
    .step-indicator { 
        display: flex; 
        align-items: center; 
        margin: 2rem auto 3rem; 
        max-width: 600px;
        position: relative;
    }
    .step { 
        flex: 1; 
        text-align: center; 
        position: relative; 
        z-index: 1;
    }
    .step-number {
        width: 50px; 
        height: 50px; 
        background-color: #e0f5f3; 
        color: var(--happyweb-secondary);
        border-radius: 50%; 
        display: inline-flex; 
        align-items: center; 
        justify-content: center; 
        font-weight: 700;
        margin: 0 auto 0.75rem; 
        border: 3px solid #e0f5f3;
        font-size: 1.1rem;
        transition: all 0.3s ease;
    }
    .step.active .step-number { 
        background-color: var(--happyweb-primary); 
        color: white; 
        border-color: var(--happyweb-primary); 
        box-shadow: 0 0 0 4px rgba(23,212,190,0.2);
    }
    .step.completed .step-number { 
        background-color: #4caf50; 
        color: white; 
        border-color: #4caf50; 
    }
    .step-title {
        font-weight: 600;
        color: #999;
        font-size: 0.9rem;
        transition: all 0.3s ease;
    }
    .step.active .step-title { 
        color: var(--happyweb-primary); 
    }
    .step.completed .step-title { 
        color: #4caf50; 
    }
    .step-connector {
        position: absolute;
        top: 25px;
        left: 50%;
        width: calc(100% - 50px);
        height: 3px;
        background-color: #e0f5f3;
    }
    .step-connector.completed {
        background-color: #4caf50;
    }
    .step:last-child .step-connector {
        display: none;
    }
    a { color: var(--happyweb-primary) !important; text-decoration: none; }
    a:hover { color: var(--happyweb-primary-dark) !important; text-decoration: underline; }
</style>
""", unsafe_allow_html=True)

# -------------------------
# Header
# -------------------------
st.markdown(f"""
<div class="header-container">
    <div class="logo-header">
        {logo_html}
        <div>
            <h1 style="margin:0;font-size:2rem;font-weight:700;color:var(--happyweb-secondary);">Shopify Data Pipeline</h1>
            <p style="margin:0;opacity:0.8;font-size:0.95rem;color:#666;">Powered by HappyWeb®</p>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="tagline">
    We help wellness brands on Shopify grow sustainably with automated data insights
</div>
""", unsafe_allow_html=True)

# -------------------------
# Helpers
# -------------------------
def normalize_shopify_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    url = url.replace("https://", "").replace("http://", "")
    url = url.split("/")[0]
    return url

def valid_shopify_url(url: str) -> bool:
    return bool(url) and url.endswith(".myshopify.com") and "/" not in url and " " not in url

def mask_token(tok: str) -> str:
    if not tok or len(tok) < 10:
        return "********"
    return tok[:10] + "*" * max(0, (len(tok) - 15)) + tok[-5:]

def load_configs():
    cfg_path = os.path.join(os.path.dirname(app_dir), "historical", "store_config.json")
    if os.path.exists(cfg_path):
        with open(cfg_path, "r") as f:
            return json.load(f), cfg_path
    return [], cfg_path

def save_configs(configs, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(configs, f, indent=2)

def upsert_config(configs, new_cfg, key="MERCHANT"):
    found = False
    for i, c in enumerate(configs):
        if c.get(key) == new_cfg.get(key):
            configs[i] = new_cfg
            found = True
            break
    if not found:
        configs.append(new_cfg)
    return configs

# -------------------------
# BigQuery: dataset & tables
# -------------------------
def create_bq_resources(project_id: str, dataset_name: str):
    from google.cloud import bigquery
    from google.api_core.exceptions import Conflict

    # Client via ADC (uses GOOGLE_APPLICATION_CREDENTIALS if set)
    client = bigquery.Client(project=project_id or None)

    dataset_id = f"{project_id}.{dataset_name}" if project_id else f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    # Create dataset
    try:
        client.create_dataset(dataset, timeout=60)
        st.success(f"✅ Created dataset: {dataset_name}")
    except Conflict:
        st.warning(f"Dataset {dataset_name} already exists")

    # ---- Schemas ----
    NUM = "NUMERIC"
    FLOAT = "FLOAT"
    INT = "INTEGER"
    STR = "STRING"
    TS = "TIMESTAMP"
    DT = "DATE"
    DTN = "DATETIME"

    # Order Items - matching the updated schema from order_items_queue.py
    order_items_insights_schema = [
        bigquery.SchemaField("processed_at_store_date", DT),
        bigquery.SchemaField("created_at", TS),
        bigquery.SchemaField("updated_at", TS),
        bigquery.SchemaField("currency_code", STR),
        bigquery.SchemaField("email", STR),
        bigquery.SchemaField("display_financial_status", STR),
        bigquery.SchemaField("name", STR),
        bigquery.SchemaField("payment_gateway_names", STR),
        bigquery.SchemaField("total_discounts", FLOAT),
        bigquery.SchemaField("total_price", FLOAT),
        bigquery.SchemaField("line_item_id", STR),
        bigquery.SchemaField("line_item_current_quantity", INT),
        bigquery.SchemaField("line_item_pre_tax_price", FLOAT),
        bigquery.SchemaField("line_item_price", FLOAT),
        bigquery.SchemaField("line_item_discount", FLOAT),
        bigquery.SchemaField("line_item_product_id", STR),
        bigquery.SchemaField("line_item_quantity", INT),
        bigquery.SchemaField("line_item_sku", STR),
        bigquery.SchemaField("line_item_title", STR),
        bigquery.SchemaField("line_item_variant_id", STR),
        bigquery.SchemaField("line_item_variant_title", STR),
        bigquery.SchemaField("line_item_tax_rate", FLOAT),
        bigquery.SchemaField("line_item_vendor", STR),
        bigquery.SchemaField("line_item_tax_price", FLOAT),
        bigquery.SchemaField("shipping_line_code", STR),
        bigquery.SchemaField("shipping_line_discounted_price", FLOAT),
        bigquery.SchemaField("discount_code", STR),
        bigquery.SchemaField("refund_restock_type", STR),
        bigquery.SchemaField("refund_subtotal", FLOAT),
    ]

    # Customers
    customer_insights_schema = [
        bigquery.SchemaField("store_name", STR),
        bigquery.SchemaField("created_at", TS),
        bigquery.SchemaField("updated_at", TS),
        bigquery.SchemaField("id", STR),
        bigquery.SchemaField("email", STR),
        bigquery.SchemaField("first_name", STR),
        bigquery.SchemaField("display_name", STR),
        bigquery.SchemaField("total_spent", FLOAT),
        bigquery.SchemaField("last_order_id", STR),
        bigquery.SchemaField("last_order_name", STR),
        bigquery.SchemaField("orders_count", INT),
        bigquery.SchemaField("currency_code", STR),
        bigquery.SchemaField("phone", STR),
        bigquery.SchemaField("note", STR),
        bigquery.SchemaField("tags", STR, mode="REPEATED"),
        bigquery.SchemaField("default_address_id", STR),
        bigquery.SchemaField("default_address_first_name", STR),
        bigquery.SchemaField("default_address_last_name", STR),
        bigquery.SchemaField("default_address_company", STR),
        bigquery.SchemaField("default_address_address1", STR),
        bigquery.SchemaField("default_address_address2", STR),
        bigquery.SchemaField("default_address_city", STR),
        bigquery.SchemaField("default_address_province", STR),
        bigquery.SchemaField("default_address_country", STR),
        bigquery.SchemaField("default_address_zip", STR),
        bigquery.SchemaField("default_address_phone", STR),
        bigquery.SchemaField("default_address_name", STR),
    ]

    # Orders (DATETIME + tz string preserves Shopify local time)
    order_insights_schema = [
        bigquery.SchemaField("store_name", STR),
        bigquery.SchemaField("created_at", TS),
        bigquery.SchemaField("updated_at", TS),
        bigquery.SchemaField("processed_at", TS),
        bigquery.SchemaField("processed_at_shopify_timezone", TS),
        bigquery.SchemaField("processed_at_store_date", DT),
        bigquery.SchemaField("currency_code", STR),
        bigquery.SchemaField("discount_codes", STR),
        bigquery.SchemaField("email", STR),
        bigquery.SchemaField("display_financial_status", STR),
        bigquery.SchemaField("name", STR),
        bigquery.SchemaField("payment_gateway_names", STR),
        bigquery.SchemaField("total_refunded", FLOAT),
        bigquery.SchemaField("shipping_line_title", STR),
        bigquery.SchemaField("shipping_line_price", FLOAT),
        bigquery.SchemaField("shipping_line_tax_rate", FLOAT),
        bigquery.SchemaField("shipping_line_tax_amount", FLOAT),
        bigquery.SchemaField("total_discounts", FLOAT),
        bigquery.SchemaField("total_price", FLOAT),
        bigquery.SchemaField("cancelled_at", STR),
        bigquery.SchemaField("confirmation_number", STR),
        bigquery.SchemaField("display_fulfillment_status", STR),
        bigquery.SchemaField("landing_page_url", STR),
        bigquery.SchemaField("note", STR),
        bigquery.SchemaField("tags", STR),
        bigquery.SchemaField("total_tip_received", FLOAT),
        bigquery.SchemaField("customer_id", STR),
        bigquery.SchemaField("customer_country", STR),
        bigquery.SchemaField("line_items", STR),
        bigquery.SchemaField("vendor", STR),
        bigquery.SchemaField("order_level_tax_amount", FLOAT),
        bigquery.SchemaField("duties", FLOAT),
        bigquery.SchemaField("additional_fees", FLOAT),
    ]

    # Products
    products_schema = [
        bigquery.SchemaField("store_name", STR),
        bigquery.SchemaField("created_at", TS),
        bigquery.SchemaField("updated_at", TS),
        bigquery.SchemaField("id", STR),
        bigquery.SchemaField("title", STR),
        bigquery.SchemaField("product_type", STR),
        bigquery.SchemaField("handle", STR),
        bigquery.SchemaField("status", STR),
        bigquery.SchemaField("published_at", TS),
        bigquery.SchemaField("tags", STR),
        bigquery.SchemaField("vendor", STR),
        bigquery.SchemaField("variant_id", STR),
        bigquery.SchemaField("variant_sku", STR),
        bigquery.SchemaField("variant_title", STR),
        bigquery.SchemaField("variant_price", STR),
        bigquery.SchemaField("variant_compareAtPrice", STR),
        bigquery.SchemaField("variant_inventoryItem_id", STR),
        bigquery.SchemaField("variant_inventoryQuantity", INT),
        bigquery.SchemaField("variant_image_url", STR),
    ]

    tables_to_create = {
        "customer_insights": (customer_insights_schema, None, None),
        "order_insights": (order_insights_schema,
                           ("processed_at_store_date", bigquery.TimePartitioningType.DAY),
                           ["customer_id", "display_financial_status"]),
        "order_items_insights": (order_items_insights_schema,
                                 ("processed_at_store_date", bigquery.TimePartitioningType.DAY),
                                 ["name", "line_item_sku"]),
        "products_insights": (products_schema, None, None),
    }

    for table_name, (schema, partitioning, clustering) in tables_to_create.items():
        table_id = f"{dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)

        if partitioning:
            field, ptype = partitioning
            table.time_partitioning = bigquery.TimePartitioning(type_=ptype, field=field)
        if clustering:
            table.clustering_fields = clustering

        try:
            client.create_table(table)
            st.success(f"✅ Created table: {table_name}")
        except Conflict:
            pass  # already exists

    # Return resolved project id for downstream messaging
    return client.project, dataset_name

# -------------------------
# Session
# -------------------------
if "current_step" not in st.session_state:
    st.session_state.current_step = 1

# -------------------------
# Helper function for stepper
# -------------------------
def render_stepper(current_step):
    # Create columns for the steps
    cols = st.columns(3)
    
    steps = [
        {"number": 1, "title": "Store Details", "icon": "🏪"},
        {"number": 2, "title": "Configuration", "icon": "⚙️"},
        {"number": 3, "title": "Load Data", "icon": "📥"}
    ]
    
    for i, (col, step) in enumerate(zip(cols, steps)):
        with col:
            step_num = step["number"]
            if step_num < current_step:
                st.markdown(f"""
                <div style="text-align: center;">
                    <div style="display: inline-block; width: 50px; height: 50px; background-color: #4caf50; color: white; border-radius: 50%; line-height: 50px; font-size: 1.5rem; font-weight: bold; margin-bottom: 0.5rem;">
                        ✓
                    </div>
                    <div style="color: #4caf50; font-weight: 600;">{step["title"]}</div>
                </div>
                """, unsafe_allow_html=True)
            elif step_num == current_step:
                st.markdown(f"""
                <div style="text-align: center;">
                    <div style="display: inline-block; width: 50px; height: 50px; background-color: #17D4BE; color: white; border-radius: 50%; line-height: 50px; font-size: 1.5rem; font-weight: bold; margin-bottom: 0.5rem; box-shadow: 0 0 0 4px rgba(23,212,190,0.2);">
                        {step_num}
                    </div>
                    <div style="color: #17D4BE; font-weight: 600;">{step["title"]}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="text-align: center;">
                    <div style="display: inline-block; width: 50px; height: 50px; background-color: #e0f5f3; color: #666; border-radius: 50%; line-height: 50px; font-size: 1.5rem; font-weight: bold; margin-bottom: 0.5rem;">
                        {step_num}
                    </div>
                    <div style="color: #999; font-weight: 600;">{step["title"]}</div>
                </div>
                """, unsafe_allow_html=True)
    
    # Add a visual separator line based on current step
    if current_step == 1:
        progress_bg = "linear-gradient(to right, #17D4BE 0%, #17D4BE 33%, #e0f5f3 33%, #e0f5f3 100%)"
    elif current_step == 2:
        progress_bg = "linear-gradient(to right, #4caf50 0%, #4caf50 33%, #17D4BE 33%, #17D4BE 66%, #e0f5f3 66%, #e0f5f3 100%)"
    else:  # step 3
        progress_bg = "linear-gradient(to right, #4caf50 0%, #4caf50 66%, #17D4BE 66%, #17D4BE 100%)"
    
    st.markdown(f"""
    <div style="margin: 1rem 0 2rem 0; height: 2px; background: {progress_bg};"></div>
    """, unsafe_allow_html=True)

# Connected stores banner
try:
    configs, _ = load_configs()
    if configs:
        st.markdown(f"""
        <div style="background-color: var(--happyweb-light); padding: 1rem; border-radius: 8px; margin-bottom: 2rem; text-align: center; border: 1px solid rgba(23,212,190,0.3);">
            <strong style="color: var(--happyweb-secondary);">🏪 {len(configs)} Store{'s' if len(configs) != 1 else ''} Connected</strong>
        </div>
        """, unsafe_allow_html=True)
except Exception:
    pass

# -------------------------
# Initialize Job Manager
# -------------------------
# Note: Job Manager uses the centralized dataset 'shopify_logs'
# This dataset contains logs and job tracking for ALL stores
try:
    # Get project ID from environment or use default
    project_id = os.environ.get("GCP_PROJECT_ID", "happyweb-340014")
    job_manager = JobManager(project_id=project_id) if JobManager is not None else None
except Exception as e:
    st.error(f"Failed to initialize job manager: {e}")
    job_manager = None

# -------------------------
# Tabs with session state tracking
# -------------------------
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0

# Create tabs
tabs = st.tabs(["🚀 Connect New Store", "📊 Connected Stores", "📈 Pipeline Jobs"])

# Handle tab navigation
selected_tab = st.session_state.get("active_tab", 0)
if selected_tab > len(tabs) - 1:
    selected_tab = 0

tab1, tab2, tab3 = tabs

# -------------------------
# Tab 1: Connect
# -------------------------
with tab1:
    if st.session_state.get("show_historical"):
        st.markdown("## 📥 Load Historical Data")
        
        # Show step 3 of the stepper
        render_stepper(3)

        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown(f"""
            <div class="feature-card">
                <h3>Ready to Load Historical Data</h3>
                <p><strong>Store:</strong> {st.session_state.get('last_merchant', 'N/A')}</p>
                <p><strong>Dataset:</strong> {st.session_state.get('last_dataset', 'N/A')}</p>
                <p><strong>Start Date:</strong> {st.session_state.get('backfill_date', 'N/A')}</p>
            </div>
            """, unsafe_allow_html=True)

            if st.button("🚀 Start Historical Data Load", type="primary", use_container_width=True):
                if job_manager:
                    try:
                        # Get store config
                        merchant = st.session_state.get('last_merchant')
                        cfgs, cfg_path = load_configs()
                        store_config = next((c for c in cfgs if c.get("MERCHANT") == merchant), None)
                        
                        if store_config:
                            # Create job and start async processing
                            job_id = job_manager.create_job(
                                store_url=merchant,
                                dataset_name=st.session_state.get('last_dataset'),
                                job_type="historical_load"
                            )
                            
                            # Start async processing
                            job_manager.run_historical_load_async(store_config, job_id)
                            
                            # Update last_updated
                            store_config["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                            cfgs = upsert_config(cfgs, store_config, key="MERCHANT")
                            save_configs(cfgs, cfg_path)
                            
                            # Set flags for navigation
                            st.session_state.show_historical = False
                            st.session_state.show_pipeline_notification = True
                            st.session_state.highlight_job_id = job_id  # Store for highlighting
                            st.session_state.just_started_job = True
                            st.session_state.started_job_id = job_id
                            
                            # Show success with clear navigation
                            st.success(f"✅ Historical data load started! Job ID: {job_id}")
                            st.markdown("""
                            <div style="background-color: #e8fffe; padding: 1.5rem; border-radius: 8px; margin: 1rem 0; border: 2px solid var(--happyweb-primary);">
                                <h3 style="margin: 0 0 1rem 0; color: var(--happyweb-secondary);">🎉 Pipeline Started Successfully!</h3>
                                <p style="margin: 0.5rem 0;">Your historical data is now being loaded. This typically takes 0.5-4 hours depending on data volume.</p>
                                <p style="margin: 0.5rem 0;"><strong>👉 Click the "Pipeline Jobs" tab above to monitor real-time progress and logs!</strong></p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Don't rerun immediately to give user time to see the message
                            time.sleep(3)
                        else:
                            st.error("Store configuration not found")
                    except Exception as e:
                        st.error(f"Error starting historical load: {e}")
                else:
                    # Fallback to synchronous processing if job manager not available
                    with st.spinner("Loading historical data... This may take 0.5–4 hours depending on data volume."):
                        try:
                            historical_script = os.path.join(os.path.dirname(app_dir), "historical", "main.py")

                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            output_container = st.empty()
                            output_text = []

                            # Get merchant name for TARGET_STORE
                            merchant = st.session_state.get('last_merchant')
                            env = os.environ.copy()
                            if merchant:
                                env["TARGET_STORE"] = merchant
                            
                            # Use text=True for line-buffered reads.
                            process = subprocess.Popen(
                                [sys.executable, historical_script],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True,
                                bufsize=1,
                                cwd=os.path.dirname(historical_script),
                                env=env
                            )

                            for line in iter(process.stdout.readline, ''):
                                if not line:
                                    break
                                ln = line.rstrip("\n")
                                output_text.append(ln)

                                # Heuristic progress updates
                                if "[PROCESSING]" in ln:
                                    progress_bar.progress(25)
                                    status_text.text("Processing store data...")
                                elif "[FETCHING]" in ln:
                                    progress_bar.progress(50)
                                    status_text.text("Fetching data from Shopify...")
                                elif "[SUCCESS]" in ln:
                                    progress_bar.progress(75)
                                    status_text.text("Loading data to BigQuery...")
                                elif "[COMPLETED]" in ln:
                                    progress_bar.progress(100)
                                    status_text.text("Completed!")

                                # Show more logs with scrollable container
                                log_display = "\n".join(output_text[-100:])  # Show last 100 lines instead of 12
                                output_container.markdown(
                                    f'<div style="background-color: #1e1e1e; color: #ffffff; padding: 1rem; '
                                    f'border-radius: 8px; font-family: monospace; font-size: 0.85rem; '
                                    f'height: 400px; overflow-y: auto; white-space: pre-wrap;">{log_display}</div>',
                                    unsafe_allow_html=True
                                )

                            process.wait()

                            if process.returncode == 0:
                                # Update last_updated in config
                                try:
                                    cfgs, cfg_path = load_configs()
                                    merchant = st.session_state.get("last_merchant")
                                    for i, cfg in enumerate(cfgs):
                                        if cfg.get("MERCHANT") == merchant:
                                            cfgs[i]["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                                            break
                                    save_configs(cfgs, cfg_path)
                                except Exception:
                                    pass

                                st.balloons()
                                st.success("✅ Historical data load completed successfully!")
                                st.info("Check the Connected Stores tab to see your data.")
                                st.session_state.show_historical = False
                                st.session_state.just_completed_sync = True
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error("❌ Historical data load failed")
                                with st.expander("View Full Output"):
                                    st.code("\n".join(output_text), language="text")
                        except Exception as e:
                            st.error(f"Error running historical load: {e}")

            if st.button("← Back to Onboarding", type="secondary"):
                st.session_state.show_historical = False
                st.rerun()

        with col2:
            st.info("""
**📊 What happens next:**
- Connects to Shopify API
- Fetches all historical data
- Loads into BigQuery tables
- ~0.5–4 hours depending on volume

**📈 Data loaded:**
- Customer profiles
- Order history
- Product catalog
- Order line items
""")
    else:
        # Check if we just started a job and show navigation prompt
        if st.session_state.get("just_started_job"):
            job_id = st.session_state.get("started_job_id", "")
            st.markdown(f"""
            <div style="background-color: #e8fffe; padding: 1.5rem; border-radius: 8px; margin-bottom: 2rem; border: 2px solid var(--happyweb-primary); text-align: center;">
                <h3 style="margin: 0 0 0.5rem 0; color: var(--happyweb-secondary);">🚀 Your Pipeline is Running!</h3>
                <p style="margin: 0.5rem 0;">Job ID: <code>{job_id[:8]}...</code></p>
                <p style="margin: 0.5rem 0; font-size: 1.1rem;"><strong>👉 Click the "Pipeline Jobs" tab to view progress and logs</strong></p>
            </div>
            """, unsafe_allow_html=True)
            # Clear the flag after showing
            del st.session_state["just_started_job"]
            if "started_job_id" in st.session_state:
                del st.session_state["started_job_id"]
        
        # Check if we just completed a sync load
        if st.session_state.get("just_completed_sync"):
            st.markdown("""
            <div style="background-color: #e8f5e9; padding: 1.5rem; border-radius: 8px; margin-bottom: 2rem; border: 2px solid #4caf50; text-align: center;">
                <h3 style="margin: 0 0 0.5rem 0; color: #1b5e20;">✅ Historical Data Load Complete!</h3>
                <p style="margin: 0.5rem 0; font-size: 1.1rem;"><strong>👉 Check the "Connected Stores" tab to see your data</strong></p>
            </div>
            """, unsafe_allow_html=True)
            del st.session_state["just_completed_sync"]
        
        st.markdown("## 🚀 Connect New Store")
        
        # Show step 1 of the stepper (Store Details + Configuration combined)
        render_stepper(1)

        with st.form(key="onboarding_form_tab1", clear_on_submit=False):
            st.markdown("### 🏪 Store Details")
            c1, c2 = st.columns(2)
            with c1:
                merchant_url = st.text_input(
                    "Shopify Store URL",
                    placeholder="example.myshopify.com",
                    help="The .myshopify.com URL of your store"
                )
                access_token = st.text_input(
                    "Access Token",
                    type="password",
                    placeholder="shpat_xxxxxxxxxxxxx",
                    help="Your Shopify Admin API access token"
                )
            with c2:
                # Use key to maintain user input
                dataset_name = st.text_input(
                    "BigQuery Dataset Name",
                    placeholder="shopify_store_name",
                    help="Dataset name for BigQuery (must start with 'shopify_')"
                )

                # Let project be editable to avoid mismatches
                project_id = st.text_input(
                    "GCP Project ID",
                    value=os.getenv("GCP_PROJECT_ID", ""),  # optional default
                    help="Target Google Cloud Project ID (must have BigQuery permissions)"
                )

            st.markdown("---")

            st.markdown("### 📅 Historical Data Settings")
            c3, c4 = st.columns([1, 1])
            with c3:
                default_date = datetime.now() - timedelta(days=730)
                backfill_date = st.date_input(
                    "Start Date for Historical Data",
                    value=default_date,
                    min_value=datetime(2015, 1, 1),
                    max_value=datetime.now(),
                    help="How far back to fetch historical data"
                )
            with c4:
                st.markdown("")
                st.info("""
📊 **Processing Info**
- Time: Few hours
- Data starts from selected date
- All historical data will be fetched
                """)

            if dataset_name:
                st.info(f"""
**📦 Resources to be created:**
- **BigQuery Dataset:** `{dataset_name}`
- **Tables:** customer_insights, order_insights, order_items_insights, products_insights
- **Service Name:** `shopify-historical-{dataset_name.replace('shopify_', '')}`
""")

            st.markdown("---")
            accept_terms = st.checkbox(
                "I confirm I have permission to access this Shopify store's data",
                help="You must be authorized to access this store's data"
            )

            col_btn = st.columns([1, 2, 1])[1]
            with col_btn:
                submitted = st.form_submit_button(
                    "🚀 Connect Store",
                    type="primary",
                    use_container_width=True
                )

        if submitted:
            errors = []

            merchant_url_norm = normalize_shopify_url(merchant_url)
            if not accept_terms:
                errors.append("You must confirm you have permission")
            if not all([merchant_url_norm, access_token, dataset_name]):
                errors.append("All fields are required")
            if not valid_shopify_url(merchant_url_norm):
                errors.append("Store URL must end with .myshopify.com and not include protocol or path")
            if not access_token.startswith("shpat_"):
                errors.append("Access token must start with 'shpat_'")
            if dataset_name:
                if not dataset_name.startswith("shopify_"):
                    errors.append("Dataset name must start with 'shopify_'")
                if not re.match(r'^[a-z0-9_]+$', dataset_name):
                    errors.append("Dataset name must contain only lowercase letters, numbers, and underscores")
            if not project_id:
                errors.append("GCP Project ID is required")

            if errors:
                for e in errors:
                    st.error(f"❌ {e}")
            else:
                progress_container = st.container()
                with progress_container:
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    try:
                        # Step 1: BigQuery
                        status_text.text("📊 Creating BigQuery dataset and tables...")
                        progress_bar.progress(20)
                        resolved_project, _ = create_bq_resources(project_id, dataset_name)

                        # Step 2: Save config
                        status_text.text("📝 Saving configuration...")
                        progress_bar.progress(60)

                        config = {
                            "MERCHANT": merchant_url_norm,
                            "TOKEN": access_token,  # For production, move this to Secret Manager.
                            "GCP_PROJECT_ID": resolved_project,
                            "BIGQUERY_DATASET": dataset_name,
                            "BIGQUERY_TABLE_CUSTOMER_INSIGHTS": "customer_insights",
                            "BIGQUERY_TABLE_ORDER_INSIGHTS": "order_insights",
                            "BIGQUERY_TABLE_ORDER_ITEMS_INSIGHTS": "order_items_insights",
                            "BIGQUERY_TABLE_PRODUCT_INSIGHTS": "products_insights",
                            "BIGQUERY_CREDENTIALS_PATH": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
                            "BACKFILL_START_DATE": backfill_date.strftime("%Y-%m-%d")
                        }

                        cfgs, cfg_path = load_configs()
                        cfgs = upsert_config(cfgs, config, key="MERCHANT")
                        save_configs(cfgs, cfg_path)

                        # Finalize
                        status_text.text("✅ Configuration complete!")
                        progress_bar.progress(100)

                        st.session_state["last_merchant"] = merchant_url_norm
                        st.session_state["last_dataset"] = dataset_name
                        st.session_state["backfill_date"] = backfill_date.strftime("%Y-%m-%d")
                        st.session_state["show_historical"] = True
                        st.session_state["current_step"] = 2  # Move to step 2

                        st.success(f"""
✅ **Store Connected Successfully!**

**{merchant_url_norm}** has been connected to HappyWeb's data pipeline.
You're ready to load historical data and start gaining insights!
""")
                        time.sleep(1)  # Brief pause to show success
                        st.rerun()

                    except Exception as e:
                        st.error(f"❌ Error during onboarding: {e}")
                        st.info("Please check your configuration and try again")

# -------------------------
# Tab 2: Connected Stores
# -------------------------
with tab2:
    st.markdown("## 📊 Connected Stores")
    try:
        cfgs, cfg_path = load_configs()
        if cfgs:
            table_data = []
            for c in cfgs:
                table_data.append({
                    "Store URL": c.get("MERCHANT"),
                    "Dataset": c.get("BIGQUERY_DATASET"),
                    "Start Date": c.get("BACKFILL_START_DATE", "N/A"),
                    "Last Updated": c.get("last_updated", "Never"),
                    "Status": "🟢 Active",
                    "_merchant": c.get("MERCHANT"),
                    "_token": c.get("TOKEN", "")
                })
            df = pd.DataFrame(table_data)

            c1, c2, c3 = st.columns(3)
            c1.metric("Total Stores", len(cfgs))
            c2.metric("Active Stores", len(cfgs))
            c3.metric("Data Volume", "~100GB+")

            st.markdown("---")
            display_df = df[["Store URL", "Dataset", "Start Date", "Last Updated", "Status"]]
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Store URL": st.column_config.TextColumn("Store URL", width="medium"),
                    "Dataset": st.column_config.TextColumn("BigQuery Dataset", width="medium"),
                    "Start Date": st.column_config.TextColumn("Start Date", width="small"),
                    "Last Updated": st.column_config.TextColumn("Last Updated", width="medium"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                }
            )

            st.markdown("---")
            st.markdown("### 🔄 Manage Store Data")

            store_options = [c.get("MERCHANT") for c in cfgs]
            selected_store = st.selectbox(
                "Select a store to manage:",
                options=store_options,
                help="Choose a store to update its configuration or reload historical data"
            )

            if selected_store:
                selected_cfg = next((c for c in cfgs if c.get("MERCHANT") == selected_store), None)
                if selected_cfg:
                    cc1, cc2 = st.columns(2)
                    with cc1:
                        st.markdown("#### Update Configuration")
                        current_mask = mask_token(selected_cfg.get("TOKEN", ""))
                        new_token = st.text_input(
                            "Access Token",
                            type="password",
                            placeholder=current_mask,
                            help="Enter a new token to update, or leave blank to keep current token"
                        )

                        default_start = selected_cfg.get("BACKFILL_START_DATE", "2023-01-01")
                        try:
                            default_dt = datetime.strptime(default_start, "%Y-%m-%d")
                        except Exception:
                            default_dt = datetime(2023, 1, 1)

                        new_start_date = st.date_input(
                            "Historical Data Start Date",
                            value=default_dt,
                            help="Update the start date for historical data"
                        )

                        if st.button("💾 Update Configuration", type="secondary"):
                            if new_token:
                                selected_cfg["TOKEN"] = new_token
                            selected_cfg["BACKFILL_START_DATE"] = new_start_date.strftime("%Y-%m-%d")
                            # Persist
                            cfgs = upsert_config(cfgs, selected_cfg, key="MERCHANT")
                            save_configs(cfgs, cfg_path)
                            st.success("✅ Configuration updated successfully!")
                            st.rerun()

                    with cc2:
                        st.markdown("#### Reload Historical Data")
                        st.info(f"""
**Current Settings:**
- Store: {selected_cfg.get('MERCHANT')}
- Dataset: {selected_cfg.get('BIGQUERY_DATASET')}
- Start Date: {selected_cfg.get('BACKFILL_START_DATE', 'N/A')}
""".strip())

                        if st.button("🔄 Restart Historical Load", type="primary"):
                            if job_manager:
                                try:
                                    # Create job and start async processing
                                    job_id = job_manager.create_job(
                                        store_url=selected_cfg['MERCHANT'],
                                        dataset_name=selected_cfg['BIGQUERY_DATASET'],
                                        job_type="historical_load"
                                    )
                                    
                                    # Start async processing
                                    job_manager.run_historical_load_async(selected_cfg, job_id)
                                    
                                    st.success(f"✅ Historical reload started! Job ID: {job_id}")
                                    st.info("👉 Check the 'Pipeline Jobs' tab to monitor progress and view logs.")
                                    
                                    # Update last_updated
                                    selected_cfg["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                                    cfgs = upsert_config(cfgs, selected_cfg, key="MERCHANT")
                                    save_configs(cfgs, cfg_path)
                                except Exception as e:
                                    st.error(f"Error starting historical reload: {e}")
                            else:
                                st.session_state["restart_store"] = selected_cfg
                                st.session_state["show_restart_historical"] = True
                                st.rerun()
        else:
            st.info("No stores connected yet. Go to the 'Connect New Store' tab to get started!")
    except Exception as e:
        st.error(f"Error loading stores: {e}")

# -------------------------
# Restart historical section
# -------------------------
if st.session_state.get("show_restart_historical"):
    restart_cfg = st.session_state.get("restart_store")
    if restart_cfg:
        st.markdown("---")
        st.markdown("## 🔄 Reloading Historical Data")
        st.info(f"Reloading data for **{restart_cfg.get('MERCHANT')}**...")

        with st.spinner(f"Loading historical data for {restart_cfg.get('MERCHANT')}... This may take 0.5–4 hours."):
            try:
                # Update last_updated immediately
                restart_cfg["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                cfgs, cfg_path = load_configs()
                cfgs = upsert_config(cfgs, restart_cfg, key="MERCHANT")
                save_configs(cfgs, cfg_path)

                historical_script = os.path.join(os.path.dirname(app_dir), "historical", "main.py")

                progress_bar = st.progress(0)
                status_text = st.empty()
                output_container = st.empty()
                output_text = []

                process = subprocess.Popen(
                    [sys.executable, historical_script],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    cwd=os.path.dirname(historical_script),
                    env={**os.environ, "TARGET_STORE": restart_cfg.get("MERCHANT", "")}
                )

                for line in iter(process.stdout.readline, ''):
                    if not line:
                        break
                    ln = line.rstrip("\n")
                    output_text.append(ln)

                    if "[PROCESSING]" in ln:
                        progress_bar.progress(25)
                        status_text.text("Processing store data...")
                    elif "[FETCHING]" in ln:
                        progress_bar.progress(50)
                        status_text.text("Fetching data from Shopify...")
                    elif "[SUCCESS]" in ln:
                        progress_bar.progress(75)
                        status_text.text("Loading data to BigQuery...")
                    elif "[COMPLETED]" in ln:
                        progress_bar.progress(100)
                        status_text.text("Completed!")

                    output_container.code("\n".join(output_text[-12:]), language="text")

                process.wait()

                if process.returncode == 0:
                    st.balloons()
                    st.success(f"✅ Historical data reload completed successfully for {restart_cfg.get('MERCHANT')}!")
                    st.session_state["show_restart_historical"] = False
                    st.session_state.pop("restart_store", None)
                    st.rerun()
                else:
                    st.error("❌ Historical data reload failed")
                    with st.expander("View Full Output"):
                        st.code("\n".join(output_text), language="text")

            except Exception as e:
                st.error(f"Error running historical reload: {e}")

        if st.button("← Back to Connected Stores"):
            st.session_state["show_restart_historical"] = False
            st.session_state.pop("restart_store", None)
            st.rerun()

# -------------------------
# Tab 3: Pipeline Jobs
# -------------------------
with tab3:
    # Header with auto-refresh controls
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.markdown("## 📈 Pipeline Jobs")
    with col2:
        auto_refresh = st.checkbox("Auto-refresh", value=False, help="Automatically refresh every 5 seconds")
    with col3:
        if st.button("🔄 Refresh Now"):
            st.rerun()
    
    # Add auto-refresh JavaScript if enabled
    if auto_refresh:
        st.markdown(
            """
            <script>
            setTimeout(function() {
                window.location.reload();
            }, 5000);
            </script>
            """,
            unsafe_allow_html=True
        )
    
    # Show notification if redirected from historical load
    if st.session_state.get("show_pipeline_notification"):
        st.success("🎉 Your historical data pipeline has started! Monitor the progress below.")
        del st.session_state["show_pipeline_notification"]
    
    if job_manager:
        # Get stats for summary
        all_active_jobs = job_manager.get_active_jobs() if job_manager else []
        recent_jobs = job_manager.get_recent_jobs(limit=50)
        
        # Calculate statistics
        active_count = len(all_active_jobs)
        completed_today = sum(1 for j in recent_jobs if j.status == "completed" and 
                            hasattr(j, 'completed_at') and j.completed_at and 
                            j.completed_at.date() == datetime.now(timezone.utc).date())
        failed_today = sum(1 for j in recent_jobs if j.status == "failed" and 
                         hasattr(j, 'started_at') and j.started_at and 
                         j.started_at.date() == datetime.now(timezone.utc).date())
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("🏃 Active Jobs", active_count, 
                     delta=f"+{active_count}" if active_count > 0 else None,
                     delta_color="normal")
        with col2:
            st.metric("✅ Completed Today", completed_today)
        with col3:
            st.metric("❌ Failed Today", failed_today,
                     delta_color="inverse" if failed_today > 0 else "off")
        with col4:
            avg_duration = 0
            completed_jobs = [j for j in recent_jobs if j.status == "completed" and hasattr(j, 'duration_seconds') and j.duration_seconds]
            if completed_jobs:
                avg_duration = sum(j.duration_seconds for j in completed_jobs) / len(completed_jobs)
                avg_hours = int(avg_duration // 3600)
                avg_minutes = int((avg_duration % 3600) // 60)
                st.metric("⏱️ Avg Duration", f"{avg_hours}h {avg_minutes}m")
            else:
                st.metric("⏱️ Avg Duration", "N/A")
        
        st.markdown("---")
        # Clean up old cancelled jobs from session state (older than 5 minutes)
        if 'cancelled_jobs' in st.session_state and st.session_state.cancelled_jobs:
            current_time = datetime.now()
            cleaned_cancelled = {}
            for job_id, cancel_time in st.session_state.cancelled_jobs.items():
                if isinstance(cancel_time, datetime) and (current_time - cancel_time).seconds < 300:
                    cleaned_cancelled[job_id] = cancel_time
            st.session_state.cancelled_jobs = cleaned_cancelled
        # Better controls
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("🔄 Refresh", help="Refresh job status"):
                st.rerun()
        with col2:
            # Add cleanup button for stuck jobs
            if st.button("🧹 Clean stuck jobs", type="secondary", help="Mark jobs pending >1hr as failed"):
                with st.spinner("Cleaning stuck jobs..."):
                    cleaned, total = job_manager.force_clean_stuck_jobs(hours=1)
                    if cleaned > 0:
                        st.success(f"✅ Cleaned {cleaned} out of {total} stuck jobs")
                        time.sleep(1)
                        st.rerun()
                    elif total > 0:
                        st.warning(f"⚠️ Found {total} stuck jobs but couldn't clean them")
                    else:
                        st.info("No stuck jobs found")
        with col3:
            # Debug mode toggle
            debug_mode = st.checkbox("🐛 Debug", value=False)
        
        # Filter out any jobs that were cancelled in this session
        cancelled_in_session = st.session_state.get('cancelled_jobs', {})
        # Only filter recently cancelled (within 5 minutes)
        active_jobs = []
        for job in all_active_jobs or []:
            if job.job_id in cancelled_in_session:
                cancelled_time = cancelled_in_session[job.job_id]
                if isinstance(cancelled_time, datetime) and (datetime.now() - cancelled_time).seconds < 300:
                    continue  # Skip this job, it was recently cancelled
            active_jobs.append(job)
        
        # Debug information
        if debug_mode:
            with st.expander("🐛 Debug Info", expanded=True):
                st.write("### Active Jobs Query Results:")
                if active_jobs:
                    for job in active_jobs:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.text(f"Job ID: {job.job_id[:16]}...")
                            st.text(f"Status: {job.status}")
                            st.text(f"Store: {job.store_url}")
                        with col2:
                            st.text(f"Started: {job.started_at}")
                            st.text(f"Running: {job.running_seconds}s")
                            if st.button(f"Check records", key=f"debug_{job.job_id}"):
                                records = job_manager.debug_job_status(job.job_id)
                                st.write("All records for this job:")
                                for r in records:
                                    st.text(f"  {r.status} at {r.started_at}")
                        st.markdown("---")
                else:
                    st.info("No active jobs found")
        
        if active_jobs:
            st.warning(f"🔄 {len(active_jobs)} active job(s)")
        
        # Show active jobs with better UI
        st.markdown("### 🏃 Active Jobs")
        if active_jobs:
            # Check if we should highlight a specific job
            highlight_job_id = st.session_state.get("highlight_job_id")
            if highlight_job_id:
                # Clear the highlight after showing it
                del st.session_state["highlight_job_id"]
            
            # Create a more compact view
            for idx, job in enumerate(active_jobs):
                with st.container():
                    # Check if this job should be highlighted
                    is_highlighted = highlight_job_id and job.job_id == highlight_job_id
                    bg_color = "#e8fffe" if is_highlighted else "#f8f9fa"
                    border_style = "border: 2px solid var(--happyweb-primary);" if is_highlighted else ""
                    
                    # Add "NEW" badge for highlighted job
                    new_badge = '<span style="background-color: var(--happyweb-primary); color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; margin-left: 10px;">NEW</span>' if is_highlighted else ""
                    
                    # Estimate progress based on running time
                    # Typical historical loads take 0.5-4 hours, let's use 2 hours (7200 seconds) as average
                    estimated_duration = 7200  # 2 hours in seconds
                    progress_percentage = min(100, int((job.running_seconds / estimated_duration) * 100))
                    
                    # Adjust progress based on logs if available (for better accuracy)
                    if job.status == 'running':
                        # Try to get recent logs to determine actual progress
                        recent_logs = job_manager.get_job_logs(job.job_id, limit=20)
                        if recent_logs:
                            for log in recent_logs:
                                if "[COMPLETED]" in log.message:
                                    progress_percentage = 100
                                elif "Loading data to BigQuery" in log.message or "[SUCCESS]" in log.message:
                                    progress_percentage = max(progress_percentage, 75)
                                elif "Fetching" in log.message or "[FETCHING]" in log.message:
                                    progress_percentage = max(progress_percentage, 50)
                                elif "Processing" in log.message or "[PROCESSING]" in log.message:
                                    progress_percentage = max(progress_percentage, 25)
                    
                    # Use a card-like layout
                    st.markdown(f"""
                    <div style="background-color: {bg_color}; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; {border_style}">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                            <div>
                                <strong>{job.store_url}</strong>{new_badge}<br>
                                <small style="color: #666;">Job ID: {job.job_id[:8]}... | Dataset: {job.dataset_name}</small>
                            </div>
                            <div style="text-align: right;">
                                <span style="font-size: 1.2rem; font-weight: bold;">
                                    {job.running_seconds // 3600}h {(job.running_seconds % 3600) // 60}m
                                </span><br>
                                <small>{'🟢 Running' if job.status == 'running' else '🟡 Pending'}</small>
                            </div>
                        </div>
                        <div style="margin-top: 0.5rem;">
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <div style="flex: 1; background-color: #e0e0e0; border-radius: 4px; height: 8px; overflow: hidden;">
                                    <div style="background-color: var(--happyweb-primary); height: 100%; width: {progress_percentage}%; transition: width 0.3s ease;"></div>
                                </div>
                                <span style="font-size: 0.85rem; color: #666; min-width: 45px; text-align: right;">{progress_percentage}%</span>
                            </div>
                            <small style="color: #888; font-size: 0.75rem;">
                                {f"Estimated: ~{max(0, (estimated_duration - job.running_seconds) // 60)} minutes remaining" if progress_percentage < 100 else "Finalizing..."}
                            </small>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Auto-expand logs for highlighted job
                    if is_highlighted:
                        st.session_state[f"show_logs_{job.job_id}"] = True
                    
                    # Action buttons
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col2:
                        # Use a more stable key that persists across reruns
                        logs_key = f"show_logs_{job.job_id}"
                        button_text = "Hide Logs" if st.session_state.get(logs_key, False) else "View Logs"
                        if st.button(button_text, key=f"btn_logs_{job.job_id}", type="secondary"):
                            if logs_key not in st.session_state:
                                st.session_state[logs_key] = False
                            st.session_state[logs_key] = not st.session_state[logs_key]
                    with col3:
                        if st.button("Cancel ❌", key=f"cancel_{job.job_id}", type="primary"):
                            cancel_container = st.container()
                            with cancel_container:
                                with st.spinner("Cancelling job..."):
                                    try:
                                        # Cancel the job
                                        success = job_manager.cancel_job(job.job_id)
                                        
                                        if success:
                                            # Add to cancelled jobs in session state with timestamp
                                            if 'cancelled_jobs' not in st.session_state:
                                                st.session_state.cancelled_jobs = {}
                                            st.session_state.cancelled_jobs[job.job_id] = datetime.now()
                                            
                                            # Also clear any logs state for this job
                                            logs_key = f"show_logs_{job.job_id}"
                                            if logs_key in st.session_state:
                                                del st.session_state[logs_key]
                                            
                                            time.sleep(2)
                                            
                                            # Verify cancellation if in debug mode
                                            if debug_mode:
                                                st.write("Debug: Checking job status after cancel...")
                                                records = job_manager.debug_job_status(job.job_id)
                                                for r in records[:3]:
                                                    st.text(f"Status: {r.status}, Time: {r.started_at}")
                                            
                                            st.success(f"✅ Job {job.job_id[:8]}... cancelled")
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error("❌ Failed to cancel job")
                                    except Exception as e:
                                        st.error(f"Error: {str(e)}")
                                        if debug_mode:
                                            import traceback
                                            st.code(traceback.format_exc())
                    
                    # Show logs if requested
                    if st.session_state.get(f"show_logs_{job.job_id}", False):
                        with st.container():
                            st.markdown("##### 📋 Real-time Logs")
                            logs = job_manager.get_job_logs(job.job_id, limit=500)  # Get more logs
                            if logs:
                                # Create a scrollable log container
                                log_html = '<div style="background-color: #1e1e1e; color: #ffffff; padding: 1rem; border-radius: 8px; font-family: monospace; font-size: 0.85rem; height: 300px; overflow-y: scroll; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word;">'
                                
                                # Show more logs and track errors
                                error_count = 0
                                warning_count = 0
                                for log in reversed(logs):  # Show all logs, container handles scrolling
                                    # Check for errors in message content
                                    is_error = any(term in log.message.upper() for term in ['ERROR', 'FAILED', 'EXCEPTION'])
                                    is_warning = any(term in log.message.upper() for term in ['WARNING', 'WARN'])
                                    
                                    if is_error:
                                        error_count += 1
                                    elif is_warning:
                                        warning_count += 1
                                    
                                    level_color = {
                                        "ERROR": "#ff6b6b",
                                        "WARNING": "#ffd93d",
                                        "INFO": "#6bcf7f",
                                        "SUCCESS": "#4caf50"
                                    }.get(log.log_level, "#ffffff")
                                    
                                    if hasattr(log, 'timestamp') and log.timestamp:
                                        timestamp = log.timestamp.strftime('%H:%M:%S')
                                    else:
                                        timestamp = "N/A"
                                    
                                    # Format the log line with background highlight for errors
                                    bg_style = ""
                                    if is_error or log.log_level == "ERROR":
                                        bg_style = "background-color: rgba(255, 107, 107, 0.1); padding: 2px 4px; border-radius: 3px;"
                                    elif is_warning or log.log_level == "WARNING":
                                        bg_style = "background-color: rgba(255, 217, 61, 0.1); padding: 2px 4px; border-radius: 3px;"
                                    
                                    log_html += f'<div style="margin-bottom: 0.25rem; {bg_style}"><span style="color: #888;">[{timestamp}]</span> <span style="color: {level_color};">[{log.log_level}]</span> {html.escape(log.message)}</div>'
                                
                                log_html += '</div>'
                                st.markdown(log_html, unsafe_allow_html=True)
                                
                                # Show error/warning summary
                                if error_count > 0 or warning_count > 0:
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        if error_count > 0:
                                            st.error(f"⚠️ {error_count} errors detected")
                                    with col2:
                                        if warning_count > 0:
                                            st.warning(f"⚠️ {warning_count} warnings detected")
                                
                                # Add refresh button for logs
                                if st.button("🔄 Refresh Logs", key=f"refresh_logs_{job.job_id}"):
                                    st.rerun()
                            else:
                                st.info("No logs available yet...")
                    
                    st.markdown("---")
        else:
            st.info("🟢 No active jobs running. Historical loads will appear here when started.")
        
        # Show recent completed jobs with better filtering
        st.markdown("### 📁 Recent Jobs")
        
        # Add filter options
        col1, col2 = st.columns([2, 1])
        with col1:
            status_filter = st.multiselect(
                "Filter by status:",
                ["completed", "failed", "cancelled", "pending"],
                default=["completed", "failed"]
            )
        with col2:
            show_limit = st.selectbox("Show:", [10, 25, 50], index=0)
        
        recent_jobs = job_manager.get_recent_jobs(limit=show_limit)
        
        if recent_jobs:
            # Filter jobs based on selection
            filtered_jobs = [j for j in recent_jobs if j.status in status_filter]
            
            if filtered_jobs:
                table_data = []
                for job in filtered_jobs:
                    status_icon = {
                        "completed": "✅",
                        "failed": "❌",
                        "running": "🟢",
                        "pending": "🟡",
                        "cancelled": "🚫"
                    }.get(job.status, "❓")
                    
                    duration = ""
                    performance_indicator = ""
                    if hasattr(job, 'duration_seconds') and job.duration_seconds:
                        hours = job.duration_seconds // 3600
                        minutes = (job.duration_seconds % 3600) // 60
                        duration = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
                        
                        # Add performance indicator based on duration
                        if job.status == "completed":
                            if job.duration_seconds < 1800:  # Less than 30 minutes
                                performance_indicator = " ⚡"  # Very fast
                            elif job.duration_seconds < 7200:  # Less than 2 hours
                                performance_indicator = " 🚀"  # Fast
                            elif job.duration_seconds < 14400:  # Less than 4 hours
                                performance_indicator = ""  # Normal
                            else:
                                performance_indicator = " 🐌"  # Slow
                    
                    # Check if job is stuck
                    is_stuck = False
                    if job.status == 'pending' and hasattr(job, 'started_at') and job.started_at:
                        age_hours = (datetime.now(timezone.utc) - job.started_at).total_seconds() / 3600
                        if age_hours > 1:
                            is_stuck = True
                            duration = f"⚠️ {int(age_hours)}h stuck"
                    
                    table_data.append({
                        "Status": f"{status_icon} {job.status.title()}",
                        "Store": job.store_url,
                        "Dataset": job.dataset_name,
                        "Started": job.started_at.strftime("%Y-%m-%d %H:%M") if hasattr(job, 'started_at') and job.started_at else "N/A",
                        "Duration": duration + performance_indicator,
                        "Errors": job.error_count if hasattr(job, 'error_count') else 0,
                        "_job_id": job.job_id,
                        "_is_stuck": is_stuck
                    })
            
                df = pd.DataFrame(table_data)
                
                # Display table
                st.dataframe(
                    df[["Status", "Store", "Dataset", "Started", "Duration", "Errors"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Status": st.column_config.TextColumn("Status", width="small"),
                        "Store": st.column_config.TextColumn("Store", width="medium"),
                        "Dataset": st.column_config.TextColumn("Dataset", width="medium"),
                        "Started": st.column_config.TextColumn("Started At", width="medium"),
                        "Duration": st.column_config.TextColumn("Duration", width="small"),
                        "Errors": st.column_config.NumberColumn("Errors", width="small"),
                    }
                )
                # Job details viewer
                if table_data:
                    st.markdown("---")
                    st.markdown("### 🔍 View Job Details")
                    job_options = [(f"{row['Store']} - {row['Started']}", row["_job_id"]) for row in table_data]
                    if job_options:
                        selected_label, selected_job_id = st.selectbox(
                            "Select a job to view detailed logs:",
                            options=job_options,
                            format_func=lambda x: x[0]
                        )
                        with st.expander("View Detailed Logs", expanded=True):
                            logs = job_manager.get_job_logs(selected_job_id, limit=100)
                            if logs:
                                log_text = []
                                for log in reversed(logs):
                                    level_icon = {
                                        "ERROR": "❌",
                                        "WARNING": "⚠️",
                                        "INFO": "ℹ️"
                                    }.get(log.log_level, "📝")
                                    component = f"[{log.component}]" if log.component else ""
                                    timestamp = log.timestamp.strftime("%H:%M:%S") if hasattr(log, 'timestamp') and log.timestamp else "N/A"
                                    log_text.append(f"{level_icon} [{timestamp}] {component} {log.message}")
                                st.text("\n".join(log_text))
                            else:
                                st.info("No logs available for this job.")
            else:
                st.info(f"No jobs found with status: {', '.join(status_filter)}")
        else:
            st.info("No jobs found. Start a historical load to see job history.")
    else:
        st.error("Job Manager not available. Pipeline jobs cannot be monitored.")

# -------------------------
# Help / footer
# -------------------------
with st.expander("📚 Getting Started Guide", expanded=False):
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
### 🔑 Getting Your Shopify Access Token
1. Go to your **Shopify Admin**
2. Navigate to **Settings → Apps → Develop apps**
3. Click **Create an app**
4. Set required permissions:
   - `read_customers`
   - `read_orders`
   - `read_products`
5. Install the app
6. Copy the **Admin API access token**

Need help? Contact us at support@happyweb.io
""")
    with c2:
        st.markdown("""
### 📝 Dataset Naming Guidelines
- Must start with `shopify_`
- Use only lowercase letters
- Numbers and underscores allowed
- No spaces or special characters

**Examples:**
- ✅ `shopify_wellness_brand`
- ✅ `shopify_store_123`
- ❌ `Shopify-Store`
- ❌ `shopify store`

HappyWeb automatically validates your dataset name!
""")

st.markdown("---")
st.markdown(f"""
<center>
    <small style='color:#666;'>
        {logo_html if logo_html else ''}<br>
        <span style='color:#002C5F;font-weight:500;'>HappyWeb® - We grow Shopify brands that grow people</span><br>
        <span style='color:#666;'>© {datetime.now().year} HappyWeb. All rights reserved.</span>
    </small>
</center>
""", unsafe_allow_html=True)
