#!/usr/bin/env python3
"""
Entrypoint that runs either the Streamlit UI or historical pipeline based on environment
"""
import os
import sys
import subprocess

# Check if this is a Cloud Run Job running historical pipeline
if os.environ.get("PIPELINE_TYPE") == "historical":
    print("[Entrypoint] Running historical pipeline...")
    # Change to historical directory and run the pipeline
    os.chdir("/app/historical")
    # Import and run the historical pipeline directly
    import main
else:
    print("[Entrypoint] Running Streamlit UI...")
    # Run Streamlit with all the configuration
    subprocess.run([
        "streamlit", "run", "/app/onboarding/app.py",
        "--server.port=" + os.environ.get("PORT", "8080"),
        "--server.address=0.0.0.0",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
        "--theme.primaryColor=#17D4BE",
        "--theme.backgroundColor=#FFFFFF",
        "--theme.secondaryBackgroundColor=#F8FFFE",
        "--theme.textColor=#002C5F"
    ])