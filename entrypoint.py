#\!/usr/bin/env python3
"""
Entrypoint that runs either the Streamlit UI or historical pipeline based on environment
"""
import os
import sys
import subprocess

# Check if this is a Cloud Run Job running historical pipeline
if os.environ.get("PIPELINE_TYPE") == "historical":
    print("[Entrypoint] Running historical pipeline...", flush=True)
    print(f"[Entrypoint] Environment variables:", flush=True)
    print(f"  PIPELINE_JOB_ID: {os.environ.get('PIPELINE_JOB_ID', 'Not set')}", flush=True)
    print(f"  TARGET_STORE: {os.environ.get('TARGET_STORE', 'Not set')}", flush=True)
    print(f"  PIPELINE_TYPE: {os.environ.get('PIPELINE_TYPE', 'Not set')}", flush=True)
    
    # Add the app directory to Python path
    sys.path.insert(0, '/app')
    sys.path.insert(0, '/app/historical')
    print(f"[Entrypoint] Python path: {sys.path}", flush=True)
    
    # Change to historical directory
    historical_dir = "/app/historical"
    if not os.path.exists(historical_dir):
        print(f"[Entrypoint] ERROR: Historical directory does not exist: {historical_dir}", flush=True)
        print(f"[Entrypoint] Contents of /app: {os.listdir('/app') if os.path.exists('/app') else 'Directory not found'}", flush=True)
        sys.exit(1)
    
    os.chdir(historical_dir)
    print(f"[Entrypoint] Current directory: {os.getcwd()}", flush=True)
    print(f"[Entrypoint] Files in current directory: {os.listdir('.')}", flush=True)
    
    # Check if main.py exists
    if not os.path.exists("main.py"):
        print(f"[Entrypoint] ERROR: main.py not found in {os.getcwd()}", flush=True)
        sys.exit(1)
    
    # Run the historical pipeline as a subprocess
    print(f"[Entrypoint] Executing: {sys.executable} main.py", flush=True)
    result = subprocess.run([sys.executable, "main.py"], capture_output=False, text=True)
    
    # Exit with the same code as the subprocess
    print(f"[Entrypoint] Pipeline completed with exit code: {result.returncode}", flush=True)
    sys.exit(result.returncode)
else:
    print("[Entrypoint] Running Streamlit UI...", flush=True)
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
ENDOFFILE < /dev/null
