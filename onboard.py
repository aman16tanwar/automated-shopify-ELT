#!/usr/bin/env python3
"""
Simple CLI interface for onboarding
Works on both Windows and Linux
"""

import os
import sys
import getpass
import subprocess

def main():
    print("🛍️  Shopify Client Onboarding Tool")
    print("==================================")
    print()
    
    # Check if web mode
    if len(sys.argv) > 1 and sys.argv[1] == "--web":
        print("🌐 Starting web interface...")
        os.chdir("onboarding")
        subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
        return
    
    # CLI mode
    print("📝 Please provide the following information:")
    print()
    
    # Get client details
    client_id = input("Client ID (e.g., ninja_kitchen_au): ").strip()
    client_name = input("Client Name (e.g., Ninja Kitchen AU): ").strip()
    merchant_url = input("Merchant URL (e.g., ninjaau.myshopify.com): ").strip()
    access_token = getpass.getpass("Access Token (shpat_...): ").strip()
    memory = input("Memory allocation (2Gi/4Gi/8Gi) [2Gi]: ").strip() or "2Gi"
    
    # Confirmation
    print()
    print("📋 Summary:")
    print(f"  Client ID: {client_id}")
    print(f"  Merchant: {merchant_url}")
    print(f"  Memory: {memory}")
    print()
    
    confirm = input("Proceed with onboarding? (y/n): ").strip().lower()
    
    if confirm == 'y':
        print()
        print("🚀 Starting onboarding process...")
        
        subprocess.run([
            sys.executable, "scripts/onboard_client.py",
            "--client-id", client_id,
            "--merchant", merchant_url,
            "--token", access_token,
            "--memory", memory
        ])
    else:
        print("❌ Onboarding cancelled")
        sys.exit(1)

if __name__ == "__main__":
    main()