#!/usr/bin/env python3
"""
Test script to verify Apify actor call works correctly.
This reproduces the exact call that fails in the pipeline.
"""

import sys
from pathlib import Path

# Add project root to path
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from config.settings import settings
from services.discovery import DiscoveryService
from core.database import Database

print("=" * 60)
print("Testing Apify Actor Call")
print("=" * 60)
print()

# 1. Verify token is loaded
print("1. Verifying token is loaded...")
if not settings.APIFY_API_TOKEN:
    print("   ✗ ERROR: APIFY_API_TOKEN is not set!")
    print("   Please check your .env file")
    sys.exit(1)
else:
    print(f"   ✓ Token loaded (length: {len(settings.APIFY_API_TOKEN)})")
print()

# 2. Initialize discovery service
print("2. Initializing DiscoveryService...")
try:
    db = Database(settings.DATABASE_PATH)
    discovery = DiscoveryService(db)
    print("   ✓ DiscoveryService initialized")
except Exception as e:
    print(f"   ✗ ERROR initializing DiscoveryService: {e}")
    sys.exit(1)
print()

# 3. Test client initialization
print("3. Testing Apify client initialization...")
try:
    client = discovery.client
    print("   ✓ Apify client initialized")
except Exception as e:
    print(f"   ✗ ERROR initializing client: {e}")
    sys.exit(1)
print()

# 4. Test actor access
print("4. Testing actor access...")
try:
    actor_id = "clockworks/tiktok-scraper"
    actor = client.actor(actor_id)
    actor_info = actor.get()
    print(f"   ✓ Actor accessible: {actor_info.get('name', 'N/A')}")
except Exception as e:
    print(f"   ✗ ERROR accessing actor: {e}")
    sys.exit(1)
print()

# 5. Test actor call (small test)
print("5. Testing actor call (this is where the error occurs)...")
try:
    run_input = {
        "hashtags": ["viral"],
        "resultsPerPage": 1,  # Just 1 result for testing
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
    }
    print(f"   Calling actor with input: {run_input}")
    
    # This is the exact call that fails
    run = client.actor(actor_id).call(run_input=run_input)
    print(f"   ✓ Actor call successful!")
    print(f"   Run ID: {run.get('id', 'N/A')}")
    print(f"   Run status: {run.get('status', 'N/A')}")
    
    # Try to get dataset
    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        print(f"   Dataset ID: {dataset_id}")
        items = list(client.dataset(dataset_id).iterate_items())
        print(f"   ✓ Retrieved {len(items)} items from dataset")
    
except Exception as e:
    print(f"   ✗ ERROR calling actor: {e}")
    print(f"   Error type: {type(e).__name__}")
    print()
    print("   This is the same error you're seeing in the pipeline.")
    print("   Possible causes:")
    print("   - Token doesn't have permission to run this actor")
    print("   - Actor requires payment/credits")
    print("   - Actor is temporarily unavailable")
    print("   - Token needs to be regenerated")
    sys.exit(1)

print()
print("=" * 60)
print("✓ All tests passed! The Apify integration should work.")
print("=" * 60)

