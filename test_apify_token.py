#!/usr/bin/env python3
"""
Diagnostic script to test Apify API token configuration.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Get the base directory
BASE_DIR = Path(__file__).parent

print("=" * 60)
print("Apify API Token Diagnostic Tool")
print("=" * 60)
print()

# 1. Check .env file location
env_path = BASE_DIR / ".env"
print(f"1. Checking .env file location...")
print(f"   Expected path: {env_path}")
print(f"   File exists: {env_path.exists()}")
if env_path.exists():
    print(f"   File size: {env_path.stat().st_size} bytes")
print()

# 2. Try loading .env file explicitly
print("2. Loading .env file...")
load_result = load_dotenv(env_path, override=True)
print(f"   load_dotenv() returned: {load_result}")
print()

# 3. Check environment variable directly
print("3. Checking APIFY_API_TOKEN from environment...")
token_from_env = os.getenv("APIFY_API_TOKEN")
if token_from_env:
    token_length = len(token_from_env)
    token_preview = token_from_env[:10] + "..." if token_length > 10 else token_from_env
    print(f"   ✓ Token found!")
    print(f"   Token length: {token_length} characters")
    print(f"   Token preview: {token_preview}")
    
    # Check for common issues
    if token_from_env.strip() != token_from_env:
        print(f"   ⚠ WARNING: Token has leading/trailing whitespace!")
    if token_from_env.startswith('"') or token_from_env.startswith("'"):
        print(f"   ⚠ WARNING: Token appears to be quoted (remove quotes)")
    if len(token_from_env) < 20:
        print(f"   ⚠ WARNING: Token seems too short (expected ~50+ characters)")
else:
    print(f"   ✗ Token NOT found in environment")
print()

# 4. Check via settings module
print("4. Checking via settings module...")
try:
    from config.settings import settings
    token_from_settings = settings.APIFY_API_TOKEN
    if token_from_settings:
        print(f"   ✓ Token found in settings!")
        print(f"   Token length: {len(token_from_settings)} characters")
    else:
        print(f"   ✗ Token NOT found in settings")
        print(f"   This means load_dotenv() didn't load it or it's empty")
except Exception as e:
    print(f"   ✗ Error importing settings: {e}")
print()

# 5. Test token validity with Apify API
print("5. Testing token validity with Apify API...")
if token_from_env:
    try:
        from apify_client import ApifyClient
        
        print(f"   Attempting to authenticate...")
        client = ApifyClient(token_from_env)
        
        # Try to get user info (simple API call to verify token)
        user_info = client.user().get()
        print(f"   ✓ Token is VALID!")
        print(f"   User ID: {user_info.get('id', 'N/A')}")
        print(f"   Username: {user_info.get('username', 'N/A')}")
        print(f"   Email: {user_info.get('email', 'N/A')}")
        
        # Check account status
        print(f"   Account status: {user_info.get('plan', {}).get('name', 'N/A')}")
        
    except Exception as e:
        print(f"   ✗ Token validation FAILED")
        print(f"   Error: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        if "not found" in str(e).lower() or "not valid" in str(e).lower():
            print()
            print("   Possible issues:")
            print("   - Token is incorrect or expired")
            print("   - Token was copied incorrectly (missing characters)")
            print("   - Token has extra whitespace or quotes")
            print("   - Token is from a different Apify account")
        elif "authentication" in str(e).lower():
            print()
            print("   Possible issues:")
            print("   - Token format is incorrect")
            print("   - Token needs to be regenerated")
else:
    print(f"   ⚠ Skipping validation (no token found)")
print()

# 6. Recommendations
print("6. Recommendations:")
print()

if not token_from_env:
    print("   ✗ No token found. Please:")
    print("   1. Open the .env file in the project root")
    print("   2. Add: APIFY_API_TOKEN=your_token_here")
    print("   3. Get your token from: https://console.apify.com/account/integrations")
    print("   4. Make sure there are NO quotes around the token")
    print("   5. Make sure there are NO spaces around the = sign")
    print("   6. Save the file and run this script again")
elif token_from_env and "Token validation FAILED" in str(sys.stdout):
    print("   ✗ Token is invalid. Please:")
    print("   1. Verify your token at: https://console.apify.com/account/integrations")
    print("   2. Copy the token again (make sure you get the full token)")
    print("   3. Update .env file with the new token")
    print("   4. Make sure the line looks like: APIFY_API_TOKEN=apify_api_...")
    print("   5. No quotes, no spaces around =")
else:
    print("   ✓ Token appears to be configured correctly!")
    print("   If you're still getting errors, check:")
    print("   - The .env file is in the project root directory")
    print("   - You're running the script from the project root")
    print("   - The token hasn't been revoked or expired")

print()
print("=" * 60)

