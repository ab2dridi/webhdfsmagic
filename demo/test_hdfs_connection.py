#!/usr/bin/env python3
"""
Simple test script to verify that webhdfsmagic works with the local HDFS system
"""

import os
import sys

# Add local path to import webhdfsmagic
sys.path.insert(0, '/workspaces/webhdfsmagic')

from IPython.terminal.interactiveshell import TerminalInteractiveShell

from webhdfsmagic.magics import WebHDFSMagics

# Create an IPython session
ipython = TerminalInteractiveShell.instance()

# Load the extension
magics = WebHDFSMagics(ipython)
ipython.register_magics(magics)

print("=" * 60)
print("Testing webhdfsmagic with local HDFS")
print("=" * 60)

# Configuration
print("\n1️⃣ Configuration...")
config_file = os.path.expanduser("~/.webhdfsmagic/config.json")
if os.path.exists(config_file):
    print(f"✓ Configuration file found: {config_file}")
    import json
    with open(config_file) as f:
        config = json.load(f)
        print(f"  URL: {config.get('knox_url')}{config.get('webhdfs_api')}")
        print(f"  User: {config.get('username')}")
        print(f"  SSL Verify: {config.get('verify_ssl')}")
else:
    print(f"✗ Configuration file not found: {config_file}")
    sys.exit(1)

# Test commands
print("\n2️⃣ Testing root directory listing...")
try:
    result = ipython.run_line_magic('hdfs', 'ls /')
    print("✓ Listing successful")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n3️⃣ Creating a test directory...")
try:
    result = ipython.run_line_magic('hdfs', 'mkdir /test_webhdfs')
    print("✓ Directory created")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n4️⃣ Verifying directory exists...")
try:
    result = ipython.run_line_magic('hdfs', 'exists /test_webhdfs')
    print(f"✓ Directory exists: {result}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n5️⃣ Creating a local test file...")
test_file = "/tmp/webhdfs_test.txt"
with open(test_file, 'w') as f:
    f.write("Hello from webhdfsmagic!\nThis is a test file.\n")
print(f"✓ File created: {test_file}")

print("\n6️⃣ Uploading file to HDFS...")
try:
    result = ipython.run_line_magic('hdfs', f'put {test_file} /test_webhdfs/test.txt')
    print("✓ Upload successful")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n7️⃣ Listing test directory...")
try:
    result = ipython.run_line_magic('hdfs', 'ls /test_webhdfs')
    print("✓ Listing successful")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n8️⃣ Reading file content...")
try:
    result = ipython.run_line_magic('hdfs', 'cat /test_webhdfs/test.txt')
    print("✓ Read successful")
    print(f"Content: {result}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n9️⃣ Downloading file from HDFS...")
try:
    download_file = "/tmp/downloaded_test.txt"
    result = ipython.run_line_magic('hdfs', f'get /test_webhdfs/test.txt {download_file}')
    print("✓ Download successful")
    if os.path.exists(download_file):
        with open(download_file) as f:
            print(f"Downloaded file content:\n{f.read()}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n🔟 Getting file statistics...")
try:
    result = ipython.run_line_magic('hdfs', 'stat /test_webhdfs/test.txt')
    print("✓ Stat successful")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 60)
print("Tests completed!")
print("=" * 60)
