#!/usr/bin/env python3
"""
Test server functionality
"""

import requests
import time
import json

def test_server():
    """Test the server endpoints"""
    
    # Wait for server to be ready
    print("⏳ Waiting for server to be ready...")
    time.sleep(3)
    
    # Test health endpoint
    try:
        print("🔍 Testing health endpoint...")
        response = requests.get("http://localhost:8000/health/detailed", timeout=10)
        print(f"Health status: {response.status_code}")
        print(f"Health response: {response.text}")
        
        if response.status_code == 200:
            print("✅ Health check passed")
        else:
            print("❌ Health check failed")
            return False
            
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False
    
    # Test the failing endpoint
    try:
        print("\n🔍 Testing costing endpoint...")
        url = "http://localhost:8000/api/costing/main-scheme/volume"
        payload = {"scheme_id": "936878"}
        headers = {"Content-Type": "application/json"}
        
        print(f"URL: {url}")
        print(f"Payload: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print("✅ Costing endpoint test passed!")
            return True
        else:
            print(f"❌ Costing endpoint test failed with status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Costing endpoint error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing FastAPI server...")
    
    # Test multiple times to catch timing issues
    for i in range(3):
        print(f"\n--- Test {i+1}/3 ---")
        if test_server():
            print("🎉 Server test successful!")
            break
        else:
            print(f"❌ Test {i+1} failed, retrying...")
            time.sleep(2)
    else:
        print("💥 All tests failed!") 