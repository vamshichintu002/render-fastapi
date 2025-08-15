"""
Test script for the tracker endpoint
"""
import requests
import json
import time

# Configuration
BASE_URL = "http://localhost:8000"  # Change this to your deployed URL
SCHEME_ID = "110425"  # Using a real scheme ID that has configuration data

def test_tracker_run():
    """Test the tracker run endpoint"""
    print("Testing tracker run endpoint...")
    
    url = f"{BASE_URL}/api/tracker/run"
    payload = {"scheme_id": SCHEME_ID}
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        return response.json()
    except requests.exceptions.Timeout:
        print("Request timed out")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def test_tracker_status():
    """Test the tracker status endpoint"""
    print("\nTesting tracker status endpoint...")
    
    url = f"{BASE_URL}/api/tracker/status/{SCHEME_ID}"
    
    try:
        response = requests.get(url)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return None

def test_tracker_background():
    """Test the background tracker endpoint"""
    print("\nTesting background tracker endpoint...")
    
    url = f"{BASE_URL}/api/tracker/run-background"
    payload = {"scheme_id": SCHEME_ID}
    
    try:
        response = requests.post(url, json=payload)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    print("=== Tracker Endpoint Tests ===")
    
    # Test 1: Run tracker
    result = test_tracker_run()
    
    # Test 2: Check status
    time.sleep(2)  # Wait a bit
    test_tracker_status()
    
    # Test 3: Run in background
    test_tracker_background()
    
    # Test 4: Check status again
    time.sleep(2)  # Wait a bit
    test_tracker_status()
    
    print("\n=== Tests Complete ===")