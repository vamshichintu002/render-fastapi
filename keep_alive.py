#!/usr/bin/env python3
"""
Keep FastAPI server alive and test endpoints
"""

import subprocess
import time
import requests
import json

def test_api_endpoint():
    """Test the API endpoint that was failing"""
    try:
        url = "http://localhost:8000/api/costing/main-scheme/volume"
        payload = {"scheme_id": "936878"}
        headers = {"Content-Type": "application/json"}
        
        print(f"Testing endpoint: {url}")
        print(f"Payload: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            print("✅ API endpoint test successful!")
            return True
        else:
            print(f"❌ API endpoint test failed with status {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to server - server may not be running")
        return False
    except Exception as e:
        print(f"❌ Error testing API endpoint: {e}")
        return False

def main():
    """Main function to keep server alive and test"""
    print("🚀 Starting FastAPI server...")
    
    # Start the server in background
    process = subprocess.Popen(["python", "start.py"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              text=True)
    
    print("⏳ Waiting for server to start...")
    time.sleep(5)  # Wait for server to start
    
    # Test the health endpoint first
    try:
        health_response = requests.get("http://localhost:8000/health/detailed", timeout=10)
        print(f"Health check status: {health_response.status_code}")
        print(f"Health check response: {health_response.text}")
    except Exception as e:
        print(f"Health check failed: {e}")
    
    # Test the API endpoint
    success = test_api_endpoint()
    
    if success:
        print("🎉 All tests passed! Server is working correctly.")
        print("Keeping server running... Press Ctrl+C to stop")
        
        try:
            while True:
                time.sleep(10)
                # Keep testing the endpoint every 10 seconds
                test_api_endpoint()
        except KeyboardInterrupt:
            print("\n🛑 Stopping server...")
            process.terminate()
            process.wait()
            print("✅ Server stopped")
    else:
        print("💥 Tests failed. Stopping server...")
        process.terminate()
        process.wait()

if __name__ == "__main__":
    main() 