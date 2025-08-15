"""
Simple test for tracker API
"""
import requests
import json

# Test the tracker endpoint
def test_tracker():
    url = "http://localhost:8000/api/tracker/run"
    payload = {"scheme_id": "110425"}
    
    print("Testing tracker endpoint...")
    print(f"URL: {url}")
    print(f"Payload: {payload}")
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success"):
                print("✅ Tracker API is working!")
                return True
            else:
                print(f"❌ Tracker failed: {result.get('message')}")
                return False
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the API is running on http://localhost:8000")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_status():
    url = "http://localhost:8000/api/tracker/status/110425"
    
    print("\nTesting status endpoint...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 200:
            print("✅ Status API is working!")
            return True
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("=== Simple Tracker API Test ===")
    
    # Test tracker run
    tracker_success = test_tracker()
    
    # Test status
    status_success = test_status()
    
    if tracker_success and status_success:
        print("\n🎉 All tests passed! The tracker API is working correctly.")
    else:
        print("\n⚠️ Some tests failed. Check the output above for details.")