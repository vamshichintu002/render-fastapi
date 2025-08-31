#!/usr/bin/env python3
"""
Test script for Smart Routing System
Tests scheme ID 853761 routing analysis
"""

import requests
import json
import time
import sys

# Configuration
BASE_URL = "http://localhost:8000"  # Change to your deployed URL if needed
SCHEME_ID = "853761"

def test_routing_analysis():
    """Test the routing analysis endpoint"""
    print(f"🔍 Testing routing analysis for scheme ID: {SCHEME_ID}")
    print("=" * 60)
    
    try:
        # Test routing analysis endpoint
        url = f"{BASE_URL}/api/costing/routing-analysis/{SCHEME_ID}"
        print(f"📡 Making request to: {url}")
        
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            print("✅ Routing Analysis Results:")
            print(f"   • Scheme ID: {data['scheme_id']}")
            print(f"   • Recommended Service: {data['recommended_service']}")
            print(f"   • Routing Reason: {data['routing_reason']}")
            print(f"   • Cache Available: {data['cache_available']}")
            print(f"   • Expected Performance: {data['expected_performance']}")
            
            print("\n📊 Complexity Analysis:")
            complexity = data['complexity_analysis']
            print(f"   • Complexity Level: {complexity['complexity']}")
            print(f"   • Complexity Score: {complexity['complexity_score']}")
            print(f"   • Estimated Rows: {complexity['estimated_rows']}")
            
            return data
        else:
            print(f"❌ Request failed with status: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - make sure your API is running")
        print("   Try: python start.py")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_optimization_status():
    """Test the optimization status endpoint"""
    print("\n🔧 Testing optimization status")
    print("=" * 60)
    
    try:
        url = f"{BASE_URL}/api/costing/optimization-status"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            print("✅ Optimization Status:")
            print(f"   • Smart Routing Enabled: {data['smart_routing_enabled']}")
            print(f"   • Performance Tracking: {data['performance_tracking']} entries")
            print(f"   • Cache Threshold: {data['cache_threshold']}s")
            
            print("\n📋 Routing Logic:")
            logic = data['routing_logic']
            for key, value in logic.items():
                print(f"   • {key.replace('_', ' ').title()}: {value}")
                
            return data
        else:
            print(f"❌ Request failed with status: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_actual_request():
    """Test an actual costing request to see routing in action"""
    print(f"\n⚡ Testing actual main scheme value request for {SCHEME_ID}")
    print("=" * 60)
    
    try:
        url = f"{BASE_URL}/api/costing/main-scheme/value"
        payload = {"scheme_id": SCHEME_ID}
        
        print(f"📡 Making request to: {url}")
        print(f"📦 Payload: {payload}")
        
        start_time = time.time()
        response = requests.post(url, json=payload, timeout=120)  # 2 minute timeout
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        if response.status_code == 200:
            data = response.json()
            
            print("✅ Request Successful!")
            print(f"   • Execution Time: {execution_time:.2f}s")
            print(f"   • API Response Time: {data.get('execution_time', 'N/A')}s")
            print(f"   • Record Count: {data.get('record_count', 'N/A')}")
            print(f"   • Message: {data.get('message', 'N/A')}")
            
            # Check if smart routing worked by looking at the message
            if 'optimized' in data.get('message', '').lower():
                print("🎯 Smart Router Selected: OPTIMIZED service")
            elif 'vectorized' in data.get('message', '').lower():
                print("🎯 Smart Router Selected: VECTORIZED service")
            else:
                print("🎯 Smart Router Selected: UNKNOWN service")
                
            return data
        else:
            print(f"❌ Request failed with status: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        print("⏱️  Request timed out (>120s) - this might indicate vectorized service compilation")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    """Main test function"""
    print("🧪 Smart Routing System Test")
    print("Testing Scheme ID: 853761")
    print("=" * 60)
    
    # Test 1: Check optimization status
    status_result = test_optimization_status()
    
    # Test 2: Analyze routing for specific scheme
    routing_result = test_routing_analysis()
    
    # Test 3: Make actual request to see routing in action
    if routing_result:
        print(f"\n🎯 Based on analysis, expecting {routing_result['recommended_service']} service")
        print(f"   Expected performance: {routing_result['expected_performance']}")
        
    request_result = test_actual_request()
    
    # Summary
    print("\n📋 TEST SUMMARY")
    print("=" * 60)
    
    if routing_result and request_result:
        predicted_service = routing_result['recommended_service']
        actual_message = request_result.get('message', '')
        
        if predicted_service in actual_message.lower():
            print("✅ Smart Routing WORKED! Prediction matched actual service used.")
        else:
            print("⚠️  Smart Routing prediction didn't match actual service.")
            
        print(f"   • Predicted: {predicted_service}")
        print(f"   • Actual Message: {actual_message}")
    
    if status_result:
        print("✅ Smart routing system is active and configured correctly")
    else:
        print("❌ Could not verify smart routing system status")

if __name__ == "__main__":
    main()