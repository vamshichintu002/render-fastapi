#!/usr/bin/env python3
"""
Offline test of Smart Routing System
Tests scheme ID 853761 complexity analysis without running API
"""

import psycopg2
import json
from datetime import datetime
import sys
import os

# Database configuration (from your config)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.ozgkgkenzpngnptdqbqf", 
    "password": "PwbcCdD?Yq4Jn.v",
    "host": "aws-0-ap-south-1.pooler.supabase.com",
    "port": 5432
}

SCHEME_ID = "853761"

def analyze_scheme_complexity(scheme_id):
    """Simulate the smart router's complexity analysis"""
    print(f"🔍 Analyzing scheme {scheme_id} complexity")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get scheme configuration
        query = "SELECT scheme_json FROM schemes_data WHERE scheme_id::text = %s"
        cursor.execute(query, [scheme_id])
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Scheme {scheme_id} not found in database")
            return None
            
        scheme_json = result[0]
        main_scheme = scheme_json.get('mainScheme', {})
        
        print("✅ Scheme found! Analyzing configuration...")
        
        # Calculate complexity factors
        complexity_score = 0
        estimated_rows = 0
        analysis_details = []
        
        # Factor 1: Number of base periods
        base_vol_sections = main_scheme.get('baseVolSections', [])
        base_periods = len(base_vol_sections)
        base_period_score = base_periods * 10
        base_period_rows = base_periods * 200
        
        complexity_score += base_period_score
        estimated_rows += base_period_rows
        analysis_details.append(f"Base periods: {base_periods} (score: +{base_period_score}, rows: +{base_period_rows})")
        
        # Factor 2: Number of slabs
        additional_schemes = main_scheme.get('additionalSchemes', [])
        if additional_schemes and len(additional_schemes) > 0:
            slabs = additional_schemes[0].get('slabs', [])
            slab_count = len(slabs)
            slab_score = slab_count * 5
            slab_rows = slab_count * 100
            
            complexity_score += slab_score
            estimated_rows += slab_rows
            analysis_details.append(f"Slabs: {slab_count} (score: +{slab_score}, rows: +{slab_rows})")
        else:
            analysis_details.append("Slabs: 0 (score: +0, rows: +0)")
        
        # Factor 3: Date range analysis
        total_months = 0
        for i, section in enumerate(base_vol_sections):
            if section and section.get('fromDate') and section.get('toDate'):
                try:
                    start = datetime.fromisoformat(section['fromDate'].replace('Z', ''))
                    end = datetime.fromisoformat(section['toDate'].replace('Z', ''))
                    months = (end.year - start.year) * 12 + end.month - start.month + 1
                    total_months += months
                    analysis_details.append(f"Period {i+1}: {months} months ({section['fromDate'][:10]} to {section['toDate'][:10]})")
                except Exception as e:
                    analysis_details.append(f"Period {i+1}: Date parsing failed - {e}")
                    
        date_score = total_months * 2
        date_rows = total_months * 50
        complexity_score += date_score
        estimated_rows += date_rows
        analysis_details.append(f"Total months: {total_months} (score: +{date_score}, rows: +{date_rows})")
        
        # Classify complexity
        if complexity_score < 50:
            complexity = "low"
        elif complexity_score < 200:
            complexity = "medium"
        else:
            complexity = "high"
            
        # Ensure minimum estimated rows
        estimated_rows = max(estimated_rows, 50)
        
        # Print detailed analysis
        print("\n📊 Complexity Analysis Details:")
        for detail in analysis_details:
            print(f"   • {detail}")
            
        print(f"\n📈 Final Scores:")
        print(f"   • Total Complexity Score: {complexity_score}")
        print(f"   • Complexity Level: {complexity.upper()}")
        print(f"   • Estimated Rows: {estimated_rows}")
        
        # Routing decision
        print(f"\n🎯 Smart Routing Decision:")
        
        # Check if this scheme would have cached data (simulate)
        has_cache = False  # We can't check cache offline
        
        if has_cache:
            recommended_service = "optimized"
            reason = "cached_data_available"
            expected_time = "0.8s"
        elif complexity == "high" or estimated_rows > 5000:
            recommended_service = "vectorized"
            reason = f"large_dataset_{estimated_rows}_rows"
            expected_time = "15-25s (after JIT compilation)"
        elif complexity == "medium" or estimated_rows > 1000:
            recommended_service = "optimized"
            reason = f"medium_dataset_{estimated_rows}_rows"
            expected_time = "5-15s"
        else:
            recommended_service = "optimized"
            reason = f"small_dataset_{estimated_rows}_rows"
            expected_time = "2-8s"
            
        print(f"   • Recommended Service: {recommended_service.upper()}")
        print(f"   • Reason: {reason}")
        print(f"   • Expected Performance: {expected_time}")
        
        # Show scheme structure summary
        print(f"\n📋 Scheme Structure Summary:")
        print(f"   • Scheme ID: {scheme_id}")
        print(f"   • Volume/Value Based: {main_scheme.get('volumeValueBased', 'unknown')}")
        print(f"   • Base Volume Sections: {len(base_vol_sections)}")
        print(f"   • Additional Schemes: {len(additional_schemes)}")
        
        if additional_schemes:
            for i, additional in enumerate(additional_schemes):
                print(f"   • Additional Scheme {i}: {len(additional.get('slabs', []))} slabs")
        
        conn.close()
        
        return {
            "scheme_id": scheme_id,
            "complexity": complexity,
            "complexity_score": complexity_score,
            "estimated_rows": estimated_rows,
            "recommended_service": recommended_service,
            "reason": reason,
            "expected_performance": expected_time,
            "analysis_details": analysis_details
        }
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return None
    except Exception as e:
        print(f"❌ Analysis error: {e}")
        return None

def test_routing_thresholds():
    """Test routing thresholds with different scenarios"""
    print(f"\n🧪 Testing Routing Thresholds")
    print("=" * 60)
    
    scenarios = [
        {"rows": 100, "complexity": "low", "expected": "optimized"},
        {"rows": 1500, "complexity": "medium", "expected": "optimized"},
        {"rows": 6000, "complexity": "high", "expected": "vectorized"},
        {"rows": 10000, "complexity": "high", "expected": "vectorized"},
    ]
    
    print("Routing Logic Test:")
    for scenario in scenarios:
        rows = scenario["rows"]
        complexity = scenario["complexity"]
        expected = scenario["expected"]
        
        # Apply routing logic
        if complexity == "high" or rows > 5000:
            actual = "vectorized"
        elif complexity == "medium" or rows > 1000:
            actual = "optimized"
        else:
            actual = "optimized"
            
        status = "✅" if actual == expected else "❌"
        print(f"   {status} {rows} rows, {complexity} complexity → {actual} (expected: {expected})")

def main():
    """Main test function"""
    print("🧪 Smart Routing System - Offline Test")
    print(f"Testing Scheme ID: {SCHEME_ID}")
    print("=" * 60)
    
    # Test complexity analysis for the specific scheme
    result = analyze_scheme_complexity(SCHEME_ID)
    
    # Test routing thresholds
    test_routing_thresholds()
    
    # Summary
    print("\n📋 TEST SUMMARY")
    print("=" * 60)
    
    if result:
        print("✅ Scheme complexity analysis completed successfully")
        print(f"   • Smart router would select: {result['recommended_service']}")
        print(f"   • Expected performance: {result['expected_performance']}")
        print(f"   • Routing reason: {result['reason']}")
        
        # Performance prediction
        if result['recommended_service'] == 'optimized':
            print("💡 This scheme should have fast response times with caching")
        else:
            print("💡 This scheme may benefit from vectorized processing for large datasets")
    else:
        print("❌ Could not analyze scheme complexity")
        
    print("\n🚀 To test with live API, start the server with: python start.py")

if __name__ == "__main__":
    main()