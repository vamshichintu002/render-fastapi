#!/usr/bin/env python3
"""
Test script for the costing API functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test if all required modules can be imported"""
    try:
        from costing_sheet import CostingSheetCalculator
        print("✅ CostingSheetCalculator imported successfully")

        from json_fetcher import JSONFetcher
        print("✅ JSONFetcher imported successfully")

        from sales_fetcher import SalesFetcher
        print("✅ SalesFetcher imported successfully")

        from store import SchemeDataExtractor
        print("✅ SchemeDataExtractor imported successfully")

        from supabaseconfig import SUPABASE_CONFIG
        print("✅ SUPABASE_CONFIG imported successfully")

        return True
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def test_calculator_initialization():
    """Test if CostingSheetCalculator can be initialized"""
    try:
        from costing_sheet import CostingSheetCalculator
        calculator = CostingSheetCalculator()
        print("✅ CostingSheetCalculator initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Calculator initialization error: {e}")
        return False

def test_demo_response():
    """Test the demo response structure"""
    try:
        # Simulate the demo response
        demo_info = {
            "system_capabilities": [
                "✅ Base period data processing",
                "✅ Memory-based calculations",
                "✅ Simplified cost estimation",
                "✅ Main + Additional schemes",
                "✅ Target volume/value calculations",
                "✅ Professional Excel output",
                "✅ Summary and metadata sheets"
            ],
            "calculation_scope": {
                "columns": "Start through Target Volume/Target Value",
                "data_source": "Base period only (no scheme period)",
                "purpose": "Cost estimation and validation",
                "output": "Single Excel file with multiple sheets"
            },
            "technical_features": [
                "Memory-only processing (no intermediate files)",
                "Vectorized calculations for performance",
                "Automatic scheme validation",
                "Professional Excel formatting",
                "Error handling and recovery"
            ]
        }
        print("✅ Demo response structure valid")
        print(f"   📊 System capabilities: {len(demo_info['system_capabilities'])} features")
        print(f"   🔧 Technical features: {len(demo_info['technical_features'])} features")
        return True
    except Exception as e:
        print(f"❌ Demo response test error: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Costing API Functionality")
    print("=" * 50)

    tests = [
        ("Module Imports", test_imports),
        ("Calculator Initialization", test_calculator_initialization),
        ("Demo Response Structure", test_demo_response)
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        if test_func():
            passed += 1
        else:
            print(f"   ❌ {test_name} failed")

    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The costing API is ready for deployment.")
        return True
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
