"""
Costing Sheet Calculator - Main Entry Point

Simplified cost calculation system for scheme estimation and validation.
Memory-based processing with single file output.

Key Features:
- Base period data only (excludes scheme period data)
- Simplified calculation model for cost estimation
- Memory-based processing without file persistence
- Output only the final costing sheet file
- Includes columns from start through Target Volume/Target Value
- Processes both Main Scheme and Additional Scheme simultaneously

Usage:
    python costing_sheet_main.py

This system is designed as a cost estimation tool, not a comprehensive cost tracker.
"""

import sys
import os
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from costing_sheet import CostingSheetCalculator


def main():
    """
    Main function for costing sheet calculator.
    """
    print("🚀 COSTING SHEET CALCULATOR SYSTEM")
    print("=" * 60)
    print("📋 SIMPLIFIED COST ESTIMATION TOOL")
    print("   • Base period data only")
    print("   • Memory-based processing")
    print("   • Single output file generation")
    print("   • Columns: Start → Target Volume/Value")
    print("   • Main + Additional schemes")
    print("=" * 60)
    
    try:
        # Get scheme ID from user
        scheme_id = input("\n🔍 Enter Scheme ID for costing analysis: ").strip()
        
        if not scheme_id:
            print("❌ No scheme ID provided. Exiting.")
            return
            
        # Initialize calculator
        print(f"\n🏗️ Initializing Costing Sheet Calculator...")
        calculator = CostingSheetCalculator()
        
        # Validate scheme first
        print(f"\n🔍 Validating scheme requirements...")
        validation = calculator.validate_scheme_requirements(scheme_id)
        
        if not validation['is_valid']:
            print(f"\n❌ SCHEME VALIDATION FAILED")
            print("Errors:")
            for error in validation['errors']:
                print(f"   • {error}")
            if validation['warnings']:
                print("Warnings:")
                for warning in validation['warnings']:
                    print(f"   • {warning}")
            return
            
        print(f"✅ Scheme validation passed")
        if validation.get('summary'):
            summary = validation['summary']
            print(f"   📊 Base periods: {summary['base_periods_count']}")
            print(f"   📊 Sales records: {summary['sales_records_count']}")
            print(f"   📊 Unique accounts: {summary['unique_accounts']}")
            print(f"   📊 Additional schemes: {summary['additional_schemes']}")
            
        # Perform costing calculation
        print(f"\n🧮 Starting costing sheet calculation...")
        result = calculator.calculate_costing_sheet(scheme_id)
        
        if result['status'] == 'success':
            print(f"\n🎉 COSTING SHEET GENERATION SUCCESSFUL!")
            print("=" * 50)
            
            # Display results
            print(f"📄 Output File: {result['output_file']}")
            print(f"⏱️  Processing Time: {result['processing_time_seconds']:.2f} seconds")
            
            # Display calculation summary
            calc_summary = result['calculation_summary']
            if 'main_scheme' in calc_summary:
                main = calc_summary['main_scheme']
                print(f"\n📊 MAIN SCHEME SUMMARY:")
                print(f"   • Total Accounts: {calc_summary['total_accounts']}")
                print(f"   • Base 1 Volume: {main['base1_volume_total']:,.2f}")
                print(f"   • Base 1 Value: {main['base1_value_total']:,.2f}")
                print(f"   • Target Volume: {main['target_volume_total']:,.2f}")
                print(f"   • Target Value: {main['target_value_total']:,.2f}")
                
            # Display additional schemes if present
            if 'additional_schemes' in calc_summary:
                additional = calc_summary['additional_schemes']
                print(f"\n📊 ADDITIONAL SCHEMES: {len(additional)} found")
                for scheme_key, scheme_data in additional.items():
                    print(f"   • {scheme_key}: Target Vol={scheme_data['target_volume_total']:,.2f}, Target Val={scheme_data['target_value_total']:,.2f}")
                    
            # Display output info
            output_info = result['output_info']
            if 'file_size_mb' in output_info:
                print(f"\n📁 FILE INFO:")
                print(f"   • Size: {output_info['file_size_mb']} MB")
                print(f"   • Created: {output_info['created_time']}")
                
            print(f"\n💡 COSTING ANALYSIS COMPLETE")
            print(f"   The generated file contains cost estimates based on base period data.")
            print(f"   This is a simplified calculation for cost estimation purposes.")
            
        else:
            print(f"\n❌ COSTING SHEET GENERATION FAILED")
            print(f"Error: {result['error_message']}")
            
    except KeyboardInterrupt:
        print(f"\n\n⚠️ Calculation cancelled by user")
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        
    finally:
        print(f"\n👋 Costing Sheet Calculator session ended")


def demo_mode():
    """
    Demo mode to show system capabilities.
    """
    print("🚀 COSTING SHEET CALCULATOR - DEMO MODE")
    print("=" * 60)
    
    print("\n📋 SYSTEM CAPABILITIES:")
    print("   ✅ Base period data processing")
    print("   ✅ Memory-based calculations") 
    print("   ✅ Simplified cost estimation")
    print("   ✅ Main + Additional schemes")
    print("   ✅ Target volume/value calculations")
    print("   ✅ Professional Excel output")
    print("   ✅ Summary and metadata sheets")
    
    print("\n📊 CALCULATION SCOPE:")
    print("   • Columns: Start → Target Volume/Target Value")
    print("   • Data: Base period only (no scheme period)")
    print("   • Purpose: Cost estimation and validation")
    print("   • Output: Single Excel file with multiple sheets")
    
    print("\n🔧 TECHNICAL FEATURES:")
    print("   • Memory-only processing (no intermediate files)")
    print("   • Vectorized calculations for performance")
    print("   • Automatic scheme validation")
    print("   • Professional Excel formatting")
    print("   • Error handling and recovery")
    
    print("\n💰 COST ESTIMATION FOCUS:")
    print("   • Simplified calculation model")
    print("   • Base data validation")
    print("   • Target projection analysis")
    print("   • Multi-scheme comparison")
    
    print("\n🎯 USE CASES:")
    print("   • Scheme cost estimation")
    print("   • Budget planning and validation")
    print("   • Quick scheme analysis")
    print("   • Cost comparison between schemes")
    
    print("\n📁 OUTPUT STRUCTURE:")
    print("   Sheet 1: Main costing calculations")
    print("   Sheet 2: Summary analysis")
    print("   Sheet 3: Metadata and configuration")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        demo_mode()
    else:
        main()
