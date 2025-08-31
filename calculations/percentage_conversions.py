"""
Percentage Conversion Module
Converts decimal fields to percentage format for display purposes

User Requirements:
Convert decimal values (e.g., 0.75) to percentage format (e.g., 75%)
This affects both main scheme and additional scheme fields

IMPORTANT EXCLUSIONS:
The following rebate percentage columns are NOT converted to avoid inflating dependent calculations:
- Phasing_Period_Rebate_Percent_1/2/3 (and _pN variants)
- Rebate_percent (and _pN variants)
- MP_Rebate_percent (and _pN variants)
- Bonus_Phasing_Period_Rebate_Percent_1/2/3 (and _pN variants)
- Bonus_Phasing_Target_Percent_1/2/3 (and _pN variants)
- Phasing_Target_Percent_1/2/3 (and _pN variants)

These fields should remain in decimal format for proper calculation results.
"""

import pandas as pd
import numpy as np
from typing import List


def convert_decimal_fields_to_percentage(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert specified decimal fields to percentage format for all schemes
    
    Args:
        tracker_df: DataFrame with decimal fields to convert
        
    Returns:
        DataFrame with converted percentage fields
    """
    print("🔢 Converting decimal fields to percentage format...")
    
    # Convert main scheme fields
    tracker_df = _convert_main_scheme_percentage_fields(tracker_df)
    
    # Convert additional scheme fields
    tracker_df = _convert_additional_scheme_percentage_fields(tracker_df)
    
    # Divide specific fields by 100 as requested by user
    tracker_df = _divide_specific_fields_by_100(tracker_df)
    
    print("✅ Percentage conversions completed!")
    return tracker_df


def _convert_main_scheme_percentage_fields(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """Convert main scheme decimal fields to percentage format"""
    
    print("   📊 Converting Main Scheme percentage fields...")
    
    # Main Scheme fields to convert
    main_scheme_fields = [
        'Bonus_Scheme_1%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_1',
        'Bonus_Scheme_2%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_2',
        'Bonus_Scheme_3%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_3',
        'Bonus_Scheme_4%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_4',
        'Bonus_Scheme_%_Achieved_1',
        'Bonus_Scheme_%_Achieved_2',
        'Bonus_Scheme_%_Achieved_3',
        'Bonus_Scheme_%_Achieved_4',
        '%_MP_Achieved_1',
        '%_MP_Achieved_2',
        '%_MP_Achieved_3',
        '%_MP_Achieved_4'
    ]
    
    converted_count = 0
    for field in main_scheme_fields:
        if field in tracker_df.columns:
            tracker_df[field] = _convert_decimal_to_percentage(tracker_df[field])
            converted_count += 1
            print(f"     ✓ Converted {field}")
    
    print(f"   📊 Main Scheme: {converted_count} fields converted to percentage")
    return tracker_df


def _convert_additional_scheme_percentage_fields(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """Convert additional scheme decimal fields to percentage format"""
    
    print("   📊 Converting Additional Scheme percentage fields...")
    
    # Additional Scheme fields (without _pN suffix - will be detected dynamically)
    # UPDATED: Removed rebate percentage fields to prevent inflation of dependent calculations
    additional_scheme_base_fields = [
        'growth_rate',
        # 'Phasing_Period_Rebate_Percent_1',  # REMOVED - causing inflated calculations
        # 'Phasing_Period_Rebate_Percent_2',  # REMOVED - causing inflated calculations
        # 'Phasing_Period_Rebate_Percent_3',  # REMOVED - causing inflated calculations
        # 'Rebate_percent',                   # REMOVED - causing inflated calculations
        # 'MP_Rebate_percent',                # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Period_Rebate_Percent_1',  # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Period_Rebate_Percent_2',  # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Period_Rebate_Percent_3',  # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Target_Percent_1',   # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Target_Percent_2',   # REMOVED - causing inflated calculations
        # 'Bonus_Phasing_Target_Percent_3',   # REMOVED - causing inflated calculations
        # 'Phasing_Target_Percent_1',         # REMOVED - causing inflated calculations
        # 'Phasing_Target_Percent_2',         # REMOVED - causing inflated calculations
        # 'Phasing_Target_Percent_3',         # REMOVED - causing inflated calculations
        'Percent_Phasing_Achieved_1',
        'Percent_Phasing_Achieved_2',
        'Percent_Phasing_Achieved_3',
        'Percent_Phasing_Achieved_4',
        'Percent_Bonus_Achieved_1',
        'Percent_Bonus_Achieved_2',
        'Percent_Bonus_Achieved_3',
        'Percent_Bonus_Achieved_4',
        'percentage_achieved',
        'Mandatory_Product_PPI_Achievement',
        'Mandatory_Product_Growth_Target_Achieved'
        # 'MP_FINAL_ACHIEVEMENT_pct'  # REMOVED - already calculated as percentage in formula
    ]
    
    converted_count = 0
    
    # Convert base additional scheme fields (without suffix)
    for field in additional_scheme_base_fields:
        if field in tracker_df.columns:
            tracker_df[field] = _convert_decimal_to_percentage(tracker_df[field])
            converted_count += 1
            print(f"     ✓ Converted {field}")
    
    # Convert additional scheme fields with _pN suffix
    # Detect available additional schemes by checking for _p pattern in columns
    additional_schemes = set()
    for col in tracker_df.columns:
        if '_p' in col and col.split('_p')[-1].isdigit():
            suffix = '_p' + col.split('_p')[-1]
            additional_schemes.add(suffix)
    
    for suffix in sorted(additional_schemes):
        scheme_num = suffix.replace('_p', '')
        print(f"     📋 Converting Additional Scheme {scheme_num} fields...")
        
        # Convert fields with _pN suffix
        for base_field in additional_scheme_base_fields:
            field_with_suffix = f"{base_field}{suffix}"
            if field_with_suffix in tracker_df.columns:
                tracker_df[field_with_suffix] = _convert_decimal_to_percentage(tracker_df[field_with_suffix])
                converted_count += 1
                print(f"       ✓ Converted {field_with_suffix}")
    
    print(f"   📊 Additional Schemes: {converted_count} fields converted to percentage")
    return tracker_df


def _divide_specific_fields_by_100(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """Divide specific fields by 100 as requested by user"""
    
    print("   📊 Dividing specific fields by 100...")
    
    # Fields to divide by 100 (as specified by user)
    fields_to_divide = [
        'Bonus_Scheme_1%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_1',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_1',
        'Bonus_Scheme_2%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_2',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_2',
        'Bonus_Scheme_3%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_3',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_3',
        'Bonus_Scheme_4%_Main_Scheme_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_4',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_4'
    ]
    
    divided_count = 0
    for field in fields_to_divide:
        if field in tracker_df.columns:
            tracker_df[field] = _divide_by_100(tracker_df[field])
            divided_count += 1
            print(f"     ✓ Divided {field} by 100")
    
    print(f"   📊 Divided {divided_count} fields by 100")
    return tracker_df


def _convert_decimal_to_percentage(series: pd.Series) -> pd.Series:
    """
    Convert decimal values to percentage format
    
    Args:
        series: Pandas series with decimal values (e.g., 0.75)
        
    Returns:
        Series with percentage values (e.g., 75.0)
    """
    # Convert to numeric, handle any non-numeric values
    numeric_series = pd.to_numeric(series, errors='coerce').fillna(0.0)
    
    # Convert decimal to percentage (multiply by 100)
    # e.g., 0.75 becomes 75.0
    percentage_series = numeric_series * 100.0
    
    return percentage_series.astype(float)

def _divide_by_100(series: pd.Series) -> pd.Series:
    """
    Divide values by 100 as requested by user
    
    Args:
        series: Pandas series with values
        
    Returns:
        Series with values divided by 100
    """
    # Convert to numeric, handle any non-numeric values
    numeric_series = pd.to_numeric(series, errors='coerce').fillna(0.0)
    
    # Divide by 100
    # e.g., 10.0 becomes 0.1
    divided_series = numeric_series / 100.0
    
    return divided_series.astype(float)