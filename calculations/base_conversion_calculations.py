"""
Base Volume/Value Conversion Calculations Module
Handles conversion calculations for value and volume schemes
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def add_base_conversion_columns(tracker_df: pd.DataFrame, structured_data: Dict) -> pd.DataFrame:
    """
    Add base volume/value conversion columns based on scheme type
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing scheme info
    
    Returns:
        Updated tracker DataFrame with conversion columns
    """
    print("🧮 Adding base volume/value conversion columns...")
    
    # Get scheme information
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    
    if scheme_info.empty:
        print("   ⚠️ No scheme info found - skipping conversion calculations")
        return tracker_df
    
    # Process main scheme
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    if not main_scheme_info.empty:
        scheme_type = main_scheme_info.iloc[0]['volume_value_based'].lower()
        tracker_df = _add_scheme_conversion_columns(tracker_df, scheme_type, '')
    
    # Process additional schemes
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['volume_value_based'].lower()
        scheme_num = scheme_row['scheme_type'].split('_')[-1] if '_' in scheme_row['scheme_type'] else '1'
        suffix = f'_p{scheme_num}'
        tracker_df = _add_scheme_conversion_columns(tracker_df, scheme_type, suffix)
    
    print("✅ Base conversion calculations completed")
    return tracker_df


def _add_scheme_conversion_columns(tracker_df: pd.DataFrame, scheme_type: str, suffix: str) -> pd.DataFrame:
    """
    Add conversion columns for a specific scheme
    
    Args:
        tracker_df: DataFrame to update
        scheme_type: 'volume' or 'value'
        suffix: Column suffix for additional schemes
    
    Returns:
        Updated DataFrame
    """
    print(f"   📊 Processing {scheme_type} scheme{suffix}")
    
    # Get required columns
    total_volume_col = f'total_volume{suffix}' if suffix else 'total_volume'
    total_value_col = f'total_value{suffix}' if suffix else 'total_value'
    base_volume_col = f'Base 1 Volume Final{suffix}' if suffix else 'Base 1 Volume Final'
    base_value_col = f'Base 1 Value Final{suffix}' if suffix else 'Base 1 Value Final'
    
    # Check if required columns exist
    required_cols = [total_volume_col, total_value_col, base_volume_col, base_value_col]
    missing_cols = [col for col in required_cols if col not in tracker_df.columns]
    
    if missing_cols:
        print(f"   ⚠️ Missing columns: {missing_cols}")
        return tracker_df
    
    # Convert columns to float to avoid Decimal issues
    for col in required_cols:
        tracker_df[col] = pd.to_numeric(tracker_df[col], errors='coerce').fillna(0.0).astype(float)
    
    if scheme_type == 'value':
        # Add Base Volume Required in Value Scheme
        # Formula: Base_Volume_Required_in_Value_Scheme = Base_Value / (Total_Value / Total_Volume)
        conversion_col = f'Base_Volume_Required_in_Value_Scheme{suffix}'
        
        # Calculate price per unit (Total_Value / Total_Volume)
        price_per_unit = np.where(
            tracker_df[total_volume_col] > 0,
            tracker_df[total_value_col] / tracker_df[total_volume_col],
            0.0
        )
        
        # Calculate base volume required
        tracker_df[conversion_col] = np.where(
            price_per_unit > 0,
            tracker_df[base_value_col] / price_per_unit,
            0.0
        )
        
        print(f"   ✅ Added {conversion_col}")
        
    elif scheme_type == 'volume':
        # Add Base Value Required in Volume Scheme
        # Formula: Base_Value_Required_in_Volume_Scheme = Base_Volume * (Total_Value / Total_Volume)
        conversion_col = f'Base_Value_Required_in_Volume_Scheme{suffix}'
        
        # Calculate price per unit (Total_Value / Total_Volume)
        price_per_unit = np.where(
            tracker_df[total_volume_col] > 0,
            tracker_df[total_value_col] / tracker_df[total_volume_col],
            0.0
        )
        
        # Calculate base value required
        tracker_df[conversion_col] = tracker_df[base_volume_col] * price_per_unit
        
        print(f"   ✅ Added {conversion_col}")
    
    return tracker_df
