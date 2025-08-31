"""
Scheme Final Payout Calculations Module

This module calculates the final payout for schemes based on:
1. Scheme type (inbuilt, ho-scheme, other)
2. Mandatory qualification checks
3. Summation of all relevant payout components

Formula Summary:
- IF scheme_type == 'inbuilt': RETURN 0
- ELSE IF any mandatory qualification fails: RETURN 0
- ELSE: Sum all relevant payout components based on scheme type
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def calculate_scheme_final_payout(tracker_df: pd.DataFrame, structured_data: Dict[str, Any], 
                                scheme_config: Dict = None) -> pd.DataFrame:
    """
    Calculate Scheme Final Payout column for all accounts
    
    Args:
        tracker_df: Main tracker DataFrame
        structured_data: Structured scheme data
        scheme_config: Scheme configuration data
        
    Returns:
        Updated tracker DataFrame with 'Scheme Final Payout' column
    """
    print("🔢 Calculating Scheme Final Payout...")
    
    # Get scheme type from structured data or default
    scheme_type = _get_scheme_type(structured_data, scheme_config)
    print(f"   📊 Detected scheme type: {scheme_type}")
    
    # Get additional schemes count
    additional_schemes_count = _get_additional_schemes_count(tracker_df)
    print(f"   📋 Additional schemes detected: {additional_schemes_count}")
    
    # Get bonus schemes count (if any)
    bonus_schemes_count = _get_bonus_schemes_count(tracker_df)
    print(f"   🎁 Bonus schemes detected: {bonus_schemes_count}")
    
    # Calculate final payout for each row
    final_payouts = []
    
    for index, row in tracker_df.iterrows():
        final_payout = _calculate_row_final_payout(
            row, scheme_type, additional_schemes_count, bonus_schemes_count
        )
        final_payouts.append(final_payout)
    
    # Add the column at the end
    tracker_df['Scheme Final Payout'] = final_payouts
    
    # 🔢 NEW: Round up values without decimals (as requested by user)
    # Convert to float first, then round up using numpy.ceil to get whole numbers
    tracker_df['Scheme Final Payout'] = pd.to_numeric(
        tracker_df['Scheme Final Payout'], errors='coerce'
    ).fillna(0.0)
    
    # Round up all values and convert to integers (no decimals)
    tracker_df['Scheme Final Payout'] = np.ceil(tracker_df['Scheme Final Payout']).astype(int)
    
    non_zero_count = (tracker_df['Scheme Final Payout'] > 0).sum()
    total_payout = tracker_df['Scheme Final Payout'].sum()
    
    print(f"   ✅ Scheme Final Payout calculated: {non_zero_count} accounts with payout")
    print(f"   🔢 Applied rounding: All values rounded up to whole numbers (no decimals)")
    print(f"   💰 Total scheme payout: ₹{total_payout:,}")
    
    return tracker_df


def _get_scheme_type(structured_data: Dict[str, Any], scheme_config: Dict = None) -> str:
    """
    Extract scheme type from structured data or scheme config
    
    Priority:
    1. structured_data['scheme_info'] -> scheme_base
    2. Default to 'target-based' (non-inbuilt)
    """
    try:
        # Check scheme_info dataframe
        scheme_info_df = structured_data.get('scheme_info', pd.DataFrame())
        if not scheme_info_df.empty:
            main_scheme_row = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
            if not main_scheme_row.empty:
                scheme_base = main_scheme_row['scheme_base'].iloc[0]
                
                # Map scheme_base to scheme type
                if scheme_base == 'inbuilt':
                    return 'inbuilt'
                elif 'ho-scheme' in str(scheme_base).lower():
                    return 'ho-scheme'
                else:
                    return 'target-based'  # Default non-inbuilt type
        
        # Default fallback
        return 'target-based'
        
    except Exception as e:
        print(f"   ⚠️ Warning: Could not determine scheme type, defaulting to 'target-based': {e}")
        return 'target-based'


def _get_additional_schemes_count(tracker_df: pd.DataFrame) -> int:
    """Count additional schemes based on column suffixes"""
    additional_cols = [col for col in tracker_df.columns if '_p1' in col]
    if additional_cols:
        # Check for _p2, _p3, etc.
        max_suffix = 1
        for i in range(2, 10):  # Check up to _p9
            if any(f'_p{i}' in col for col in tracker_df.columns):
                max_suffix = i
        return max_suffix
    return 0


def _get_bonus_schemes_count(tracker_df: pd.DataFrame) -> int:
    """Count bonus schemes based on bonus payout columns"""
    bonus_payout_cols = [col for col in tracker_df.columns if 'Bonus Scheme' in col and 'Payout' in col]
    
    # Extract scheme numbers
    scheme_numbers = []
    for col in bonus_payout_cols:
        try:
            # Extract number from "Bonus Scheme X Bonus Payout" or "Bonus Scheme X MP Payout"
            parts = col.split()
            if len(parts) >= 3 and parts[0] == 'Bonus' and parts[1] == 'Scheme':
                scheme_num = int(parts[2])
                scheme_numbers.append(scheme_num)
        except (ValueError, IndexError):
            continue
    
    return max(scheme_numbers) if scheme_numbers else 0


def _calculate_row_final_payout(row: pd.Series, scheme_type: str, 
                              additional_schemes_count: int, bonus_schemes_count: int) -> float:
    """
    Calculate final payout for a single row based on scheme type and mandatory checks
    """
    
    # STEP 1: Check if scheme type is 'inbuilt'
    if scheme_type == 'inbuilt':
        return 0.0
    
    # STEP 2: Mandatory Qualification Checks
    # Check Main Scheme Mandatory Qualification
    if row.get("Mandatory Qualify") == "Yes":
        main_achieved = row.get("percentage_achieved")
        if main_achieved is None or main_achieved < 1.0:
            return 0.0  # Failed main scheme mandatory qualification
    
    # Check Additional Schemes Mandatory Qualification
    for i in range(1, additional_schemes_count + 1):
        mandatory_qualify_col = f"Mandatory Qualify_p{i}"
        percentage_achieved_col = f"percentage_achieved_p{i}"
        
        if row.get(mandatory_qualify_col) == "Yes":
            achieved = row.get(percentage_achieved_col)
            if achieved is None or achieved < 1.0:
                return 0.0  # Failed additional scheme mandatory qualification
    
    # STEP 3: Calculate Total Payout
    total_payout = 0.0
    
    # Main Scheme Base Payouts (for all non-inbuilt schemes)
    main_payout_cols = [
        "Total_Payout",
        "MP_Final_Payout", 
        "FINAL_PHASING_PAYOUT"
    ]
    
    for col in main_payout_cols:
        value = row.get(col, 0)
        if pd.notna(value):
            total_payout += float(value)
    
    # Additional Schemes Payouts
    for i in range(1, additional_schemes_count + 1):
        additional_payout_cols = [
            f"Total_Payout_p{i}",
            f"MP_Final_Payout_p{i}",
            f"FINAL_PHASING_PAYOUT_p{i}"
        ]
        
        for col in additional_payout_cols:
            value = row.get(col, 0)
            if pd.notna(value):
                total_payout += float(value)
    
    # For HO-SCHEME: Add Bonus Payouts
    if scheme_type == 'ho-scheme':
        for i in range(1, bonus_schemes_count + 1):
            bonus_payout_cols = [
                f"Bonus Scheme {i} Bonus Payout",
                f"Bonus Scheme {i} MP Payout"
            ]
            
            for col in bonus_payout_cols:
                value = row.get(col, 0)
                if pd.notna(value):
                    total_payout += float(value)
    
    return total_payout


def _safe_float_conversion(value) -> float:
    """Safely convert value to float, handling NaN and None"""
    if value is None or pd.isna(value):
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


# Column mapping information for reference
COLUMN_MAPPING = {
    'mandatory_qualify_main': 'Mandatory Qualify',
    'mandatory_qualify_additional': 'Mandatory Qualify_p{i}',  # i = 1, 2, 3, etc.
    'percentage_achieved_main': 'percentage_achieved',
    'percentage_achieved_additional': 'percentage_achieved_p{i}',  # i = 1, 2, 3, etc.
    'main_payouts': ['Total_Payout', 'MP_Final_Payout', 'FINAL_PHASING_PAYOUT'],
    'additional_payouts': ['Total_Payout_p{i}', 'MP_Final_Payout_p{i}', 'FINAL_PHASING_PAYOUT_p{i}'],
    'bonus_payouts': ['Bonus Scheme {i} Bonus Payout', 'Bonus Scheme {i} MP Payout']
}
