"""
Enhanced Costing Tracker Fields Module
Implements comprehensive fixes and additions for costing tracker fields as per specifications

TASK: FIX/ADD THE FOLLOWING COSTING TRACKER FIELDS:
1) Mandatory_Product_Actual_Volume — CAPTURE CORRECTLY
2) Mandatory_Product_Actual_Value — CAPTURE CORRECTLY
3) Payout_Products_Volume — ADD/CAPTURE
4) Payout_Products_Value — ADD/CAPTURE
5) Mandatory_Product_Growth — FIX & NORMALIZE TO PERCENT
6) MP_FINAL_TARGET — FIX WITH SCHEME-TYPE LOGIC
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime


def calculate_enhanced_costing_tracker_fields(tracker_df: pd.DataFrame, structured_data: Dict, 
                                            sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate all enhanced costing tracker fields according to specifications
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing schemes configuration
        sales_df: Sales data from Supabase
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with enhanced costing tracker fields
    """
    print("🧮 Calculating enhanced costing tracker fields...")
    
    # Process main scheme
    print("   📊 Processing main scheme fields...")
    tracker_df = _process_scheme_enhanced_fields(
        tracker_df, structured_data, sales_df, scheme_config, 
        scheme_type='main_scheme', suffix=''
    )
    
    # Process additional schemes
    additional_schemes = _get_additional_schemes_info(structured_data)
    for scheme_index, scheme_info in additional_schemes.items():
        suffix = f'_p{scheme_index}'
        print(f"   📊 Processing additional scheme {scheme_index} fields...")
        tracker_df = _process_scheme_enhanced_fields(
            tracker_df, structured_data, sales_df, scheme_config,
            scheme_type='additional_scheme', suffix=suffix, scheme_index=scheme_index
        )
    
    print("✅ Enhanced costing tracker fields calculation completed!")
    return tracker_df


def _process_scheme_enhanced_fields(tracker_df: pd.DataFrame, structured_data: Dict, 
                                  sales_df: pd.DataFrame, scheme_config: Dict,
                                  scheme_type: str, suffix: str, scheme_index: Optional[int] = None) -> pd.DataFrame:
    """Process enhanced fields for a specific scheme (main or additional)"""
    
    # Get scheme configuration
    config_manager = structured_data.get('config_manager')
    scheme_info = _get_scheme_info(structured_data, scheme_type, scheme_index)
    
    # Determine if scheme is volume or value based
    is_volume_based = scheme_info.get('volume_value_based', 'volume').lower() == 'volume'
    
    # Check feature enablement
    if scheme_type == 'main_scheme':
        mandatory_enabled = config_manager.should_calculate_main_mandatory_products() if config_manager else True
        payout_enabled = config_manager.should_calculate_main_payouts() if config_manager else True
    else:
        mandatory_enabled = config_manager.should_calculate_additional_mandatory_products(str(scheme_index)) if config_manager else True
        payout_enabled = config_manager.should_calculate_additional_payouts(str(scheme_index)) if config_manager else True
    
    print(f"     📋 Scheme: {'Volume' if is_volume_based else 'Value'} based")
    print(f"     📋 Features: Mandatory={mandatory_enabled}, Payout={payout_enabled}")
    
    # 1) Mandatory_Product_Actual_Volume & 2) Mandatory_Product_Actual_Value
    tracker_df = _calculate_mandatory_product_actuals(
        tracker_df, structured_data, sales_df, scheme_config, 
        scheme_type, suffix, scheme_index, mandatory_enabled
    )
    
    # 3) Payout_Products_Volume & 4) Payout_Products_Value
    tracker_df = _calculate_payout_products_actuals(
        tracker_df, structured_data, sales_df, scheme_config,
        scheme_type, suffix, scheme_index, payout_enabled
    )
    
    # 5) Mandatory_Product_Growth — FIX & NORMALIZE TO PERCENT
    tracker_df = _fix_mandatory_product_growth(
        tracker_df, structured_data, scheme_type, suffix, scheme_index, is_volume_based
    )
    
    # 5.5) 🔧 NEW: Calculate Mandatory_Product_Growth_Target_Volume and Value
    tracker_df = _calculate_mandatory_growth_targets(
        tracker_df, scheme_type, suffix
    )
    
    # 6) MP_FINAL_TARGET — FIX WITH SCHEME-TYPE LOGIC
    tracker_df = _fix_mp_final_target(
        tracker_df, structured_data, scheme_type, suffix, scheme_index, is_volume_based
    )
    
    # 7) MP_FINAL_ACHIEVEMENT_pct — Calculate after MP_FINAL_TARGET is set
    tracker_df = _calculate_mp_final_achievement_pct(tracker_df, structured_data, scheme_type, suffix, scheme_index)
    
    return tracker_df


def _calculate_mandatory_product_actuals(tracker_df: pd.DataFrame, structured_data: Dict,
                                        sales_df: pd.DataFrame, scheme_config: Dict,
                                        scheme_type: str, suffix: str, scheme_index: Optional[int], 
                                        mandatory_enabled: bool) -> pd.DataFrame:
    """
    Calculate Mandatory_Product_Actual_Volume and Mandatory_Product_Actual_Value
    Definition: Sum of sales volume/value for all materials in the scheme's Mandatory Products set
    """
    
    volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
    value_col = f'Mandatory_Product_Actual_Value{suffix}'
    
    if not mandatory_enabled:
        print(f"     🚫 Mandatory products disabled - setting {volume_col} and {value_col} to 0")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    # Get mandatory products for this scheme
    mandatory_materials = _get_mandatory_products_materials(structured_data, scheme_type, scheme_index)
    
    if not mandatory_materials:
        print(f"     ℹ️ No mandatory products found for {scheme_type}")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    print(f"     📦 Found {len(mandatory_materials)} mandatory materials")
    
    # Filter sales data for scheme period and mandatory products
    scheme_sales = _filter_scheme_period_sales(sales_df, scheme_config, mandatory_materials)
    
    if scheme_sales.empty:
        print(f"     ℹ️ No mandatory product sales in scheme period")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    # Aggregate by credit_account using vectorized operations
    mandatory_actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    mandatory_actuals['credit_account'] = mandatory_actuals['credit_account'].astype(str)
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    
    # Create mapping for efficient vectorized assignment
    volume_mapping = dict(zip(mandatory_actuals['credit_account'], mandatory_actuals['volume']))
    value_mapping = dict(zip(mandatory_actuals['credit_account'], mandatory_actuals['value']))
    
    # Vectorized assignment
    tracker_df[volume_col] = tracker_df['credit_account'].map(volume_mapping).fillna(0.0).astype(float)
    tracker_df[value_col] = tracker_df['credit_account'].map(value_mapping).fillna(0.0).astype(float)
    
    accounts_with_data = (tracker_df[volume_col] > 0).sum()
    print(f"     ✅ {volume_col}: {accounts_with_data} accounts with mandatory volume > 0")
    
    return tracker_df


def _calculate_mp_final_achievement_pct(tracker_df: pd.DataFrame, structured_data: Dict, scheme_type: str, suffix: str, scheme_index: Optional[int]) -> pd.DataFrame:
    """
    Calculate MP_FINAL_ACHIEVEMENT_pct after mandatory actuals are calculated
    Formula: 
    - Volume Scheme: MP_FINAL_ACHIEVEMENT_pct = (Mandatory_Product_Actual_Volume / MP_FINAL_TARGET) * 100
    - Value Scheme: MP_FINAL_ACHIEVEMENT_pct = (Mandatory_Product_Actual_Value / MP_FINAL_TARGET) * 100
    """
    
    achievement_col = f'MP_FINAL_ACHIEVEMENT_pct{suffix}'
    target_col = f'MP_FINAL_TARGET{suffix}'
    volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
    value_col = f'Mandatory_Product_Actual_Value{suffix}'
    scheme_type_col = f'Scheme Type{suffix}'
    
    print(f"     🧮 Calculating {achievement_col}...")
    
    # Check if required columns exist
    if target_col not in tracker_df.columns:
        print(f"     ⚠️ {target_col} column not found - setting {achievement_col} to 0")
        tracker_df[achievement_col] = 0.0
        return tracker_df
    
    # Create scheme type mask
    volume_scheme_mask = tracker_df[scheme_type_col].astype(str).str.upper() == 'VOLUME'
    
    # Calculate volume ratio (for volume schemes)
    volume_ratio = np.where(
        tracker_df[target_col] > 0,
        (tracker_df[volume_col].astype(float) / tracker_df[target_col].astype(float)) * 100.0,
        0.0
    )
    
    # Calculate value ratio (for value schemes)
    value_ratio = np.where(
        tracker_df[target_col] > 0,
        (tracker_df[value_col].astype(float) / tracker_df[target_col].astype(float)) * 100.0,
        0.0
    )
    
    # Apply scheme-specific formula with robust assignment
    calculated_achievement = np.where(
        volume_scheme_mask,
        volume_ratio,  # Volume scheme: use volume ratio
        value_ratio    # Value scheme: use value ratio
    )
    
    # Use .loc for more robust assignment to avoid DataFrame fragmentation issues
    if achievement_col not in tracker_df.columns:
        tracker_df[achievement_col] = 0.0
    
    tracker_df.loc[:, achievement_col] = calculated_achievement
    
    # Verify assignment worked for problematic accounts
    problematic_accounts = ['261622', '3082993']
    for acc in problematic_accounts:
        acc_data = tracker_df[tracker_df['credit_account'].astype(str) == acc]
        if len(acc_data) > 0:
            idx = acc_data.index[0]
            target = tracker_df.at[idx, target_col]
            achievement_after = tracker_df.at[idx, achievement_col]
            print(f"     ✅ VERIFY Account {acc}: Target={target:.2f}, Achievement after assignment={achievement_after:.2f}")
    
    # Debug specific problematic accounts with full details
    problematic_accounts = ['261622', '3082993']
    for acc in problematic_accounts:
        acc_data = tracker_df[tracker_df['credit_account'].astype(str) == acc]
        if len(acc_data) > 0:
            row = acc_data.iloc[0]
            target = row.get(target_col, 0)
            actual_vol = row.get(volume_col, 0)
            actual_val = row.get(value_col, 0)
            scheme_type = row.get(scheme_type_col, '')
            achievement = row.get(achievement_col, 0)
            is_volume = scheme_type.upper() == 'VOLUME'
            
            # Check if columns exist
            target_exists = target_col in tracker_df.columns
            volume_exists = volume_col in tracker_df.columns
            value_exists = value_col in tracker_df.columns
            scheme_exists = scheme_type_col in tracker_df.columns
            
            print(f"     🔍 DEBUG MP_CALC Account {acc}: Target={target:.2f} (col_exists={target_exists}), Actual_Vol={actual_vol:.2f} (col_exists={volume_exists}), Actual_Val={actual_val:.2f} (col_exists={value_exists}), SchemeType='{scheme_type}' (col_exists={scheme_exists}), IsVolume={is_volume}, Achievement={achievement:.2f}")
            
            # Manual calculation to verify
            if target > 0:
                if is_volume:
                    manual_calc = (actual_vol / target) * 100.0
                    print(f"         Manual Volume Calc: ({actual_vol} / {target}) * 100 = {manual_calc:.2f}")
                else:
                    manual_calc = (actual_val / target) * 100.0
                    print(f"         Manual Value Calc: ({actual_val} / {target}) * 100 = {manual_calc:.2f}")
            else:
                print(f"         ⚠️ Target is 0, cannot calculate achievement")
    
    # Debug output
    accounts_with_achievement = (tracker_df[achievement_col] > 0).sum()
    print(f"     ✅ {achievement_col}: {accounts_with_achievement} accounts with achievement > 0")
    
    # Calculate MP_Final_Payout after MP_FINAL_ACHIEVEMENT_pct is calculated
    tracker_df = _calculate_mp_final_payout(tracker_df, structured_data, scheme_type, suffix, scheme_index)
    
    return tracker_df


def _calculate_payout_products_actuals(tracker_df: pd.DataFrame, structured_data: Dict,
                                      sales_df: pd.DataFrame, scheme_config: Dict,
                                      scheme_type: str, suffix: str, scheme_index: Optional[int],
                                      payout_enabled: bool) -> pd.DataFrame:
    """
    Calculate Payout_Products_Volume and Payout_Products_Value
    Definition: Sum of sales volume/value for all materials in the scheme's Payout Products selection
    """
    
    volume_col = f'Payout_Products_Volume{suffix}'
    value_col = f'Payout_Products_Value{suffix}'
    
    if not payout_enabled:
        print(f"     🚫 Payout products disabled - setting {volume_col} and {value_col} to 0")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    # Get payout products for this scheme
    payout_materials = _get_payout_products_materials(structured_data, scheme_type, scheme_index)
    
    if not payout_materials:
        print(f"     ℹ️ No payout products found for {scheme_type}")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    print(f"     📦 Found {len(payout_materials)} payout materials")
    
    # Filter sales data for scheme period and payout products
    scheme_sales = _filter_scheme_period_sales(sales_df, scheme_config, payout_materials)
    
    if scheme_sales.empty:
        print(f"     ℹ️ No payout product sales in scheme period")
        tracker_df[volume_col] = 0.0
        tracker_df[value_col] = 0.0
        return tracker_df
    
    # Aggregate by credit_account using vectorized operations
    payout_actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    payout_actuals['credit_account'] = payout_actuals['credit_account'].astype(str)
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    
    # Create mapping for efficient vectorized assignment
    volume_mapping = dict(zip(payout_actuals['credit_account'], payout_actuals['volume']))
    value_mapping = dict(zip(payout_actuals['credit_account'], payout_actuals['value']))
    
    # Vectorized assignment
    tracker_df[volume_col] = tracker_df['credit_account'].map(volume_mapping).fillna(0.0).astype(float)
    tracker_df[value_col] = tracker_df['credit_account'].map(value_mapping).fillna(0.0).astype(float)
    
    accounts_with_data = (tracker_df[volume_col] > 0).sum()
    print(f"     ✅ {volume_col}: {accounts_with_data} accounts with payout volume > 0")
    
    return tracker_df


def _fix_mandatory_product_growth(tracker_df: pd.DataFrame, structured_data: Dict,
                                 scheme_type: str, suffix: str, scheme_index: Optional[int],
                                 is_volume_based: bool) -> pd.DataFrame:
    """
    Fix Mandatory_Product_Growth — normalize to percentage using slab configuration
    Source: scheme JSON slab configuration, normalize to percentage format
    """
    
    growth_col = f'Mandatory_Product_Growth{suffix}'
    
    # Get slabs for this scheme
    slabs_df = structured_data.get('slabs', pd.DataFrame())
    
    if slabs_df.empty:
        print(f"     ⚠️ No slabs found - setting {growth_col} to 0")
        tracker_df[growth_col] = 0.0
        return tracker_df
    
    # Filter slabs for this specific scheme
    if scheme_type == 'main_scheme':
        scheme_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
    else:
        scheme_slabs = slabs_df[
            (slabs_df['scheme_type'] == 'additional_scheme') &
            (slabs_df['scheme_name'].str.contains(f'Additional Scheme {scheme_index}', na=False))
        ].copy()
    
    if scheme_slabs.empty:
        print(f"     ⚠️ No slabs found for {scheme_type} - setting {growth_col} to 0")
        tracker_df[growth_col] = 0.0
        return tracker_df
    
    # Sort slabs by slab_start for proper lookup
    scheme_slabs = scheme_slabs.sort_values('slab_start')
    
    # Determine base column for slab lookup
    if is_volume_based:
        base_column = f'total_volume{suffix}' if suffix else 'total_volume'
    else:
        base_column = f'total_value{suffix}' if suffix else 'total_value'
    
    if base_column not in tracker_df.columns:
        print(f"     ⚠️ Base column {base_column} not found - setting {growth_col} to 0")
        tracker_df[growth_col] = 0.0
        return tracker_df
    
    # Initialize growth column
    tracker_df[growth_col] = 0.0
    
    # Get first slab's mandatory product growth as fallback
    first_slab_growth = 0.0
    if not scheme_slabs.empty and 'mandatory_product_growth_percent' in scheme_slabs.columns:
        # Get first slab's growth value (raw from DB)
        first_slab_growth = float(scheme_slabs.iloc[0]['mandatory_product_growth_percent'])
        print(f"     📋 First slab mandatory growth (fallback): {first_slab_growth}%")
    
    # Apply vectorized slab lookup for mandatory product growth
    base_values = tracker_df[base_column].values
    
    for _, slab_row in scheme_slabs.iterrows():
        slab_start = float(slab_row['slab_start'])
        slab_end = float(slab_row['slab_end']) if pd.notna(slab_row['slab_end']) else float('inf')
        
        # Get mandatory product growth from slab - convert from raw DB value to percentage
        # e.g., 5000 in DB becomes 50%
        mp_growth_raw = float(slab_row.get('mandatory_product_growth_percent', 0.0))
        mp_growth = mp_growth_raw / 100.0  # Convert from raw DB value to percentage
        
        # Create mask for values in this slab range
        mask = (base_values >= slab_start) & (base_values <= slab_end)
        
        if mask.sum() > 0:
            # Store as proper percentage value (e.g., 50.0 for 50%)
            tracker_df.loc[mask, growth_col] = mp_growth
            print(f"       📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts, growth: {mp_growth}% (from raw DB: {mp_growth_raw})")
    
    # Apply fallback for accounts without slab match
    unmatched_mask = (tracker_df[growth_col] == 0.0)
    unmatched_count = unmatched_mask.sum()
    
    if unmatched_count > 0 and first_slab_growth > 0:
        # Convert first slab growth from raw DB value to percentage
        first_slab_growth_pct = first_slab_growth / 100.0
        tracker_df.loc[unmatched_mask, growth_col] = first_slab_growth_pct
        print(f"     🔧 Applied fallback growth ({first_slab_growth_pct}%) to {unmatched_count} unmatched accounts (from raw DB: {first_slab_growth})")
    
    return tracker_df


def _calculate_mandatory_growth_targets(tracker_df: pd.DataFrame, scheme_type: str, suffix: str) -> pd.DataFrame:
    """
    🔧 NEW: Calculate Mandatory_Product_Growth_Target_Volume and Value using user's exact formulas
    
    Main Scheme: Mandatory_Product_Growth_Target_Volume = IF(Mandatory_Product_Growth > 0, ((1 + Mandatory_Product_Growth) * Mandatory_Product_Base_Volume), 0)
    Additional Scheme: Mandatory_Product_Growth_Target_Volume_pN = IF(Mandatory_Product_Growth_pN > 0, ((1 + Mandatory_Product_Growth_pN) * Mandatory_Product_Base_Volume_pN), 0)
    """
    
    print(f"     🧮 Calculating mandatory growth targets for {scheme_type}")
    
    # Define column names
    growth_col = f'Mandatory_Product_Growth{suffix}'
    base_volume_col = f'Mandatory_Product_Base_Volume{suffix}'
    base_value_col = f'Mandatory_Product_Base_Value{suffix}'
    target_volume_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
    target_value_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
    
    # Check if required columns exist
    if growth_col not in tracker_df.columns:
        print(f"     ⚠️ Growth column {growth_col} not found - skipping growth target calculations")
        return tracker_df
    
    # Calculate Growth Target Volume
    if base_volume_col in tracker_df.columns:
        # Clean data types
        tracker_df[growth_col] = pd.to_numeric(tracker_df[growth_col], errors='coerce').fillna(0.0).astype(float)
        tracker_df[base_volume_col] = pd.to_numeric(tracker_df[base_volume_col], errors='coerce').fillna(0.0).astype(float)
        
        # Apply formula: IF(Growth > 0, (1 + Growth) * Base_Volume, 0)
        import numpy as np
        growth_values = tracker_df[growth_col].values
        base_volume_values = tracker_df[base_volume_col].values
        
        tracker_df[target_volume_col] = np.where(
            growth_values > 0,
            (1 + growth_values) * base_volume_values,
            0.0
        ).astype(float)
        
        # Count successful calculations
        non_zero_count = (tracker_df[target_volume_col] > 0).sum()
        print(f"       ✅ {target_volume_col}: {non_zero_count} accounts with calculated targets")
        
        # 🔍 DEBUG: Check for problematic accounts
        problematic_accounts = ['3082993', '3157676', '3213927', '3223653', '3335246', '3429591', '453004', '799417', '821044', '833761']
        for acc in problematic_accounts:
            if str(acc) in tracker_df['credit_account'].astype(str).values:
                mask = tracker_df['credit_account'].astype(str) == str(acc)
                if mask.any():
                    idx = tracker_df[mask].index[0]
                    growth_val = growth_values[idx]
                    base_val = base_volume_values[idx]
                    target_val = tracker_df.loc[idx, target_volume_col]
                    expected = (1 + growth_val) * base_val if growth_val > 0 else 0
                    print(f"       🔍 Account {acc}: Growth={growth_val:.4f}, Base={base_val:.2f}, Target={target_val:.2f}, Expected={expected:.2f}")
    
    # Calculate Growth Target Value
    if base_value_col in tracker_df.columns:
        # Clean data types
        tracker_df[base_value_col] = pd.to_numeric(tracker_df[base_value_col], errors='coerce').fillna(0.0).astype(float)
        
        # Apply formula: IF(Growth > 0, (1 + Growth) * Base_Value, 0)
        growth_values = tracker_df[growth_col].values
        base_value_values = tracker_df[base_value_col].values
        
        tracker_df[target_value_col] = np.where(
            growth_values > 0,
            (1 + growth_values) * base_value_values,
            0.0
        ).astype(float)
        
        # Count successful calculations
        non_zero_count = (tracker_df[target_value_col] > 0).sum()
        print(f"       ✅ {target_value_col}: {non_zero_count} accounts with calculated targets")
    
    return tracker_df


def _fix_mp_final_target(tracker_df: pd.DataFrame, structured_data: Dict,
                        scheme_type: str, suffix: str, scheme_index: Optional[int],
                        is_volume_based: bool) -> pd.DataFrame:
    """
    🔧 FIXED: MP_FINAL_TARGET with correct MAX formula
    
    If Scheme Type = "volume":
        MP_FINAL_TARGET = MAX(Mandatory_Product_Fixed_Target, 
                             (Mandatory_Product_pct_to_Actual_Target_Volume + Mandatory_Product_Growth_Target_Volume))
    
    If Scheme Type = "value":
        MP_FINAL_TARGET = MAX(Mandatory_Product_Fixed_Target, 
                             (Mandatory_Product_pct_to_Actual_Target_Value + Mandatory_Product_Growth_Target_Value))
    """
    
    mp_final_target_col = f'MP_FINAL_TARGET{suffix}'
    
    # Initialize fixed target column
    fixed_target_col = f'Mandatory_Product_Fixed_Target{suffix}'
    if fixed_target_col not in tracker_df.columns:
        tracker_df[fixed_target_col] = 0.0
    else:
        tracker_df[fixed_target_col] = pd.to_numeric(tracker_df[fixed_target_col], errors='coerce').fillna(0.0).astype(float)
    
    # Define target component columns based on scheme type
    if is_volume_based:
        pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Volume{suffix}'
        growth_target_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
        print(f"     📊 Volume scheme MP_FINAL_TARGET: MAX(Fixed, pct+growth)")
    else:
        pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Value{suffix}'
        growth_target_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
        print(f"     📊 Value scheme MP_FINAL_TARGET: MAX(Fixed, pct+growth)")
    
    # Initialize target component columns
    if pct_target_col not in tracker_df.columns:
        tracker_df[pct_target_col] = 0.0
    else:
        tracker_df[pct_target_col] = pd.to_numeric(tracker_df[pct_target_col], errors='coerce').fillna(0.0).astype(float)
    
    # 🔧 CRITICAL FIX: Do not overwrite already calculated growth target values
    # The growth target calculation happens in mandatory_product_calculations.py and should not be overwritten
    if growth_target_col not in tracker_df.columns:
        tracker_df[growth_target_col] = 0.0
        print(f"       ➕ Initialized {growth_target_col} column with zeros")
    else:
        # Only clean data types but preserve calculated values - preserve ALL existing values including zeros
        # This preserves the correctly calculated growth target values from mandatory_product_calculations.py
        original_values = tracker_df[growth_target_col].copy()
        tracker_df[growth_target_col] = pd.to_numeric(tracker_df[growth_target_col], errors='coerce')
        # Fill NaN values with original values where possible, otherwise 0.0
        tracker_df[growth_target_col] = tracker_df[growth_target_col].fillna(original_values).fillna(0.0).astype(float)
        non_zero_count = (tracker_df[growth_target_col] > 0).sum()
        print(f"       ✅ Preserved {growth_target_col} column with {non_zero_count} calculated values")
    
    # 🔧 FIXED: Calculate MP_FINAL_TARGET using MAX formula
    # MP_FINAL_TARGET = MAX(Fixed_Target, (pct_Target + Growth_Target))
    import numpy as np
    combined_target = tracker_df[pct_target_col] + tracker_df[growth_target_col]
    tracker_df[mp_final_target_col] = np.maximum(
        tracker_df[fixed_target_col],
        combined_target
    ).astype(float)
    
    # Debug specific problematic accounts in _fix_mp_final_target
    problematic_accounts = ['261622', '3082993']
    for acc in problematic_accounts:
        acc_data = tracker_df[tracker_df['credit_account'].astype(str) == acc]
        if len(acc_data) > 0:
            row = acc_data.iloc[0]
            target = row.get(mp_final_target_col, 0)
            fixed = row.get(fixed_target_col, 0)
            growth = row.get(growth_target_col, 0)
            pct = row.get(pct_target_col, 0)
            combined = pct + growth
            expected = max(fixed, combined)
            print(f"     🔍 DEBUG _fix_mp_final_target Account {acc}: Fixed={fixed:.2f}, Growth={growth:.2f}, Pct={pct:.2f}, Combined={combined:.2f}, Expected={expected:.2f}, Got={target:.2f}")
    
    # Report summary
    accounts_with_target = (tracker_df[mp_final_target_col] > 0).sum()
    max_target = tracker_df[mp_final_target_col].max()
    avg_fixed = tracker_df[fixed_target_col].mean()
    avg_combined = combined_target.mean()
    print(f"     ✅ {mp_final_target_col}: {accounts_with_target} accounts with target > 0, max: {max_target:.2f}")
    print(f"     📊 Components - Avg Fixed: {avg_fixed:.2f}, Avg Combined(pct+growth): {avg_combined:.2f}")
    
    return tracker_df


def _get_scheme_info(structured_data: Dict, scheme_type: str, scheme_index: Optional[int]) -> Dict:
    """Get scheme information from structured data"""
    scheme_info_df = structured_data.get('scheme_info', pd.DataFrame())
    
    if scheme_info_df.empty:
        return {'volume_value_based': 'volume'}  # Default fallback
    
    if scheme_type == 'main_scheme':
        main_scheme_info = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
        if not main_scheme_info.empty:
            return main_scheme_info.iloc[0].to_dict()
    else:
        # For additional schemes
        additional_scheme_info = scheme_info_df[scheme_info_df['scheme_type'] == 'additional_scheme']
        if not additional_scheme_info.empty and scheme_index is not None:
            if len(additional_scheme_info) >= scheme_index:
                return additional_scheme_info.iloc[scheme_index - 1].to_dict()
    
    return {'volume_value_based': 'volume'}  # Default fallback


def _get_additional_schemes_info(structured_data: Dict) -> Dict[int, Dict]:
    """Get information about all additional schemes"""
    scheme_info_df = structured_data.get('scheme_info', pd.DataFrame())
    additional_schemes = {}
    
    if not scheme_info_df.empty:
        additional_info = scheme_info_df[scheme_info_df['scheme_type'] == 'additional_scheme']
        for index, row in additional_info.iterrows():
            scheme_index = len(additional_schemes) + 1  # 1-based indexing
            additional_schemes[scheme_index] = row.to_dict()
    
    return additional_schemes


def _get_mandatory_products_materials(structured_data: Dict, scheme_type: str, 
                                     scheme_index: Optional[int]) -> set:
    """Get mandatory product materials for a specific scheme"""
    products_df = structured_data.get('products', pd.DataFrame())
    
    if products_df.empty:
        return set()
    
    if scheme_type == 'main_scheme':
        mandatory_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') &
            (products_df['is_mandatory_product'] == True)
        ]
    else:
        mandatory_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') &
            (products_df['scheme_name'] == f'Additional Scheme {scheme_index}') &
            (products_df['is_mandatory_product'] == True)
        ]
    
    return set(mandatory_products['material_code'].tolist())


def _get_payout_products_materials(structured_data: Dict, scheme_type: str, 
                                  scheme_index: Optional[int]) -> set:
    """Get payout product materials for a specific scheme"""
    products_df = structured_data.get('products', pd.DataFrame())
    
    if products_df.empty:
        return set()
    
    if scheme_type == 'main_scheme':
        payout_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') &
            (products_df['is_payout_product'] == True)
        ]
    else:
        payout_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') &
            (products_df['scheme_name'] == f'Additional Scheme {scheme_index}') &
            (products_df['is_payout_product'] == True)
        ]
    
    return set(payout_products['material_code'].tolist())


def _filter_scheme_period_sales(sales_df: pd.DataFrame, scheme_config: Dict, materials: set) -> pd.DataFrame:
    """Filter sales data for scheme period and specific materials"""
    if sales_df.empty or not materials:
        return pd.DataFrame()
    
    # Get scheme period dates
    scheme_from = pd.to_datetime(scheme_config.get('scheme_from'))
    scheme_to = pd.to_datetime(scheme_config.get('scheme_to'))
    
    if pd.isna(scheme_from) or pd.isna(scheme_to):
        return pd.DataFrame()
    
    # Filter sales data
    sales_temp = sales_df.copy()
    
    # Ensure sale_date is datetime
    if 'sale_date' in sales_temp.columns:
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        
        # Filter by period and materials
        filtered_sales = sales_temp[
            (sales_temp['sale_date'] >= scheme_from) &
            (sales_temp['sale_date'] <= scheme_to) &
            (sales_temp['material'].isin(materials))
        ].copy()
        
        # Handle nulls as 0, keep negative values
        filtered_sales['volume'] = pd.to_numeric(filtered_sales['volume'], errors='coerce').fillna(0.0)
        filtered_sales['value'] = pd.to_numeric(filtered_sales['value'], errors='coerce').fillna(0.0)
        
        return filtered_sales
    
    return pd.DataFrame()


def _calculate_mp_final_payout(tracker_df: pd.DataFrame, structured_data: Dict, scheme_type: str, suffix: str, scheme_index: Optional[int]) -> pd.DataFrame:
    """
    Calculate MP_Final_Payout after MP_FINAL_ACHIEVEMENT_pct is calculated
    
    Formulas:
    - Main Scheme (Volume): MP_Final_Payout = IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Volume*Mandatory_Product_Rebate), 0)
    - Main Scheme (Value): MP_Final_Payout = IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Value*MP_Rebate_percent), 0)
    - Additional Scheme (Volume): MP_Final_Payout_pN = IF(MP_FINAL_ACHIEVEMENT_pct_pN>=100%, (Mandatory_Product_Actual_Volume_pN*Mandatory_Product_Rebate_pN), 0)
    - Additional Scheme (Value): MP_Final_Payout_pN = IF(MP_FINAL_ACHIEVEMENT_pct_pN>=100%, (Mandatory_Product_Actual_Value_pN*MP_Rebate_percent_pN), 0)
    """
    
    print(f"     🔍 DEBUG: Starting MP_Final_Payout calculation for {scheme_type}")
    
    # Determine if scheme is volume or value based from the tracker DataFrame
    scheme_type_col = 'Scheme Type' if suffix == '' else f'Scheme Type{suffix}'
    if scheme_type_col in tracker_df.columns:
        # Get the scheme type from the first row (all rows should have the same scheme type)
        actual_scheme_type = tracker_df[scheme_type_col].iloc[0]
        is_volume_based = actual_scheme_type.lower() == 'volume'
        print(f"     📋 Scheme type: {actual_scheme_type} ({'Volume' if is_volume_based else 'Value'} based)")
    else:
        # Fallback to scheme info if column not found
        scheme_info = _get_scheme_info(structured_data, scheme_type, scheme_index)
        is_volume_based = scheme_info.get('volume_value_based', 'volume').lower() == 'volume'
        print(f"     📋 Scheme type: {'Volume' if is_volume_based else 'Value'} based (from scheme info)")
    
    # Initialize the final payout column if it doesn't exist
    if f'MP_Final_Payout{suffix}' not in tracker_df.columns:
        tracker_df[f'MP_Final_Payout{suffix}'] = 0.0
    
    # Apply the new user formulas
    if f'MP_FINAL_ACHIEVEMENT_pct{suffix}' in tracker_df.columns:
        achievement_mask = tracker_df[f'MP_FINAL_ACHIEVEMENT_pct{suffix}'] >= 100.0  # 100% (achievement is already in percentage format)
        print(f"       🔍 DEBUG: Found {achievement_mask.sum()} accounts with achievement >= 100%")
        
        if is_volume_based:
            # Volume-based: MP_Final_Payout = IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Volume*Mandatory_Product_Rebate), 0)
            # Use Mandatory_Product_Rebate directly from slab data
            if (f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns and 
                f'Mandatory_Product_Rebate{suffix}' in tracker_df.columns):
                volume_payout = (
                    tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float) * 
                    tracker_df[f'Mandatory_Product_Rebate{suffix}'].astype(float)
                )
                tracker_df.loc[achievement_mask, f'MP_Final_Payout{suffix}'] = volume_payout.loc[achievement_mask]
                print(f"       ✅ Volume MP_Final_Payout: IF(achievement>=100%, volume*Mandatory_Product_Rebate, 0)")
                
                # Debug: Check what values were set
                accounts_with_payout = (tracker_df[f'MP_Final_Payout{suffix}'] > 0).sum()
                print(f"       🔍 DEBUG: {accounts_with_payout} accounts have MP_Final_Payout > 0")
                if accounts_with_payout > 0:
                    sample_payouts = tracker_df[tracker_df[f'MP_Final_Payout{suffix}'] > 0][['credit_account', f'MP_FINAL_ACHIEVEMENT_pct{suffix}', f'Mandatory_Product_Actual_Volume{suffix}', f'Mandatory_Product_Rebate{suffix}', f'MP_Final_Payout{suffix}']].head(3)
                    print(f"       🔍 DEBUG: Sample payouts:")
                    for _, row in sample_payouts.iterrows():
                        print(f"         Account {row['credit_account']}: Achievement={row[f'MP_FINAL_ACHIEVEMENT_pct{suffix}']:.2f}%, Volume={row[f'Mandatory_Product_Actual_Volume{suffix}']:.2f}, Rebate={row[f'Mandatory_Product_Rebate{suffix}']:.2f}, Payout={row[f'MP_Final_Payout{suffix}']:.2f}")
        else:
            # Value-based: MP_Final_Payout = IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Value*MP_Rebate_percent), 0)
            # NOTE: MP_Rebate_percent is already in percentage format (e.g., 1.5 means 1.5%)
            if (f'Mandatory_Product_Actual_Value{suffix}' in tracker_df.columns and 
                f'MP_Rebate_percent{suffix}' in tracker_df.columns):
                value_payout = (
                    tracker_df[f'Mandatory_Product_Actual_Value{suffix}'].astype(float) * 
                    tracker_df[f'MP_Rebate_percent{suffix}'].astype(float)  # MP_Rebate_percent is already in percentage format
                )
                # Divide by 100 as per user requirement
                value_payout = value_payout / 100.0
                tracker_df.loc[achievement_mask, f'MP_Final_Payout{suffix}'] = value_payout.loc[achievement_mask]
                print(f"       ✅ Value MP_Final_Payout: IF(achievement>=100%, (value*rebate_percent)/100, 0)")
                
                # Debug: Check what values were set
                accounts_with_payout = (tracker_df[f'MP_Final_Payout{suffix}'] > 0).sum()
                print(f"       🔍 DEBUG: {accounts_with_payout} accounts have MP_Final_Payout > 0")
                if accounts_with_payout > 0:
                    sample_payouts = tracker_df[tracker_df[f'MP_Final_Payout{suffix}'] > 0][['credit_account', f'MP_FINAL_ACHIEVEMENT_pct{suffix}', f'Mandatory_Product_Actual_Value{suffix}', f'MP_Rebate_percent{suffix}', f'MP_Final_Payout{suffix}']].head(3)
                    print(f"       🔍 DEBUG: Sample payouts:")
                    for _, row in sample_payouts.iterrows():
                        print(f"         Account {row['credit_account']}: Achievement={row[f'MP_FINAL_ACHIEVEMENT_pct{suffix}']:.2f}%, Value={row[f'Mandatory_Product_Actual_Value{suffix}']:.2f}, Rebate%={row[f'MP_Rebate_percent{suffix}']:.2f}%, Payout={row[f'MP_Final_Payout{suffix}']:.2f}")
                else:
                    print(f"       ⚠️ DEBUG: No accounts have MP_Final_Payout > 0 after calculation")
                    # Check a specific account
                    test_account = 105616
                    test_mask = tracker_df['credit_account'] == test_account
                    if test_mask.any():
                        test_row = tracker_df[test_mask].iloc[0]
                        achievement = test_row[f'MP_FINAL_ACHIEVEMENT_pct{suffix}']
                        actual_value = test_row[f'Mandatory_Product_Actual_Value{suffix}']
                        rebate_percent = test_row[f'MP_Rebate_percent{suffix}']
                        payout = test_row[f'MP_Final_Payout{suffix}']
                        print(f"       🔍 DEBUG: Test account {test_account}: Achievement={achievement:.2f}%, Value={actual_value:.2f}, Rebate%={rebate_percent:.2f}%, Payout={payout:.2f}")
                        print(f"       🔍 DEBUG: Expected payout: {actual_value:.2f} * {rebate_percent:.2f} = {actual_value * rebate_percent:.2f}")
                        print(f"       🔍 DEBUG: Achievement >= 100%: {achievement >= 100}")
                        print(f"       🔍 DEBUG: Achievement >= 1.0: {achievement >= 1.0}")
    else:
        print(f"       ⚠️ DEBUG: MP_FINAL_ACHIEVEMENT_pct{suffix} column not found - cannot calculate MP_Final_Payout")
    
    return tracker_df