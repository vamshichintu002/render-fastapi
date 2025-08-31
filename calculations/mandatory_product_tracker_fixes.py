"""
Mandatory Product Tracker Fixes Module
Handles two main tasks for mandatory product fields in tracker:
1. Apply first slab values for empty cells in specific percentage-based fields
2. Fetch data from JSON structured data for specific columns and apply first slab fallback
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def apply_mandatory_product_tracker_fixes(tracker_df: pd.DataFrame, structured_data: Dict, 
                                        json_data: Dict, scheme_config: Dict) -> pd.DataFrame:
    """
    Apply mandatory product tracker fixes for both main and additional schemes
    
    Args:
        tracker_df: Main tracker DataFrame
        structured_data: Structured JSON data containing slabs, products, etc.
        json_data: Raw JSON data
        scheme_config: Scheme configuration
    
    Returns:
        Fixed tracker DataFrame with mandatory product fixes applied
    """
    print("🔧 Applying mandatory product tracker fixes...")
    
    # Task 1: Apply first slab values for empty cells in percentage-based fields
    print("📊 Task 1: Applying first slab values for empty percentage fields...")
    tracker_df = _apply_first_slab_values_for_empty_cells(tracker_df, structured_data, json_data)
    
    # Task 2: Fetch JSON data for specific columns and apply first slab fallback
    print("📋 Task 2: Fetching JSON data for mandatory product columns...")
    tracker_df = _fetch_json_data_with_first_slab_fallback(tracker_df, structured_data, json_data)
    
    print("✅ All mandatory product tracker fixes applied successfully!")
    return tracker_df


def _apply_first_slab_values_for_empty_cells(tracker_df: pd.DataFrame, structured_data: Dict, 
                                           json_data: Dict) -> pd.DataFrame:
    """
    Task 1: Apply respective scheme first slab values for empty cells in:
    - Mandatory_Product_pct_Target_to_Actual_Sales_pn
    - MP_Rebate_percent_pn
    - Mandatory_Product_Rebate_pn
    """
    
    # Get all scheme types from structured data
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    slabs_df = structured_data.get('slabs', pd.DataFrame())
    
    if scheme_info.empty or slabs_df.empty:
        print("   ⚠️ No scheme info or slabs data available")
        return tracker_df
    
    # Process main scheme
    _apply_first_slab_for_main_scheme(tracker_df, slabs_df, json_data, '')
    
    # Process additional schemes
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📋 Processing additional scheme {scheme_num}...")
        _apply_first_slab_for_additional_scheme(tracker_df, slabs_df, json_data, suffix, scheme_num)
    
    return tracker_df


def _apply_first_slab_for_main_scheme(tracker_df: pd.DataFrame, slabs_df: pd.DataFrame, 
                                    json_data: Dict, suffix: str) -> None:
    """Apply first slab values for main scheme mandatory product fields"""
    
    print("   📊 Applying first slab values for main scheme...")
    
    # Get main scheme slabs
    main_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
    if main_slabs.empty:
        print("   ⚠️ No main scheme slabs found")
        return
    
    # Sort slabs and get first slab values
    main_slabs_sorted = main_slabs.sort_values('slab_start')
    first_slab = main_slabs_sorted.iloc[0]
    
    # Get first slab values from JSON data for more accurate extraction
    first_slab_values = _extract_first_slab_values_from_json(json_data, 'main_scheme')
    
    # Apply first slab values for empty cells in specified fields
    fields_to_fix = [
        'Mandatory_Product_pct_Target_to_Actual_Sales',
        'MP_Rebate_percent', 
        'Mandatory_Product_Rebate'
    ]
    
    for field in fields_to_fix:
        column_name = f'{field}{suffix}'
        if column_name in tracker_df.columns:
            # Count empty/zero cells before fix
            empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
            empty_count = empty_mask.sum()
            
            if empty_count > 0:
                # Apply appropriate first slab value
                if field == 'Mandatory_Product_pct_Target_to_Actual_Sales':
                    first_slab_value = first_slab_values.get('mandatoryProductTargetToActual', 10.0)  # Default 10%
                elif field == 'MP_Rebate_percent':
                    first_slab_value = first_slab_values.get('mandatoryProductRebatePercent', 2.0)  # Default 2%
                elif field == 'Mandatory_Product_Rebate':
                    # For rebate amount, calculate based on rebate percent and actual value
                    rebate_percent = first_slab_values.get('mandatoryProductRebatePercent', 2.0)
                    actual_value_col = f'Mandatory_Product_Actual_Value{suffix}'
                    if actual_value_col in tracker_df.columns:
                        first_slab_value = (rebate_percent * tracker_df[actual_value_col]) / 100.0
                    else:
                        first_slab_value = 0.0
                else:
                    first_slab_value = 0.0
                
                # Apply the first slab value to empty cells
                if field == 'Mandatory_Product_Rebate':
                    # Special handling for rebate calculation
                    tracker_df.loc[empty_mask, column_name] = first_slab_value.loc[empty_mask] if hasattr(first_slab_value, 'loc') else first_slab_value
                else:
                    tracker_df.loc[empty_mask, column_name] = first_slab_value
                
                print(f"     ✅ Applied first slab value {first_slab_value} to {empty_count} empty cells in {column_name}")
            else:
                print(f"     ℹ️ No empty cells found in {column_name}")
        else:
            print(f"     ⚠️ Column {column_name} not found in tracker")


def _apply_first_slab_for_additional_scheme(tracker_df: pd.DataFrame, slabs_df: pd.DataFrame, 
                                          json_data: Dict, suffix: str, scheme_num: str) -> None:
    """Apply first slab values for additional scheme mandatory product fields"""
    
    print(f"   📊 Applying first slab values for additional scheme {scheme_num}...")
    
    # Get additional scheme slabs
    additional_slabs = slabs_df[slabs_df['scheme_type'] == 'additional_scheme'].copy()
    if additional_slabs.empty:
        print(f"   ⚠️ No additional scheme slabs found for scheme {scheme_num}")
        return
    
    # Filter for specific scheme
    scheme_slabs = additional_slabs[
        additional_slabs['scheme_name'].str.contains(f'Additional Scheme {scheme_num}', na=False)
    ].copy()
    
    if scheme_slabs.empty:
        print(f"   ⚠️ No slabs found for additional scheme {scheme_num}")
        return
    
    # Sort slabs and get first slab values
    scheme_slabs_sorted = scheme_slabs.sort_values('slab_start')
    first_slab = scheme_slabs_sorted.iloc[0]
    
    # Get first slab values from JSON data
    first_slab_values = _extract_first_slab_values_from_json(json_data, f'additional_scheme_{scheme_num}')
    
    # Apply first slab values for empty cells in specified fields
    fields_to_fix = [
        'Mandatory_Product_pct_Target_to_Actual_Sales',
        'MP_Rebate_percent',
        'Mandatory_Product_Rebate'
    ]
    
    for field in fields_to_fix:
        column_name = f'{field}{suffix}'
        if column_name in tracker_df.columns:
            # Count empty/zero cells before fix
            empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
            empty_count = empty_mask.sum()
            
            if empty_count > 0:
                # Apply appropriate first slab value
                if field == 'Mandatory_Product_pct_Target_to_Actual_Sales':
                    first_slab_value = first_slab_values.get('mandatoryProductTargetToActual', 10.0)  # Default 10%
                elif field == 'MP_Rebate_percent':
                    first_slab_value = first_slab_values.get('mandatoryProductRebatePercent', 2.0)  # Default 2%
                elif field == 'Mandatory_Product_Rebate':
                    # For rebate amount, calculate based on rebate percent and actual value
                    rebate_percent = first_slab_values.get('mandatoryProductRebatePercent', 2.0)
                    actual_value_col = f'Mandatory_Product_Actual_Value{suffix}'
                    if actual_value_col in tracker_df.columns:
                        first_slab_value = (rebate_percent * tracker_df[actual_value_col]) / 100.0
                    else:
                        first_slab_value = 0.0
                else:
                    first_slab_value = 0.0
                
                # Apply the first slab value to empty cells
                if field == 'Mandatory_Product_Rebate':
                    # Special handling for rebate calculation
                    tracker_df.loc[empty_mask, column_name] = first_slab_value.loc[empty_mask] if hasattr(first_slab_value, 'loc') else first_slab_value
                else:
                    tracker_df.loc[empty_mask, column_name] = first_slab_value
                
                print(f"     ✅ Applied first slab value {first_slab_value} to {empty_count} empty cells in {column_name}")
            else:
                print(f"     ℹ️ No empty cells found in {column_name}")
        else:
            print(f"     ⚠️ Column {column_name} not found in tracker")


def _fetch_json_data_with_first_slab_fallback(tracker_df: pd.DataFrame, structured_data: Dict, 
                                            json_data: Dict) -> pd.DataFrame:
    """
    Task 2: Fetch data from JSON structured data for specific columns and apply first slab fallback:
    - Mandatory_Min_Shades_PPI_pn from JSON "mandatoryMinShadesPPI"
    - Mandatory_Product_Fixed_Target_pn from JSON "mandatoryProductTarget"
    """
    
    # Get all scheme types from structured data
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    slabs_df = structured_data.get('slabs', pd.DataFrame())
    
    if scheme_info.empty:
        print("   ⚠️ No scheme info available")
        return tracker_df
    
    # Process main scheme
    _fetch_json_data_for_main_scheme(tracker_df, structured_data, json_data, slabs_df, '')
    
    # Process additional schemes
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📋 Processing JSON data for additional scheme {scheme_num}...")
        _fetch_json_data_for_additional_scheme(tracker_df, structured_data, json_data, slabs_df, suffix, scheme_num)
    
    return tracker_df


def _fetch_json_data_for_main_scheme(tracker_df: pd.DataFrame, structured_data: Dict, 
                                   json_data: Dict, slabs_df: pd.DataFrame, suffix: str) -> None:
    """Fetch JSON data for main scheme mandatory product fields"""
    
    print("   📊 Fetching JSON data for main scheme...")
    
    if not json_data:
        print("   ⚠️ No JSON data available")
        return
    
    # Get main scheme data from JSON
    main_scheme = json_data.get('mainScheme', {})
    scheme_data = main_scheme.get('schemeData', {})
    
    # Get first slab values as fallback
    first_slab_values = _extract_first_slab_values_from_json(json_data, 'main_scheme')
    
    # Fetch and apply Mandatory_Min_Shades_PPI
    column_name = f'Mandatory_Min_Shades_PPI{suffix}'
    if column_name not in tracker_df.columns:
        tracker_df[column_name] = 0.0
    
    # Try to get value from JSON scheme data
    mandatory_min_shades = scheme_data.get('mandatoryMinShadesPPI', '')
    if mandatory_min_shades and str(mandatory_min_shades).strip():
        try:
            min_shades_value = float(mandatory_min_shades)
            tracker_df[column_name] = min_shades_value
            print(f"     ✅ Set {column_name} from JSON: {min_shades_value}")
        except (ValueError, TypeError):
            # Use first slab fallback
            fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)  # Default 5 shades
            tracker_df[column_name] = fallback_value
            print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    else:
        # Use first slab fallback
        fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)  # Default 5 shades
        tracker_df[column_name] = fallback_value
        print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    
    # Ensure no empty cells
    empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
    if empty_mask.sum() > 0:
        fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)
        tracker_df.loc[empty_mask, column_name] = fallback_value
        print(f"     🔧 Applied fallback to {empty_mask.sum()} empty cells in {column_name}")
    
    # Fetch and apply Mandatory_Product_Fixed_Target
    column_name = f'Mandatory_Product_Fixed_Target{suffix}'
    if column_name not in tracker_df.columns:
        tracker_df[column_name] = 0.0
    
    # Try to get value from JSON scheme data
    mandatory_fixed_target = scheme_data.get('mandatoryProductTarget', '')
    if mandatory_fixed_target and str(mandatory_fixed_target).strip():
        try:
            fixed_target_value = float(mandatory_fixed_target)
            tracker_df[column_name] = fixed_target_value
            print(f"     ✅ Set {column_name} from JSON: {fixed_target_value}")
        except (ValueError, TypeError):
            # Use first slab fallback
            fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)  # Default target
            tracker_df[column_name] = fallback_value
            print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    else:
        # Use first slab fallback
        fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)  # Default target
        tracker_df[column_name] = fallback_value
        print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    
    # Ensure no empty cells
    empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
    if empty_mask.sum() > 0:
        fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)
        tracker_df.loc[empty_mask, column_name] = fallback_value
        print(f"     🔧 Applied fallback to {empty_mask.sum()} empty cells in {column_name}")


def _fetch_json_data_for_additional_scheme(tracker_df: pd.DataFrame, structured_data: Dict, 
                                         json_data: Dict, slabs_df: pd.DataFrame, 
                                         suffix: str, scheme_num: str) -> None:
    """Fetch JSON data for additional scheme mandatory product fields"""
    
    print(f"   📊 Fetching JSON data for additional scheme {scheme_num}...")
    
    if not json_data:
        print("   ⚠️ No JSON data available")
        return
    
    # Get additional scheme data from JSON
    additional_schemes = json_data.get('additionalSchemes', [])
    if not isinstance(additional_schemes, list) or len(additional_schemes) < int(scheme_num):
        print(f"   ⚠️ Additional scheme {scheme_num} not found in JSON data")
        return
    
    additional_scheme = additional_schemes[int(scheme_num) - 1]
    scheme_data = additional_scheme.get('schemeData', {})
    
    # Get first slab values as fallback
    first_slab_values = _extract_first_slab_values_from_json(json_data, f'additional_scheme_{scheme_num}')
    
    # Fetch and apply Mandatory_Min_Shades_PPI
    column_name = f'Mandatory_Min_Shades_PPI{suffix}'
    if column_name not in tracker_df.columns:
        tracker_df[column_name] = 0.0
    
    # Try to get value from JSON scheme data
    mandatory_min_shades = scheme_data.get('mandatoryMinShadesPPI', '')
    if mandatory_min_shades and str(mandatory_min_shades).strip():
        try:
            min_shades_value = float(mandatory_min_shades)
            tracker_df[column_name] = min_shades_value
            print(f"     ✅ Set {column_name} from JSON: {min_shades_value}")
        except (ValueError, TypeError):
            # Use first slab fallback
            fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)  # Default 5 shades
            tracker_df[column_name] = fallback_value
            print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    else:
        # Use first slab fallback
        fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)  # Default 5 shades
        tracker_df[column_name] = fallback_value
        print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    
    # Ensure no empty cells
    empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
    if empty_mask.sum() > 0:
        fallback_value = first_slab_values.get('mandatoryMinShadesPPI', 5.0)
        tracker_df.loc[empty_mask, column_name] = fallback_value
        print(f"     🔧 Applied fallback to {empty_mask.sum()} empty cells in {column_name}")
    
    # Fetch and apply Mandatory_Product_Fixed_Target
    column_name = f'Mandatory_Product_Fixed_Target{suffix}'
    if column_name not in tracker_df.columns:
        tracker_df[column_name] = 0.0
    
    # Try to get value from JSON scheme data
    mandatory_fixed_target = scheme_data.get('mandatoryProductTarget', '')
    if mandatory_fixed_target and str(mandatory_fixed_target).strip():
        try:
            fixed_target_value = float(mandatory_fixed_target)
            tracker_df[column_name] = fixed_target_value
            print(f"     ✅ Set {column_name} from JSON: {fixed_target_value}")
        except (ValueError, TypeError):
            # Use first slab fallback
            fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)  # Default target
            tracker_df[column_name] = fallback_value
            print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    else:
        # Use first slab fallback
        fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)  # Default target
        tracker_df[column_name] = fallback_value
        print(f"     🔄 Used first slab fallback for {column_name}: {fallback_value}")
    
    # Ensure no empty cells
    empty_mask = (tracker_df[column_name].isna()) | (tracker_df[column_name] == 0.0)
    if empty_mask.sum() > 0:
        fallback_value = first_slab_values.get('mandatoryProductTarget', 1000.0)
        tracker_df.loc[empty_mask, column_name] = fallback_value
        print(f"     🔧 Applied fallback to {empty_mask.sum()} empty cells in {column_name}")


def _extract_first_slab_values_from_json(json_data: Dict, scheme_type: str) -> Dict[str, float]:
    """
    Extract first slab values from JSON data for fallback purposes
    
    Args:
        json_data: Raw JSON scheme data
        scheme_type: Type of scheme ('main_scheme' or 'additional_scheme_N')
    
    Returns:
        Dictionary with first slab values
    """
    
    first_slab_values = {}
    
    try:
        if scheme_type == 'main_scheme':
            # Get from mainScheme.slabData.slabs
            main_scheme = json_data.get('mainScheme', {})
            slab_data = main_scheme.get('slabData', {})
            slabs = slab_data.get('slabs', [])
        else:
            # For additional schemes, extract scheme number
            scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
            additional_schemes = json_data.get('additionalSchemes', [])
            
            if isinstance(additional_schemes, list) and len(additional_schemes) >= int(scheme_num):
                additional_scheme = additional_schemes[int(scheme_num) - 1]
                slab_data = additional_scheme.get('slabData', {})
                main_scheme_slab = slab_data.get('mainScheme', {})
                slabs = main_scheme_slab.get('slabs', [])
            else:
                slabs = []
        
        if slabs:
            # Sort slabs by slabStart and get first slab
            sorted_slabs = sorted(slabs, key=lambda x: float(x.get('slabStart', 0)))
            first_slab = sorted_slabs[0]
            
            # Extract values from first slab
            first_slab_values['mandatoryProductTargetToActual'] = float(first_slab.get('mandatoryProductTargetToActual', 10.0))
            first_slab_values['mandatoryProductRebatePercent'] = float(first_slab.get('mandatoryProductRebatePercent', 2.0))
            first_slab_values['mandatoryMinShadesPPI'] = float(first_slab.get('mandatoryMinShadesPPI', 5.0))
            first_slab_values['mandatoryProductTarget'] = float(first_slab.get('mandatoryProductTarget', 1000.0))
            
            print(f"     📋 Extracted first slab values for {scheme_type}: {first_slab_values}")
        else:
            print(f"     ⚠️ No slabs found for {scheme_type}, using defaults")
            # Use default values
            first_slab_values = {
                'mandatoryProductTargetToActual': 10.0,
                'mandatoryProductRebatePercent': 2.0,
                'mandatoryMinShadesPPI': 5.0,
                'mandatoryProductTarget': 1000.0
            }
            
    except Exception as e:
        print(f"     ⚠️ Error extracting first slab values for {scheme_type}: {e}")
        # Use default values
        first_slab_values = {
            'mandatoryProductTargetToActual': 10.0,
            'mandatoryProductRebatePercent': 2.0,
            'mandatoryMinShadesPPI': 5.0,
            'mandatoryProductTarget': 1000.0
        }
    
    return first_slab_values