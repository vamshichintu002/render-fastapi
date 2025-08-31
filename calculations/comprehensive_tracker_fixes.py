"""
Comprehensive Tracker Fixes Module
Addresses three main issues:
1. Ensure required base volume/value columns are present for both main and additional schemes
2. Fix data misalignment in scheme metadata columns 
3. Add Scheme Category column from scheme_subtype in JSON
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def apply_comprehensive_tracker_fixes(tracker_df: pd.DataFrame, structured_data: Dict, 
                                    json_data: Dict, scheme_config: Dict) -> pd.DataFrame:
    """
    Apply all comprehensive tracker fixes
    
    Args:
        tracker_df: Main tracker DataFrame
        structured_data: Structured JSON data
        json_data: Raw JSON data
        scheme_config: Scheme configuration
    
    Returns:
        Fixed tracker DataFrame
    """
    print("🔧 Applying comprehensive tracker fixes...")
    
    # Task 1: Ensure required base columns are present
    print("📊 Task 1: Adding/fixing required base columns...")
    tracker_df = _fix_base_columns(tracker_df, structured_data, json_data)
    
    # Task 3: Add Scheme Category column (before Task 2 to avoid alignment issues)
    print("📋 Task 3: Adding Scheme Category column...")
    tracker_df = _add_scheme_category_column(tracker_df, json_data, structured_data)
    
    # Task 2: Fix data misalignment in scheme metadata columns
    print("🔄 Task 2: Fixing data misalignment in scheme columns...")
    tracker_df = _fix_scheme_data_alignment(tracker_df, structured_data, json_data)
    
    print("✅ All comprehensive tracker fixes applied successfully!")
    return tracker_df


def _fix_base_columns(tracker_df: pd.DataFrame, structured_data: Dict, json_data: Dict) -> pd.DataFrame:
    """
    Task 1: Ensure all required base volume/value columns are present for both volume and value schemes
    """
    
    required_main_columns = [
        'Base 1 Volume Final', 'Base 2 Volume Final', 'total_volume',
        'Base 1 Value Final', 'Base 2 Value Final', 'total_value'
    ]
    
    # Add main scheme base columns if missing
    for col in required_main_columns:
        if col not in tracker_df.columns:
            print(f"   ➕ Adding missing main column: {col}")
            tracker_df[col] = 0.0
        else:
            # Ensure float type and fill nulls
            tracker_df[col] = pd.to_numeric(tracker_df[col], errors='coerce').fillna(0.0).astype(float)
    
    # Add additional scheme base columns if additional schemes exist
    if json_data and 'additionalSchemes' in json_data:
        additional_schemes = json_data['additionalSchemes']
        
        for i, scheme in enumerate(additional_schemes, 1):
            suffix = f'_p{i}'
            
            required_additional_columns = [
                f'Base 1 Volume Final{suffix}', f'Base 2 Volume Final{suffix}', f'total_volume{suffix}',
                f'Base 1 Value Final{suffix}', f'Base 2 Value Final{suffix}', f'total_value{suffix}'
            ]
            
            for col in required_additional_columns:
                if col not in tracker_df.columns:
                    print(f"   ➕ Adding missing additional column: {col}")
                    tracker_df[col] = 0.0
                else:
                    # Check if column has valid calculated values (not all zeros)
                    non_zero_count = (pd.to_numeric(tracker_df[col], errors='coerce') != 0).sum()
                    
                    if non_zero_count == 0:
                        # Only copy from main scheme if no valid calculated values exist
                        main_col = col.replace(suffix, '')
                        if main_col in tracker_df.columns:
                            print(f"   🔄 No calculated values found for {col}, copying from {main_col}")
                            tracker_df[col] = tracker_df[main_col].copy()
                        else:
                            print(f"   ⚠️ Column {col} missing and no main column {main_col} to copy from")
                            tracker_df[col] = 0.0
                    else:
                        print(f"   ✅ Preserving calculated values for {col} ({non_zero_count} non-zero values)")
                    
                    # Ensure float type and fill nulls
                    tracker_df[col] = pd.to_numeric(tracker_df[col], errors='coerce').fillna(0.0).astype(float)
    
    print("   ✅ Required base columns ensured for all schemes")
    return tracker_df


def _add_scheme_category_column(tracker_df: pd.DataFrame, json_data: Dict, structured_data: Dict) -> pd.DataFrame:
    """
    Task 3: Add Scheme Category column with value from scheme_subtype in JSON structure data
    """
    
    if not json_data:
        print("   ⚠️ No JSON data available for scheme category")
        return tracker_df
    
    # Try to extract scheme category from JSON structure dynamically
    scheme_category = None
    
    # Primary: Check in basicInfo for schemeType (most reliable - dynamic extraction)
    basic_info = json_data.get('basicInfo', {})
    if basic_info:
        # Try multiple possible fields in basicInfo for scheme category
        scheme_category = (basic_info.get('schemeType') or 
                          basic_info.get('schemeCategory') or
                          basic_info.get('category') or
                          basic_info.get('type'))
        
        # Clean up the scheme type if it exists (remove dashes, capitalize)
        if scheme_category:
            # Convert from 'ho-scheme' to 'HO Scheme' format for better readability
            scheme_category = scheme_category.replace('-', ' ').title()
    
    # Fallback: Check in products data structure for scheme_subtype
    if not scheme_category:
        products_data = structured_data.get('products', pd.DataFrame())
        if not products_data.empty and 'scheme_subtype' in products_data.columns:
            # Get the first non-null scheme_subtype value
            scheme_subtype_values = products_data['scheme_subtype'].dropna()
            if not scheme_subtype_values.empty:
                scheme_category = scheme_subtype_values.iloc[0]
    
    # Fallback: Check in mainScheme
    if not scheme_category:
        main_scheme = json_data.get('mainScheme', {})
        scheme_category = main_scheme.get('schemeType', main_scheme.get('schemeCategory', ''))
    
    # Final fallback
    if not scheme_category:
        scheme_category = 'HO Scheme'  # Default value with proper formatting
    
    print(f"   📋 Scheme category extraction:")
    print(f"       - Found in basicInfo: {basic_info}")
    print(f"       - Extracted scheme category: '{scheme_category}'")
    
    # Add the Scheme Category column at the beginning (after Scheme ID)
    if 'Scheme Category' not in tracker_df.columns:
        # Insert after Scheme ID if it exists, otherwise at the beginning
        insert_pos = 1 if 'Scheme ID' in tracker_df.columns else 0
        tracker_df.insert(insert_pos, 'Scheme Category', scheme_category)
        print(f"   ✅ Added 'Scheme Category' column with value '{scheme_category}' for all {len(tracker_df)} rows")
    else:
        # Fill existing column
        tracker_df['Scheme Category'] = scheme_category
        print(f"   ✅ Updated 'Scheme Category' column with value '{scheme_category}' for all {len(tracker_df)} rows")
    
    return tracker_df


def _fix_scheme_data_alignment(tracker_df: pd.DataFrame, structured_data: Dict, json_data: Dict) -> pd.DataFrame:
    """
    Task 2: Fix data misalignment issues in columns like 'PU Bonanza', 'Mandatory Qualify_pn', 'Scheme Type_pn'
    where first row has incorrect data from next row onwards correct data is coming
    """
    
    # Get scheme info for correct values
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    
    if scheme_info.empty:
        print("   ⚠️ No scheme info available for alignment fix")
        # If no scheme info, use data from majority of rows to fix misalignment
        return _fix_alignment_from_majority_data(tracker_df)
    
    # Fix main scheme columns
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    if not main_scheme_info.empty:
        main_info = main_scheme_info.iloc[0]
        
        main_columns_to_fix = {
            'Scheme Name': main_info.get('scheme_name', ''),
            'Scheme Type': main_info.get('volume_value_based', '').title(),
            'Mandatory Qualify': 'Yes' if main_info.get('mandatory_qualify', '').lower() == 'yes' else 'No'
        }
        
        for col_name, correct_value in main_columns_to_fix.items():
            if col_name in tracker_df.columns and correct_value:
                # Set all rows to the correct value
                tracker_df[col_name] = correct_value
                print(f"   🔄 Fixed main scheme '{col_name}' to '{correct_value}' for all rows")
    
    # Fix additional scheme columns
    additional_schemes_info = scheme_info[scheme_info['scheme_type'].str.contains('additional_scheme', na=False)]
    
    if not additional_schemes_info.empty and json_data and 'additionalSchemes' in json_data:
        additional_schemes_json = json_data['additionalSchemes']
        
        for i, (_, scheme_row) in enumerate(additional_schemes_info.iterrows(), 1):
            suffix = f'_p{i}'
            
            # Get scheme name from JSON (schemeNumber or schemeTitle)
            scheme_name = ''
            if i <= len(additional_schemes_json):
                json_scheme = additional_schemes_json[i-1]
                scheme_name = json_scheme.get('schemeNumber', json_scheme.get('schemeTitle', f'Additional Scheme {i}'))
            
            if not scheme_name:
                scheme_name = scheme_row.get('scheme_name', f'Additional Scheme {i}')
            
            additional_columns_to_fix = {
                f'Scheme Name{suffix}': scheme_name,
                f'Scheme Type{suffix}': scheme_row.get('volume_value_based', '').title(),
                f'Mandatory Qualify{suffix}': 'Yes' if scheme_row.get('mandatory_qualify', '').lower() == 'yes' else 'No'
            }
            
            for col_name, correct_value in additional_columns_to_fix.items():
                if col_name in tracker_df.columns and correct_value:
                    # Set all rows to the correct value
                    tracker_df[col_name] = correct_value
                    print(f"   🔄 Fixed additional scheme '{col_name}' to '{correct_value}' for all rows")
    else:
        # If no additional scheme info, fix from majority data
        tracker_df = _fix_alignment_from_majority_data(tracker_df)
    
    print("   ✅ Data alignment fixes applied")
    return tracker_df


def _fix_alignment_from_majority_data(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix data misalignment by using the majority value in each column
    """
    
    # Columns that typically have alignment issues
    columns_to_check = [
        'Scheme Name_p1', 'Scheme Type_p1', 'Mandatory Qualify_p1',
        'Scheme Name_p2', 'Scheme Type_p2', 'Mandatory Qualify_p2'
    ]
    
    for col in columns_to_check:
        if col in tracker_df.columns:
            # Get the most common value (excluding the first row if it's different)
            values = tracker_df[col].dropna()
            if len(values) > 1:
                # Check if first row is different from majority
                majority_value = values.iloc[1:].mode().iloc[0] if len(values.iloc[1:]) > 0 else values.iloc[0]
                
                # Special handling for known issues
                if 'Mandatory Qualify' in col and majority_value.lower() in ['yes', 'no']:
                    majority_value = 'Yes' if majority_value.lower() == 'yes' else 'No'
                elif 'Scheme Type' in col and majority_value.lower() == 'value':
                    majority_value = 'Value'  # Ensure proper capitalization
                
                # Set all values to the majority value
                tracker_df[col] = majority_value
                print(f"   🔄 Fixed alignment for '{col}' to '{majority_value}' for all rows")
    
    return tracker_df


def get_comprehensive_column_order(tracker_df: pd.DataFrame, json_data: Dict) -> List[str]:
    """
    Get the comprehensive column order including all required base columns and scheme category
    """
    
    all_cols = list(tracker_df.columns)
    
    # Updated main scheme order with Scheme Category and all required base columns
    main_order = [
        'Scheme ID', 'Scheme Category', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 
        'Scheme Period To', 'Mandatory Qualify', 'credit_account', 'state_name', 'so_name', 
        'region', 'customer_name',
        'Base 1 Volume Final', 'Base 1 Value Final', 'Base 2 Volume Final', 'Base 2 Value Final',
        'total_volume', 'total_value', 'growth_rate', 'target_volume', 'target_value', 
        'actual_volume', 'actual_value', 'percentage_achieved',
        # Estimated calculation columns
        'ASP', 'qualification_rate', 'Estimated_Qualifiers', 'Estimated_Volume', 'Estimated_Value',
        'Est.basic_payout', 'Est.additional_payout', 'Est.total_payout', 'Est.MP_Final_Payout',
        'Est.Phasing_Volume_1', 'Est.Phasing_Volume_2', 'Est.Phasing_Volume_3',
        'Est.Phasing_Value_1', 'Est.Phasing_Value_2', 'Est.Phasing_Value_3',
        'Est.Phasing_Payout_1', 'Est.Phasing_Payout_2', 'Est.Phasing_Payout_3',
        'Est.FINAL_PHASING_PAYOUT', 'Est.Final_Scheme_Payout'
    ]
    
    # Start with main scheme columns
    ordered_cols = []
    for col in main_order:
        if col in all_cols:
            ordered_cols.append(col)
    
    # Add additional scheme columns in perfect order
    if json_data and 'additionalSchemes' in json_data:
        additional_schemes = json_data['additionalSchemes']
        
        for i in range(1, len(additional_schemes) + 1):
            prefix = f'_p{i}'
            
            # Perfect order for additional scheme with all required base columns
            additional_order = [
                f'Scheme Name{prefix}', f'Scheme Type{prefix}', f'Mandatory Qualify{prefix}',
                f'Base 1 Volume Final{prefix}', f'Base 1 Value Final{prefix}', 
                f'Base 2 Volume Final{prefix}', f'Base 2 Value Final{prefix}',
                f'total_volume{prefix}', f'total_value{prefix}', f'growth_rate{prefix}', 
                f'target_volume{prefix}', f'target_value{prefix}', f'actual_volume{prefix}', 
                f'actual_value{prefix}', f'percentage_achieved{prefix}',
                # Estimated calculation columns for additional schemes
                f'ASP{prefix}', f'qualification_rate{prefix}', f'Estimated_Volume{prefix}', f'Estimated_Value{prefix}',
                f'Est.basic_payout{prefix}', f'Est.additional_payout{prefix}', f'Est.total_Payout{prefix}', f'Est.MP_Final_Payout{prefix}'
            ]
            
            for col in additional_order:
                if col in all_cols:
                    ordered_cols.append(col)
    
    # Add any remaining columns (except final columns)
    final_columns = ['Scheme Final Payout', 'Rewards']
    for col in all_cols:
        if col not in ordered_cols and col not in final_columns:
            ordered_cols.append(col)
    
    # Add final columns at the end
    for col in final_columns:
        if col in all_cols:
            ordered_cols.append(col)
    
    return ordered_cols