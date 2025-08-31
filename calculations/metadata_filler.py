"""
Metadata Filler Module
Handles forward filling of scheme metadata columns and region backfilling using vectorized operations
"""

import pandas as pd
import numpy as np
from typing import Dict, List


def fill_scheme_metadata_columns(tracker_df: pd.DataFrame, structured_data: Dict, sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null/empty values in scheme metadata columns using vectorized operations
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing scheme info
        sales_df: Sales data for region backfilling
    
    Returns:
        Updated tracker DataFrame with filled metadata columns
    """
    print("📋 Filling scheme metadata columns...")
    
    # Step 1: Forward fill main scheme metadata columns
    tracker_df = _fill_main_scheme_metadata(tracker_df, structured_data)
    
    # Step 2: Forward fill additional scheme metadata columns
    tracker_df = _fill_additional_scheme_metadata(tracker_df, structured_data)
    
    # Step 3: Backfill region column from sales data
    tracker_df = _fill_region_column(tracker_df, sales_df)
    
    print("✅ Scheme metadata columns filled successfully")
    return tracker_df


def _fill_main_scheme_metadata(tracker_df: pd.DataFrame, structured_data: Dict) -> pd.DataFrame:
    """Fill main scheme metadata columns using vectorized operations"""
    
    print("   📊 Filling main scheme metadata...")
    
    # Get main scheme info
    scheme_info = structured_data['scheme_info']
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    
    if main_scheme_info.empty:
        print("   ⚠️ No main scheme info found")
        return tracker_df
    
    main_info = main_scheme_info.iloc[0]
    
    # Define main scheme metadata columns and their values
    main_metadata = {
        'Scheme ID': main_info.get('scheme_id', 'main'),
        'Scheme Name': main_info.get('scheme_name', ''),
        'Scheme Type': main_info.get('volume_value_based', ''),
        'Scheme Period From': '',  # Will be filled from scheme config if available
        'Scheme Period To': '',    # Will be filled from scheme config if available
        'Mandatory Qualify': main_info.get('mandatory_qualify', '')
    }
    
    # Fill each column using vectorized operations
    for col_name, fill_value in main_metadata.items():
        if col_name in tracker_df.columns:
            # Use forward fill first, then fill remaining nulls with the correct value
            tracker_df[col_name] = tracker_df[col_name].ffill()
            tracker_df[col_name] = tracker_df[col_name].fillna(fill_value)
            
            # Also fill empty strings
            if tracker_df[col_name].dtype == 'object':
                tracker_df[col_name] = tracker_df[col_name].replace('', fill_value)
            
            filled_count = len(tracker_df) - tracker_df[col_name].isnull().sum()
            print(f"     ✅ {col_name}: {filled_count}/{len(tracker_df)} rows filled")
        else:
            print(f"     ⚠️ {col_name}: Column not found")
    
    return tracker_df


def _fill_additional_scheme_metadata(tracker_df: pd.DataFrame, structured_data: Dict) -> pd.DataFrame:
    """Fill additional scheme metadata columns using vectorized operations"""
    
    print("   📊 Filling additional scheme metadata...")
    
    # Get additional scheme info
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.contains('additional_scheme', na=False)]
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found")
        return tracker_df
    
    # Process each additional scheme
    for _, scheme_row in additional_schemes.iterrows():
        # Determine the scheme suffix (e.g., _p1, _p2)
        scheme_id = scheme_row.get('scheme_id', '')
        scheme_name = scheme_row.get('scheme_name', '')
        
        # Try to find the suffix based on existing columns
        suffix = None
        for col in tracker_df.columns:
            if 'Scheme Name_p' in col:
                suffix = col.replace('Scheme Name', '')
                break
        
        if not suffix:
            suffix = '_p1'  # Default fallback
        
        # Define additional scheme metadata columns and their values
        additional_metadata = {
            f'Scheme Name{suffix}': scheme_name,
            f'Scheme Type{suffix}': scheme_row.get('volume_value_based', ''),
            f'Mandatory Qualify{suffix}': scheme_row.get('mandatory_qualify', '')
        }
        
        # Fill each column using vectorized operations
        for col_name, fill_value in additional_metadata.items():
            if col_name in tracker_df.columns:
                # Use forward fill first, then fill remaining nulls with the correct value
                tracker_df[col_name] = tracker_df[col_name].ffill()
                tracker_df[col_name] = tracker_df[col_name].fillna(fill_value)
                
                # Also fill empty strings
                if tracker_df[col_name].dtype == 'object':
                    tracker_df[col_name] = tracker_df[col_name].replace('', fill_value)
                
                filled_count = len(tracker_df) - tracker_df[col_name].isnull().sum()
                print(f"     ✅ {col_name}: {filled_count}/{len(tracker_df)} rows filled")
            else:
                print(f"     ⚠️ {col_name}: Column not found")
    
    return tracker_df


def _fill_region_column(tracker_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
    """Fill region column by mapping from sales data using vectorized operations"""
    
    print("   🌍 Filling region column from sales data...")
    
    if 'region' not in tracker_df.columns:
        print("     ⚠️ Region column not found in tracker")
        return tracker_df
    
    if 'region_name' not in sales_df.columns:
        print("     ⚠️ region_name column not found in sales data")
        return tracker_df
    
    # Create a mapping of credit_account to region using the most frequent region per account
    # This handles cases where an account might appear in multiple regions
    sales_temp = sales_df.copy()
    sales_temp['credit_account'] = sales_temp['credit_account'].astype(str)
    
    # Get the most frequent region for each credit account
    account_region_mapping = (
        sales_temp.groupby('credit_account')['region_name']
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
        .to_dict()
    )
    
    print(f"     📊 Created region mapping for {len(account_region_mapping)} accounts")
    
    # Convert credit_account to string for mapping
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    
    # Method 1: Forward fill existing region values first
    tracker_df['region'] = tracker_df['region'].ffill()
    
    # Method 2: Map regions from sales data for remaining null values
    mask_null_region = tracker_df['region'].isnull() | (tracker_df['region'] == '')
    
    if mask_null_region.any():
        # Use vectorized mapping
        tracker_df.loc[mask_null_region, 'region'] = tracker_df.loc[mask_null_region, 'credit_account'].map(
            account_region_mapping
        )
    
    # Method 3: Backward fill any remaining nulls within the same credit account
    tracker_df['region'] = tracker_df.groupby('credit_account')['region'].bfill()
    tracker_df['region'] = tracker_df.groupby('credit_account')['region'].ffill()
    
    # Final check and summary
    null_count = tracker_df['region'].isnull().sum()
    filled_count = len(tracker_df) - null_count
    
    print(f"     ✅ Region: {filled_count}/{len(tracker_df)} rows filled")
    
    if null_count > 0:
        print(f"     ⚠️ {null_count} rows still have null regions (accounts not in sales data)")
        # Fill remaining nulls with a default value
        tracker_df['region'] = tracker_df['region'].fillna('Unknown')
        print(f"     ✅ Filled remaining nulls with 'Unknown'")
    
    return tracker_df


def validate_metadata_completeness(tracker_df: pd.DataFrame) -> Dict[str, int]:
    """
    Validate that all metadata columns are properly filled
    
    Returns:
        Dictionary with column names and their null counts
    """
    print("🔍 Validating metadata completeness...")
    
    # Define expected metadata columns
    main_scheme_cols = ['Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To', 'Mandatory Qualify', 'region']
    
    # Find additional scheme columns
    additional_cols = []
    for col in tracker_df.columns:
        if any(base in col for base in ['Scheme Name_p', 'Scheme Type_p', 'Mandatory Qualify_p']):
            additional_cols.append(col)
    
    all_metadata_cols = main_scheme_cols + additional_cols
    
    validation_results = {}
    
    for col in all_metadata_cols:
        if col in tracker_df.columns:
            null_count = tracker_df[col].isnull().sum()
            empty_count = (tracker_df[col] == '').sum() if tracker_df[col].dtype == 'object' else 0
            total_issues = null_count + empty_count
            
            validation_results[col] = total_issues
            
            if total_issues == 0:
                print(f"   ✅ {col}: Complete")
            else:
                print(f"   ❌ {col}: {total_issues} issues")
        else:
            print(f"   ⚠️ {col}: Column not found")
    
    return validation_results
