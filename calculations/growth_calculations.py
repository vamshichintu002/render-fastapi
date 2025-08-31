"""
Growth Rate and Strata Growth Calculations Module
Handles dynamic column generation for main scheme and all additional schemes
Uses vectorized pandas operations for maximum performance
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def add_growth_columns_vectorized(tracker_df, structured_data, json_data, strata_growth_df=None):
    """
    Add SINGLE growth column per scheme (either Strata Growth % OR growth_rate)
    Uses vectorized pandas operations for maximum performance
    
    Args:
        tracker_df: DataFrame with base calculations
        structured_data: Dict with extracted JSON data (slabs, scheme_info, etc.)
        json_data: Raw JSON data
        strata_growth_df: DataFrame from strata_growth table
    
    Returns:
        DataFrame with growth columns added
    """
    print("🚀 Adding growth columns with vectorization...")
    
    # Get scheme configuration
    main_scheme = json_data.get('mainScheme', {})
    additional_schemes = json_data.get('additionalSchemes', {})
    
    # Handle case where additionalSchemes might be a list instead of dict
    if isinstance(additional_schemes, list):
        # Convert list to dict with index as key
        additional_schemes = {str(i): scheme for i, scheme in enumerate(additional_schemes)}
    
    # Prepare strata growth lookup if available
    strata_lookup = {}
    if strata_growth_df is not None and not strata_growth_df.empty:
        strata_lookup = dict(zip(
            strata_growth_df['credit_account'].astype(str), 
            strata_growth_df['strata_growth_percentage'].astype(float)
        ))
    
    # Process main scheme growth
    tracker_df = _add_main_scheme_growth_vectorized(
        tracker_df, structured_data, main_scheme, strata_lookup
    )
    
    # Process additional schemes growth
    print(f"📊 Processing {len(additional_schemes)} additional schemes...")
    print(f"   Additional schemes type: {type(additional_schemes)}")
    
    if isinstance(additional_schemes, dict):
        print(f"   Dictionary keys: {list(additional_schemes.keys())}")
        for i, (scheme_id, scheme_data) in enumerate(additional_schemes.items(), 1):
            # Always use 1-based indexing for consistent column naming
            scheme_index = i
            
            print(f"   Processing additional scheme {scheme_index} (ID: {scheme_id})")
            tracker_df = _add_additional_scheme_growth_vectorized(
                tracker_df, scheme_data, scheme_index, strata_lookup, structured_data
            )
    elif isinstance(additional_schemes, list):
        print(f"   List with {len(additional_schemes)} items")
        for i, scheme_data in enumerate(additional_schemes, 1):
            print(f"   Processing additional scheme {i} (index {i-1})")
            tracker_df = _add_additional_scheme_growth_vectorized(
                tracker_df, scheme_data, i, strata_lookup, structured_data
            )
    else:
        print(f"⚠️  Unexpected additional_schemes format: {type(additional_schemes)}")
    
    print(f"✅ Growth columns added successfully!")
    
    # Print summary of growth rate assignments
    print(f"\n📊 GROWTH RATE SUMMARY:")
    growth_columns = [col for col in tracker_df.columns if 'growth_rate' in col or 'Strata Growth %' in col]
    for col in growth_columns:
        non_zero_count = (tracker_df[col] > 0).sum()
        total_count = len(tracker_df)
        print(f"   {col}: {non_zero_count}/{total_count} accounts have growth rates > 0")
    
    return tracker_df

def _add_main_scheme_growth_vectorized(tracker_df, structured_data, main_scheme_config, strata_lookup):
    """Add main scheme growth column using vectorization - ONLY ONE COLUMN"""
    
    # Check enableStrataGrowth for main scheme
    enable_strata_growth = main_scheme_config.get('slabData', {}).get('enableStrataGrowth', False)
    
    if enable_strata_growth and strata_lookup:
        print("📊 Main scheme: Using Strata Growth % from database...")
        # Use strata growth from database - ONLY this column
        tracker_df['Strata Growth %'] = tracker_df['credit_account'].astype(str).map(strata_lookup).fillna(0.0)
    else:
        print("📊 Main scheme: Using growth_rate from slabs...")
        # Use growth rate from slabs - ONLY this column
        
        # Get main scheme slabs
        main_slabs = structured_data['slabs']
        main_slabs_filtered = main_slabs[main_slabs['scheme_type'] == 'main_scheme'].copy()
        
        if main_slabs_filtered.empty:
            print("⚠️  No main scheme slabs found")
            tracker_df['growth_rate'] = 0.0
            return tracker_df
        
        # Sort slabs by slab_start for proper lookup
        main_slabs_filtered = main_slabs_filtered.sort_values('slab_start')
        
        # Get scheme info for volume/value determination
        scheme_info = structured_data['scheme_info']
        main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme'].iloc[0]
        
        volume_value_based = main_scheme_info['volume_value_based'].lower()
        if volume_value_based == 'volume':
            base_column = 'total_volume'
        else:
            base_column = 'total_value'
        
        # Check if column exists
        if base_column not in tracker_df.columns:
            print(f"⚠️  Column {base_column} not found! Available columns: {list(tracker_df.columns)}")
            tracker_df['growth_rate'] = 0.0
            return tracker_df
        
        # Vectorized slab lookup with ROUNDED values for proper slab matching
        base_values = np.round(tracker_df[base_column], 0)
        tracker_df['growth_rate'] = _vectorized_slab_lookup(
            base_values, main_slabs_filtered, 'growth_percent'
        ) / 100.0
    
    return tracker_df

def _add_additional_scheme_growth_vectorized(tracker_df, scheme_data, scheme_index, strata_lookup, structured_data):
    """Add additional scheme growth column using vectorization - ONLY ONE COLUMN"""
    
    prefix = f"_p{scheme_index}"
    
    # Check enableStrataGrowth for this additional scheme
    enable_strata_growth = scheme_data.get('slabData', {}).get('enableStrataGrowth', False)
    
    if enable_strata_growth and strata_lookup:
        print(f"📊 Additional scheme {scheme_index}: Using Strata Growth % from database...")
        # Use strata growth from database - ONLY this column
        tracker_df[f'Strata Growth %{prefix}'] = tracker_df['credit_account'].astype(str).map(strata_lookup).fillna(0.0)
    else:
        print(f"📊 Additional scheme {scheme_index}: Using growth_rate from slabs...")
        # Use growth rate from slabs - ONLY this column
        
        # Get additional scheme slabs - FIXED SLAB MATCHING LOGIC
        additional_slabs = structured_data['slabs']
        
        print(f"   🔍 Available slabs in structured_data:")
        print(f"      Total slabs: {len(additional_slabs)}")
        print(f"      Scheme types: {additional_slabs['scheme_type'].value_counts().to_dict()}")
        if 'scheme_id' in additional_slabs.columns:
            print(f"      Scheme IDs: {additional_slabs['scheme_id'].unique()}")
        if 'scheme_name' in additional_slabs.columns:
            print(f"      Scheme names: {additional_slabs['scheme_name'].unique()}")
        
        # First try to find slabs by scheme_type and scheme_name pattern
        scheme_slabs = additional_slabs[
            (additional_slabs['scheme_type'] == 'additional_scheme') & 
            (additional_slabs['scheme_name'].str.contains(f'Additional Scheme {scheme_index}', na=False))
        ].copy()
        
        # If that doesn't work, try to find by scheme_id from the additional scheme data
        if scheme_slabs.empty:
            scheme_id = scheme_data.get('schemeId', None)
            if scheme_id:
                scheme_slabs = additional_slabs[
                    additional_slabs['scheme_id'] == scheme_id
                ].copy()
        
        # If still empty, try to find by index in the slabs table
        if scheme_slabs.empty:
            # Get all additional scheme slabs and try to match by order
            all_additional_slabs = additional_slabs[
                additional_slabs['scheme_type'] == 'additional_scheme'
            ].copy()
            
            if not all_additional_slabs.empty:
                # Group by scheme_id and get the nth additional scheme
                unique_scheme_ids = all_additional_slabs['scheme_id'].unique()
                if len(unique_scheme_ids) >= scheme_index:
                    target_scheme_id = unique_scheme_ids[scheme_index - 1]
                    scheme_slabs = all_additional_slabs[
                        all_additional_slabs['scheme_id'] == target_scheme_id
                    ].copy()
        
        if scheme_slabs.empty:
            print(f"⚠️  No slabs found for additional scheme {scheme_index} (ID: {scheme_data.get('schemeId', 'unknown')})")
            print(f"   Available additional scheme slabs: {len(additional_slabs[additional_slabs['scheme_type'] == 'additional_scheme'])}")
            tracker_df[f'growth_rate{prefix}'] = 0.0
            return tracker_df
        
        # Sort slabs by slab_start
        scheme_slabs = scheme_slabs.sort_values('slab_start')
        print(f"📊 Found {len(scheme_slabs)} slabs for additional scheme {scheme_index}")
        print(f"   Slab ranges: {scheme_slabs[['slab_start', 'slab_end', 'growth_percent']].to_dict('records')}")
        
        # Get scheme base from structured data (more reliable than JSON)
        scheme_info = structured_data['scheme_info']
        additional_scheme_info = scheme_info[scheme_info['scheme_type'] == 'additional_scheme']
        
        scheme_base = 'volume'  # default fallback
        
        if not additional_scheme_info.empty:
            # Find the specific additional scheme by index (they are in order)
            if len(additional_scheme_info) >= scheme_index:
                scheme_info_row = additional_scheme_info.iloc[scheme_index - 1]  # 0-based index
                scheme_base = scheme_info_row['volume_value_based'].lower()
                print(f"   📊 Additional scheme {scheme_index} base from structured data: {scheme_base}")
            else:
                print(f"⚠️  Additional scheme {scheme_index} not found in structured data")
        else:
            print(f"⚠️  No additional scheme info in structured data")
        
        # Fallback to JSON data if needed
        if scheme_base in ['', 'unknown']:
            json_base = scheme_data.get('volumeValueBased', '').lower()
            if not json_base:
                json_base = scheme_data.get('schemeBase', 'volume').lower()
            scheme_base = json_base if json_base else 'volume'
            print(f"   📊 Additional scheme {scheme_index} base from JSON fallback: {scheme_base}")
        
        print(f"   📊 Additional scheme {scheme_index} final base: {scheme_base}")
        
        # Determine base column - additional schemes typically use their own totals
        if 'volume' in scheme_base.lower():
            base_column = f'total_volume{prefix}'
        else:
            base_column = f'total_value{prefix}'
        
        # Check if the base column exists, fallback to main scheme totals
        if base_column not in tracker_df.columns:
            print(f"⚠️  Column {base_column} not found, using main scheme total_value")
            base_column = 'total_value'
        
        if base_column in tracker_df.columns:
            # Vectorized slab lookup with ROUNDED values for proper slab matching
            base_values = np.round(tracker_df[base_column], 0)
            tracker_df[f'growth_rate{prefix}'] = _vectorized_slab_lookup(
                base_values, scheme_slabs, 'growth_percent'
            ) / 100.0
            print(f"✅ Growth rates calculated for additional scheme {scheme_index} using {base_column}")
        else:
            print(f"⚠️  Base column {base_column} not found for additional scheme {scheme_index}")
            tracker_df[f'growth_rate{prefix}'] = 0.0
    
    return tracker_df

def _vectorized_slab_lookup(values_series, slabs_df, rate_column):
    """
    Vectorized slab lookup using pandas operations
    Much faster than iterating through each row
    
    Args:
        values_series: pandas Series of values to lookup
        slabs_df: DataFrame with slab_start, slab_end, and rate columns
        rate_column: Column name for the rate (e.g., 'growth_percent')
    
    Returns:
        pandas Series of rates
    """
    if slabs_df.empty:
        return pd.Series(0.0, index=values_series.index)
    
    # Create a result series initialized with zeros
    result = pd.Series(0.0, index=values_series.index)
    
    # Sort slabs by slab_start to ensure proper precedence
    slabs_sorted = slabs_df.sort_values('slab_start')
    
    print(f"   🔍 Slab lookup: {len(slabs_sorted)} slabs, {len(values_series)} values")
    print(f"   📊 Value range: {values_series.min():.2f} to {values_series.max():.2f}")
    
    # Get first slab's growth rate as fallback
    first_slab_rate = float(slabs_sorted.iloc[0][rate_column]) if not slabs_sorted.empty and pd.notna(slabs_sorted.iloc[0][rate_column]) else 0.0
    print(f"   📋 First slab growth rate (fallback): {first_slab_rate}%")
    
    # Count accounts with base = 0 (new accounts with no base period data)
    zero_base_count = (values_series == 0.0).sum()
    if zero_base_count > 0:
        print(f"   🆕 Found {zero_base_count} accounts with base = 0 (new accounts)")
    
    # Vectorized lookup: for each slab, find values that fall within its range
    for _, slab in slabs_sorted.iterrows():
        slab_start = float(slab['slab_start'])
        slab_end = float(slab['slab_end']) if pd.notna(slab['slab_end']) and slab['slab_end'] != '' else float('inf')
        rate = float(slab[rate_column]) if pd.notna(slab[rate_column]) else 0.0
        
        # Vectorized condition: values >= slab_start AND values <= slab_end
        mask = (values_series >= slab_start) & (values_series <= slab_end)
        values_in_slab = values_series[mask]
        
        if not values_in_slab.empty:
            print(f"     📍 Slab {slab_start}-{slab_end}: {len(values_in_slab)} values, rate: {rate}%")
        
        result.loc[mask] = rate
    
    # ENHANCED FIX: Apply first slab growth rate to accounts with base = 0 AND any other unmatched accounts
    unmatched_mask = (result == 0.0)
    zero_base_mask = (values_series == 0.0)
    
    # Combine masks: accounts with base = 0 OR unmatched accounts should get first slab rate
    fallback_mask = unmatched_mask | zero_base_mask
    fallback_count = fallback_mask.sum()
    
    if fallback_count > 0 and first_slab_rate > 0:
        result.loc[fallback_mask] = first_slab_rate
        zero_accounts = (fallback_mask & zero_base_mask).sum()
        unmatched_accounts = (fallback_mask & ~zero_base_mask).sum()
        print(f"   🔧 Applied first slab rate ({first_slab_rate}%) to {fallback_count} accounts:")
        print(f"      - {zero_accounts} new accounts with base = 0")
        print(f"      - {unmatched_accounts} other unmatched accounts")
    
    # Count how many values got rates assigned
    non_zero_count = (result > 0).sum()
    print(f"   ✅ Assigned rates to {non_zero_count}/{len(values_series)} values")
    
    return result

def fetch_strata_growth_data():
    """
    Fetch strata growth data from database using MCP
    Returns DataFrame with credit_account and strata_growth_percentage
    """
    try:
        # Note: This will be handled by MCP calls from main.py
        # For now, return empty DataFrame and let MCP handle it
        print(f"📊 Strata growth data will be fetched via MCP...")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"⚠️  Error preparing strata growth fetch: {e}")
        return pd.DataFrame()

def get_enable_strata_growth_config(json_data):
    """
    Extract enableStrataGrowth configuration for main and additional schemes
    
    Returns:
        dict: {
            'main_scheme': bool,
            'additional_schemes': {scheme_id: bool}
        }
    """
    config = {
        'main_scheme': False,
        'additional_schemes': {}
    }
    
    # Main scheme configuration
    main_scheme = json_data.get('mainScheme', {})
    config['main_scheme'] = main_scheme.get('slabData', {}).get('enableStrataGrowth', False)
    
    # Additional schemes configuration
    additional_schemes = json_data.get('additionalSchemes', {})
    
    # Handle both dict and list formats
    if isinstance(additional_schemes, dict):
        for scheme_id, scheme_data in additional_schemes.items():
            config['additional_schemes'][scheme_id] = scheme_data.get('slabData', {}).get('enableStrataGrowth', False)
    elif isinstance(additional_schemes, list):
        for i, scheme_data in enumerate(additional_schemes):
            scheme_id = str(i + 1)
            config['additional_schemes'][scheme_id] = scheme_data.get('slabData', {}).get('enableStrataGrowth', False)
    
    return config

def calculate_growth_metrics_vectorized(tracker_df, structured_data, json_data, strata_growth_df=None):
    """
    Main function to calculate all growth metrics using vectorization
    Dynamically handles any number of additional schemes
    
    Args:
        tracker_df: DataFrame with base calculations
        structured_data: Extracted JSON data
        json_data: Raw JSON data
        strata_growth_df: Optional DataFrame with strata growth data
    
    Returns:
        DataFrame with growth columns added
    """
    print("🚀 CALCULATING GROWTH METRICS WITH VECTORIZATION")
    print("=" * 60)
    
    # Use provided strata growth data or fetch if not provided
    if strata_growth_df is None or strata_growth_df.empty:
        strata_growth_df = fetch_strata_growth_data()
    
    # Add growth columns
    tracker_df = add_growth_columns_vectorized(
        tracker_df, structured_data, json_data, strata_growth_df
    )
    
    # Get growth configuration summary
    growth_config = get_enable_strata_growth_config(json_data)
    
    print(f"📊 GROWTH CONFIGURATION:")
    print(f"   Main Scheme enableStrataGrowth: {growth_config['main_scheme']}")
    for scheme_id, enabled in growth_config['additional_schemes'].items():
        print(f"   Additional Scheme {scheme_id} enableStrataGrowth: {enabled}")
    
    print(f"✅ Growth calculations completed!")
    return tracker_df
