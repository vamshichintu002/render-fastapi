"""
Target Calculations Module
Implements target volume/value calculations for main and additional schemes
Uses growth rates, strata growth, and slab thresholds with vectorized operations
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def calculate_targets_and_actuals_vectorized(tracker_df, structured_data, json_data, sales_df, scheme_config=None):
    """
    Calculate target and actual values for all schemes using vectorization
    
    Args:
        tracker_df: DataFrame with base calculations and growth rates
        structured_data: Extracted JSON data (slabs, products, etc.)
        json_data: Raw JSON data
        sales_df: Sales data for actual calculations
        scheme_config: Scheme configuration with dates
    
    Returns:
        DataFrame with target and actual columns added
    """
    print("🎯 CALCULATING TARGETS AND ACTUALS WITH VECTORIZATION")
    print("=" * 60)
    
    # Get scheme configuration
    main_scheme = json_data.get('mainScheme', {})
    additional_schemes = json_data.get('additionalSchemes', {})
    
    # Handle list format
    if isinstance(additional_schemes, list):
        additional_schemes = {str(i): scheme for i, scheme in enumerate(additional_schemes)}
    
    # Process main scheme targets and actuals
    tracker_df = _calculate_main_scheme_targets_vectorized(
        tracker_df, structured_data, main_scheme, sales_df, scheme_config
    )
    
    # Process ALL additional schemes in parallel
    print(f"🎯 Processing {len(additional_schemes)} additional schemes for targets...")
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i, (scheme_id, scheme_data) in enumerate(additional_schemes.items(), 1):
            future = executor.submit(
                _calculate_additional_scheme_targets_vectorized,
                tracker_df, structured_data, scheme_data, i, sales_df, scheme_config
            )
            futures.append((i, future))
        
        # Collect results in order
        for scheme_index, future in futures:
            tracker_df = future.result()
            print(f"   ✅ Additional scheme {scheme_index} targets completed")
    
    print("✅ Target and actual calculations completed!")
    return tracker_df

def _calculate_main_scheme_targets_vectorized(tracker_df, structured_data, main_scheme, sales_df, scheme_config=None):
    """Calculate main scheme targets and actuals using vectorization"""
    
    print("🎯 Main scheme: Calculating targets and actuals...")
    
    # Get scheme info
    scheme_info = structured_data['scheme_info']
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    
    if main_scheme_info.empty:
        print("⚠️  No main scheme info found")
        return tracker_df
    
    calculation_mode = main_scheme_info.iloc[0]['volume_value_based'].lower()
    
    # Get main scheme slabs for threshold logic
    slabs_df = structured_data['slabs']
    main_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
    main_slabs = main_slabs.sort_values('slab_start')
    
    # Step 1: Calculate base targets using growth rates
    print(f"   🧮 Calculating targets for main scheme using {calculation_mode} mode")
    
    if calculation_mode == 'volume':
        # Volume-based calculation - ONLY target_volume needed
        total_column = 'total_volume'
        
        # Base target = total_volume * (1 + growth_rate)
        if 'growth_rate' in tracker_df.columns:
            tracker_df['base_target_volume'] = tracker_df[total_column] * (1 + tracker_df['growth_rate'])
            print(f"      ✅ Base target volume calculated using {total_column} and growth_rate")
        else:
            tracker_df['base_target_volume'] = tracker_df[total_column]
            print(f"      ⚠️  No growth_rate column, using {total_column} directly")
        
        # Apply slab minimum thresholds
        tracker_df = _apply_slab_thresholds_vectorized(
            tracker_df, main_slabs, total_column, 'base_target_volume', 'target_volume'
        )
        
        # For volume schemes, target_value = 0 (not needed)
        tracker_df['target_value'] = 0.0
        
    else:
        # Value-based calculation - ONLY target_value needed
        total_column = 'total_value'
        
        # Base target = total_value * (1 + growth_rate)
        if 'growth_rate' in tracker_df.columns:
            tracker_df['base_target_value'] = tracker_df[total_column] * (1 + tracker_df['growth_rate'])
            print(f"      ✅ Base target value calculated using {total_column} and growth_rate")
        else:
            tracker_df['base_target_value'] = tracker_df[total_column]
            print(f"      ⚠️  No growth_rate column, using {total_column} directly")
        
        # Apply slab minimum thresholds
        tracker_df = _apply_slab_thresholds_vectorized(
            tracker_df, main_slabs, total_column, 'base_target_value', 'target_value'
        )
        
        # For value schemes, target_volume = 0 (not needed)
        tracker_df['target_volume'] = 0.0
    
    # Calculate actuals for main scheme
    tracker_df = _calculate_main_scheme_actuals_vectorized(
        tracker_df, structured_data, sales_df, scheme_config
    )
    
    # Calculate percentage achieved for main scheme
    tracker_df = _calculate_percentage_achieved_vectorized(
        tracker_df, calculation_mode, ""
    )
    
    # Calculate payout columns for main scheme (if enabled in configuration)
    config_manager = structured_data.get('config_manager')
    if config_manager and config_manager.should_calculate_main_payouts():
        try:
            from calculations.payout_calculations import _apply_slab_payout_calculations
            
            print(f"   🎯 Main scheme payouts: ENABLED - calculating...")
            # Get main scheme slabs
            slabs_df = structured_data['slabs']
            main_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
            
            if not main_slabs.empty:
                # Determine base column for payout calculations
                base_column = 'total_volume' if calculation_mode == 'volume' else 'total_value'
                
                if base_column in tracker_df.columns:
                    tracker_df = _apply_slab_payout_calculations(
                        tracker_df, main_slabs, base_column, '', 
                        structured_data, sales_df, scheme_config, 'main_scheme'
                    )
                else:
                    print(f"   ⚠️ Base column {base_column} not found for main scheme payout calculations")
            else:
                print(f"   ℹ️ No main scheme slabs found - skipping payout calculations")
        except Exception as e:
            print(f"   ❌ Error in main scheme payout calculations: {e}")
            # Continue without payout calculations
    else:
        print(f"   ⚡ Main scheme payouts: DISABLED - skipping for performance")
    
    # Calculate mandatory product columns for main scheme (if enabled in configuration)
    if config_manager and config_manager.should_calculate_main_mandatory_products():
        try:
            from calculations.mandatory_product_calculations import _add_main_scheme_mandatory_columns
            
            print(f"   🎯 Main scheme mandatory products: ENABLED - calculating...")
            print(f"   🔍 DEBUG: _add_main_scheme_mandatory_columns called from target_calculations.py line 169")
            tracker_df = _add_main_scheme_mandatory_columns(
                tracker_df, structured_data, sales_df, scheme_config
            )
        except Exception as e:
            print(f"   ❌ Error in main scheme mandatory product calculations: {e}")
            # Continue without mandatory product calculations
    else:
        print(f"   ⚡ Main scheme mandatory products: DISABLED - skipping for performance")
    
    # Ensure main scheme has all required columns
    main_required = ['target_volume', 'target_value', 'actual_volume', 'actual_value', 'percentage_achieved']
    for col in main_required:
        if col not in tracker_df.columns:
            tracker_df[col] = 0.0
    
    print(f"✅ Main scheme targets, actuals, and payouts calculated")
    return tracker_df

def _calculate_additional_scheme_targets_vectorized(tracker_df, structured_data, scheme_data, scheme_index, sales_df, scheme_config=None):
    """Calculate additional scheme targets and actuals using vectorization"""
    
    prefix = f"_p{scheme_index}"
    print(f"🎯 Additional scheme {scheme_index}: Calculating targets and actuals...")
    
    # ALWAYS add all required columns for this additional scheme
    required_columns = [
        f'target_volume{prefix}', f'target_value{prefix}',
        f'actual_volume{prefix}', f'actual_value{prefix}', 
        f'percentage_achieved{prefix}'
    ]
    
    # Initialize all columns
    for col in required_columns:
        if col not in tracker_df.columns:
            tracker_df[col] = 0.0
    
    # Get calculation mode from structured data first (more reliable)
    scheme_info = structured_data['scheme_info']
    additional_scheme_info = scheme_info[scheme_info['scheme_type'] == 'additional_scheme']
    
    calculation_mode = 'volume'  # default fallback
    
    if not additional_scheme_info.empty:
        # Find the specific additional scheme by index (they are in order)
        if len(additional_scheme_info) >= scheme_index:
            scheme_info_row = additional_scheme_info.iloc[scheme_index - 1]  # 0-based index
            calculation_mode = scheme_info_row['volume_value_based'].lower()
            print(f"   📊 Additional scheme {scheme_index} from structured data: {calculation_mode}")
        else:
            print(f"⚠️  Additional scheme {scheme_index} not found in structured data")
    else:
        print(f"⚠️  No additional scheme info in structured data")
    
    # Fallback to JSON data if needed
    if calculation_mode in ['', 'unknown']:
        json_mode = scheme_data.get('volumeValueBased', '').lower()
        if not json_mode:
            json_mode = scheme_data.get('schemeBase', 'volume').lower()
        calculation_mode = json_mode if json_mode else 'volume'
        print(f"   📊 Additional scheme {scheme_index} from JSON fallback: {calculation_mode}")
    
    print(f"   📊 Additional scheme {scheme_index} final calculation mode: {calculation_mode}")
    
    # Get additional scheme slabs
    slabs_df = structured_data['slabs']
    additional_slabs = slabs_df[slabs_df['scheme_type'] == 'additional_scheme'].copy()
    
    # Match slabs by index (since we process schemes in order)
    unique_scheme_ids = additional_slabs['scheme_id'].unique()
    if len(unique_scheme_ids) >= scheme_index:
        target_scheme_id = unique_scheme_ids[scheme_index - 1]
        scheme_slabs = additional_slabs[additional_slabs['scheme_id'] == target_scheme_id].copy()
        scheme_slabs = scheme_slabs.sort_values('slab_start')
    else:
        scheme_slabs = pd.DataFrame()
    
    # Calculate targets based on mode
    print(f"   🧮 Calculating targets for additional scheme {scheme_index} using {calculation_mode} mode")
    
    if calculation_mode == 'volume':
        # Volume-based calculation - ONLY target_volume needed
        total_column = f'total_volume{prefix}'
        
        if total_column in tracker_df.columns and f'growth_rate{prefix}' in tracker_df.columns:
            # Calculate target_volume directly without intermediate base_target_volume column
            temp_base_target = tracker_df[total_column] * (1 + tracker_df[f'growth_rate{prefix}'])
            print(f"      ✅ Base target volume calculated using {total_column} and growth_rate{prefix}")
        else:
            temp_base_target = 0.0
            print(f"      ⚠️  Missing columns: {total_column} or growth_rate{prefix}")
        
        # Apply slab thresholds directly to target_volume
        tracker_df[f'target_volume{prefix}'] = temp_base_target
        if not scheme_slabs.empty:
            tracker_df = _apply_slab_thresholds_vectorized(
                tracker_df, scheme_slabs, total_column, f'target_volume{prefix}', f'target_volume{prefix}'
            )
        
        # For volume schemes, target_value = 0 (not needed)
        tracker_df[f'target_value{prefix}'] = 0.0
            
    else:
        # Value-based calculation - ONLY target_value needed
        total_column = f'total_value{prefix}'
        
        if total_column in tracker_df.columns and f'growth_rate{prefix}' in tracker_df.columns:
            # Base target = total_value * (1 + growth_rate)
            tracker_df[f'base_target_value{prefix}'] = tracker_df[total_column] * (1 + tracker_df[f'growth_rate{prefix}'])
            print(f"      ✅ Base target value calculated using {total_column} and growth_rate{prefix}")
        else:
            tracker_df[f'base_target_value{prefix}'] = 0.0
            print(f"      ⚠️  Missing columns: {total_column} or growth_rate{prefix}")
        
        # Apply slab thresholds
        if not scheme_slabs.empty:
            tracker_df = _apply_slab_thresholds_vectorized(
                tracker_df, scheme_slabs, total_column, f'base_target_value{prefix}', f'target_value{prefix}'
            )
        else:
            tracker_df[f'target_value{prefix}'] = tracker_df[f'base_target_value{prefix}']
        
        # For value schemes, target_volume = 0 (not needed)
        tracker_df[f'target_volume{prefix}'] = 0.0
    
    # Calculate actuals for additional scheme
    tracker_df = _calculate_additional_scheme_actuals_vectorized(
        tracker_df, structured_data, sales_df, scheme_index, scheme_config
    )
    
    # Calculate percentage achieved
    tracker_df = _calculate_percentage_achieved_vectorized(
        tracker_df, calculation_mode, prefix
    )
    
    # Calculate payout columns for this additional scheme (if enabled in configuration)
    config_manager = structured_data.get('config_manager')
    if config_manager and config_manager.should_calculate_additional_payouts(str(scheme_index)):
        try:
            from calculations.payout_calculations import _apply_slab_payout_calculations

            print(f"   🎯 Additional scheme {scheme_index} payouts: ENABLED - calculating...")
            # Get slabs for this specific additional scheme
            slabs_df = structured_data['slabs']
            scheme_slabs = slabs_df[
                (slabs_df['scheme_type'] == 'additional_scheme') &
                (slabs_df['scheme_name'].str.contains(f'Additional Scheme {scheme_index}', na=False))
            ].copy()

            if not scheme_slabs.empty:
                # Determine base column for payout calculations
                base_column = f'total_volume_p{scheme_index}' if calculation_mode == 'volume' else f'total_value_p{scheme_index}'

                if base_column in tracker_df.columns:
                    tracker_df = _apply_slab_payout_calculations(
                        tracker_df, scheme_slabs, base_column, prefix,
                        structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_index}'
                    )
                else:
                    print(f"   ⚠️ Base column {base_column} not found for additional scheme {scheme_index} payout calculations")
            else:
                print(f"   ℹ️ No slabs found for additional scheme {scheme_index} - skipping payout calculations")
        except Exception as e:
            print(f"   ❌ Error in additional scheme {scheme_index} payout calculations: {e}")
            # Continue without payout calculations
    else:
        print(f"   ⚡ Additional scheme {scheme_index} payouts: DISABLED - skipping for performance")
    
    # Calculate mandatory product columns for this additional scheme (if enabled in configuration)
    config_manager = structured_data.get('config_manager')
    if config_manager and config_manager.should_calculate_additional_mandatory_products(str(scheme_index)):
        try:
            from calculations.mandatory_product_calculations import _calculate_mandatory_product_metrics_p1_to_p5, _apply_mandatory_product_calculations, _fetch_and_update_mandatory_min_shades_ppi
            
            print(f"   🎯 Additional scheme {scheme_index} mandatory products: ENABLED - calculating...")
            # Calculate mandatory product metrics P1-P5
            tracker_df = _calculate_mandatory_product_metrics_p1_to_p5(
                tracker_df, structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_index}', prefix
            )
            
            # Fetch Mandatory_Min_Shades_PPI from JSON and update P5 calculation
            tracker_df = _fetch_and_update_mandatory_min_shades_ppi(tracker_df, structured_data, f'additional_scheme_{scheme_index}', prefix)
            
            # Get slabs for mandatory product calculations
            slabs_df = structured_data['slabs']
            scheme_slabs = slabs_df[
                (slabs_df['scheme_type'] == 'additional_scheme') &
                (slabs_df['scheme_name'].str.contains(f'Additional Scheme {scheme_index}', na=False))
            ].copy()
            
            if not scheme_slabs.empty:
                is_volume_based = calculation_mode == 'volume'
                tracker_df = _apply_mandatory_product_calculations(
                    tracker_df, scheme_slabs, prefix, structured_data, is_volume_based, f'additional_scheme_{scheme_index}'
                )
            else:
                print(f"   ℹ️ No slabs found for additional scheme {scheme_index} mandatory products")
                
        except Exception as e:
            print(f"   ❌ Error in additional scheme {scheme_index} mandatory product calculations: {e}")
            # Continue without mandatory product calculations
    else:
        print(f"   ⚡ Additional scheme {scheme_index} mandatory products: DISABLED - skipping for performance")
    
    print(f"✅ Additional scheme {scheme_index} targets, actuals, and conditional calculations completed")
    return tracker_df

def _apply_slab_thresholds_vectorized(tracker_df, slabs_df, total_column, base_target_column, final_target_column):
    """Apply slab minimum thresholds using vectorization
    
    UPDATED LOGIC: Check if calculated target (after growth) is less than first slab,
    not the original total_volume/total_value
    """
    
    if slabs_df.empty or base_target_column not in tracker_df.columns:
        tracker_df[final_target_column] = tracker_df.get(base_target_column, 0.0)
        return tracker_df
    
    # Get first slab minimum
    first_slab_start = slabs_df.iloc[0]['slab_start']
    
    # FIXED LOGIC: Check if the calculated target (base_target_column) is less than first slab
    # If calculated target < first slab, then use first slab value
    # Otherwise use the calculated target
    tracker_df[final_target_column] = np.where(
        tracker_df[base_target_column] < first_slab_start,
        first_slab_start,  # Use first slab minimum if calculated target is too low
        tracker_df[base_target_column]  # Use calculated target if it meets minimum threshold
    )
    
    print(f"     🔧 Applied first slab minimum check: target >= {first_slab_start}")
    
    return tracker_df

def _calculate_proportional_targets(tracker_df, primary_target_column, primary_base_column, secondary_base_column):
    """Calculate proportional targets using vectorization"""
    
    # Avoid division by zero
    ratio = np.where(
        tracker_df[primary_base_column] != 0,
        tracker_df[primary_target_column] / tracker_df[primary_base_column],
        1.0
    )
    
    return tracker_df[secondary_base_column] * ratio

def _calculate_main_scheme_actuals_vectorized(tracker_df, structured_data, sales_df, scheme_config=None):
    """Calculate actual volume/value for main scheme using vectorization"""
    
    # Get main scheme product materials
    products_df = structured_data['products']
    main_products = products_df[
        (products_df['scheme_type'] == 'main_scheme') & 
        (products_df['is_product_data'] == True)
    ]
    
    if main_products.empty:
        print("⚠️  No main scheme products found for actuals")
        tracker_df['actual_volume'] = 0.0
        tracker_df['actual_value'] = 0.0
        return tracker_df
    
    # Get material list
    main_materials = main_products['material_code'].unique().tolist()
    print(f"   📋 Main scheme materials: {len(main_materials)} materials")
    
    # Get scheme running period from scheme configuration - DYNAMIC EXTRACTION
    try:
        if scheme_config and 'scheme_from' in scheme_config and 'scheme_to' in scheme_config:
            scheme_start = pd.to_datetime(scheme_config['scheme_from'])
            scheme_end = pd.to_datetime(scheme_config['scheme_to'])
            print(f"📅 Main scheme running period (from config): {scheme_start.date()} to {scheme_end.date()}")
        else:
            # 🔧 DYNAMIC FALLBACK: Extract scheme period from structured data
            scheme_info_df = structured_data.get('scheme_info')
            if scheme_info_df is not None and not scheme_info_df.empty:
                main_scheme_info = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
                if not main_scheme_info.empty:
                    scheme_from = main_scheme_info.iloc[0].get('scheme_from')
                    scheme_to = main_scheme_info.iloc[0].get('scheme_to')
                    if scheme_from and scheme_to:
                        scheme_start = pd.to_datetime(scheme_from)
                        scheme_end = pd.to_datetime(scheme_to)
                        print(f"📅 Main scheme running period (from structured_data): {scheme_start.date()} to {scheme_end.date()}")
                    else:
                        raise ValueError("Scheme dates not found in structured data")
                else:
                    raise ValueError("Main scheme info not found in structured data")
            else:
                raise ValueError("No scheme_info available in structured data")
    except Exception as e:
        print(f"⚠️  Error getting scheme period for main scheme: {e}")
        print(f"⚠️  CRITICAL: Cannot proceed without dynamic scheme dates")
        print(f"⚠️  Please ensure scheme_config contains 'scheme_from' and 'scheme_to' parameters")
        # Return with zero values rather than using hardcoded dates
        tracker_df['actual_volume'] = 0.0
        tracker_df['actual_value'] = 0.0
        return tracker_df
    
    # Filter sales data for scheme period and main scheme materials
    # Ensure sale_date is datetime
    if 'sale_date' in sales_df.columns:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    
    scheme_sales = sales_df[
        (sales_df['material'].isin(main_materials)) &
        (sales_df['sale_date'] >= scheme_start) &
        (sales_df['sale_date'] <= scheme_end)
    ]
    
    print(f"   📊 Scheme period sales data: {len(scheme_sales)} records")
    
    if scheme_sales.empty:
        print("   ⚠️  No sales data found for main scheme in running period")
        tracker_df['actual_volume'] = 0.0
        tracker_df['actual_value'] = 0.0
        return tracker_df
    
    # Aggregate actuals by credit account
    actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    print(f"   📊 Aggregated actuals for {len(actuals)} accounts")
    print(f"      Total actual volume: {actuals['volume'].sum():,.2f}")
    print(f"      Total actual value: {actuals['value'].sum():,.2f}")
    
    # Merge with tracker_df using vectorized operations
    actuals['credit_account'] = actuals['credit_account'].astype(str)
    tracker_df['credit_account_str'] = tracker_df['credit_account'].astype(str)
    
    # Create lookup dictionaries for fast vectorized mapping
    volume_lookup = dict(zip(actuals['credit_account'], actuals['volume']))
    value_lookup = dict(zip(actuals['credit_account'], actuals['value']))
    
    # Vectorized mapping with explicit float conversion
    tracker_df['actual_volume'] = tracker_df['credit_account_str'].map(volume_lookup).fillna(0.0).astype(float)
    tracker_df['actual_value'] = tracker_df['credit_account_str'].map(value_lookup).fillna(0.0).astype(float)
    
    # Clean up temporary column
    tracker_df.drop('credit_account_str', axis=1, inplace=True)
    
    print(f"✅ Main scheme actuals calculated for {len(actuals)} accounts")
    return tracker_df

def _calculate_additional_scheme_actuals_vectorized(tracker_df, structured_data, sales_df, scheme_index, scheme_config=None):
    """Calculate actual volume/value for additional scheme using vectorization"""
    
    prefix = f"_p{scheme_index}"
    
    # Get additional scheme product materials
    products_df = structured_data['products']
    
    # 🔧 FIX: Use a more robust lookup strategy for additional scheme products
    # First try to find products by scheme_id (index-based)
    additional_products = products_df[
        (products_df['scheme_type'] == 'additional_scheme') & 
        (products_df['is_product_data'] == True)
    ]
    
    if additional_products.empty:
        print(f"⚠️  No additional scheme products found at all")
        tracker_df[f'actual_volume{prefix}'] = 0.0
        tracker_df[f'actual_value{prefix}'] = 0.0
        return tracker_df
    
    # Debug: Show all available additional schemes
    available_schemes = additional_products['scheme_name'].unique()
    print(f"   🔍 Available additional schemes: {list(available_schemes)}")
    
    # Get the nth additional scheme (scheme_index is 1-based)
    unique_scheme_names = sorted(additional_products['scheme_name'].unique())
    if scheme_index <= len(unique_scheme_names):
        target_scheme_name = unique_scheme_names[scheme_index - 1]
        print(f"   🎯 Using additional scheme '{target_scheme_name}' (index {scheme_index})")
        
        # Filter products for this specific scheme
        scheme_products = additional_products[
            additional_products['scheme_name'] == target_scheme_name
        ]
    else:
        print(f"⚠️  Additional scheme index {scheme_index} out of range (available: {len(unique_scheme_names)})")
        tracker_df[f'actual_volume{prefix}'] = 0.0
        tracker_df[f'actual_value{prefix}'] = 0.0
        return tracker_df
    
    # Get material list
    additional_materials = scheme_products['material_code'].unique().tolist()
    print(f"   📋 Additional scheme {scheme_index} materials: {len(additional_materials)} materials")
    
    # 🔧 CRITICAL FIX: Get scheme running period dynamically - MUST use scheme period dates, not base period dates
    # For actual volume/value calculations, we need the scheme period dates for this specific scheme
    try:
        if scheme_config and 'scheme_from' in scheme_config and 'scheme_to' in scheme_config:
            scheme_start = pd.to_datetime(scheme_config['scheme_from'])
            scheme_end = pd.to_datetime(scheme_config['scheme_to'])
            print(f"📅 Additional scheme {scheme_index} SCHEME PERIOD (from config): {scheme_start.date()} to {scheme_end.date()}")
        else:
            # 🔧 DYNAMIC FALLBACK: Extract scheme period from structured data
            scheme_info_df = structured_data.get('scheme_info')
            if scheme_info_df is not None and not scheme_info_df.empty:
                main_scheme_info = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
                if not main_scheme_info.empty:
                    scheme_from = main_scheme_info.iloc[0].get('scheme_from')
                    scheme_to = main_scheme_info.iloc[0].get('scheme_to')
                    if scheme_from and scheme_to:
                        scheme_start = pd.to_datetime(scheme_from)
                        scheme_end = pd.to_datetime(scheme_to)
                        print(f"📅 Additional scheme {scheme_index} SCHEME PERIOD (from structured_data): {scheme_start.date()} to {scheme_end.date()}")
                    else:
                        raise ValueError("Scheme dates not found in structured data")
                else:
                    raise ValueError("Main scheme info not found in structured data")
            else:
                raise ValueError("No scheme_info available in structured data")
    except Exception as e:
        print(f"⚠️  Error getting scheme period for additional scheme {scheme_index}: {e}")
        print(f"⚠️  CRITICAL: Cannot proceed without dynamic scheme dates for scheme {scheme_index}")
        print(f"⚠️  Please ensure scheme_config contains 'scheme_from' and 'scheme_to' parameters")
        # Return with zero values rather than using hardcoded dates
        tracker_df[f'actual_volume{prefix}'] = 0.0
        tracker_df[f'actual_value{prefix}'] = 0.0
        return tracker_df
    
    # Filter sales data for scheme period and additional scheme materials
    # Ensure sale_date is datetime
    if 'sale_date' in sales_df.columns:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    
    scheme_sales = sales_df[
        (sales_df['material'].isin(additional_materials)) &
        (sales_df['sale_date'] >= scheme_start) &
        (sales_df['sale_date'] <= scheme_end)
    ]
    
    print(f"   📊 Additional scheme {scheme_index} sales data: {len(scheme_sales)} records")
    
    if scheme_sales.empty:
        print(f"   ⚠️  No sales data found for additional scheme {scheme_index} in running period")
        tracker_df[f'actual_volume{prefix}'] = 0.0
        tracker_df[f'actual_value{prefix}'] = 0.0
        return tracker_df
    
    # Aggregate actuals by credit account
    actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    print(f"   📊 Aggregated actuals for {len(actuals)} accounts")
    print(f"      Total actual volume: {actuals['volume'].sum():,.2f}")
    print(f"      Total actual value: {actuals['value'].sum():,.2f}")
    
    # Merge with tracker_df using vectorized operations
    actuals['credit_account'] = actuals['credit_account'].astype(str)
    tracker_df['credit_account_str'] = tracker_df['credit_account'].astype(str)
    
    # Create lookup dictionaries
    volume_lookup = dict(zip(actuals['credit_account'], actuals['volume']))
    value_lookup = dict(zip(actuals['credit_account'], actuals['value']))
    
    # Vectorized mapping with explicit float conversion
    tracker_df[f'actual_volume{prefix}'] = tracker_df['credit_account_str'].map(volume_lookup).fillna(0.0).astype(float)
    tracker_df[f'actual_value{prefix}'] = tracker_df['credit_account_str'].map(value_lookup).fillna(0.0).astype(float)
    
    # Clean up temporary column
    tracker_df.drop('credit_account_str', axis=1, inplace=True)
    
    print(f"✅ Additional scheme {scheme_index} actuals calculated for {len(actuals)} accounts")
    return tracker_df

def _calculate_percentage_achieved_vectorized(tracker_df, calculation_mode, prefix):
    """
    Calculate percentage achieved using vectorization
    Formula: 
    - If Volume-based: percentage_achieved = (actual_volume / target_volume) * 100
    - If Value-based: percentage_achieved = (actual_value / target_value) * 100
    """
    
    print(f"   🧮 Calculating percentage achieved for {prefix or 'main scheme'} (mode: {calculation_mode})")
    
    if calculation_mode.lower() == 'volume':
        target_col = f'target_volume{prefix}'
        actual_col = f'actual_volume{prefix}'
        print(f"      Using volume-based formula: {actual_col} / {target_col}")
    else:
        target_col = f'target_value{prefix}'
        actual_col = f'actual_value{prefix}'
        print(f"      Using value-based formula: {actual_col} / {target_col}")
    
    # Ensure target and actual columns exist and are float
    if target_col not in tracker_df.columns:
        tracker_df[target_col] = 0.0
        print(f"      ⚠️  {target_col} column not found, initialized to 0")
    else:
        tracker_df[target_col] = tracker_df[target_col].astype(float)
    
    if actual_col not in tracker_df.columns:
        tracker_df[actual_col] = 0.0
        print(f"      ⚠️  {actual_col} column not found, initialized to 0")
    else:
        tracker_df[actual_col] = tracker_df[actual_col].astype(float)
    
    # Vectorized percentage calculation with division by zero protection
    percentage_col = f'percentage_achieved{prefix}'
    tracker_df[percentage_col] = np.where(
        tracker_df[target_col] != 0,
        (tracker_df[actual_col] / tracker_df[target_col]),
        0.0
    )
    
    # Log some statistics
    non_zero_targets = (tracker_df[target_col] > 0).sum()
    non_zero_actuals = (tracker_df[actual_col] > 0).sum()
    non_zero_percentages = (tracker_df[percentage_col] > 0).sum()
    
    print(f"   📊 {percentage_col}: {non_zero_targets} targets > 0, {non_zero_actuals} actuals > 0, {non_zero_percentages} percentages > 0")
    return tracker_df

def handle_new_credit_accounts_vectorized(tracker_df, sales_df, structured_data, scheme_config=None):
    """
    Handle accounts that exist in scheme period but not in base period
    Uses vectorized operations for efficiency
    """
    print("🔍 Handling new credit accounts...")
    
    # Get all accounts from scheme period sales
    # Ensure sale_date is datetime
    if 'sale_date' in sales_df.columns:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    
    # 🔧 DYNAMIC DATE EXTRACTION: Get scheme period dates dynamically
    try:
        if scheme_config and 'scheme_from' in scheme_config and 'scheme_to' in scheme_config:
            scheme_start = pd.to_datetime(scheme_config['scheme_from'])
            scheme_end = pd.to_datetime(scheme_config['scheme_to'])
            print(f"📅 Using scheme period (from config): {scheme_start.date()} to {scheme_end.date()}")
        else:
            # Extract from structured data
            scheme_info_df = structured_data.get('scheme_info')
            if scheme_info_df is not None and not scheme_info_df.empty:
                main_scheme_info = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
                if not main_scheme_info.empty:
                    scheme_from = main_scheme_info.iloc[0].get('scheme_from')
                    scheme_to = main_scheme_info.iloc[0].get('scheme_to')
                    if scheme_from and scheme_to:
                        scheme_start = pd.to_datetime(scheme_from)
                        scheme_end = pd.to_datetime(scheme_to)
                        print(f"📅 Using scheme period (from structured_data): {scheme_start.date()} to {scheme_end.date()}")
                    else:
                        raise ValueError("Scheme dates not found in structured data")
                else:
                    raise ValueError("Main scheme info not found in structured data")
            else:
                raise ValueError("No scheme_info available in structured data")
    except Exception as e:
        print(f"⚠️  Error getting scheme period: {e}")
        print(f"⚠️  Cannot handle new accounts without dynamic scheme dates")
        return tracker_df
    
    scheme_period_sales = sales_df[
        (sales_df['sale_date'] >= scheme_start) &
        (sales_df['sale_date'] <= scheme_end)
    ]
    
    scheme_accounts = set(scheme_period_sales['credit_account'].astype(str).unique())
    tracker_accounts = set(tracker_df['credit_account'].astype(str).unique())
    
    # Find new accounts
    new_accounts = scheme_accounts - tracker_accounts
    
    if new_accounts:
        print(f"📊 Found {len(new_accounts)} new accounts in scheme period")
        
        # Create rows for new accounts with zero base values
        new_rows = []
        for account in new_accounts:
            # Get account details from sales data
            account_sales = scheme_period_sales[
                scheme_period_sales['credit_account'].astype(str) == account
            ]
            
            if not account_sales.empty:
                first_record = account_sales.iloc[0]
                
                new_row = {
                    'credit_account': account,
                    'customer_name': first_record.get('customer_name', ''),
                    'state_name': first_record.get('state_name', ''),
                    'so_name': first_record.get('so_name', ''),
                    'region': first_record.get('region', ''),
                    # Initialize base values to zero
                    'Base 1 Volume Final': 0.0,
                    'Base 1 Value Final': 0.0,
                    'Base 2 Volume Final': 0.0,
                    'Base 2 Value Final': 0.0,
                    'total_volume': 0.0,
                    'total_value': 0.0,
                }
                
                new_rows.append(new_row)
        
        if new_rows:
            # Create DataFrame for new accounts
            new_accounts_df = pd.DataFrame(new_rows)
            
            # Concatenate with existing tracker
            tracker_df = pd.concat([tracker_df, new_accounts_df], ignore_index=True)
            
            print(f"✅ Added {len(new_rows)} new accounts to tracker")
    
    return tracker_df

def calculate_all_targets_and_actuals(tracker_df, structured_data, json_data, sales_df, scheme_config=None):
    """
    Main function to calculate all targets and actuals
    Handles new accounts and applies all calculations
    """
    print("🎯 STARTING TARGET AND ACTUAL CALCULATIONS")
    print("=" * 60)
    
    # Step 1: Handle new credit accounts
    tracker_df = handle_new_credit_accounts_vectorized(tracker_df, sales_df, structured_data, scheme_config)
    
    # Step 2: Calculate targets and actuals for all schemes
    tracker_df = calculate_targets_and_actuals_vectorized(
        tracker_df, structured_data, json_data, sales_df, scheme_config
    )
    
    # Step 3: Calculate phasing columns for all schemes (if enabled)
    config_manager = structured_data.get('config_manager')
    if config_manager and (config_manager.should_calculate_main_phasing() or 
                          any(config_manager.should_calculate_additional_phasing(str(i)) for i in range(1, 10))):
        try:
            from calculations.phasing_calculations import calculate_phasing_columns
            
            print("🎯 STARTING PHASING CALCULATIONS")
            print("=" * 60)
            tracker_df = calculate_phasing_columns(
                tracker_df, structured_data, sales_df, scheme_config
            )
        except Exception as e:
            print(f"❌ Error in phasing calculations: {e}")
            # Continue without phasing calculations
    else:
        print("⚡ Phasing calculations: DISABLED - skipping for performance")
    
    # Step 4: Calculate bonus scheme columns (main scheme only, if enabled)
    if config_manager and config_manager.is_main_feature_enabled('bonusSchemes'):
        try:
            from calculations.bonus_scheme_calculations import calculate_bonus_scheme_columns
            
            print("🎯 STARTING BONUS SCHEME CALCULATIONS")
            print("=" * 60)
            tracker_df = calculate_bonus_scheme_columns(
                tracker_df, structured_data, sales_df, scheme_config
            )
        except Exception as e:
            print(f"❌ Error in bonus scheme calculations: {e}")
            # Continue without bonus scheme calculations
    else:
        print("⚡ Bonus scheme calculations: DISABLED - skipping for performance")
    
    print("✅ All target and actual calculations completed!")
    return tracker_df
