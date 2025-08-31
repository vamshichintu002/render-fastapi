"""
High-Performance Vectorized and Parallelized Calculations

This module implements ultra-fast calculations using:
1. Pandas vectorization (no loops)
2. Parallel execution with threading
3. Optimized memory usage
4. Concurrent processing of main and additional schemes
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def calculate_base_and_scheme_metrics_vectorized(sales_df: pd.DataFrame, scheme_config: Dict[str, Any], 
                                               json_data: Dict[str, Any] = None, structured_data: Dict = None) -> pd.DataFrame:
    """
    Ultra-fast vectorized calculation with parallel execution
    """
    
    # IMMEDIATE DEBUG OUTPUT - FIRST LINE
    print("🚨 VECTORIZED FUNCTION STARTED - IMMEDIATE DEBUG")
    import sys
    sys.stdout.flush()
    
    print("🔧 DEBUG: Starting vectorized calculations function")
    print(f"🔧 DEBUG: Sales DF shape: {sales_df.shape if not sales_df.empty else 'EMPTY'}")
    print(f"🔧 DEBUG: JSON data available: {json_data is not None}")
    if json_data:
        print(f"🔧 DEBUG: Additional schemes in JSON: {'additionalSchemes' in json_data}")
        if 'additionalSchemes' in json_data:
            print(f"🔧 DEBUG: Number of additional schemes: {len(json_data['additionalSchemes'])}")
    
    print("🚀 STARTING VECTORIZED & PARALLEL CALCULATIONS")
    print("=" * 60)
    start_time = time.time()
    
    try:
        # Validate input
        if sales_df.empty:
            return _create_empty_vectorized_result()
        
        # Prepare sales data with optimizations
        sales_clean = _prepare_sales_data_vectorized(sales_df)
        
        # Extract configuration
        base_periods = scheme_config['base_periods']
        scheme_from = scheme_config['scheme_from']
        scheme_to = scheme_config['scheme_to']
        calculation_mode = scheme_config.get('calculation_mode', 'volume')
        is_volume_based = calculation_mode.lower() == 'volume'
        
        print(f"📊 Processing {len(sales_clean):,} records with vectorization")
        print(f"📅 Base periods: {len(base_periods)}")
        print(f"🧮 Mode: {calculation_mode.upper()}")
        
        # 🔧 FIX: Get ALL scheme-configured accounts (including zero sales accounts)
        scheme_accounts = _get_all_scheme_configured_accounts(json_data, sales_clean)
        print(f"📋 Scheme configured accounts: {len(scheme_accounts):,}")
        
        # Parallel execution for main scheme and additional schemes
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            # Submit main scheme calculation
            main_future = executor.submit(
                _calculate_main_scheme_vectorized,
                sales_clean, base_periods, scheme_accounts, is_volume_based, scheme_config, json_data
            )
            futures.append(('main', main_future))
            
            # Submit additional scheme calculations in parallel
            if json_data and 'additionalSchemes' in json_data:
                additional_schemes = json_data['additionalSchemes']
                print(f"🔧 DEBUG: Found {len(additional_schemes)} additional schemes in JSON data")
                
                for i, add_scheme in enumerate(additional_schemes, 1):
                    print(f"🔧 DEBUG: Submitting additional scheme {i} calculation")
                    add_future = executor.submit(
                        _calculate_additional_scheme_vectorized,
                        sales_clean, base_periods, add_scheme, i, is_volume_based, structured_data
                    )
                    futures.append((f'additional_{i}', add_future))
            else:
                print(f"🔧 DEBUG: No additional schemes found in JSON data")
            
            # Collect results
            results = {}
            for scheme_type, future in futures:
                try:
                    result = future.result(timeout=300)  # 5 minute timeout
                    results[scheme_type] = result
                    print(f"🔧 DEBUG: {scheme_type} scheme completed - result type: {type(result)}")
                    if scheme_type.startswith('additional_') and result:
                        print(f"🔧 DEBUG: Additional scheme result data: {result.get('data') is not None}")
                        if result.get('data') is not None:
                            print(f"🔧 DEBUG: Additional scheme data shape: {result['data'].shape}")
                    print(f"   ✅ {scheme_type} scheme completed")
                except Exception as e:
                    print(f"   ❌ {scheme_type} scheme failed: {str(e)}")
                    results[scheme_type] = None
        
        # Merge all results
        final_result = _merge_parallel_results(results, scheme_accounts, scheme_config, json_data)
        
        # Add scheme metadata columns at the beginning
        print("📋 Adding scheme metadata columns...")
        final_result = _add_vectorized_metadata_columns(final_result, scheme_config, json_data, is_volume_based)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"\n🎉 VECTORIZED CALCULATIONS COMPLETE!")
        print(f"⚡ Execution time: {execution_time:.2f} seconds")
        print(f"📊 Accounts processed: {len(final_result):,}")
        print(f"🚀 Performance: {len(sales_clean)/execution_time:,.0f} records/second")
        
        return final_result
        
    except Exception as e:
        print(f"❌ Error in vectorized calculations: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _prepare_sales_data_vectorized(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare sales data with vectorized operations"""
    
    print("📋 Preparing data with vectorization...")
    
    # Create copy and ensure required columns
    sales_clean = sales_df.copy()
    
    # Vectorized column standardization
    if 'region_name' in sales_clean.columns and 'region' not in sales_clean.columns:
        sales_clean['region'] = sales_clean['region_name']
    if 'area_head_name' in sales_clean.columns and 'so_name' not in sales_clean.columns:
        sales_clean['so_name'] = sales_clean['area_head_name']
    
    # Vectorized data type conversions
    sales_clean['credit_account'] = sales_clean['credit_account'].astype(str)
    sales_clean['sale_date'] = pd.to_datetime(sales_clean['sale_date'])
    
    # Vectorized null handling
    numeric_cols = ['volume', 'value']
    sales_clean[numeric_cols] = sales_clean[numeric_cols].fillna(0).astype(float)
    
    string_cols = ['state_name', 'so_name', 'region', 'customer_name']
    for col in string_cols:
        if col in sales_clean.columns:
            sales_clean[col] = sales_clean[col].fillna('Unknown')
    
    print(f"   ✅ Data prepared: {len(sales_clean):,} records")
    return sales_clean

def _get_all_scheme_configured_accounts(json_data: Dict = None, sales_df: pd.DataFrame = None) -> List[str]:
    """🔧 FIX: Get ALL scheme-configured accounts (including zero sales accounts)"""
    
    if not json_data:
        # Fallback to sales accounts if no JSON configuration
        return sorted(sales_df['credit_account'].unique().tolist()) if sales_df is not None else []
    
    # Get scheme-configured accounts
    main_scheme = json_data.get('mainScheme', {})
    scheme_applicable = main_scheme.get('schemeApplicable', {})
    configured_accounts = [str(ca) for ca in scheme_applicable.get('selectedCreditAccounts', [])]
    
    print(f"   📋 Configured accounts from scheme: {len(configured_accounts):,}")
    
    # If no configured accounts, fall back to all sales accounts
    if not configured_accounts:
        configured_accounts = sorted(sales_df['credit_account'].unique().tolist()) if sales_df is not None else []
        print(f"   📋 Fallback to sales accounts: {len(configured_accounts):,}")
    
    # Filter by product materials if they exist
    product_data = main_scheme.get('productData', {})
    product_materials = set()
    for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                        'wandaGroups', 'productNames', 'thinnerGroups']:
        if product_type in product_data:
            product_materials.update(product_data[product_type])
    
    if product_materials and sales_df is not None:
        # Get accounts that have ANY sales (historical or current) with product materials
        material_mask = sales_df['material'].isin(product_materials)
        product_accounts = set(sales_df[material_mask]['credit_account'].astype(str).tolist())
        
        # Keep only configured accounts that have product relevance
        relevant_configured_accounts = [acc for acc in configured_accounts if acc in product_accounts]
        
        print(f"   📋 Product materials: {len(product_materials):,}")
        print(f"   📋 Product-relevant configured accounts: {len(relevant_configured_accounts):,}")
        
        # 🔧 CRITICAL: Include ALL configured accounts, not just those with product sales
        # If a configured account has no product sales, we still include it with zero values
        final_accounts = configured_accounts
        print(f"   📋 Final accounts (ALL configured): {len(final_accounts):,}")
        
        return sorted(final_accounts)
    
    print(f"   📋 All configured accounts (no product filter): {len(configured_accounts):,}")
    return sorted(configured_accounts)

def _get_product_filtered_accounts_vectorized(sales_df: pd.DataFrame, json_data: Dict = None) -> List[str]:
    """⚠️ DEPRECATED: Use _get_all_scheme_configured_accounts instead
    This function only gets accounts with sales data (causing missing accounts issue)
    """
    
    if not json_data:
        return sorted(sales_df['credit_account'].unique().tolist())
    
    # Get main scheme product materials
    main_scheme = json_data.get('mainScheme', {})
    product_data = main_scheme.get('productData', {})
    
    product_materials = set()
    for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                        'wandaGroups', 'productNames', 'thinnerGroups']:
        if product_type in product_data:
            product_materials.update(product_data[product_type])
    
    # Vectorized filtering
    material_mask = sales_df['material'].isin(product_materials)
    filtered_accounts = sorted(sales_df[material_mask]['credit_account'].unique().tolist())
    
    print(f"   📋 Product materials: {len(product_materials):,}")
    print(f"   📋 Filtered accounts: {len(filtered_accounts):,}")
    
    return filtered_accounts

def _calculate_main_scheme_vectorized(sales_df: pd.DataFrame, base_periods: List[Dict], 
                                    all_configured_accounts: List[str], is_volume_based: bool,
                                    scheme_config: Dict, json_data: Dict) -> pd.DataFrame:
    """🔧 FIXED: Calculate main scheme for ALL configured accounts with BOTH volume and value columns"""
    
    print("📋 Main scheme calculation (ALL accounts with BOTH volume and value columns)...")
    
    # 🔧 NEW: Always calculate BOTH volume and value for main scheme
    # Don't limit to just the scheme's primary metric
    
    # Filter sales data by product materials first
    main_scheme = json_data.get('mainScheme', {}) if json_data else {}
    product_data = main_scheme.get('productData', {})
    
    product_materials = set()
    for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                        'wandaGroups', 'productNames', 'thinnerGroups']:
        if product_type in product_data:
            product_materials.update(product_data[product_type])
    
    # Filter sales by product materials
    if product_materials:
        material_mask = sales_df['material'].isin(product_materials)
        relevant_sales = sales_df[material_mask].copy()
    else:
        relevant_sales = sales_df.copy()
    
    print(f"   📋 Sales with product materials: {len(relevant_sales):,}")
    
    # Process base periods for accounts that have sales
    accounts_with_sales = set()
    volume_base_results = []
    value_base_results = []
    
    for i, base_period in enumerate(base_periods, 1):
        from_date = pd.to_datetime(base_period['from_date'])
        to_date = pd.to_datetime(base_period['to_date'])
        sum_avg_method = base_period.get('sum_avg_method', 'sum')
        
        print(f"   📅 Processing Base {i}: {from_date.date()} to {to_date.date()}")
        
        # Vectorized period filtering
        period_mask = (relevant_sales['sale_date'] >= from_date) & (relevant_sales['sale_date'] <= to_date)
        period_data = relevant_sales[period_mask]
        
        print(f"     📈 Base {i} records: {len(period_data):,}")
        
        if not period_data.empty:
            # 🔧 NEW: Calculate BOTH volume and value aggregations
            period_agg = period_data.groupby('credit_account').agg({
                'volume': 'sum',
                'value': 'sum'
            }).reset_index()
            
            period_accounts = set(period_agg['credit_account'].astype(str).tolist())
            accounts_with_sales.update(period_accounts)
            
            print(f"     📋 Base {i} accounts with sales: {len(period_accounts):,}")
            print(f"     💰 Base {i} total volume: {period_agg['volume'].sum():,.2f}")
            print(f"     💰 Base {i} total value: {period_agg['value'].sum():,.2f}")
            
            # Apply averaging if needed (vectorized) - FIXED MONTH CALCULATION
            if sum_avg_method.lower() == 'average':
                # Use same month calculation as SQL: EXTRACT(MONTH FROM AGE()) + 1
                num_months = ((to_date.year - from_date.year) * 12 + to_date.month - from_date.month) + 1
                period_agg[f'Base {i} Volume Final'] = (period_agg['volume'] / num_months).astype(float)
                period_agg[f'Base {i} Value Final'] = (period_agg['value'] / num_months).astype(float)
                print(f"     💰 Applied averaging: ÷{num_months} months")
            else:
                period_agg[f'Base {i} Volume Final'] = period_agg['volume'].astype(float)
                period_agg[f'Base {i} Value Final'] = period_agg['value'].astype(float)
                print(f"     💰 Applied sum (no averaging)")
            
            # Store both volume and value results
            volume_base_results.append(period_agg[['credit_account', f'Base {i} Volume Final']])
            value_base_results.append(period_agg[['credit_account', f'Base {i} Value Final']])
        else:
            print(f"     ⚠️ No data for Base {i}")
    
    print(f"   📋 Total accounts with sales in base periods: {len(accounts_with_sales):,}")
    
    # Handle missing base periods - ensure both Base 1 and Base 2 for both volume and value
    if len(base_periods) == 1 and volume_base_results:
        # Copy Base 1 to Base 2 for both volume and value
        base2_volume_data = volume_base_results[0].copy()
        base2_volume_data.columns = ['credit_account', 'Base 2 Volume Final']
        volume_base_results.append(base2_volume_data)
        
        base2_value_data = value_base_results[0].copy()
        base2_value_data.columns = ['credit_account', 'Base 2 Value Final']
        value_base_results.append(base2_value_data)
    elif len(volume_base_results) == 0:
        # 🔧 NEW: Handle case where no base periods have data
        print(f"     ⚠️ No base period data found - will create zero values for all accounts")
    
    # 🔧 CRITICAL FIX: Create result for ALL configured accounts (not just those with sales)
    all_accounts = set(all_configured_accounts)  # All configured accounts
    accounts_without_sales = all_accounts - accounts_with_sales
    
    print(f"   📋 Total configured accounts: {len(all_accounts):,}")
    print(f"   📋 Configured accounts without sales: {len(accounts_without_sales):,}")
    
    # 1. Get account details for accounts WITH sales
    if accounts_with_sales:
        accounts_with_sales_list = list(accounts_with_sales)
        account_mask = relevant_sales['credit_account'].astype(str).isin(accounts_with_sales_list)
        account_sales = relevant_sales[account_mask]
        
        account_details_with_sales = account_sales.groupby('credit_account').agg({
            'state_name': 'first',
            'so_name': 'first', 
            'region': 'first',
            'customer_name': 'first'
        }).reset_index()
        
        # 🔧 NEW: Merge BOTH volume and value base results for accounts with sales
        main_result_with_sales = account_details_with_sales.copy()
        
        # Merge volume results (if available)
        if volume_base_results:
            for base_df in volume_base_results:
                main_result_with_sales = main_result_with_sales.merge(base_df, on='credit_account', how='left')
        else:
            # Add zero columns if no volume base results
            main_result_with_sales['Base 1 Volume Final'] = 0.0
            main_result_with_sales['Base 2 Volume Final'] = 0.0
        
        # Merge value results (if available)
        if value_base_results:
            for base_df in value_base_results:
                main_result_with_sales = main_result_with_sales.merge(base_df, on='credit_account', how='left')
        else:
            # Add zero columns if no value base results
            main_result_with_sales['Base 1 Value Final'] = 0.0
            main_result_with_sales['Base 2 Value Final'] = 0.0
        
        # Fill missing values for accounts with sales
        base_cols = ['Base 1 Volume Final', 'Base 2 Volume Final', 'Base 1 Value Final', 'Base 2 Value Final']
        main_result_with_sales[base_cols] = main_result_with_sales[base_cols].fillna(0).astype(float)
        
        # 🔧 NEW: Calculate BOTH total_volume and total_value (vectorized)
        main_result_with_sales['total_volume'] = np.round(
            np.maximum(
                main_result_with_sales['Base 1 Volume Final'], 
                main_result_with_sales['Base 2 Volume Final']
            ), 0
        ).astype(float)
        
        main_result_with_sales['total_value'] = np.round(
            np.maximum(
                main_result_with_sales['Base 1 Value Final'], 
                main_result_with_sales['Base 2 Value Final']
            ), 0
        ).astype(float)
        
        print(f"   ✅ Main scheme accounts with sales: {len(main_result_with_sales)} accounts processed")
        print(f"       • Total volume: {main_result_with_sales['total_volume'].sum():,.2f}")
        print(f"       • Total value: {main_result_with_sales['total_value'].sum():,.2f}")
    else:
        main_result_with_sales = pd.DataFrame()
    
    # 2. 🔧 FIXED: Handle accounts WITHOUT product sales
    # Strategy: Only include accounts that have sales data to avoid Unknown states
    
    # Get configured states for filtering
    configured_states = set()
    if json_data:
        main_scheme = json_data.get('mainScheme', {})
        scheme_applicable = main_scheme.get('schemeApplicable', {})
        configured_states = set(scheme_applicable.get('selectedStates', []))
    
    # Get account details from ALL sales data (not just product-filtered)
    all_sales_account_details = pd.DataFrame()
    if not sales_df.empty:
        all_sales_account_details = sales_df.groupby('credit_account').agg({
            'state_name': 'first',
            'so_name': 'first',
            'region': 'first', 
            'customer_name': 'first'
        }).reset_index()
        all_sales_account_details['credit_account'] = all_sales_account_details['credit_account'].astype(str)
    
    accounts_without_sales = all_accounts - accounts_with_sales
    included_zero_sales_accounts = 0
    excluded_accounts = 0
    
    main_result_zero_sales = pd.DataFrame()
    
    if accounts_without_sales:
        print(f"   🔧 Processing {len(accounts_without_sales)} accounts without product sales...")
        print(f"   📋 Configured states: {list(configured_states) if configured_states else 'All states allowed'}")
        
        zero_sales_records = []
        
        for account in accounts_without_sales:
            # Try to get account details from ALL sales data
            account_info = all_sales_account_details[all_sales_account_details['credit_account'] == account]
            
            if not account_info.empty:
                account_state = account_info.iloc[0]['state_name']
                
                # Check if account state matches configured states  
                if configured_states and account_state not in configured_states:
                    print(f"      ❌ Account {account}: Excluded (state '{account_state}' not in configured states)")
                    excluded_accounts += 1
                    continue
                
                # 🔧 FIXED: Include account with ALL 6 columns (both volume and value)
                record = {
                    'credit_account': account,
                    'state_name': account_state,
                    'so_name': account_info.iloc[0]['so_name'],
                    'region': account_info.iloc[0]['region'], 
                    'customer_name': account_info.iloc[0]['customer_name'],
                    'Base 1 Volume Final': 0.0,
                    'Base 1 Value Final': 0.0,
                    'Base 2 Volume Final': 0.0,
                    'Base 2 Value Final': 0.0,
                    'total_volume': 0.0,
                    'total_value': 0.0
                }
                zero_sales_records.append(record)
                included_zero_sales_accounts += 1
                print(f"      ✅ Account {account}: Included with zero values (state: {account_state})")
            else:
                # 🔧 KEY FIX: Exclude accounts with no sales data when state filtering is active
                # This prevents Unknown state records
                if configured_states:
                    print(f"      ❌ Account {account}: Excluded (no sales data found and state filter active)")
                    excluded_accounts += 1
                    # Do NOT create Unknown record - skip this account entirely
                    continue
                else:
                    # 🔧 FIXED: Only create Unknown records if no state filtering (rare case)
                    record = {
                        'credit_account': account,
                        'state_name': 'Unknown',
                        'so_name': 'Unknown',
                        'region': 'Unknown', 
                        'customer_name': 'Unknown',
                        'Base 1 Volume Final': 0.0,
                        'Base 1 Value Final': 0.0,
                        'Base 2 Volume Final': 0.0,
                        'Base 2 Value Final': 0.0,
                        'total_volume': 0.0,
                        'total_value': 0.0
                    }
                    zero_sales_records.append(record)
                    included_zero_sales_accounts += 1
                    print(f"      ⚠️ Account {account}: Included as Unknown (no state filter)")
        
        if zero_sales_records:
            main_result_zero_sales = pd.DataFrame(zero_sales_records)
            
        print(f"   📋 Zero-sales accounts summary:")
        print(f"      • Included: {included_zero_sales_accounts}")
        print(f"      • Excluded: {excluded_accounts}")
    
    # 3. Combine both results
    if not main_result_with_sales.empty and not main_result_zero_sales.empty:
        main_result = pd.concat([main_result_with_sales, main_result_zero_sales], ignore_index=True)
    elif not main_result_with_sales.empty:
        main_result = main_result_with_sales
    elif not main_result_zero_sales.empty:
        main_result = main_result_zero_sales
    else:
        # 🔧 FIXED: No accounts at all - create empty result with ALL 6 columns
        main_result = pd.DataFrame(columns=[
            'credit_account', 'state_name', 'so_name', 'region', 'customer_name',
            'Base 1 Volume Final', 'Base 1 Value Final', 'Base 2 Volume Final', 'Base 2 Value Final',
            'total_volume', 'total_value'
        ])
    
    print(f"   ✅ Main scheme: {len(main_result):,} accounts processed (valid accounts only)")
    print(f"      • With product sales: {len(accounts_with_sales):,}")
    print(f"      • Zero product sales (included): {included_zero_sales_accounts:,}")
    print(f"      • Excluded (no sales data): {excluded_accounts:,}")
    print(f"      • Total in tracker: {len(main_result):,}")
    
    return main_result

def _calculate_additional_scheme_vectorized(sales_df: pd.DataFrame, base_periods: List[Dict],
                                          add_scheme: Dict, scheme_num: int, is_volume_based: bool, structured_data: Dict = None) -> Dict:
    """Calculate additional scheme using vectorized operations"""
    
    scheme_name = add_scheme.get('schemeNumber', add_scheme.get('schemeTitle', f'Additional {scheme_num}'))
    print(f"🧮 Additional scheme {scheme_num} ({scheme_name}) calculation (vectorized)...")
    
    # Determine THIS additional scheme's individual Volume/Value type
    individual_is_volume_based = is_volume_based  # fallback to main scheme
    
    if structured_data:
        scheme_info = structured_data['scheme_info']
        additional_scheme_info = scheme_info[scheme_info['scheme_type'] == 'additional_scheme']
        
        if not additional_scheme_info.empty and len(additional_scheme_info) >= scheme_num:
            # Find the specific additional scheme by index (they are in order)
            scheme_info_row = additional_scheme_info.iloc[scheme_num - 1]  # 0-based index
            scheme_volume_value = scheme_info_row['volume_value_based'].lower()
            individual_is_volume_based = (scheme_volume_value == 'volume')
            print(f"   📊 Additional scheme {scheme_num} individual type: {scheme_volume_value.upper()}")
        else:
            print(f"   📊 Additional scheme {scheme_num} using fallback type: {'VOLUME' if individual_is_volume_based else 'VALUE'}")
    
    # Get additional scheme product materials
    # Additional schemes store product data directly under 'productData', not 'productData.mainScheme'
    add_product_data = add_scheme.get('productData', {})
    add_product_materials = set()
    
    for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                        'wandaGroups', 'productNames', 'thinnerGroups']:
        if product_type in add_product_data:
            add_product_materials.update(add_product_data[product_type])
    
    if not add_product_materials:
        print(f"   ⚠️ No products for additional scheme {scheme_num}")
        return {'scheme_num': scheme_num, 'data': None}
    
    print(f"   📋 Additional scheme {scheme_num} has {len(add_product_materials)} product materials")
    
    # Vectorized filtering by additional scheme products
    material_mask = sales_df['material'].isin(add_product_materials)
    scheme_sales = sales_df[material_mask].copy()
    
    print(f"   📊 Sales records for additional scheme {scheme_num} products: {len(scheme_sales)}")
    
    # Get ALL credit accounts from the main sales data to ensure consistency
    all_accounts = sorted(sales_df['credit_account'].unique())
    print(f"   📋 Processing all {len(all_accounts)} accounts for additional scheme {scheme_num}")
    
    # 🔧 FIXED: Calculate BOTH volume and value for additional schemes (like main scheme)
    # Don't limit to just the scheme's primary metric
    
    # Calculate base periods vectorized - BOTH volume and value
    volume_base_results = []
    value_base_results = []
    
    for i, base_period in enumerate(base_periods, 1):
        from_date = pd.to_datetime(base_period['from_date'])
        to_date = pd.to_datetime(base_period['to_date'])
        sum_avg_method = base_period.get('sum_avg_method', 'sum')
        
        # Vectorized period filtering (only from scheme_sales with product filter)
        period_mask = (scheme_sales['sale_date'] >= from_date) & (scheme_sales['sale_date'] <= to_date)
        period_data = scheme_sales[period_mask]
        
        # Create result for ALL accounts, not just those with sales
        all_accounts_df = pd.DataFrame({'credit_account': all_accounts})
        
        if not period_data.empty:
            # 🔧 FIXED: Calculate BOTH volume and value aggregations
            period_agg = period_data.groupby('credit_account').agg({
                'volume': 'sum',
                'value': 'sum'
            }).reset_index()
            
            # Apply averaging if needed (vectorized) - FIXED MONTH CALCULATION
            if sum_avg_method.lower() == 'average':
                # Use same month calculation as SQL: EXTRACT(MONTH FROM AGE()) + 1
                num_months = ((to_date.year - from_date.year) * 12 + to_date.month - from_date.month) + 1
                period_agg[f'Base {i} Volume Final_p{scheme_num}'] = (period_agg['volume'] / num_months).astype(float)
                period_agg[f'Base {i} Value Final_p{scheme_num}'] = (period_agg['value'] / num_months).astype(float)
                print(f"     💰 Additional scheme {scheme_num} base {i}: averaging ÷{num_months} months")
            else:
                period_agg[f'Base {i} Volume Final_p{scheme_num}'] = period_agg['volume'].astype(float)
                period_agg[f'Base {i} Value Final_p{scheme_num}'] = period_agg['value'].astype(float)
                print(f"     💰 Additional scheme {scheme_num} base {i}: sum (no averaging)")
            
            # Merge with all accounts to ensure consistency - BOTH volume and value
            volume_result = all_accounts_df.merge(
                period_agg[['credit_account', f'Base {i} Volume Final_p{scheme_num}']], 
                on='credit_account', 
                how='left'
            )
            volume_result[f'Base {i} Volume Final_p{scheme_num}'] = volume_result[f'Base {i} Volume Final_p{scheme_num}'].fillna(0.0)
            
            value_result = all_accounts_df.merge(
                period_agg[['credit_account', f'Base {i} Value Final_p{scheme_num}']], 
                on='credit_account', 
                how='left'
            )
            value_result[f'Base {i} Value Final_p{scheme_num}'] = value_result[f'Base {i} Value Final_p{scheme_num}'].fillna(0.0)
            
            accounts_with_sales = len(period_agg)
            print(f"     📊 Base {i}: {accounts_with_sales}/{len(all_accounts)} accounts have sales")
            print(f"     💰 Base {i} total volume: {period_agg['volume'].sum():,.2f}")
            print(f"     💰 Base {i} total value: {period_agg['value'].sum():,.2f}")
        else:
            # No sales data for this period - create zeros for all accounts
            volume_result = all_accounts_df.copy()
            volume_result[f'Base {i} Volume Final_p{scheme_num}'] = 0.0
            
            value_result = all_accounts_df.copy()
            value_result[f'Base {i} Value Final_p{scheme_num}'] = 0.0
            
            print(f"     📊 Base {i}: 0/{len(all_accounts)} accounts have sales")
        
        # Store both volume and value results
        volume_base_results.append(volume_result)
        value_base_results.append(value_result)
    
    # Handle missing base periods - ensure both Base 1 and Base 2 for both volume and value
    if len(base_periods) == 1 and volume_base_results:
        # Copy Base 1 to Base 2 for both volume and value
        base2_volume_data = volume_base_results[0].copy()
        base2_volume_col = f'Base 2 Volume Final_p{scheme_num}'
        base1_volume_col = f'Base 1 Volume Final_p{scheme_num}'
        base2_volume_data[base2_volume_col] = base2_volume_data[base1_volume_col]
        volume_base_results.append(base2_volume_data[['credit_account', base2_volume_col]])
        
        base2_value_data = value_base_results[0].copy()
        base2_value_col = f'Base 2 Value Final_p{scheme_num}'
        base1_value_col = f'Base 1 Value Final_p{scheme_num}'
        base2_value_data[base2_value_col] = base2_value_data[base1_value_col]
        value_base_results.append(base2_value_data[['credit_account', base2_value_col]])
    
    # 🔧 FIXED: Merge BOTH volume and value results like main scheme
    if volume_base_results and value_base_results:
        # Start with first volume result
        merged_scheme = volume_base_results[0].copy()
        
        # Merge remaining volume results
        for result in volume_base_results[1:]:
            merged_scheme = merged_scheme.merge(result, on='credit_account', how='left')
        
        # Merge all value results
        for result in value_base_results:
            merged_scheme = merged_scheme.merge(result, on='credit_account', how='left')
        
        # Ensure all base columns exist and are numeric
        base1_volume_col = f'Base 1 Volume Final_p{scheme_num}'
        base2_volume_col = f'Base 2 Volume Final_p{scheme_num}'
        base1_value_col = f'Base 1 Value Final_p{scheme_num}'
        base2_value_col = f'Base 2 Value Final_p{scheme_num}'
        
        for col in [base1_volume_col, base2_volume_col, base1_value_col, base2_value_col]:
            if col not in merged_scheme.columns:
                merged_scheme[col] = 0.0
            merged_scheme[col] = merged_scheme[col].fillna(0).astype(float)
        
        # 🔧 FIXED: Calculate BOTH total_volume_pN AND total_value_pN (like main scheme)
        merged_scheme[f'total_volume_p{scheme_num}'] = np.round(
            np.maximum(
                merged_scheme[base1_volume_col], 
                merged_scheme[base2_volume_col]
            ), 0
        ).astype(float)
        
        merged_scheme[f'total_value_p{scheme_num}'] = np.round(
            np.maximum(
                merged_scheme[base1_value_col], 
                merged_scheme[base2_value_col]
            ), 0
        ).astype(float)
        
        # Add metadata columns using individual scheme type (but with both totals calculated)
        merged_scheme[f'Scheme Name_p{scheme_num}'] = scheme_name
        merged_scheme[f'Scheme Type_p{scheme_num}'] = 'Volume' if individual_is_volume_based else 'Value'  # This determines formula logic, not column presence
        merged_scheme[f'Mandatory Qualify_p{scheme_num}'] = 'Yes' if add_scheme.get('mandatoryQualify', False) else 'No'
        
        # Report summary for both volume and value
        non_zero_accounts = {
            'base1_volume': (merged_scheme[base1_volume_col] > 0).sum(),
            'base2_volume': (merged_scheme[base2_volume_col] > 0).sum(),
            'total_volume': (merged_scheme[f'total_volume_p{scheme_num}'] > 0).sum(),
            'base1_value': (merged_scheme[base1_value_col] > 0).sum(),
            'base2_value': (merged_scheme[base2_value_col] > 0).sum(),
            'total_value': (merged_scheme[f'total_value_p{scheme_num}'] > 0).sum()
        }
        
        print(f"   ✅ Additional scheme {scheme_num}: {len(merged_scheme):,} accounts processed")
        print(f"      📊 Non-zero volume accounts - Base1: {non_zero_accounts['base1_volume']}, Base2: {non_zero_accounts['base2_volume']}, Total: {non_zero_accounts['total_volume']}")
        print(f"      📊 Non-zero value accounts - Base1: {non_zero_accounts['base1_value']}, Base2: {non_zero_accounts['base2_value']}, Total: {non_zero_accounts['total_value']}")
        print(f"      💰 Total volume: {merged_scheme[f'total_volume_p{scheme_num}'].sum():,.2f}")
        print(f"      💰 Total value: {merged_scheme[f'total_value_p{scheme_num}'].sum():,.2f}")
        return {'scheme_num': scheme_num, 'data': merged_scheme}
    
    return {'scheme_num': scheme_num, 'data': None}



def _add_main_scheme_metadata_vectorized(result_df: pd.DataFrame, scheme_config: Dict, 
                                       json_data: Dict, is_volume_based: bool) -> pd.DataFrame:
    """Add main scheme metadata using vectorized operations"""
    
    # Extract scheme info
    scheme_id = scheme_config.get('scheme_id', 'Unknown')
    scheme_from = scheme_config.get('scheme_from', 'Unknown')
    scheme_to = scheme_config.get('scheme_to', 'Unknown')
    calculation_mode = 'Volume' if is_volume_based else 'Value'
    
    scheme_name = 'Main Scheme'
    mandatory_qualify = 'Unknown'
    
    if json_data:
        basic_info = json_data.get('basicInfo', {})
        main_scheme = json_data.get('mainScheme', {})
        scheme_name = basic_info.get('schemeTitle', 'Main Scheme')
        mandatory_qualify = 'Yes' if main_scheme.get('mandatoryQualify', False) else 'No'
    
    # Add metadata columns (vectorized - single assignment)
    result_df.insert(0, 'Scheme ID', scheme_id)
    result_df.insert(1, 'Scheme Name', scheme_name)
    result_df.insert(2, 'Scheme Type', calculation_mode)
    result_df.insert(3, 'Scheme Period From', scheme_from)
    result_df.insert(4, 'Scheme Period To', scheme_to)
    result_df.insert(5, 'Mandatory Qualify', mandatory_qualify)
    
    return result_df

def _merge_parallel_results(results: Dict, all_configured_accounts: List[str], 
                          scheme_config: Dict, json_data: Dict) -> pd.DataFrame:
    """Merge results from parallel execution"""
    
    print("🔗 Merging parallel calculation results...")
    
    # Start with main scheme result
    main_result = results.get('main')
    if main_result is None or main_result.empty:
        print("   ❌ No main scheme results")
        return _create_empty_vectorized_result()
    
    final_result = main_result.copy()
    
    # Merge additional scheme results
    additional_schemes = [k for k in results.keys() if k.startswith('additional_')]
    
    for scheme_key in additional_schemes:
        add_result = results[scheme_key]
        if add_result and add_result['data'] is not None:
            add_data = add_result['data']
            final_result = final_result.merge(add_data, on='credit_account', how='left')
    
    # Fill missing additional scheme values with 0
    additional_cols = [col for col in final_result.columns if '_p' in col and any(word in col for word in ['Base', 'total'])]
    for col in additional_cols:
        if final_result[col].dtype in ['object', 'string']:
            continue  # Skip metadata columns
        final_result[col] = final_result[col].fillna(0).astype(float)
    
    print(f"   ✅ Merged: {len(final_result):,} accounts")
    return final_result

def _create_empty_vectorized_result() -> pd.DataFrame:
    """Create empty result with proper structure"""
    return pd.DataFrame(columns=[
        'Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To',
        'Mandatory Qualify', 'credit_account', 'state_name', 'so_name', 'region', 'customer_name'
    ])

def _add_vectorized_metadata_columns(result_df: pd.DataFrame, scheme_config: Dict, 
                                   json_data: Dict = None, is_volume_based: bool = True) -> pd.DataFrame:
    """Add scheme metadata columns with proper ordering"""
    
    # Extract basic scheme info
    scheme_id = scheme_config.get('scheme_id', 'Unknown')
    scheme_from = scheme_config.get('scheme_from', 'Unknown')
    scheme_to = scheme_config.get('scheme_to', 'Unknown')
    calculation_mode = 'Volume' if is_volume_based else 'Value'
    
    # Default values
    scheme_name = 'Main Scheme'
    mandatory_qualify = 'Unknown'
    
    # Extract from JSON if available
    if json_data:
        basic_info = json_data.get('basicInfo', {})
        main_scheme = json_data.get('mainScheme', {})
        
        scheme_name = basic_info.get('schemeTitle', 'Main Scheme')
        mandatory_qualify = 'Yes' if main_scheme.get('mandatoryQualify', False) else 'No'
    
    # Add main scheme columns at the beginning
    result_df.insert(0, 'Scheme ID', scheme_id)
    result_df.insert(1, 'Scheme Name', scheme_name)
    result_df.insert(2, 'Scheme Type', calculation_mode)
    result_df.insert(3, 'Scheme Period From', scheme_from)
    result_df.insert(4, 'Scheme Period To', scheme_to)
    result_df.insert(5, 'Mandatory Qualify', mandatory_qualify)
    
    # Add additional scheme metadata columns if they don't exist
    if json_data and 'additionalSchemes' in json_data:
        additional_schemes = json_data['additionalSchemes']
        
        for i, add_scheme in enumerate(additional_schemes, 1):
            suffix = f'_p{i}'
            
            # Get scheme metadata
            add_scheme_name = add_scheme.get('schemeNumber', add_scheme.get('schemeTitle', f'Additional Scheme {i}'))
            add_scheme_type = 'Volume' if add_scheme.get('volumeValueBased', 'volume').lower() == 'volume' else 'Value'
            add_mandatory_qualify = 'Yes' if add_scheme.get('mandatoryQualify', False) else 'No'
            
            # Create additional scheme metadata columns if they don't exist
            if f'Scheme Name{suffix}' not in result_df.columns:
                result_df[f'Scheme Name{suffix}'] = add_scheme_name
                print(f"   ✅ Created Scheme Name{suffix}: {add_scheme_name}")
            
            if f'Scheme Type{suffix}' not in result_df.columns:
                result_df[f'Scheme Type{suffix}'] = add_scheme_type
                print(f"   ✅ Created Scheme Type{suffix}: {add_scheme_type}")
            
            if f'Mandatory Qualify{suffix}' not in result_df.columns:
                result_df[f'Mandatory Qualify{suffix}'] = add_mandatory_qualify
                print(f"   ✅ Created Mandatory Qualify{suffix}: {add_mandatory_qualify}")
    
    # Reorder columns for proper additional scheme grouping
    result_df = _reorder_columns_properly(result_df, json_data)
    
    print(f"   ✅ Added main scheme metadata columns with proper ordering")
    return result_df

def _reorder_columns_properly(result_df: pd.DataFrame, json_data: Dict = None) -> pd.DataFrame:
    """Reorder columns with PERFECT ordering as specified by user"""
    
    all_cols = list(result_df.columns)
    
    # MAIN SCHEME PERFECT ORDER
    main_order = [
        'Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To', 'Mandatory Qualify',
        'credit_account', 'state_name', 'so_name', 'region', 'customer_name',
        'Base 1 Volume Final', 'Base 2 Volume Final', 'total_volume', 'growth_rate', 
        'target_volume', 'target_value', 'actual_volume', 'actual_value', 'percentage_achieved'
    ]
    
    # Start with main scheme columns that exist
    ordered_cols = []
    for col in main_order:
        if col in all_cols:
            ordered_cols.append(col)
    
    # Get number of additional schemes
    additional_schemes = json_data.get('additionalSchemes', []) if json_data else []
    num_additional = len(additional_schemes)
    
    # Add additional scheme columns in PERFECT ORDER for each scheme
    for i in range(1, num_additional + 1):
        prefix = f'_p{i}'
        
        # PERFECT ORDER for additional scheme (excluding period dates as they're same for all schemes)
        additional_order = [
            f'Scheme Name{prefix}', f'Scheme Type{prefix}', f'Mandatory Qualify{prefix}',
            f'Base 1 Volume Final{prefix}', f'Base 2 Volume Final{prefix}', f'total_volume{prefix}',
            f'growth_rate{prefix}', f'target_volume{prefix}', f'target_value{prefix}',
            f'actual_volume{prefix}', f'actual_value{prefix}', f'percentage_achieved{prefix}'
        ]
        
        # Add columns that exist
        for col in additional_order:
            if col in all_cols:
                ordered_cols.append(col)
    
    # Add any remaining columns not captured
    for col in all_cols:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    print(f"   📋 Reordered to perfect column sequence: {len(ordered_cols)} columns")
    return result_df[ordered_cols]
