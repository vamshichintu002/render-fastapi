"""
Base Period and Scheme Period Calculations

This module implements the core base period calculations for sales incentive processing.
Handles both single and double base period scenarios with proper aggregation.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from .vectorized_calculations import calculate_base_and_scheme_metrics_vectorized

def calculate_base_and_scheme_metrics(sales_df: pd.DataFrame, scheme_config: Dict[str, Any], 
                                     json_data: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Calculate base period and scheme period metrics for sales incentive calculations.
    
    NEW LOGIC: Get ALL credit accounts from base period, select higher base period if 2 exist,
    add scheme metadata columns including additional schemes.
    
    Args:
        sales_df: DataFrame with sales data
        scheme_config: Dict with base_periods, scheme_from, scheme_to
        json_data: Full JSON data for scheme metadata
    
    Returns:
        pd.DataFrame with columns:
        ['Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To',
         'Mandatory Qualify', 'Scheme Name_p1', 'Mandatory Qualify_p1', ...,
         'state_name', 'so_name', 'region', 'credit_account', 'customer_name',
         'Base 1 Volume Final', 'Base 1 Value Final', 'Base 2 Volume Final', 'Base 2 Value Final',
         'total_volume', 'total_value']
    """
    
    print("🧮 Starting Base and Scheme Period Calculations...")
    print("=" * 60)
    
    try:
        # Use vectorized calculation system for performance
        print("🚀 Using FIXED vectorized & parallel calculation system...")
        final_result = calculate_base_and_scheme_metrics_vectorized(sales_df, scheme_config, json_data)
        
        print(f"\n✅ Calculations Complete!")
        print(f"📊 Result Summary:")
        print(f"   - Total accounts: {len(final_result)}")
        
        # Dynamic column checking based on calculation mode
        if 'Base 1 Volume Final' in final_result.columns:
            print(f"   - Base 1 Volume: {final_result['Base 1 Volume Final'].sum():,.2f}")
            print(f"   - Base 2 Volume: {final_result['Base 2 Volume Final'].sum():,.2f}")
            print(f"   - Total Volume: {final_result['total_volume'].sum():,.2f}")
        elif 'Base 1 Value Final' in final_result.columns:
            print(f"   - Base 1 Value: {final_result['Base 1 Value Final'].sum():,.2f}")
            print(f"   - Base 2 Value: {final_result['Base 2 Value Final'].sum():,.2f}")
            print(f"   - Total Value: {final_result['total_value'].sum():,.2f}")
        else:
            print(f"   - Columns: {list(final_result.columns)}")
        
        return final_result
        
    except Exception as e:
        print(f"❌ Error in base calculations: {str(e)}")
        raise

def _prepare_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare and clean sales data for calculations"""
    
    # Create a copy to avoid modifying original data
    sales_clean = sales_df.copy()
    
    # Standardize column names (handle potential variations)
    column_mapping = {
        'region_name': 'region',
        'so_name': 'so_name',
        'area_head_name': 'so_name'  # Fallback if so_name doesn't exist
    }
    
    # Apply column mapping
    for old_col, new_col in column_mapping.items():
        if old_col in sales_clean.columns and new_col not in sales_clean.columns:
            sales_clean[new_col] = sales_clean[old_col]
    
    # Ensure required columns exist
    required_columns = ['state_name', 'so_name', 'region', 'credit_account', 'customer_name', 'volume', 'value', 'sale_date']
    missing_columns = [col for col in required_columns if col not in sales_clean.columns]
    
    if missing_columns:
        print(f"⚠️ Missing columns detected: {missing_columns}")
        # Add missing columns with default values
        for col in missing_columns:
            if col in ['so_name', 'region']:
                sales_clean[col] = sales_clean.get('area_head_name', 'Unknown')
            else:
                sales_clean[col] = 'Unknown'
    
    # Convert sale_date to datetime
    if 'sale_date' in sales_clean.columns:
        sales_clean['sale_date'] = pd.to_datetime(sales_clean['sale_date'])
    
    # Fill NaN values
    numeric_columns = ['volume', 'value']
    for col in numeric_columns:
        if col in sales_clean.columns:
            sales_clean[col] = sales_clean[col].fillna(0)
    
    string_columns = ['state_name', 'so_name', 'region', 'customer_name']
    for col in string_columns:
        if col in sales_clean.columns:
            sales_clean[col] = sales_clean[col].fillna('Unknown')
    
    # Convert credit_account to string for consistent grouping
    if 'credit_account' in sales_clean.columns:
        sales_clean['credit_account'] = sales_clean['credit_account'].astype(str)
    
    print(f"📋 Data prepared: {len(sales_clean)} records with {len(sales_clean.columns)} columns")
    return sales_clean

def _calculate_base_period(sales_df: pd.DataFrame, base_period: Dict[str, Any], 
                          group_columns: List[str], period_name: str) -> pd.DataFrame:
    """Calculate metrics for a single base period"""
    
    from_date = pd.to_datetime(base_period['from_date'])
    to_date = pd.to_datetime(base_period['to_date'])
    sum_avg_method = base_period.get('sum_avg_method', 'sum')
    
    print(f"📅 Calculating {period_name}: {from_date.date()} to {to_date.date()} ({sum_avg_method})")
    
    # Filter data for the period
    period_data = sales_df[
        (sales_df['sale_date'] >= from_date) & 
        (sales_df['sale_date'] <= to_date)
    ].copy()
    
    print(f"   📊 Period records: {len(period_data)}")
    
    if period_data.empty:
        print(f"   ⚠️ No data found for {period_name}")
        return _create_empty_period_result(group_columns, period_name)
    
    # Group and aggregate
    aggregated = period_data.groupby(group_columns).agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    # Apply averaging if required
    if sum_avg_method == 'average':
        # Calculate number of days in period
        num_days = (to_date - from_date).days + 1
        aggregated['volume'] = aggregated['volume'] / num_days
        aggregated['value'] = aggregated['value'] / num_days
        print(f"   📊 Applied averaging over {num_days} days")
    
    # Rename columns to final format
    column_mapping = {
        'volume': f'{period_name} Volume Final',
        'value': f'{period_name} Value Final'
    }
    aggregated.rename(columns=column_mapping, inplace=True)
    
    print(f"   ✅ {period_name}: {len(aggregated)} accounts processed")
    return aggregated

def _calculate_scheme_period(sales_df: pd.DataFrame, scheme_from: str, scheme_to: str,
                           group_columns: List[str]) -> pd.DataFrame:
    """Calculate total metrics for scheme period"""
    
    from_date = pd.to_datetime(scheme_from)
    to_date = pd.to_datetime(scheme_to)
    
    print(f"📅 Calculating Scheme Period: {from_date.date()} to {to_date.date()}")
    
    # Filter data for the scheme period
    scheme_data = sales_df[
        (sales_df['sale_date'] >= from_date) & 
        (sales_df['sale_date'] <= to_date)
    ].copy()
    
    print(f"   📊 Scheme period records: {len(scheme_data)}")
    
    if scheme_data.empty:
        print(f"   ⚠️ No data found for scheme period")
        return _create_empty_scheme_result(group_columns)
    
    # Group and aggregate
    aggregated = scheme_data.groupby(group_columns).agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    # Rename columns to final format
    aggregated.rename(columns={
        'volume': 'total_volume',
        'value': 'total_value'
    }, inplace=True)
    
    print(f"   ✅ Scheme Period: {len(aggregated)} accounts processed")
    return aggregated

def _merge_calculation_results(base1_result: pd.DataFrame, base2_result: pd.DataFrame,
                              scheme_result: pd.DataFrame, group_columns: List[str]) -> pd.DataFrame:
    """Merge all calculation results into final DataFrame"""
    
    print("🔗 Merging calculation results...")
    
    # Start with base1 as the foundation
    final_result = base1_result.copy()
    
    # Merge base2 results
    final_result = final_result.merge(
        base2_result, on=group_columns, how='outer'
    )
    
    # Merge scheme results
    final_result = final_result.merge(
        scheme_result, on=group_columns, how='outer'
    )
    
    # Fill missing values with 0
    numeric_columns = [
        'Base 1 Volume Final', 'Base 1 Value Final',
        'Base 2 Volume Final', 'Base 2 Value Final',
        'total_volume', 'total_value'
    ]
    
    for col in numeric_columns:
        if col in final_result.columns:
            final_result[col] = final_result[col].fillna(0)
    
    # Fill missing string values
    string_columns = ['state_name', 'so_name', 'region', 'customer_name']
    for col in string_columns:
        if col in final_result.columns:
            final_result[col] = final_result[col].fillna('Unknown')
    
    # Ensure proper column order
    expected_columns = [
        'state_name', 'so_name', 'region', 'credit_account', 'customer_name',
        'Base 1 Volume Final', 'Base 1 Value Final',
        'Base 2 Volume Final', 'Base 2 Value Final',
        'total_volume', 'total_value'
    ]
    
    # Reorder columns (keep only expected ones)
    final_columns = [col for col in expected_columns if col in final_result.columns]
    final_result = final_result[final_columns]
    
    print(f"   ✅ Merged results: {len(final_result)} total accounts")
    return final_result

def _create_empty_result_df() -> pd.DataFrame:
    """Create empty result DataFrame with proper structure"""
    columns = [
        'state_name', 'so_name', 'region', 'credit_account', 'customer_name',
        'Base 1 Volume Final', 'Base 1 Value Final',
        'Base 2 Volume Final', 'Base 2 Value Final',
        'total_volume', 'total_value'
    ]
    return pd.DataFrame(columns=columns)

def _create_empty_period_result(group_columns: List[str], period_name: str) -> pd.DataFrame:
    """Create empty period result DataFrame"""
    columns = group_columns + [f'{period_name} Volume Final', f'{period_name} Value Final']
    return pd.DataFrame(columns=columns)

def _create_empty_scheme_result(group_columns: List[str]) -> pd.DataFrame:
    """Create empty scheme result DataFrame"""
    columns = group_columns + ['total_volume', 'total_value']
    return pd.DataFrame(columns=columns)

def _get_credit_accounts_from_base_periods(sales_df: pd.DataFrame, base_periods: List[Dict]) -> List[str]:
    """Get all unique credit accounts that had transactions in any base period"""
    
    all_credit_accounts = set()
    
    for i, base_period in enumerate(base_periods):
        from_date = pd.to_datetime(base_period['from_date'])
        to_date = pd.to_datetime(base_period['to_date'])
        
        # Filter data for this base period
        period_data = sales_df[
            (sales_df['sale_date'] >= from_date) & 
            (sales_df['sale_date'] <= to_date)
        ]
        
        # Get unique credit accounts from this period
        period_accounts = set(period_data['credit_account'].unique())
        all_credit_accounts.update(period_accounts)
        
        print(f"   📋 Base Period {i+1}: {len(period_accounts)} unique accounts")
    
    return sorted(list(all_credit_accounts))

def _select_higher_base_period(sales_df: pd.DataFrame, base_periods: List[Dict]) -> tuple:
    """Select the base period with higher total volume"""
    
    if len(base_periods) == 1:
        return base_periods[0], "Base 1 (only period)"
    
    period_volumes = []
    
    for i, base_period in enumerate(base_periods):
        from_date = pd.to_datetime(base_period['from_date'])
        to_date = pd.to_datetime(base_period['to_date'])
        
        # Calculate total volume for this period
        period_data = sales_df[
            (sales_df['sale_date'] >= from_date) & 
            (sales_df['sale_date'] <= to_date)
        ]
        
        total_volume = period_data['volume'].sum()
        period_volumes.append(total_volume)
        
        print(f"   📊 Base Period {i+1} total volume: {total_volume:,.2f}")
    
    # Select period with higher volume
    if period_volumes[0] >= period_volumes[1]:
        selected_period = base_periods[0]
        period_name = f"Base 1 (higher volume: {period_volumes[0]:,.2f})"
    else:
        selected_period = base_periods[1] 
        period_name = f"Base 2 (higher volume: {period_volumes[1]:,.2f})"
    
    return selected_period, period_name

def _calculate_base_metrics_for_accounts(sales_df: pd.DataFrame, base_period: Dict, 
                                       credit_accounts: List[str], period_name: str) -> pd.DataFrame:
    """Calculate base metrics for all specified credit accounts"""
    
    from_date = pd.to_datetime(base_period['from_date'])
    to_date = pd.to_datetime(base_period['to_date'])
    sum_avg_method = base_period.get('sum_avg_method', 'sum')
    
    print(f"📅 Calculating base metrics: {from_date.date()} to {to_date.date()}")
    
    # Filter data for the base period
    base_data = sales_df[
        (sales_df['sale_date'] >= from_date) & 
        (sales_df['sale_date'] <= to_date)
    ].copy()
    
    results = []
    
    for credit_account in credit_accounts:
        # Get account data for base period
        account_data = base_data[base_data['credit_account'] == credit_account]
        
        if account_data.empty:
            # Account had no transactions in base period
            base_volume = 0
            base_value = 0
            state_name = 'Unknown'
            so_name = 'Unknown'
            region = 'Unknown'
            customer_name = 'Unknown'
        else:
            # Calculate base metrics
            base_volume = account_data['volume'].sum()
            base_value = account_data['value'].sum()
            
            # Apply averaging if required
            if sum_avg_method == 'average':
                num_days = (to_date - from_date).days + 1
                base_volume = base_volume / num_days
                base_value = base_value / num_days
            
            # Get account details (use first record)
            first_record = account_data.iloc[0]
            state_name = first_record.get('state_name', 'Unknown')
            so_name = first_record.get('so_name', 'Unknown')
            region = first_record.get('region', 'Unknown')
            customer_name = first_record.get('customer_name', 'Unknown')
        
        results.append({
            'credit_account': credit_account,
            'state_name': state_name,
            'so_name': so_name,
            'region': region,
            'customer_name': customer_name,
            'Base 1 Volume Final': base_volume,
            'Base 1 Value Final': base_value,
            'Base 2 Volume Final': base_volume,  # Same as Base 1
            'Base 2 Value Final': base_value     # Same as Base 1
        })
    
    result_df = pd.DataFrame(results)
    print(f"   ✅ Base metrics calculated for {len(result_df)} accounts")
    return result_df

def _calculate_scheme_metrics_for_accounts(sales_df: pd.DataFrame, scheme_from: str, scheme_to: str,
                                         credit_accounts: List[str]) -> pd.DataFrame:
    """Calculate scheme period metrics for all specified credit accounts"""
    
    from_date = pd.to_datetime(scheme_from)
    to_date = pd.to_datetime(scheme_to)
    
    print(f"📅 Calculating scheme metrics: {from_date.date()} to {to_date.date()}")
    
    # Filter data for the scheme period
    scheme_data = sales_df[
        (sales_df['sale_date'] >= from_date) & 
        (sales_df['sale_date'] <= to_date)
    ].copy()
    
    results = []
    
    for credit_account in credit_accounts:
        # Get account data for scheme period
        account_data = scheme_data[scheme_data['credit_account'] == credit_account]
        
        if account_data.empty:
            # Account had no transactions in scheme period
            scheme_volume = 0
            scheme_value = 0
        else:
            # Calculate scheme metrics
            scheme_volume = account_data['volume'].sum()
            scheme_value = account_data['value'].sum()
        
        results.append({
            'credit_account': credit_account,
            'total_volume': scheme_volume,
            'total_value': scheme_value
        })
    
    result_df = pd.DataFrame(results)
    print(f"   ✅ Scheme metrics calculated for {len(result_df)} accounts")
    return result_df

def _merge_base_and_scheme_results(base_result: pd.DataFrame, scheme_result: pd.DataFrame) -> pd.DataFrame:
    """Merge base and scheme results"""
    
    print("🔗 Merging base and scheme results...")
    
    # Merge on credit_account
    merged_result = base_result.merge(scheme_result, on='credit_account', how='left')
    
    # Fill missing values with 0
    merged_result['total_volume'] = merged_result['total_volume'].fillna(0)
    merged_result['total_value'] = merged_result['total_value'].fillna(0)
    
    print(f"   ✅ Merged {len(merged_result)} account records")
    return merged_result

def _add_scheme_metadata_columns(result_df: pd.DataFrame, scheme_config: Dict, json_data: Dict = None) -> pd.DataFrame:
    """Add scheme metadata columns at the beginning"""
    
    print("📋 Adding scheme metadata columns...")
    
    # Extract basic scheme info
    scheme_id = scheme_config.get('scheme_id', 'Unknown')
    scheme_from = scheme_config.get('scheme_from', 'Unknown')
    scheme_to = scheme_config.get('scheme_to', 'Unknown')
    calculation_mode = scheme_config.get('calculation_mode', 'volume')
    
    # Default values
    scheme_name = 'Main Scheme'
    mandatory_qualify = 'Unknown'
    
    # Extract from JSON if available
    if json_data:
        basic_info = json_data.get('basicInfo', {})
        main_scheme = json_data.get('mainScheme', {})
        
        scheme_name = basic_info.get('schemeTitle', 'Main Scheme')
        mandatory_qualify = 'Yes' if main_scheme.get('mandatoryQualify', False) else 'No'
    
    # Add main scheme columns
    result_df.insert(0, 'Scheme ID', scheme_id)
    result_df.insert(1, 'Scheme Name', scheme_name)
    result_df.insert(2, 'Scheme Type', calculation_mode)
    result_df.insert(3, 'Scheme Period From', scheme_from)
    result_df.insert(4, 'Scheme Period To', scheme_to)
    result_df.insert(5, 'Mandatory Qualify', mandatory_qualify)
    
    # Add additional scheme columns if they exist
    if json_data and 'additionalSchemes' in json_data:
        additional_schemes = json_data['additionalSchemes']
        
        for i, add_scheme in enumerate(additional_schemes, 1):
            prefix = f"_p{i}"
            
            # Get scheme name from schemeNumber if available, fallback to schemeTitle
            add_scheme_name = add_scheme.get('schemeNumber', add_scheme.get('schemeTitle', f'Additional Scheme {i}'))
            add_mandatory_qualify = 'Yes' if add_scheme.get('mandatoryQualify', False) else 'No'
            
            result_df[f'Scheme Name{prefix}'] = add_scheme_name
            result_df[f'Mandatory Qualify{prefix}'] = add_mandatory_qualify
    
    print(f"   ✅ Added scheme metadata columns")
    return result_df

def _calculate_with_volume_value_logic(sales_df: pd.DataFrame, base_periods: List[Dict], 
                                     scheme_from: str, scheme_to: str, scheme_config: Dict, 
                                     json_data: Dict = None) -> pd.DataFrame:
    """
    Calculate with proper Volume/Value logic and product filtering
    """
    print("🧮 Implementing Volume/Value calculation logic...")
    
    # Get product-filtered credit accounts
    filtered_accounts = _get_product_filtered_accounts(sales_df, json_data)
    print(f"📋 Found {len(filtered_accounts)} accounts with matching product data")
    
    # Determine calculation mode
    calculation_mode = scheme_config.get('calculation_mode', 'volume')
    is_volume_based = calculation_mode.lower() == 'volume'
    
    print(f"📊 Calculation mode: {calculation_mode.upper()}")
    
    results = []
    
    for account in filtered_accounts:
        # Ensure consistent data type matching
        account_data = sales_df[sales_df['credit_account'].astype(str) == str(account)]
        
        if account_data.empty:
            continue
            
        # Get account details
        first_record = account_data.iloc[0]
        account_info = {
            'credit_account': account,
            'state_name': first_record.get('state_name', 'Unknown'),
            'so_name': first_record.get('so_name', 'Unknown'),
            'region': first_record.get('region', 'Unknown'),
            'customer_name': first_record.get('customer_name', 'Unknown')
        }
        
        # Calculate base periods
        base_results = _calculate_base_periods_with_logic(account_data, base_periods, is_volume_based)
        
        # Calculate scheme period (not needed for final columns but for validation)
        scheme_results = _calculate_scheme_period_data(account_data, scheme_from, scheme_to, is_volume_based)
        
        # Merge all results
        final_account_data = {**account_info, **base_results}
        results.append(final_account_data)
    
    # Create DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        print("⚠️ No accounts with matching product data found")
        return _create_empty_result_df_new()
    
    # Add scheme metadata columns
    result_df = _add_scheme_metadata_columns_new(result_df, scheme_config, json_data, is_volume_based)
    
    # Add additional scheme columns if they exist
    result_df = _add_additional_scheme_columns(result_df, json_data, is_volume_based, sales_df, base_periods)
    
    return result_df

def _get_product_filtered_accounts(sales_df: pd.DataFrame, json_data: Dict = None) -> List[str]:
    """Get credit accounts that have products matching is_product_data = True"""
    
    if not json_data:
        # If no JSON data, return all accounts
        return sorted(sales_df['credit_account'].unique().tolist())
    
    # Get materials from main scheme product data where is_product_data = True
    main_scheme = json_data.get('mainScheme', {})
    product_data = main_scheme.get('productData', {})
    
    # Collect all materials from product data
    product_materials = set()
    
    # Check all product types
    for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                        'wandaGroups', 'productNames', 'thinnerGroups']:
        if product_type in product_data:
            product_materials.update(product_data[product_type])
    
    # Filter sales data by these materials
    filtered_sales = sales_df[sales_df['material'].isin(product_materials)]
    
    # Get unique credit accounts - ensure consistent string conversion
    filtered_accounts = sorted([str(acc) for acc in filtered_sales['credit_account'].unique().tolist()])
    
    print(f"   📋 Product materials found: {len(product_materials)}")
    print(f"   📋 Accounts with product data: {len(filtered_accounts)}")
    
    return filtered_accounts

def _calculate_base_periods_with_logic(account_data: pd.DataFrame, base_periods: List[Dict], 
                                     is_volume_based: bool) -> Dict:
    """Calculate base periods with proper Volume/Value logic"""
    
    results = {}
    metric_name = 'volume' if is_volume_based else 'value'
    
    # Calculate Base Period 1
    if len(base_periods) >= 1:
        base1_data = _calculate_single_base_period(account_data, base_periods[0], metric_name, "Base 1")
        results.update(base1_data)
    
    # Calculate Base Period 2
    if len(base_periods) >= 2:
        base2_data = _calculate_single_base_period(account_data, base_periods[1], metric_name, "Base 2")
        results.update(base2_data)
    else:
        # Copy Base 1 to Base 2
        if f'Base 1 {metric_name.title()} Final' in results:
            results[f'Base 2 {metric_name.title()} Final'] = results[f'Base 1 {metric_name.title()} Final']
        else:
            results[f'Base 2 {metric_name.title()} Final'] = 0
    
    # Calculate total (maximum of Base 1 and Base 2)
    base1_final = results.get(f'Base 1 {metric_name.title()} Final', 0)
    base2_final = results.get(f'Base 2 {metric_name.title()} Final', 0)
    results[f'total_{metric_name}'] = max(base1_final, base2_final)
    
    return results

def _calculate_single_base_period(account_data: pd.DataFrame, base_period: Dict, 
                                metric_name: str, period_name: str) -> Dict:
    """Calculate single base period with SUM/AVG logic"""
    
    from_date = pd.to_datetime(base_period['from_date'])
    to_date = pd.to_datetime(base_period['to_date'])
    sum_avg_method = base_period.get('sum_avg_method', 'sum')
    
    # Filter data for this period
    period_data = account_data[
        (account_data['sale_date'] >= from_date) & 
        (account_data['sale_date'] <= to_date)
    ]
    
    if period_data.empty:
        return {f'{period_name} {metric_name.title()} Final': 0}
    
    # Calculate base metric
    base_metric = period_data[metric_name].sum()
    
    # Apply SUM/AVG logic
    if sum_avg_method.lower() == 'average':
        # Calculate number of months
        num_months = ((to_date.year - from_date.year) * 12 + to_date.month - from_date.month) + 1
        base_final = base_metric / num_months if num_months > 0 else base_metric
    else:
        # SUM method - divide by 1
        base_final = base_metric / 1
    
    return {f'{period_name} {metric_name.title()} Final': base_final}

def _calculate_scheme_period_data(account_data: pd.DataFrame, scheme_from: str, scheme_to: str, 
                                is_volume_based: bool) -> Dict:
    """Calculate scheme period data for validation"""
    
    from_date = pd.to_datetime(scheme_from)
    to_date = pd.to_datetime(scheme_to)
    metric_name = 'volume' if is_volume_based else 'value'
    
    # Filter data for scheme period
    scheme_data = account_data[
        (account_data['sale_date'] >= from_date) & 
        (account_data['sale_date'] <= to_date)
    ]
    
    if scheme_data.empty:
        return {f'scheme_{metric_name}': 0}
    
    scheme_metric = scheme_data[metric_name].sum()
    return {f'scheme_{metric_name}': scheme_metric}

def _add_scheme_metadata_columns_new(result_df: pd.DataFrame, scheme_config: Dict, 
                                   json_data: Dict = None, is_volume_based: bool = True) -> pd.DataFrame:
    """Add scheme metadata columns with proper naming"""
    
    print("📋 Adding scheme metadata columns...")
    
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
    
    # Add main scheme columns
    result_df.insert(0, 'Scheme ID', scheme_id)
    result_df.insert(1, 'Scheme Name', scheme_name)
    result_df.insert(2, 'Scheme Type', calculation_mode)
    result_df.insert(3, 'Scheme Period From', scheme_from)
    result_df.insert(4, 'Scheme Period To', scheme_to)
    result_df.insert(5, 'Mandatory Qualify', mandatory_qualify)
    
    return result_df

def _add_additional_scheme_columns(result_df: pd.DataFrame, json_data: Dict = None, 
                                 is_volume_based: bool = True, sales_df: pd.DataFrame = None,
                                 base_periods: List[Dict] = None) -> pd.DataFrame:
    """Add additional scheme columns with prefixes and calculate for their specific products"""
    
    if not json_data or 'additionalSchemes' not in json_data:
        return result_df
    
    additional_schemes = json_data['additionalSchemes']
    print(f"📋 Adding {len(additional_schemes)} additional scheme columns...")
    
    for i, add_scheme in enumerate(additional_schemes, 1):
        prefix = f"_p{i}"
        
        # Get scheme name from schemeNumber if available, fallback to schemeTitle
        add_scheme_name = add_scheme.get('schemeNumber', add_scheme.get('schemeTitle', f'Additional Scheme {i}'))
        add_mandatory_qualify = 'Yes' if add_scheme.get('mandatoryQualify', False) else 'No'
        
        # Add metadata columns for additional scheme
        result_df[f'Scheme Name{prefix}'] = add_scheme_name
        result_df[f'Scheme Type{prefix}'] = 'Volume' if is_volume_based else 'Value'
        result_df[f'Mandatory Qualify{prefix}'] = add_mandatory_qualify
        
        # Get product data for this additional scheme
        add_product_data = add_scheme.get('productData', {}).get('mainScheme', {})
        add_product_materials = set()
        
        # Collect all materials from additional scheme product data
        for product_type in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                            'wandaGroups', 'productNames', 'thinnerGroups']:
            if product_type in add_product_data:
                add_product_materials.update(add_product_data[product_type])
        
        print(f"   📋 Additional Scheme {i}: {len(add_product_materials)} product materials")
        
        # 🔧 FIXED: Calculate BOTH volume and value for additional schemes (like main scheme)
        # Don't limit to just the scheme's primary metric name
        
        # Calculate for each account based on additional scheme products
        if sales_df is not None and base_periods is not None and add_product_materials:
            result_df = _calculate_additional_scheme_metrics_both(
                result_df, sales_df, base_periods, add_product_materials, prefix
            )
        else:
            # Add placeholder columns if no calculation possible - BOTH volume and value
            result_df[f'Base 1 Volume Final{prefix}'] = 0
            result_df[f'Base 2 Volume Final{prefix}'] = 0
            result_df[f'total_volume{prefix}'] = 0
            result_df[f'Base 1 Value Final{prefix}'] = 0
            result_df[f'Base 2 Value Final{prefix}'] = 0
            result_df[f'total_value{prefix}'] = 0
    
    return result_df

def _calculate_additional_scheme_metrics_both(result_df: pd.DataFrame, sales_df: pd.DataFrame,
                                            base_periods: List[Dict], product_materials: set,
                                            prefix: str) -> pd.DataFrame:
    """🔧 FIXED: Calculate BOTH volume and value metrics for additional scheme based on its specific products"""
    
    # Filter sales data by additional scheme products
    filtered_sales = sales_df[sales_df['material'].isin(product_materials)]
    
    if filtered_sales.empty:
        # No sales for these products, set all to 0 - BOTH volume and value
        result_df[f'Base 1 Volume Final{prefix}'] = 0
        result_df[f'Base 2 Volume Final{prefix}'] = 0
        result_df[f'total_volume{prefix}'] = 0
        result_df[f'Base 1 Value Final{prefix}'] = 0
        result_df[f'Base 2 Value Final{prefix}'] = 0
        result_df[f'total_value{prefix}'] = 0
        return result_df
    
    # Calculate for each account in result_df
    for idx, row in result_df.iterrows():
        credit_account = str(row['credit_account'])
        account_sales = filtered_sales[filtered_sales['credit_account'].astype(str) == credit_account]
        
        if account_sales.empty:
            # No sales for this account with additional scheme products - BOTH volume and value
            result_df.at[idx, f'Base 1 Volume Final{prefix}'] = 0
            result_df.at[idx, f'Base 2 Volume Final{prefix}'] = 0
            result_df.at[idx, f'total_volume{prefix}'] = 0
            result_df.at[idx, f'Base 1 Value Final{prefix}'] = 0
            result_df.at[idx, f'Base 2 Value Final{prefix}'] = 0
            result_df.at[idx, f'total_value{prefix}'] = 0
            continue
        
        # Calculate Base Period 1 - BOTH volume and value
        base1_volume = _calculate_single_period_for_account(
            account_sales, base_periods[0] if base_periods else None, 'volume'
        )
        base1_value = _calculate_single_period_for_account(
            account_sales, base_periods[0] if base_periods else None, 'value'
        )
        
        # Calculate Base Period 2 (or copy Base 1) - BOTH volume and value
        if len(base_periods) >= 2:
            base2_volume = _calculate_single_period_for_account(
                account_sales, base_periods[1], 'volume'
            )
            base2_value = _calculate_single_period_for_account(
                account_sales, base_periods[1], 'value'
            )
        else:
            base2_volume = base1_volume
            base2_value = base1_value
        
        # Set ALL values with proper float conversion
        result_df.at[idx, f'Base 1 Volume Final{prefix}'] = float(base1_volume)
        result_df.at[idx, f'Base 2 Volume Final{prefix}'] = float(base2_volume)
        result_df.at[idx, f'total_volume{prefix}'] = float(max(base1_volume, base2_volume))
        result_df.at[idx, f'Base 1 Value Final{prefix}'] = float(base1_value)
        result_df.at[idx, f'Base 2 Value Final{prefix}'] = float(base2_value)
        result_df.at[idx, f'total_value{prefix}'] = float(max(base1_value, base2_value))
    
    return result_df

def _calculate_additional_scheme_metrics(result_df: pd.DataFrame, sales_df: pd.DataFrame,
                                       base_periods: List[Dict], product_materials: set,
                                       metric_name: str, prefix: str, is_volume_based: bool) -> pd.DataFrame:
    """Calculate metrics for additional scheme based on its specific products"""
    
    # Filter sales data by additional scheme products
    filtered_sales = sales_df[sales_df['material'].isin(product_materials)]
    
    if filtered_sales.empty:
        # No sales for these products, set all to 0
        result_df[f'Base 1 {metric_name} Final{prefix}'] = 0
        result_df[f'Base 2 {metric_name} Final{prefix}'] = 0
        result_df[f'total_{metric_name.lower()}{prefix}'] = 0
        return result_df
    
    metric_col = 'volume' if is_volume_based else 'value'
    
    # Calculate for each account in result_df
    for idx, row in result_df.iterrows():
        credit_account = str(row['credit_account'])
        account_sales = filtered_sales[filtered_sales['credit_account'].astype(str) == credit_account]
        
        if account_sales.empty:
            # No sales for this account with additional scheme products
            result_df.at[idx, f'Base 1 {metric_name} Final{prefix}'] = 0
            result_df.at[idx, f'Base 2 {metric_name} Final{prefix}'] = 0
            result_df.at[idx, f'total_{metric_name.lower()}{prefix}'] = 0
            continue
        
        # Calculate Base Period 1
        base1_value = _calculate_single_period_for_account(
            account_sales, base_periods[0] if base_periods else None, metric_col
        )
        
        # Calculate Base Period 2 (or copy Base 1)
        if len(base_periods) >= 2:
            base2_value = _calculate_single_period_for_account(
                account_sales, base_periods[1], metric_col
            )
        else:
            base2_value = base1_value
        
        # Set values with proper float conversion
        result_df.at[idx, f'Base 1 {metric_name} Final{prefix}'] = float(base1_value)
        result_df.at[idx, f'Base 2 {metric_name} Final{prefix}'] = float(base2_value)
        result_df.at[idx, f'total_{metric_name.lower()}{prefix}'] = float(max(base1_value, base2_value))
    
    return result_df

def _calculate_single_period_for_account(account_sales: pd.DataFrame, base_period: Dict, 
                                       metric_col: str) -> float:
    """Calculate single period metric for an account"""
    
    if base_period is None or account_sales.empty:
        return 0.0
    
    from_date = pd.to_datetime(base_period['from_date'])
    to_date = pd.to_datetime(base_period['to_date'])
    sum_avg_method = base_period.get('sum_avg_method', 'sum')
    
    # Filter data for this period
    period_data = account_sales[
        (account_sales['sale_date'] >= from_date) & 
        (account_sales['sale_date'] <= to_date)
    ]
    
    if period_data.empty:
        return 0.0
    
    # Calculate base metric
    base_metric = period_data[metric_col].sum()
    
    # Apply SUM/AVG logic
    if sum_avg_method.lower() == 'average':
        # Calculate number of months
        num_months = ((to_date.year - from_date.year) * 12 + to_date.month - from_date.month) + 1
        return base_metric / num_months if num_months > 0 else base_metric
    else:
        # SUM method - divide by 1
        return base_metric

def _create_empty_result_df_new() -> pd.DataFrame:
    """Create empty result DataFrame with new structure"""
    columns = [
        'Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To',
        'Mandatory Qualify', 'credit_account', 'state_name', 'so_name', 'region', 'customer_name',
        'Base 1 Volume Final', 'Base 2 Volume Final', 'total_volume'
    ]
    return pd.DataFrame(columns=columns)
