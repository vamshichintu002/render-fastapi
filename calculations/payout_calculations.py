"""
Payout Calculations Module
Handles vectorized payout calculations for main and additional schemes
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def calculate_payout_columns_vectorized(tracker_df: pd.DataFrame, structured_data: Dict, 
                                      sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate all payout-related columns for main and additional schemes
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing slabs, products, etc.
        sales_df: Sales data for payout product calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with payout columns
    """
    print("🧮 Calculating payout columns (vectorized)...")
    
    # Add main scheme payout columns
    tracker_df = _add_main_scheme_payout_columns(tracker_df, structured_data, sales_df, scheme_config)
    
    # Add additional scheme payout columns
    tracker_df = _add_additional_scheme_payout_columns(tracker_df, structured_data, sales_df, scheme_config)
    
    print("✅ Payout calculations completed")
    return tracker_df


def _add_main_scheme_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                   sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add payout columns for main scheme"""
    print("   📊 Adding main scheme payout columns...")
    
    # Get main scheme slabs
    slabs_df = structured_data['slabs']
    main_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
    
    if main_slabs.empty:
        print("   ⚠️ No main scheme slabs found - skipping main payout calculations")
        return tracker_df
    
    print(f"   📋 Found {len(main_slabs)} main scheme slabs")
    
    # Determine if main scheme is volume or value based
    scheme_info = structured_data['scheme_info']
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    
    if main_scheme_info.empty:
        print("   ⚠️ No main scheme info found - defaulting to volume")
        is_volume_based = True
    else:
        volume_value_based = main_scheme_info.iloc[0]['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
    
    print(f"   📊 Main scheme type: {'Volume' if is_volume_based else 'Value'}")
    
    # Get the base column for slab lookup
    base_column = 'total_volume' if is_volume_based else 'total_value'
    
    if base_column not in tracker_df.columns:
        print(f"   ⚠️ Base column {base_column} not found - skipping main payout calculations")
        return tracker_df
    
    # Apply slab-based payout calculations
    tracker_df = _apply_slab_payout_calculations(
        tracker_df, main_slabs, base_column, '', 
        structured_data, sales_df, scheme_config, 'main_scheme'
    )
    
    return tracker_df


def _add_additional_scheme_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                        sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add payout columns for additional schemes"""
    print("   📊 Adding additional scheme payout columns...")
    
    # Get additional scheme slabs
    slabs_df = structured_data['slabs']
    additional_slabs = slabs_df[slabs_df['scheme_type'] == 'additional_scheme'].copy()
    
    if additional_slabs.empty:
        print("   ℹ️ No additional scheme slabs found - skipping additional payout calculations")
        return tracker_df
    
    # Get unique additional scheme names/numbers
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        # Extract scheme number (e.g., 'additional_scheme_1' -> 1)
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        
        print(f"   📋 Processing additional scheme {scheme_num}")
        
        # Determine if this additional scheme is volume or value based
        volume_value_based = scheme_row['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
        
        print(f"   📊 Additional scheme {scheme_num} type: {'Volume' if is_volume_based else 'Value'}")
        
        # Get the base column for this additional scheme
        base_column = f'total_volume_p{scheme_num}' if is_volume_based else f'total_value_p{scheme_num}'
        
        if base_column not in tracker_df.columns:
            print(f"   ⚠️ Base column {base_column} not found - skipping additional scheme {scheme_num}")
            continue
        
        # Get slabs for this specific additional scheme
        scheme_slabs = additional_slabs[additional_slabs['scheme_name'].str.contains(f'Additional Scheme {scheme_num}', na=False)]
        
        if scheme_slabs.empty:
            print(f"   ⚠️ No slabs found for additional scheme {scheme_num}")
            continue
        
        # Apply slab-based payout calculations with _p{scheme_num} suffix
        tracker_df = _apply_slab_payout_calculations(
            tracker_df, scheme_slabs, base_column, f'_p{scheme_num}', 
            structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_num}'
        )
    
    return tracker_df


def _apply_slab_payout_calculations(tracker_df: pd.DataFrame, slabs: pd.DataFrame, 
                                  base_column: str, suffix: str, structured_data: Dict,
                                  sales_df: pd.DataFrame, scheme_config: Dict, scheme_type: str) -> pd.DataFrame:
    """Apply vectorized slab-based payout calculations"""
    
    print(f"   🧮 Applying payout calculations for {scheme_type} (base: {base_column})")
    
    # Sort slabs by slab_start for proper lookup
    slabs_sorted = slabs.sort_values('slab_start').copy()
    
    # Initialize payout columns
    rebate_per_litre_col = f'Rebate_per_Litre{suffix}'
    rebate_percent_col = f'Rebate_percent{suffix}'
    basic_payout_col = f'basic_payout{suffix}'
    additional_rebate_growth_col = f'Additional_Rebate_on_Growth_per_Litre{suffix}'
    additional_payout_col = f'additional_payout{suffix}'
    fixed_rebate_col = f'Fixed_Rebate{suffix}'
    payout_volume_col = f'Payout_Products_Volume{suffix}'
    payout_value_col = f'Payout_Products_Value{suffix}'
    payout_product_payout_col = f'Payout_Product_Payout{suffix}'
    total_payout_col = f'Total_Payout{suffix}'
    
    # Initialize all columns with zeros
    for col in [rebate_per_litre_col, rebate_percent_col, basic_payout_col, 
                additional_rebate_growth_col, additional_payout_col, fixed_rebate_col,
                payout_volume_col, payout_value_col, payout_product_payout_col, total_payout_col]:
        tracker_df[col] = 0.0
    
    # Apply vectorized slab lookup for each slab
    base_values = tracker_df[base_column].values
    
    # Track which accounts have been matched to slabs
    matched_accounts = pd.Series([False] * len(tracker_df), index=tracker_df.index)
    
    # Get first slab values for fallback
    first_slab = slabs_sorted.iloc[0] if not slabs_sorted.empty else None
    
    for _, slab_row in slabs_sorted.iterrows():
        slab_start = float(slab_row['slab_start'])
        slab_end = float(slab_row['slab_end'])
        
        # Create mask for values in this slab range
        mask = (base_values >= slab_start) & (base_values <= slab_end)
        
        if mask.sum() > 0:
            print(f"     📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts")
            
            # Apply slab values to matching accounts
            tracker_df.loc[mask, rebate_per_litre_col] = float(slab_row['rebate_per_litre'])
            tracker_df.loc[mask, rebate_percent_col] = float(slab_row['rebate_percent'])
            tracker_df.loc[mask, additional_rebate_growth_col] = float(slab_row['additional_rebate_on_growth'])
            tracker_df.loc[mask, fixed_rebate_col] = float(slab_row['fixed_rebate'])
            
            # Mark these accounts as matched
            matched_accounts[mask] = True
    
    # Apply fallback for unmatched accounts using first slab values
    if first_slab is not None:
        unmatched_mask = ~matched_accounts
        unmatched_count = unmatched_mask.sum()
        
        if unmatched_count > 0:
            print(f"     🔧 Applying first slab fallback for {unmatched_count} unmatched accounts")
            
            # Apply first slab values as fallback
            tracker_df.loc[unmatched_mask, rebate_per_litre_col] = float(first_slab['rebate_per_litre'])
            tracker_df.loc[unmatched_mask, rebate_percent_col] = float(first_slab['rebate_percent'])
            tracker_df.loc[unmatched_mask, additional_rebate_growth_col] = float(first_slab['additional_rebate_on_growth'])
            tracker_df.loc[unmatched_mask, fixed_rebate_col] = float(first_slab['fixed_rebate'])
            
            print(f"       📌 Fallback values applied: Rebate/L={first_slab['rebate_per_litre']}, Rebate%={first_slab['rebate_percent']}, Additional={first_slab['additional_rebate_on_growth']}, Fixed={first_slab['fixed_rebate']}")
    
    # Calculate payout product volumes and values
    tracker_df = _calculate_scheme_payout_actuals(tracker_df, structured_data, sales_df, scheme_config, scheme_type, suffix)
    
    # Calculate derived payout columns using vectorized operations
    payout_value_actual_col = f'payoutproductactualvalue{suffix}' if suffix else 'payoutproductactualvalue'
    payout_volume_actual_col = f'payoutproductactualvolume{suffix}' if suffix else 'payoutproductactualvolume'
    
    # Ensure the payout actual columns exist (they should be created by _calculate_payout_product_actuals)
    if payout_value_actual_col in tracker_df.columns:
        tracker_df[basic_payout_col] = (tracker_df[rebate_percent_col] * tracker_df[payout_value_actual_col]) / 100.0
    
    if payout_volume_actual_col in tracker_df.columns:
        tracker_df[additional_payout_col] = tracker_df[additional_rebate_growth_col] * tracker_df[payout_volume_actual_col]
    
    # Calculate final payout columns
    tracker_df[payout_product_payout_col] = tracker_df[basic_payout_col] + tracker_df[additional_payout_col]
    tracker_df[total_payout_col] = tracker_df[payout_product_payout_col] + tracker_df[fixed_rebate_col]
    
    print(f"   ✅ Payout calculations completed for {scheme_type}")
    
    return tracker_df


def _calculate_payout_product_actuals(tracker_df: pd.DataFrame, structured_data: Dict, 
                                    sales_df: pd.DataFrame, scheme_config: Dict, 
                                    scheme_type: str, suffix: str) -> pd.DataFrame:
    """Calculate actual volume/value for payout products during scheme period"""
    
    print(f"   📊 Calculating payout product actuals for {scheme_type}")
    
    # Get payout products for this scheme
    products_df = structured_data['products']
    
    if scheme_type == 'main_scheme':
        payout_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') & 
            (products_df['is_payout_product'] == True)
        ]
    else:
        # For additional schemes, extract scheme number
        scheme_num = suffix.replace('_p', '') if suffix else '1'
        payout_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') & 
            (products_df['scheme_name'] == f'Additional Scheme {scheme_num}') &
            (products_df['is_payout_product'] == True)
        ]
    
    print(f"   📋 Found {len(payout_products)} payout products for {scheme_type}")
    
    if payout_products.empty:
        # No payout products - set columns to 0
        tracker_df[f'Payout_Products_Volume{suffix}'] = 0.0
        tracker_df[f'Payout_Products_Value{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvolume{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvalue{suffix}'] = 0.0
        return tracker_df
    
    # Get payout product materials
    payout_materials = set()
    for _, product_row in payout_products.iterrows():
        payout_materials.add(product_row['material_code'])
    
    print(f"   📦 Payout materials: {len(payout_materials)} unique materials")
    
    # Filter sales data for scheme period and payout products
    scheme_from = pd.to_datetime(scheme_config['scheme_from'])
    scheme_to = pd.to_datetime(scheme_config['scheme_to'])
    
    # Filter sales data for scheme period and payout products
    if 'sale_date' in sales_df.columns:
        # Create a copy to avoid modifying original DataFrame  
        sales_temp = sales_df.copy()
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        scheme_sales = sales_temp[
            (sales_temp['sale_date'] >= scheme_from) &
            (sales_temp['sale_date'] <= scheme_to) &
            (sales_temp['material'].isin(payout_materials))
        ].copy()
    else:
        print("   ⚠️ No sale_date column found for scheme period filtering")
        scheme_sales = pd.DataFrame()  # Empty DataFrame
    
    print(f"   📊 Scheme period payout sales: {len(scheme_sales)} records")
    
    if scheme_sales.empty:
        # No sales for payout products
        tracker_df[f'Payout_Products_Volume{suffix}'] = 0.0
        tracker_df[f'Payout_Products_Value{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvolume{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvalue{suffix}'] = 0.0
        return tracker_df
    
    # Aggregate by credit_account for payout products
    payout_actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    payout_actuals.columns = ['credit_account', 'payout_volume', 'payout_value']
    payout_actuals['credit_account'] = payout_actuals['credit_account'].astype(str)
    
    print(f"   📈 Payout actuals calculated for {len(payout_actuals)} accounts")
    
    # Merge with tracker_df
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    tracker_df = tracker_df.merge(
        payout_actuals, 
        on='credit_account', 
        how='left', 
        suffixes=('', '_payout_temp')
    )
    
    # Fill NaN values with 0 and assign to final columns
    tracker_df[f'Payout_Products_Volume{suffix}'] = tracker_df['payout_volume'].fillna(0.0)
    tracker_df[f'Payout_Products_Value{suffix}'] = tracker_df['payout_value'].fillna(0.0)
    
    # Create the actual columns used in calculations
    tracker_df[f'payoutproductactualvolume{suffix}'] = tracker_df[f'Payout_Products_Volume{suffix}']
    tracker_df[f'payoutproductactualvalue{suffix}'] = tracker_df[f'Payout_Products_Value{suffix}']
    
    # Clean up temporary columns
    tracker_df = tracker_df.drop(['payout_volume', 'payout_value'], axis=1, errors='ignore')
    
    print(f"   ✅ Main scheme payout actuals: {(tracker_df[f'Payout_Products_Volume{suffix}'] > 0).sum()} accounts with volume")
    
    return tracker_df


def _add_additional_scheme_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                        sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add payout columns for all additional schemes"""
    
    # Get unique additional schemes
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found - skipping additional payout calculations")
        return tracker_df
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        # Extract scheme number
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📊 Adding payout columns for additional scheme {scheme_num}")
        
        # Determine if this additional scheme is volume or value based
        volume_value_based = scheme_row['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
        
        print(f"   📊 Additional scheme {scheme_num} type: {'Volume' if is_volume_based else 'Value'}")
        
        # Get the base column for this additional scheme
        base_column = f'total_volume_p{scheme_num}' if is_volume_based else f'total_value_p{scheme_num}'
        
        if base_column not in tracker_df.columns:
            print(f"   ⚠️ Base column {base_column} not found - skipping additional scheme {scheme_num}")
            continue
        
        # Get slabs for this specific additional scheme
        slabs_df = structured_data['slabs']
        scheme_slabs = slabs_df[
            (slabs_df['scheme_type'] == 'additional_scheme') &
            (slabs_df['scheme_name'].str.contains(f'Additional Scheme {scheme_num}', na=False))
        ].copy()
        
        if scheme_slabs.empty:
            print(f"   ⚠️ No slabs found for additional scheme {scheme_num}")
            continue
        
        # Apply slab-based payout calculations
        tracker_df = _apply_slab_payout_calculations(
            tracker_df, scheme_slabs, base_column, suffix, 
            structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_num}'
        )
    
    return tracker_df


def _apply_slab_payout_calculations(tracker_df: pd.DataFrame, slabs: pd.DataFrame, 
                                  base_column: str, suffix: str, structured_data: Dict,
                                  sales_df: pd.DataFrame, scheme_config: Dict, scheme_type: str) -> pd.DataFrame:
    """Apply vectorized slab-based payout calculations for a specific scheme"""
    
    print(f"     🧮 Applying slab calculations for {scheme_type} (base: {base_column})")
    
    # Sort slabs by slab_start for proper lookup
    slabs_sorted = slabs.sort_values('slab_start').copy()
    
    # Initialize payout columns
    rebate_per_litre_col = f'Rebate_per_Litre{suffix}'
    rebate_percent_col = f'Rebate_percent{suffix}'
    basic_payout_col = f'basic_payout{suffix}'
    additional_rebate_growth_col = f'Additional_Rebate_on_Growth_per_Litre{suffix}'
    additional_payout_col = f'additional_payout{suffix}'
    fixed_rebate_col = f'Fixed_Rebate{suffix}'
    payout_volume_col = f'Payout_Products_Volume{suffix}'
    payout_value_col = f'Payout_Products_Value{suffix}'
    payout_product_payout_col = f'Payout_Product_Payout{suffix}'
    total_payout_col = f'Total_Payout{suffix}'
    
    # Initialize all columns with zeros
    for col in [rebate_per_litre_col, rebate_percent_col, basic_payout_col, 
                additional_rebate_growth_col, additional_payout_col, fixed_rebate_col,
                payout_volume_col, payout_value_col, payout_product_payout_col, total_payout_col]:
        tracker_df[col] = 0.0
    
    # Apply vectorized slab lookup
    base_values = tracker_df[base_column].values
    
    # Track which accounts have been matched to slabs
    matched_accounts = pd.Series([False] * len(tracker_df), index=tracker_df.index)
    
    # Get first slab values for fallback
    first_slab = slabs_sorted.iloc[0] if not slabs_sorted.empty else None
    
    for _, slab_row in slabs_sorted.iterrows():
        slab_start = float(slab_row['slab_start'])
        slab_end = float(slab_row['slab_end'])
        
        # Create mask for values in this slab range
        mask = (base_values >= slab_start) & (base_values <= slab_end)
        
        if mask.sum() > 0:
            print(f"       📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts")
            
            # Apply slab values to matching accounts
            tracker_df.loc[mask, rebate_per_litre_col] = float(slab_row['rebate_per_litre'])
            tracker_df.loc[mask, rebate_percent_col] = float(slab_row['rebate_percent'])
            tracker_df.loc[mask, additional_rebate_growth_col] = float(slab_row['additional_rebate_on_growth'])
            tracker_df.loc[mask, fixed_rebate_col] = float(slab_row['fixed_rebate'])
            
            # Mark these accounts as matched
            matched_accounts[mask] = True
    
    # Apply fallback for unmatched accounts using first slab values
    if first_slab is not None:
        unmatched_mask = ~matched_accounts
        unmatched_count = unmatched_mask.sum()
        
        if unmatched_count > 0:
            print(f"       🔧 Applying first slab fallback for {unmatched_count} unmatched accounts")
            
            # Apply first slab values as fallback
            tracker_df.loc[unmatched_mask, rebate_per_litre_col] = float(first_slab['rebate_per_litre'])
            tracker_df.loc[unmatched_mask, rebate_percent_col] = float(first_slab['rebate_percent'])
            tracker_df.loc[unmatched_mask, additional_rebate_growth_col] = float(first_slab['additional_rebate_on_growth'])
            tracker_df.loc[unmatched_mask, fixed_rebate_col] = float(first_slab['fixed_rebate'])
            
            print(f"         📌 Fallback values applied: Rebate/L={first_slab['rebate_per_litre']}, Rebate%={first_slab['rebate_percent']}, Additional={first_slab['additional_rebate_on_growth']}, Fixed={first_slab['fixed_rebate']}")
    
    # Calculate payout product actuals
    tracker_df = _calculate_scheme_payout_actuals(
        tracker_df, structured_data, sales_df, scheme_config, scheme_type, suffix
    )
    
    # Calculate derived payout columns using vectorized operations with user's specific formulas
    payout_value_actual_col = f'payoutproductactualvalue{suffix}'
    payout_volume_actual_col = f'payoutproductactualvolume{suffix}'
    
    # Get configuration manager to check payout product configuration
    config_manager = structured_data.get('config_manager')
    
    # Determine if this is a volume or value based scheme
    scheme_info = structured_data['scheme_info']
    if scheme_type == 'main_scheme':
        main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
        if not main_scheme_info.empty:
            volume_value_based = main_scheme_info.iloc[0]['volume_value_based'].lower()
            is_volume_based = volume_value_based == 'volume'
        else:
            is_volume_based = True
        payout_products_enabled = config_manager.should_calculate_main_payouts() if config_manager else True
        print(f"     📊 Main scheme is {'volume' if is_volume_based else 'value'} based, payout products enabled: {payout_products_enabled}")
    else:
        # For additional schemes, extract scheme number
        scheme_num = suffix.replace('_p', '') if suffix else '1'
        # Try different patterns for additional scheme lookup
        additional_scheme_patterns = [
            f'additional_scheme_{scheme_num}',
            f'additional_scheme',
            'additional_scheme_1'
        ]
        
        additional_scheme_info = None
        for pattern in additional_scheme_patterns:
            temp_info = scheme_info[scheme_info['scheme_type'] == pattern]
            if not temp_info.empty:
                additional_scheme_info = temp_info
                break
        
        if additional_scheme_info is not None and not additional_scheme_info.empty:
            volume_value_based = additional_scheme_info.iloc[0]['volume_value_based'].lower()
            is_volume_based = volume_value_based == 'volume'
        else:
            # Fallback: check if total_value column exists for this scheme
            value_col = f'total_value{suffix}' if suffix else 'total_value'
            is_volume_based = value_col not in tracker_df.columns or tracker_df[value_col].sum() == 0
        
        payout_products_enabled = config_manager.should_calculate_additional_payouts(scheme_num) if config_manager else True
        print(f"     📊 Additional scheme {scheme_num} is {'volume' if is_volume_based else 'value'} based, payout products enabled: {payout_products_enabled}")
    
    # Ensure all columns are float to avoid Decimal type issues
    if payout_value_actual_col in tracker_df.columns:
        tracker_df[payout_value_actual_col] = tracker_df[payout_value_actual_col].astype(float)
    if payout_volume_actual_col in tracker_df.columns:
        tracker_df[payout_volume_actual_col] = tracker_df[payout_volume_actual_col].astype(float)
    
    tracker_df[rebate_percent_col] = tracker_df[rebate_percent_col].astype(float)
    tracker_df[rebate_per_litre_col] = tracker_df[rebate_per_litre_col].astype(float)
    tracker_df[additional_rebate_growth_col] = tracker_df[additional_rebate_growth_col].astype(float)
    
    # Calculate basic_payout using user's specific formula
    percentage_achieved_col = f'percentage_achieved{suffix}' if suffix else 'percentage_achieved'
    actual_volume_col = f'actual_volume{suffix}' if suffix else 'actual_volume'
    actual_value_col = f'actual_value{suffix}' if suffix else 'actual_value'
    
    if is_volume_based:
        # Volume-based scheme basic_payout formula
        if payout_products_enabled and payout_volume_actual_col in tracker_df.columns:
            # If payout product config = true: basic_payout = Rebate_per_Litre * Payout_Products_Volume
            tracker_df[basic_payout_col] = tracker_df[rebate_per_litre_col] * tracker_df[payout_volume_actual_col]
        else:
            # Else: basic_payout = Rebate_per_Litre * actual_volume
            if actual_volume_col in tracker_df.columns:
                tracker_df[basic_payout_col] = tracker_df[rebate_per_litre_col] * tracker_df[actual_volume_col].astype(float)
            else:
                tracker_df[basic_payout_col] = 0.0
    else:
        # Value-based scheme basic_payout formula
        if percentage_achieved_col in tracker_df.columns:
            try:
                tracker_df[percentage_achieved_col] = pd.to_numeric(tracker_df[percentage_achieved_col], errors='coerce').fillna(0.0).astype(float)
                
                if payout_products_enabled and payout_value_actual_col in tracker_df.columns:
                    # If payout product config = true: basic_payout = if(percentage_achieved >= 100%, (Rebate_percent * Payout_Products_Value), 0)
                    tracker_df[basic_payout_col] = np.where(
                        tracker_df[percentage_achieved_col] >= 100.0,
                        (tracker_df[rebate_percent_col] * tracker_df[payout_value_actual_col]) / 100.0,
                        0.0
                    )
                else:
                    # Else: basic_payout = if(percentage_achieved >= 100%, (Rebate_percent * actual_value), 0)
                    if actual_value_col in tracker_df.columns:
                        tracker_df[actual_value_col] = pd.to_numeric(tracker_df[actual_value_col], errors='coerce').fillna(0.0).astype(float)
                        tracker_df[basic_payout_col] = np.where(
                            tracker_df[percentage_achieved_col] >= 100.0,
                            (tracker_df[rebate_percent_col] * tracker_df[actual_value_col]) / 100.0,
                            0.0
                        )
                    else:
                        tracker_df[basic_payout_col] = 0.0
            except Exception as e:
                print(f"     ⚠️ Error in value-based basic_payout calculation: {e}")
                tracker_df[basic_payout_col] = 0.0
        else:
            print(f"     ⚠️ Column {percentage_achieved_col} not found for basic_payout calculation")
            tracker_df[basic_payout_col] = 0.0
    
    # Calculate additional_payout using user's specific formula with achievement threshold
    # Determine the correct percentage achieved column based on scheme type
    if suffix and '_p' in suffix:
        # For additional schemes, use the scheme's own percentage achieved column
        percentage_achieved_col = f'percentage_achieved{suffix}'
    else:
        # For main scheme, use percentage_achieved
        percentage_achieved_col = 'percentage_achieved'
    
    if is_volume_based:
        # Volume-based scheme additional payout logic
        if payout_products_enabled and f'Payout_Products_Volume{suffix}' in tracker_df.columns:
            # If schemeType = volume and payoutProductConfiguration = TRUE
            # additional_payout = IF(percentage_achieved_p1 >= 100%, Additional_Rebate_on_Growth_per_Litre * Payout_Products_Volume)
            payout_volume_col = f'Payout_Products_Volume{suffix}'
            tracker_df[additional_payout_col] = np.where(
                tracker_df[percentage_achieved_col] >= 1.0,  # Updated to use 1.0 instead of 100%
                tracker_df[additional_rebate_growth_col] * tracker_df[payout_volume_col],
                0.0
            )
        elif actual_volume_col in tracker_df.columns:
            # If schemeType = volume and payoutProductConfiguration = FALSE
            # additional_payout = IF(percentage_achieved_p1 >= 100%, Additional_Rebate_on_Growth_per_Litre * actual_volume)
            tracker_df[additional_payout_col] = np.where(
                tracker_df[percentage_achieved_col] >= 1.0,  # Updated to use 1.0 instead of 100%
                tracker_df[additional_rebate_growth_col] * tracker_df[actual_volume_col].astype(float),
                0.0
            )
        else:
            tracker_df[additional_payout_col] = 0.0
    else:
        # Value-based scheme - rules already handled in phasing/bonus as per user's note
        if payout_products_enabled and payout_volume_actual_col in tracker_df.columns:
            tracker_df[additional_payout_col] = tracker_df[additional_rebate_growth_col] * tracker_df[payout_volume_actual_col]
        elif actual_volume_col in tracker_df.columns:
            tracker_df[additional_payout_col] = tracker_df[additional_rebate_growth_col] * tracker_df[actual_volume_col].astype(float)
        else:
            tracker_df[additional_payout_col] = 0.0
    
    # Calculate final payout columns
    tracker_df[basic_payout_col] = tracker_df[basic_payout_col].astype(float)
    tracker_df[additional_payout_col] = tracker_df[additional_payout_col].astype(float)
    tracker_df[fixed_rebate_col] = tracker_df[fixed_rebate_col].astype(float)
    
    tracker_df[payout_product_payout_col] = tracker_df[basic_payout_col] + tracker_df[additional_payout_col]
    tracker_df[total_payout_col] = tracker_df[payout_product_payout_col] + tracker_df[fixed_rebate_col]
    
    print(f"     ✅ Payout calculations completed for {scheme_type}")
    
    return tracker_df


def _calculate_scheme_payout_actuals(tracker_df: pd.DataFrame, structured_data: Dict, 
                                   sales_df: pd.DataFrame, scheme_config: Dict, 
                                   scheme_type: str, suffix: str) -> pd.DataFrame:
    """Calculate actual volume/value for payout products during scheme period"""
    
    print(f"     📊 Calculating payout product actuals for {scheme_type}")
    
    # Get payout products for this scheme
    products_df = structured_data.get('products', pd.DataFrame())
    
    # Safety check: ensure products_df exists and has the required columns
    if products_df.empty or 'scheme_type' not in products_df.columns:
        print(f"     ⚠️ No valid products data found for {scheme_type}")
        # Set columns to 0
        tracker_df[f'Payout_Products_Volume{suffix}'] = 0.0
        tracker_df[f'Payout_Products_Value{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvolume{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvalue{suffix}'] = 0.0
        return tracker_df
    
    if scheme_type == 'main_scheme':
        payout_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') & 
            (products_df['is_payout_product'] == True)
        ]
    else:
        # For additional schemes, extract scheme number
        scheme_num = suffix.replace('_p', '') if suffix else '1'
        payout_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') & 
            (products_df['scheme_name'] == f'Additional Scheme {scheme_num}') &
            (products_df['is_payout_product'] == True)
        ]
    
    if payout_products.empty:
        print(f"     ℹ️ No payout products for {scheme_type}")
        # Set columns to 0
        tracker_df[f'Payout_Products_Volume{suffix}'] = 0.0
        tracker_df[f'Payout_Products_Value{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvolume{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvalue{suffix}'] = 0.0
        return tracker_df
    
    # Get payout product materials
    payout_materials = set()
    for _, product_row in payout_products.iterrows():
        payout_materials.add(product_row['material_code'])
    
    print(f"     📦 Payout materials: {len(payout_materials)} unique materials")
    
    # Filter sales data for scheme period and payout products
    scheme_from = pd.to_datetime(scheme_config['scheme_from'])
    scheme_to = pd.to_datetime(scheme_config['scheme_to'])
    
    if 'sale_date' in sales_df.columns:
        # Create a copy to avoid modifying original DataFrame  
        sales_temp = sales_df.copy()
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        scheme_sales = sales_temp[
            (sales_temp['sale_date'] >= scheme_from) &
            (sales_temp['sale_date'] <= scheme_to) &
            (sales_temp['material'].isin(payout_materials))
        ].copy()
    else:
        print("     ⚠️ No sale_date column found for scheme period filtering")
        scheme_sales = pd.DataFrame()  # Empty DataFrame
    
    print(f"     📊 Scheme period payout sales: {len(scheme_sales)} records")
    
    if scheme_sales.empty:
        # No sales for payout products
        tracker_df[f'Payout_Products_Volume{suffix}'] = 0.0
        tracker_df[f'Payout_Products_Value{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvolume{suffix}'] = 0.0
        tracker_df[f'payoutproductactualvalue{suffix}'] = 0.0
        return tracker_df
    
    # Aggregate by credit_account for payout products
    payout_actuals = scheme_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    payout_actuals.columns = ['credit_account', 'payout_volume_temp', 'payout_value_temp']
    payout_actuals['credit_account'] = payout_actuals['credit_account'].astype(str)
    
    print(f"     📈 Payout actuals calculated for {len(payout_actuals)} accounts")
    
    # Merge with tracker_df
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    tracker_df = tracker_df.merge(
        payout_actuals, 
        on='credit_account', 
        how='left', 
        suffixes=('', f'_payout_merge{suffix}')
    )
    
    # Fill NaN values with 0 and assign to final columns
    tracker_df[f'Payout_Products_Volume{suffix}'] = tracker_df['payout_volume_temp'].fillna(0.0)
    tracker_df[f'Payout_Products_Value{suffix}'] = tracker_df['payout_value_temp'].fillna(0.0)
    
    # Create the actual columns used in calculations (same values)
    tracker_df[f'payoutproductactualvolume{suffix}'] = tracker_df[f'Payout_Products_Volume{suffix}']
    tracker_df[f'payoutproductactualvalue{suffix}'] = tracker_df[f'Payout_Products_Value{suffix}']
    
    # Clean up temporary columns
    tracker_df = tracker_df.drop(['payout_volume_temp', 'payout_value_temp'], axis=1, errors='ignore')
    
    print(f"     ✅ Payout actuals: {(tracker_df[f'Payout_Products_Volume{suffix}'] > 0).sum()} accounts with volume")
    
    return tracker_df
