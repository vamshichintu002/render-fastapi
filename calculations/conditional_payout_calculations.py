"""
Conditional Payout Calculations Module
Implements user's specific conditional payout formulas based on payout products configuration

User Requirements:
For Main Scheme - Value Type:
IF Payout Product Configuration is FALSE:
    basic_payout = IF(Scheme Type="Value", (IF(percentage_achieved>=100%, (Rebate_percent*actual_value), 0)))
ELSE:
    basic_payout = IF(Scheme Type="Value", (IF(percentage_achieved>=100%, (Rebate_percent*Payout_Products_Value), 0)))

For Additional Scheme - Value Type:
IF Payout Product Configuration is FALSE:
    basic_payout_pN = IF(Scheme_Type_pN="Value", (IF(percentage_achieved_pN>=100%, (Rebate_percent_pN*actual_value_pN), 0)))
ELSE:
    basic_payout_pN = IF(Scheme_Type_pN="Value", (IF(percentage_achieved_pN>=100%, (Rebate_percent_pN*Payout_Products_Value_pN), 0)))

For Volume schemes: Uses existing working formulas (unchanged)

Note: percentage_achieved is stored as decimal (1.0 = 100%, 0.5 = 50%, etc.)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def calculate_conditional_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                        sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate payout columns using conditional formulas based on payout products configuration
    """
    print("🧠 Calculating conditional payout columns based on configuration...")
    
    # Add main scheme conditional payouts
    tracker_df = _add_main_scheme_conditional_payouts(tracker_df, structured_data, sales_df, scheme_config)
    
    # Add additional scheme conditional payouts
    tracker_df = _add_additional_schemes_conditional_payouts(tracker_df, structured_data, sales_df, scheme_config)
    
    print("✅ Conditional payout calculations completed")
    return tracker_df


def _add_main_scheme_conditional_payouts(tracker_df: pd.DataFrame, structured_data: Dict, 
                                        sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Add conditional payout calculations for main scheme
    """
    print("   📊 Adding main scheme conditional payouts...")
    
    # Get configuration manager to check payout products enablement
    config_manager = structured_data.get('config_manager')
    payout_products_enabled = config_manager.should_calculate_main_payouts() if config_manager else True
    
    # Determine scheme type (volume or value)
    scheme_info = structured_data['scheme_info']
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    
    if main_scheme_info.empty:
        print("   ⚠️ No main scheme info found - defaulting to volume")
        is_volume_based = True
    else:
        volume_value_based = main_scheme_info.iloc[0]['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
    
    print(f"   📋 Main scheme: {'Volume' if is_volume_based else 'Value'} based, Payout products enabled: {payout_products_enabled}")
    
    # Get or calculate required columns
    tracker_df = _ensure_main_scheme_payout_columns(tracker_df, structured_data, sales_df, scheme_config)
    
    # Apply conditional formulas
    tracker_df = _apply_conditional_formulas(
        tracker_df, is_volume_based, payout_products_enabled, '', 'main_scheme'
    )
    
    return tracker_df


def _add_additional_schemes_conditional_payouts(tracker_df: pd.DataFrame, structured_data: Dict, 
                                               sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Add conditional payout calculations for additional schemes
    """
    print("   📊 Adding additional schemes conditional payouts...")
    
    # Get additional schemes
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'] == 'additional_scheme']
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found")
        return tracker_df
    
    config_manager = structured_data.get('config_manager')
    
    # Process each additional scheme
    for i, (_, scheme_row) in enumerate(additional_schemes.iterrows()):
        scheme_index = i + 1  # 1-based indexing (now correctly using enumerate)
        suffix = f'_p{scheme_index}'
        
        # Check payout products enablement for this additional scheme
        payout_products_enabled = config_manager.should_calculate_additional_payouts(str(scheme_index)) if config_manager else True
        
        # Determine scheme type
        volume_value_based = scheme_row['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
        
        print(f"   📋 Additional scheme {scheme_index}: {'Volume' if is_volume_based else 'Value'} based, Payout products enabled: {payout_products_enabled}")
        
        # Get or calculate required columns
        tracker_df = _ensure_additional_scheme_payout_columns(tracker_df, structured_data, sales_df, scheme_config, scheme_index, suffix)
        
        # Apply conditional formulas
        tracker_df = _apply_conditional_formulas(
            tracker_df, is_volume_based, payout_products_enabled, suffix, f'additional_scheme_{scheme_index}'
        )
    
    return tracker_df


def _ensure_main_scheme_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                      sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Ensure all required payout columns exist for main scheme
    """
    required_columns = {
        'Rebate_per_Litre': 0.0,
        'Rebate_percent': 0.0,
        'Additional_Rebate_on_Growth_per_Litre': 0.0,
        'Payout_Products_Volume': 0.0,
        'Payout_Products_Value': 0.0,
        'payoutproductactualvolume': 0.0,
        'payoutproductactualvalue': 0.0
    }
    
    # Initialize missing columns
    for col, default_val in required_columns.items():
        if col not in tracker_df.columns:
            tracker_df[col] = default_val
    
    # Calculate slab-based values (rebate rates)
    tracker_df = _calculate_scheme_slab_values(tracker_df, structured_data, 'main_scheme', '')
    
    # Calculate payout product actuals
    tracker_df = _calculate_scheme_payout_actuals(tracker_df, structured_data, sales_df, scheme_config, 'main_scheme', '')
    
    return tracker_df


def _ensure_additional_scheme_payout_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                            sales_df: pd.DataFrame, scheme_config: Dict, 
                                            scheme_index: int, suffix: str) -> pd.DataFrame:
    """
    Ensure all required payout columns exist for additional scheme
    """
    required_columns = {
        f'Rebate_per_Litre{suffix}': 0.0,
        f'Rebate_percent{suffix}': 0.0,
        f'Additional_Rebate_on_Growth_per_Litre{suffix}': 0.0,
        f'Payout_Products_Volume{suffix}': 0.0,
        f'Payout_Products_Value{suffix}': 0.0,
        f'payoutproductactualvolume{suffix}': 0.0,
        f'payoutproductactualvalue{suffix}': 0.0
    }
    
    # Initialize missing columns
    for col, default_val in required_columns.items():
        if col not in tracker_df.columns:
            tracker_df[col] = default_val
    
    # Calculate slab-based values (rebate rates)
    tracker_df = _calculate_scheme_slab_values(tracker_df, structured_data, f'additional_scheme_{scheme_index}', suffix)
    
    # Calculate payout product actuals
    tracker_df = _calculate_scheme_payout_actuals(tracker_df, structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_index}', suffix)
    
    return tracker_df


def _calculate_scheme_slab_values(tracker_df: pd.DataFrame, structured_data: Dict, 
                                 scheme_type: str, suffix: str) -> pd.DataFrame:
    """
    Calculate slab-based rebate values using actual_volume/actual_value instead of total_volume/total_value
    UPDATED: Changed from total_volume/total_value slabs to actual_volume/actual_value slabs
    """
    # Get slabs for this scheme
    slabs_df = structured_data['slabs']
    
    # Determine scheme info to get the volume/value type
    scheme_info = structured_data['scheme_info']
    is_volume_based = True  # Default
    
    if scheme_type == 'main_scheme':
        scheme_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
        # Get main scheme info to determine if volume or value based
        main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
        if not main_scheme_info.empty:
            volume_value_based = main_scheme_info.iloc[0]['volume_value_based'].lower()
            is_volume_based = volume_value_based == 'volume'
        
        # UPDATED: Use total_volume/total_value for costing sheet
        base_column = 'total_volume' if is_volume_based else 'total_value'
    else:
        # Extract scheme number from scheme_type (e.g., 'additional_scheme_1' -> '1')
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        scheme_slabs = slabs_df[
            (slabs_df['scheme_type'] == 'additional_scheme') &
            (slabs_df['scheme_name'].str.contains(f'Additional Scheme {scheme_num}', na=False))
        ].copy()
        
        # Get additional scheme info to determine if volume or value based
        additional_scheme_info = scheme_info[
            (scheme_info['scheme_type'] == 'additional_scheme') &
            (scheme_info['scheme_name'] == f'Additional Scheme {scheme_num}')
        ]
        if not additional_scheme_info.empty:
            volume_value_based = additional_scheme_info.iloc[0]['volume_value_based'].lower()
            is_volume_based = volume_value_based == 'volume'
        
        # UPDATED: Use total_volume_pN/total_value_pN for costing sheet
        base_column = f'total_volume{suffix}' if is_volume_based else f'total_value{suffix}'
    
    print(f"     📊 Rebate slab calculation: Using {base_column} ({'Volume' if is_volume_based else 'Value'} scheme)")
    
    if scheme_slabs.empty:
        print(f"     ⚠️ No slabs found for {scheme_type}")
        return tracker_df
    
    # Sort slabs and apply slab lookup
    slabs_sorted = scheme_slabs.sort_values('slab_start')
    
    # Column names
    rebate_per_litre_col = f'Rebate_per_Litre{suffix}'
    rebate_percent_col = f'Rebate_percent{suffix}'
    additional_rebate_col = f'Additional_Rebate_on_Growth_per_Litre{suffix}'
    
    # Get base values for slab lookup
    if base_column in tracker_df.columns:
        base_values = tracker_df[base_column].values
        
        # Initialize all rebate columns to 0
        tracker_df[rebate_per_litre_col] = 0.0
        tracker_df[rebate_percent_col] = 0.0
        tracker_df[additional_rebate_col] = 0.0
        
        # Get first and last slab info for fallback logic
        first_slab = slabs_sorted.iloc[0]
        last_slab = slabs_sorted.iloc[-1]
        first_slab_start = float(first_slab['slab_start'])
        last_slab_end = float(last_slab['slab_end'])
        
        # Track which accounts have been matched to slabs
        matched_accounts = pd.Series([False] * len(tracker_df), index=tracker_df.index)
        
        # Apply slab lookup for accounts within slab ranges
        for _, slab_row in slabs_sorted.iterrows():
            slab_start = float(slab_row['slab_start'])
            slab_end = float(slab_row['slab_end'])
            
            # Create mask for values in this slab range
            mask = (base_values >= slab_start) & (base_values <= slab_end)
            
            if mask.sum() > 0:
                # Apply slab values to matching accounts
                tracker_df.loc[mask, rebate_per_litre_col] = float(slab_row['rebate_per_litre'])
                tracker_df.loc[mask, rebate_percent_col] = float(slab_row['rebate_percent'])
                tracker_df.loc[mask, additional_rebate_col] = float(slab_row['additional_rebate_on_growth'])
                
                # Mark these accounts as matched
                matched_accounts[mask] = True
                
                print(f"     ✅ Slab {slab_start}-{slab_end}: Applied rebate_per_litre={slab_row['rebate_per_litre']} to {mask.sum()} accounts")
        
        # Apply first slab fallback for accounts below minimum slab range (including negative/zero)
        below_first_slab_mask = (base_values < first_slab_start) & (~matched_accounts)
        if below_first_slab_mask.sum() > 0:
            tracker_df.loc[below_first_slab_mask, rebate_per_litre_col] = float(first_slab['rebate_per_litre'])
            tracker_df.loc[below_first_slab_mask, rebate_percent_col] = float(first_slab['rebate_percent'])
            tracker_df.loc[below_first_slab_mask, additional_rebate_col] = float(first_slab['additional_rebate_on_growth'])
            matched_accounts[below_first_slab_mask] = True
            print(f"     ✅ First slab fallback: Applied rebate_per_litre={first_slab['rebate_per_litre']} to {below_first_slab_mask.sum()} accounts below {first_slab_start} (including negative/zero)")
        
        # Report accounts above last slab (these will keep rebate values as 0)
        above_last_slab_mask = (base_values > last_slab_end) & (~matched_accounts)
        if above_last_slab_mask.sum() > 0:
            print(f"     📊 {above_last_slab_mask.sum()} accounts above last slab ({last_slab_end}) - rebate values set to 0")
    
    return tracker_df


def _calculate_scheme_payout_actuals(tracker_df: pd.DataFrame, structured_data: Dict, 
                                    sales_df: pd.DataFrame, scheme_config: Dict, 
                                    scheme_type: str, suffix: str) -> pd.DataFrame:
    """
    Calculate actual volume/value for payout products during scheme period
    """
    # Get payout products for this scheme
    products_df = structured_data['products']
    
    if scheme_type == 'main_scheme':
        payout_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') & 
            (products_df['is_payout_product'] == True)
        ]
    else:
        # For additional schemes, extract scheme number
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
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
    
    # Filter sales data for scheme period and payout products
    scheme_from = pd.to_datetime(scheme_config['scheme_from'])
    scheme_to = pd.to_datetime(scheme_config['scheme_to'])
    
    if 'sale_date' in sales_df.columns:
        sales_temp = sales_df.copy()
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        scheme_sales = sales_temp[
            (sales_temp['sale_date'] >= scheme_from) &
            (sales_temp['sale_date'] <= scheme_to) &
            (sales_temp['material'].isin(payout_materials))
        ].copy()
    else:
        scheme_sales = pd.DataFrame()
    
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
    
    return tracker_df


def _apply_conditional_formulas(tracker_df: pd.DataFrame, is_volume_based: bool, 
                               payout_products_enabled: bool, suffix: str, scheme_name: str) -> pd.DataFrame:
    """
    Apply the user's conditional payout formulas
    """
    # Column names with suffix
    rebate_per_litre_col = f'Rebate_per_Litre{suffix}'
    rebate_percent_col = f'Rebate_percent{suffix}'
    additional_rebate_col = f'Additional_Rebate_on_Growth_per_Litre{suffix}'
    basic_payout_col = f'basic_payout{suffix}'
    additional_payout_col = f'additional_payout{suffix}'
    percentage_achieved_col = f'percentage_achieved{suffix}' if suffix else 'percentage_achieved'
    
    # Payout product columns
    payout_volume_col = f'Payout_Products_Volume{suffix}'
    payout_value_col = f'Payout_Products_Value{suffix}'
    
    # Actual columns
    actual_volume_col = f'actual_volume{suffix}' if suffix else 'actual_volume'
    actual_value_col = f'actual_value{suffix}' if suffix else 'actual_value'
    
    print(f"     📝 Applying conditional formulas for {scheme_name}")
    
    # Ensure all columns are float type
    for col in [rebate_per_litre_col, rebate_percent_col, additional_rebate_col]:
        if col in tracker_df.columns:
            tracker_df[col] = tracker_df[col].astype(float)
    
    # UPDATED USER'S CONDITIONAL BASIC PAYOUT FORMULAS - TASK GROUP 1
    # Ensure percentage_achieved column exists and is numeric
    if percentage_achieved_col in tracker_df.columns:
        tracker_df[percentage_achieved_col] = pd.to_numeric(tracker_df[percentage_achieved_col], errors='coerce').fillna(0.0).astype(float)
    else:
        tracker_df[percentage_achieved_col] = 0.0
    
    if is_volume_based:
        # VOLUME-BASED SCHEME
        if payout_products_enabled:
            # If Payout Product Configuration = TRUE
            # basic_payout = IF(Scheme Type="Volume", (Rebate_per_Litre * Payout_Products_Volume), ...)
            if payout_volume_col in tracker_df.columns:
                tracker_df[basic_payout_col] = tracker_df[rebate_per_litre_col] * tracker_df[payout_volume_col].astype(float)
                print(f"       ✓ Volume + Payout enabled: basic_payout = Rebate_per_Litre * Payout_Products_Volume")
            else:
                tracker_df[basic_payout_col] = 0.0
        else:
            # Else (Payout Product Configuration = FALSE)
            # basic_payout = IF(Scheme Type="Volume", (Rebate_per_Litre * actual_volume), ...)
            if actual_volume_col in tracker_df.columns:
                tracker_df[basic_payout_col] = tracker_df[rebate_per_litre_col] * tracker_df[actual_volume_col].astype(float)
                print(f"       ✓ Volume + Payout disabled: basic_payout = Rebate_per_Litre * actual_volume")
            else:
                tracker_df[basic_payout_col] = 0.0
    else:
        # VALUE-BASED SCHEME - User's specific formulas
        # For Value schemes: basic_payout = IF(percentage_achieved >= 100%, (Rebate_percent * Value), 0)
        if not payout_products_enabled:
            # Payout Product Configuration = FALSE
            # basic_payout = IF(Scheme Type="Value", (IF(percentage_achieved >= 100%, (Rebate_percent * actual_value), 0)))
            if actual_value_col in tracker_df.columns:
                tracker_df[basic_payout_col] = np.where(
                    tracker_df[percentage_achieved_col] >= 1.0,  # 100% as decimal = 1.0
                    tracker_df[rebate_percent_col] * tracker_df[actual_value_col].astype(float),
                    0.0
                )
                print(f"       ✓ Value + Payout disabled: basic_payout = IF(achievement>=1.0, Rebate_percent * actual_value, 0)")
            else:
                tracker_df[basic_payout_col] = 0.0
        else:
            # Payout Product Configuration = TRUE  
            # basic_payout = IF(Scheme Type="Value", (IF(percentage_achieved >= 100%, (Rebate_percent * Payout_Products_Value), 0)))
            if payout_value_col in tracker_df.columns:
                tracker_df[basic_payout_col] = np.where(
                    tracker_df[percentage_achieved_col] >= 1.0,  # 100% as decimal = 1.0
                    tracker_df[rebate_percent_col] * tracker_df[payout_value_col].astype(float),
                    0.0
                )
                print(f"       ✓ Value + Payout enabled: basic_payout = IF(achievement>=1.0, Rebate_percent * Payout_Products_Value, 0)")
            else:
                tracker_df[basic_payout_col] = 0.0
        
        # ADDITIONAL REQUIREMENT: Divide basic_payout by 100 for Value schemes only
        # This applies to both payout enabled and disabled cases for Value schemes
        tracker_df[basic_payout_col] = tracker_df[basic_payout_col] / 100.0
        print(f"       ✓ Value scheme: basic_payout divided by 100 (per user requirement)")
    
    # USER'S CONDITIONAL ADDITIONAL PAYOUT FORMULAS (Only for volume schemes per user's spec)
    if is_volume_based and percentage_achieved_col in tracker_df.columns:
        # Ensure percentage_achieved is numeric
        tracker_df[percentage_achieved_col] = pd.to_numeric(tracker_df[percentage_achieved_col], errors='coerce').fillna(0.0).astype(float)
        
        if payout_products_enabled:
            # additional_payout = IF(Scheme Type=volume AND percentage_achieved>=100%, Additional_Rebate_on_Growth_per_Litre*Payout_Products_Volume, 0)
            if payout_volume_col in tracker_df.columns:
                tracker_df[additional_payout_col] = np.where(
                    tracker_df[percentage_achieved_col] >= 1.0,  # 100% stored as 1.0
                    tracker_df[additional_rebate_col] * tracker_df[payout_volume_col].astype(float),
                    0.0
                )
                print(f"       ✓ Volume + Payout enabled: additional_payout = IF(achievement>=100%, Growth_Rebate * Payout_Products_Volume, 0)")
            else:
                tracker_df[additional_payout_col] = 0.0
        else:
            # additional_payout = IF(Scheme Type=volume AND percentage_achieved>=100%, Additional_Rebate_on_Growth_per_Litre*actual_volume, 0)
            if actual_volume_col in tracker_df.columns:
                tracker_df[additional_payout_col] = np.where(
                    tracker_df[percentage_achieved_col] >= 1.0,  # 100% stored as 1.0
                    tracker_df[additional_rebate_col] * tracker_df[actual_volume_col].astype(float),
                    0.0
                )
                print(f"       ✓ Volume + Payout disabled: additional_payout = IF(achievement>=100%, Growth_Rebate * actual_volume, 0)")
            else:
                tracker_df[additional_payout_col] = 0.0
    else:
        # For value schemes or missing percentage_achieved, set to 0
        tracker_df[additional_payout_col] = 0.0
        if not is_volume_based:
            print(f"       ℹ️ Value scheme: additional_payout set to 0 (per user specification)")
    
    # Calculate total payout columns
    total_payout_col = f'Total_Payout{suffix}'
    tracker_df[total_payout_col] = tracker_df[basic_payout_col].astype(float) + tracker_df[additional_payout_col].astype(float)
    
    return tracker_df