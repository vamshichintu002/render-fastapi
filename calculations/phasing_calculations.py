"""
Phasing Calculations Module
Handles phasing product calculations for main and additional schemes with dynamic period and bonus handling
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from datetime import datetime


def calculate_phasing_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                             sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate all phasing-related columns for main and additional schemes
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing phasing, slabs, products, etc.
        sales_df: Sales data for phasing calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with phasing columns
    """
    print("📊 Calculating phasing columns...")
    
    # Get configuration manager to check phasing configuration
    config_manager = structured_data.get('config_manager')
    
    # Add main scheme phasing columns (if enabled)
    if config_manager and config_manager.is_main_feature_enabled('bonusSchemes'):
        print("   🎯 Main scheme phasing: ENABLED - calculating...")
        tracker_df = _add_main_scheme_phasing_columns(tracker_df, structured_data, sales_df, scheme_config)
    else:
        print("   ⚡ Main scheme phasing: DISABLED - skipping for performance")
    
    # Add additional scheme phasing columns (if enabled)
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        
        if config_manager and config_manager.should_calculate_additional_payouts(scheme_num):
            # Check if this additional scheme has bonus schemes enabled 
            bonus_enabled = config_manager.is_additional_feature_enabled(scheme_num, 'bonusSchemes')
            if bonus_enabled:
                print(f"   🎯 Additional scheme {scheme_num} phasing: ENABLED - calculating...")
                tracker_df = _add_additional_scheme_phasing_columns(
                    tracker_df, structured_data, sales_df, scheme_config, scheme_num
                )
            else:
                print(f"   ⚡ Additional scheme {scheme_num} phasing: DISABLED - skipping for performance")
        else:
            print(f"   ⚡ Additional scheme {scheme_num} phasing: DISABLED - parent feature disabled")
    
    print("✅ Phasing calculations completed")
    return tracker_df


def _add_main_scheme_phasing_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                   sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add phasing columns for main scheme"""
    print("   📊 Adding main scheme phasing columns...")
    
    # Get main scheme phasing data
    phasing_df = structured_data['phasing']
    main_phasing = phasing_df[phasing_df['scheme_type'] == 'main_scheme'].copy()
    
    if main_phasing.empty:
        print("   ℹ️ No main scheme phasing data found - skipping")
        return tracker_df
    
    # Determine main scheme type
    scheme_info = structured_data['scheme_info']
    main_scheme_info = scheme_info[scheme_info['scheme_type'] == 'main_scheme']
    
    if main_scheme_info.empty:
        is_volume_based = True
    else:
        volume_value_based = main_scheme_info.iloc[0]['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
    
    print(f"   📊 Main scheme type: {'Volume' if is_volume_based else 'Value'}")
    
    # Sort phasing by phase_id
    main_phasing = main_phasing.sort_values('phase_id')
    
    # Apply phasing calculations for each period
    tracker_df = _apply_phasing_calculations(
        tracker_df, main_phasing, '', structured_data, sales_df, scheme_config, is_volume_based, 'main_scheme'
    )
    
    return tracker_df


def _add_additional_scheme_phasing_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                         sales_df: pd.DataFrame, scheme_config: Dict, scheme_num: str) -> pd.DataFrame:
    """Add phasing columns for additional schemes"""
    print(f"   📊 Adding additional scheme {scheme_num} phasing columns...")
    
    # Get additional scheme phasing data
    phasing_df = structured_data['phasing']
    scheme_info = structured_data['scheme_info']
    
    # Find the specific additional scheme
    additional_scheme_info = scheme_info[scheme_info['scheme_type'] == f'additional_scheme_{scheme_num}']
    if additional_scheme_info.empty:
        # Fallback to general additional_scheme pattern
        additional_scheme_info = scheme_info[scheme_info['scheme_type'] == 'additional_scheme']
    
    if additional_scheme_info.empty:
        print(f"   ⚠️ No additional scheme {scheme_num} info found")
        return tracker_df
    
    # Get scheme_id for filtering phasing data
    scheme_id = additional_scheme_info.iloc[0]['scheme_id']
    additional_phasing = phasing_df[
        (phasing_df['scheme_type'] == 'additional_scheme') & 
        (phasing_df['scheme_id'] == scheme_id)
    ].copy()
    
    if additional_phasing.empty:
        print(f"   ℹ️ No additional scheme {scheme_num} phasing data found - skipping")
        return tracker_df
    
    # Determine additional scheme type
    volume_value_based = additional_scheme_info.iloc[0]['volume_value_based'].lower()
    is_volume_based = volume_value_based == 'volume'
    
    print(f"   📊 Additional scheme {scheme_num} type: {'Volume' if is_volume_based else 'Value'}")
    
    # Sort phasing by phase_id
    additional_phasing = additional_phasing.sort_values('phase_id')
    
    # Apply phasing calculations for each period with _pN suffix
    suffix = f'_p{scheme_num}'
    tracker_df = _apply_phasing_calculations(
        tracker_df, additional_phasing, suffix, structured_data, sales_df, scheme_config, 
        is_volume_based, f'additional_scheme_{scheme_num}'
    )
    
    return tracker_df


def _apply_phasing_calculations(tracker_df: pd.DataFrame, phasing_data: pd.DataFrame, 
                              suffix: str, structured_data: Dict, sales_df: pd.DataFrame, 
                              scheme_config: Dict, is_volume_based: bool, scheme_type: str) -> pd.DataFrame:
    """Apply phasing calculations for all periods"""
    
    print(f"     🧮 Applying phasing calculations for {scheme_type}")
    
    # Get configuration manager for payout product checks
    config_manager = structured_data.get('config_manager')
    if scheme_type == 'main_scheme':
        payout_products_enabled = config_manager.should_calculate_main_payouts() if config_manager else True
    else:
        scheme_num = suffix.replace('_p', '') if suffix else '1'
        payout_products_enabled = config_manager.should_calculate_additional_payouts(scheme_num) if config_manager else True
    
    # Get target columns for calculations
    target_volume_col = f'target_volume{suffix}' if suffix else 'target_volume'
    target_value_col = f'target_value{suffix}' if suffix else 'target_value'
    
    # Initialize final phasing payout column
    final_phasing_payout_col = f'FINAL_PHASING_PAYOUT{suffix}'
    tracker_df[final_phasing_payout_col] = 0.0
    
    # Storage for bonus and regular payouts
    bonus_payouts = []
    regular_payouts = []
    
    # Process each phasing period
    for _, phasing_row in phasing_data.iterrows():
        period_num = int(phasing_row['phase_id'])
        is_bonus = bool(phasing_row['is_bonus'])
        
        print(f"       📍 Processing period {period_num} ({'bonus' if is_bonus else 'regular'})")
        
        # Add regular phasing columns
        tracker_df = _add_regular_phasing_columns(
            tracker_df, phasing_row, period_num, suffix, structured_data, sales_df, 
            is_volume_based, payout_products_enabled, target_volume_col, target_value_col
        )
        
        # Store regular payout for final calculation
        regular_payout_col = f'Phasing_Payout_{period_num}{suffix}'
        if regular_payout_col in tracker_df.columns:
            regular_payouts.append(regular_payout_col)
        
        # Add bonus columns if enabled
        if is_bonus:
            tracker_df = _add_bonus_phasing_columns(
                tracker_df, phasing_row, period_num, suffix, structured_data, sales_df, 
                is_volume_based, payout_products_enabled, target_volume_col, target_value_col
            )
            
            # Store bonus payout for final calculation
            bonus_payout_col = f'Bonus_Payout_{period_num}{suffix}'
            if bonus_payout_col in tracker_df.columns:
                bonus_payouts.append(bonus_payout_col)
    
    # Calculate final phasing payout
    tracker_df = _calculate_final_phasing_payout(
        tracker_df, bonus_payouts, regular_payouts, final_phasing_payout_col
    )
    
    print(f"     ✅ Phasing calculations completed for {scheme_type}")
    return tracker_df


def _add_regular_phasing_columns(tracker_df: pd.DataFrame, phasing_row: pd.Series, 
                               period_num: int, suffix: str, structured_data: Dict, 
                               sales_df: pd.DataFrame, is_volume_based: bool, 
                               payout_products_enabled: bool, target_volume_col: str, target_value_col: str) -> pd.DataFrame:
    """Add regular phasing columns for a specific period"""
    
    # Column naming for this period
    period_suffix = f'_{period_num}{suffix}'
    
    # Extract dates and convert to datetime
    phasing_from = pd.to_datetime(phasing_row['phasing_from_date']).tz_localize(None)
    phasing_to = pd.to_datetime(phasing_row['phasing_to_date']).tz_localize(None)
    payout_from = pd.to_datetime(phasing_row['payout_from_date']).tz_localize(None)
    payout_to = pd.to_datetime(phasing_row['payout_to_date']).tz_localize(None)
    
    # Basic phasing info columns
    tracker_df[f'Phasing_Period_No{period_suffix}'] = period_num
    tracker_df[f'Phasing_Target_Percent{period_suffix}'] = float(phasing_row['phasing_target_percent'])
    
    # Calculate phasing targets
    phasing_target_pct = float(phasing_row['phasing_target_percent']) / 100.0
    
    if target_volume_col in tracker_df.columns:
        tracker_df[f'Phasing_Target_Volume{period_suffix}'] = (
            tracker_df[target_volume_col].astype(float) * phasing_target_pct
        )
    else:
        tracker_df[f'Phasing_Target_Volume{period_suffix}'] = 0.0
    
    if target_value_col in tracker_df.columns:
        tracker_df[f'Phasing_Target_Value{period_suffix}'] = (
            tracker_df[target_value_col].astype(float) * phasing_target_pct
        )
    else:
        tracker_df[f'Phasing_Target_Value{period_suffix}'] = 0.0
    
    # Calculate phasing period actuals
    phasing_actuals = _calculate_period_sales(
        sales_df, phasing_from, phasing_to, tracker_df['credit_account']
    )
    
    tracker_df[f'Phasing_Period_Volume{period_suffix}'] = phasing_actuals['volume']
    tracker_df[f'Phasing_Period_Value{period_suffix}'] = phasing_actuals['value']
    
    # Calculate payout period actuals
    payout_actuals = _calculate_period_sales(
        sales_df, payout_from, payout_to, tracker_df['credit_account']
    )
    
    tracker_df[f'Phasing_Payout_Period_Volume{period_suffix}'] = payout_actuals['volume']
    tracker_df[f'Phasing_Payout_Period_Value{period_suffix}'] = payout_actuals['value']
    
    # Calculate payout product actuals
    payout_product_actuals = _calculate_payout_product_period_sales(
        structured_data, sales_df, payout_from, payout_to, tracker_df['credit_account'], suffix
    )
    
    tracker_df[f'Phasing_Period_Payout_Product_Volume{period_suffix}'] = payout_product_actuals['volume']
    tracker_df[f'Phasing_Period_Payout_Product_Value{period_suffix}'] = payout_product_actuals['value']
    
    # Calculate percentage achieved
    if is_volume_based:
        target_col = f'Phasing_Target_Volume{period_suffix}'
        actual_col = f'Phasing_Period_Volume{period_suffix}'
    else:
        target_col = f'Phasing_Target_Value{period_suffix}'
        actual_col = f'Phasing_Period_Value{period_suffix}'
    
    tracker_df[f'Percent_Phasing_Achieved{period_suffix}'] = np.where(
        tracker_df[target_col] > 0,
        tracker_df[actual_col] / tracker_df[target_col],
        0.0
    )
    
    # Get rebate values
    rebate_value = float(phasing_row['rebate_value']) if pd.notna(phasing_row['rebate_value']) else 0.0
    rebate_percent = float(phasing_row['rebate_percentage']) if pd.notna(phasing_row['rebate_percentage']) else 0.0
    
    tracker_df[f'Phasing_Period_Rebate{period_suffix}'] = rebate_value
    tracker_df[f'Phasing_Period_Rebate_Percent{period_suffix}'] = rebate_percent
    
    # Calculate phasing payout with achievement threshold - following user's requirements
    percent_achieved_col = f'Percent_Phasing_Achieved{period_suffix}'
    
    if is_volume_based:
        # Volume-based phasing payout with achievement threshold
        if payout_products_enabled:
            # schemeType = volume & payoutProductConfiguration = TRUE
            # Phasing_Payout_1 = IF(Percent_Phasing_Achieved_1 >= 100%, Phasing_Period_Rebate_1 * Phasing_Period_Payout_Product_Volume_1)
            volume_col = f'Phasing_Period_Payout_Product_Volume{period_suffix}'
            tracker_df[f'Phasing_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_achieved_col] >= 1.0,
                tracker_df[f'Phasing_Period_Rebate{period_suffix}'] * tracker_df[volume_col],
                0.0
            )
        else:
            # schemeType = volume & payoutProductConfiguration = FALSE
            # Phasing_Payout_1 = IF(Percent_Phasing_Achieved_1 >= 100%, Phasing_Period_Rebate_1 * Phasing_Payout_Period_Volume_1)
            volume_col = f'Phasing_Payout_Period_Volume{period_suffix}'
            tracker_df[f'Phasing_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_achieved_col] >= 1.0,
                tracker_df[f'Phasing_Period_Rebate{period_suffix}'] * tracker_df[volume_col],
                0.0
            )
    else:
        # Value-based phasing payout with achievement threshold
        if payout_products_enabled:
            # schemeType = value & payoutProductConfiguration = TRUE
            # Phasing_Payout_1 = IF(Percent_Phasing_Achieved_1 >= 100%, Phasing_Period_Rebate_Percent_1 * Phasing_Period_Payout_Product_Value_1)
            value_col = f'Phasing_Period_Payout_Product_Value{period_suffix}'
            tracker_df[f'Phasing_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_achieved_col] >= 1.0,
                tracker_df[f'Phasing_Period_Rebate_Percent{period_suffix}'] * tracker_df[value_col] / 100.0,
                0.0
            )
        else:
            # schemeType = value & payoutProductConfiguration = FALSE
            # Phasing_Payout_1 = IF(Percent_Phasing_Achieved_1 >= 100%, Phasing_Period_Rebate_Percent_1 * Phasing_Payout_Period_Value_1)
            value_col = f'Phasing_Payout_Period_Value{period_suffix}'
            tracker_df[f'Phasing_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_achieved_col] >= 1.0,
                tracker_df[f'Phasing_Period_Rebate_Percent{period_suffix}'] * tracker_df[value_col] / 100.0,
                0.0
            )
    
    return tracker_df


def _add_bonus_phasing_columns(tracker_df: pd.DataFrame, phasing_row: pd.Series, 
                             period_num: int, suffix: str, structured_data: Dict, 
                             sales_df: pd.DataFrame, is_volume_based: bool, 
                             payout_products_enabled: bool, target_volume_col: str, target_value_col: str) -> pd.DataFrame:
    """Add bonus phasing columns for a specific period"""
    
    # Column naming for this period
    period_suffix = f'_{period_num}{suffix}'
    
    # Extract bonus dates and convert to datetime
    bonus_phasing_from = pd.to_datetime(phasing_row['bonus_phasing_from_date']).tz_localize(None)
    bonus_phasing_to = pd.to_datetime(phasing_row['bonus_phasing_to_date']).tz_localize(None)
    bonus_payout_from = pd.to_datetime(phasing_row['bonus_payout_from_date']).tz_localize(None)
    bonus_payout_to = pd.to_datetime(phasing_row['bonus_payout_to_date']).tz_localize(None)
    
    # Basic bonus info columns
    tracker_df[f'Is_Bonus{period_suffix}'] = bool(phasing_row['is_bonus'])
    
    # Get bonus values
    bonus_rebate_value = float(phasing_row['bonus_rebate_value']) if pd.notna(phasing_row['bonus_rebate_value']) else 0.0
    bonus_rebate_percent = float(phasing_row['bonus_rebate_percentage']) if pd.notna(phasing_row['bonus_rebate_percentage']) else 0.0
    bonus_target_percent = float(phasing_row['bonus_phasing_target_percent']) if pd.notna(phasing_row['bonus_phasing_target_percent']) else 0.0
    
    tracker_df[f'Bonus_Rebate_Value{period_suffix}'] = bonus_rebate_value
    tracker_df[f'Bonus_Phasing_Period_Rebate_Percent{period_suffix}'] = bonus_rebate_percent
    tracker_df[f'Bonus_Phasing_Target_Percent{period_suffix}'] = bonus_target_percent
    
    # Calculate bonus targets
    bonus_target_pct = bonus_target_percent / 100.0
    
    if target_volume_col in tracker_df.columns:
        tracker_df[f'Bonus_Target_Volume{period_suffix}'] = (
            tracker_df[target_volume_col].astype(float) * bonus_target_pct
        )
    else:
        tracker_df[f'Bonus_Target_Volume{period_suffix}'] = 0.0
    
    if target_value_col in tracker_df.columns:
        tracker_df[f'Bonus_Target_Value{period_suffix}'] = (
            tracker_df[target_value_col].astype(float) * bonus_target_pct
        )
    else:
        tracker_df[f'Bonus_Target_Value{period_suffix}'] = 0.0
    
    # Calculate bonus phasing period actuals
    bonus_phasing_actuals = _calculate_period_sales(
        sales_df, bonus_phasing_from, bonus_phasing_to, tracker_df['credit_account']
    )
    
    tracker_df[f'Bonus_Phasing_Period_Volume{period_suffix}'] = bonus_phasing_actuals['volume']
    tracker_df[f'Bonus_Phasing_Period_Value{period_suffix}'] = bonus_phasing_actuals['value']
    
    # Calculate bonus payout period actuals
    bonus_payout_actuals = _calculate_period_sales(
        sales_df, bonus_payout_from, bonus_payout_to, tracker_df['credit_account']
    )
    
    tracker_df[f'Bonus_Payout_Period_Volume{period_suffix}'] = bonus_payout_actuals['volume']
    tracker_df[f'Bonus_Payout_Period_Value{period_suffix}'] = bonus_payout_actuals['value']
    
    # Calculate bonus percentage achieved
    if is_volume_based:
        bonus_target_col = f'Bonus_Target_Volume{period_suffix}'
        bonus_actual_col = f'Bonus_Phasing_Period_Volume{period_suffix}'
    else:
        bonus_target_col = f'Bonus_Target_Value{period_suffix}'
        bonus_actual_col = f'Bonus_Phasing_Period_Value{period_suffix}'
    
    tracker_df[f'Percent_Bonus_Achieved{period_suffix}'] = np.where(
        tracker_df[bonus_target_col] > 0,
        tracker_df[bonus_actual_col] / tracker_df[bonus_target_col],
        0.0
    )
    
    # Calculate bonus payout product actuals
    bonus_payout_product_actuals = _calculate_payout_product_period_sales(
        structured_data, sales_df, bonus_payout_from, bonus_payout_to, tracker_df['credit_account'], suffix
    )
    
    tracker_df[f'Bonus_Payout_Period_Payout_Product_Volume{period_suffix}'] = bonus_payout_product_actuals['volume']
    tracker_df[f'Bonus_Payout_Period_Payout_Product_Value{period_suffix}'] = bonus_payout_product_actuals['value']
    
    # Calculate bonus payout with achievement threshold
    # Bonus_Payout_n = IF(Percent_Bonus_Achieved_n >= 100%, payout_formula, 0)
    percent_bonus_achieved_col = f'Percent_Bonus_Achieved{period_suffix}'
    
    if is_volume_based:
        # Volume-based bonus payout - with achievement threshold
        if payout_products_enabled:
            volume_col = f'Bonus_Payout_Period_Payout_Product_Volume{period_suffix}'
            # Bonus_Payout_n = IF(Percent_Bonus_Achieved_n >= 100%, Bonus_Payout_Period_Payout_Product_Volume_n * Bonus_Phasing_Period_Rebate_Value_n, 0)
            tracker_df[f'Bonus_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_bonus_achieved_col] >= 1.0,
                tracker_df[volume_col] * bonus_rebate_value,
                0.0
            )
        else:
            volume_col = f'Bonus_Payout_Period_Volume{period_suffix}'
            # Bonus_Payout_n = IF(Percent_Bonus_Achieved_n >= 100%, Bonus_Payout_Period_Volume_n * Bonus_Phasing_Period_Rebate_Value_n, 0)
            tracker_df[f'Bonus_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_bonus_achieved_col] >= 1.0,
                tracker_df[volume_col] * bonus_rebate_value,
                0.0
            )
    else:
        # Value-based bonus payout with achievement threshold
        if payout_products_enabled:
            value_col = f'Bonus_Payout_Period_Payout_Product_Value{period_suffix}'
            # Bonus_Payout_n = IF(Percent_Bonus_Achieved_n >= 100%, Bonus_Payout_Period_Payout_Product_Value_n * Bonus_Phasing_Period_Rebate_Percent_n, 0)
            tracker_df[f'Bonus_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_bonus_achieved_col] >= 1.0,
                tracker_df[value_col] * bonus_rebate_percent / 100.0,
                0.0
            )
        else:
            value_col = f'Bonus_Payout_Period_Value{period_suffix}'
            # Bonus_Payout_n = IF(Percent_Bonus_Achieved_n >= 100%, Bonus_Payout_Period_Value_n * Bonus_Phasing_Period_Rebate_Percent_n, 0)
            tracker_df[f'Bonus_Payout_{period_num}{suffix}'] = np.where(
                tracker_df[percent_bonus_achieved_col] >= 1.0,
                tracker_df[value_col] * bonus_rebate_percent / 100.0,
                0.0
            )
    
    return tracker_df


def _calculate_period_sales(sales_df: pd.DataFrame, date_from: datetime, date_to: datetime, 
                          credit_accounts: pd.Series) -> Dict[str, pd.Series]:
    """Calculate sales volume and value for a specific period"""
    
    # Filter sales data for the period
    sales_temp = sales_df.copy()
    sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date']).dt.tz_localize(None)
    
    period_sales = sales_temp[
        (sales_temp['sale_date'] >= date_from) &
        (sales_temp['sale_date'] <= date_to)
    ].copy()
    
    if period_sales.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Aggregate by credit_account
    period_actuals = period_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    period_actuals['credit_account'] = period_actuals['credit_account'].astype(str)
    credit_accounts_str = credit_accounts.astype(str)
    
    # Create a DataFrame to merge
    result_df = pd.DataFrame({'credit_account': credit_accounts_str})
    result_df = result_df.merge(
        period_actuals, on='credit_account', how='left'
    ).fillna(0.0)
    
    return {
        'volume': result_df['volume'].astype(float),
        'value': result_df['value'].astype(float)
    }


def _calculate_payout_product_period_sales(structured_data: Dict, sales_df: pd.DataFrame, 
                                         date_from: datetime, date_to: datetime, 
                                         credit_accounts: pd.Series, suffix: str) -> Dict[str, pd.Series]:
    """Calculate payout product sales for a specific period"""
    
    # Get payout products
    products_df = structured_data['products']
    
    if suffix:  # Additional scheme
        scheme_num = suffix.replace('_p', '')
        payout_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') & 
            (products_df['scheme_name'] == f'Additional Scheme {scheme_num}') &
            (products_df['is_payout_product'] == True)
        ]
    else:  # Main scheme
        payout_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') & 
            (products_df['is_payout_product'] == True)
        ]
    
    if payout_products.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Get payout product materials
    payout_materials = set(payout_products['material_code'].tolist())
    
    # Filter sales data for the period and payout products
    sales_temp = sales_df.copy()
    sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date']).dt.tz_localize(None)
    
    period_payout_sales = sales_temp[
        (sales_temp['sale_date'] >= date_from) &
        (sales_temp['sale_date'] <= date_to) &
        (sales_temp['material'].isin(payout_materials))
    ].copy()
    
    if period_payout_sales.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Aggregate by credit_account
    payout_actuals = period_payout_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    payout_actuals['credit_account'] = payout_actuals['credit_account'].astype(str)
    credit_accounts_str = credit_accounts.astype(str)
    
    # Create a DataFrame to merge
    result_df = pd.DataFrame({'credit_account': credit_accounts_str})
    result_df = result_df.merge(
        payout_actuals, on='credit_account', how='left'
    ).fillna(0.0)
    
    return {
        'volume': result_df['volume'].astype(float),
        'value': result_df['value'].astype(float)
    }


def _calculate_final_phasing_payout(tracker_df: pd.DataFrame, bonus_payouts: List[str], 
                                  regular_payouts: List[str], final_col: str) -> pd.DataFrame:
    """Calculate final phasing payout using nested IF conditions checking bonus achievements sequentially
    
    Formula: FINAL_PHASING_PAYOUT=if(Percent_Bonus_Achieved_1>=100%,Bonus_Payout_1,
                                     if(Percent_Bonus_Achieved_2>=100%,Bonus_Payout_2,
                                        if(Percent_Bonus_Achieved_3>=100%,Bonus_Payout_3,
                                           (Phasing_Payout_1+Phasing_Payout_2+Phasing_Payout_3))))
    """
    
    # Initialize the final payout column with zeros
    tracker_df[final_col] = 0.0
    
    # Get suffix from final_col (e.g., "_p1" from "FINAL_PHASING_PAYOUT_p1")
    suffix = ''
    if final_col.endswith('_p1'):
        suffix = '_p1'
    elif final_col.endswith('_p2'):
        suffix = '_p2'
    elif final_col.endswith('_p3'):
        suffix = '_p3'
    # For main scheme, suffix remains empty
    
    # Define the nested IF logic: check bonus achievements in sequence (1, 2, 3)
    bonus_achievement_cols = [
        f'Percent_Bonus_Achieved_1{suffix}',
        f'Percent_Bonus_Achieved_2{suffix}',
        f'Percent_Bonus_Achieved_3{suffix}'
    ]
    
    bonus_payout_cols = [
        f'Bonus_Payout_1{suffix}',
        f'Bonus_Payout_2{suffix}',
        f'Bonus_Payout_3{suffix}'
    ]
    
    regular_payout_cols = [
        f'Phasing_Payout_1{suffix}',
        f'Phasing_Payout_2{suffix}',
        f'Phasing_Payout_3{suffix}'
    ]
    
    # Initialize missing columns with zeros
    for col in bonus_achievement_cols + bonus_payout_cols + regular_payout_cols:
        if col not in tracker_df.columns:
            tracker_df[col] = 0.0
    
    # Implement nested IF logic: check Percent_Bonus_Achieved_1 >= 100% first, then 2, then 3
    # Start with the fallback: sum of regular phasing payouts
    available_regular_payouts = [col for col in regular_payout_cols if col in tracker_df.columns]
    if available_regular_payouts:
        tracker_df[final_col] = tracker_df[available_regular_payouts].sum(axis=1)
    
    # Apply nested IF logic in correct order (highest priority first)
    # Check Percent_Bonus_Achieved_1 >= 100% (highest priority)
    # Note: Percentage values are in decimal format (e.g., 3.6923 for 369.23%), so check >= 1.0
    if (bonus_achievement_cols[0] in tracker_df.columns and 
        bonus_payout_cols[0] in tracker_df.columns):
        condition_1 = tracker_df[bonus_achievement_cols[0]] >= 1.0
        tracker_df.loc[condition_1, final_col] = tracker_df.loc[condition_1, bonus_payout_cols[0]]
    
    # Check Percent_Bonus_Achieved_2 >= 100% (second priority) - only if condition_1 is not met
    if (bonus_achievement_cols[1] in tracker_df.columns and 
        bonus_payout_cols[1] in tracker_df.columns):
        condition_2 = (tracker_df[bonus_achievement_cols[1]] >= 1.0) & (tracker_df[bonus_achievement_cols[0]] < 1.0)
        tracker_df.loc[condition_2, final_col] = tracker_df.loc[condition_2, bonus_payout_cols[1]]
    
    # Check Percent_Bonus_Achieved_3 >= 100% (lowest priority) - only if condition_1 and condition_2 are not met
    if (bonus_achievement_cols[2] in tracker_df.columns and 
        bonus_payout_cols[2] in tracker_df.columns):
        condition_3 = (tracker_df[bonus_achievement_cols[2]] >= 1.0) & (tracker_df[bonus_achievement_cols[0]] < 1.0) & (tracker_df[bonus_achievement_cols[1]] < 1.0)
        tracker_df.loc[condition_3, final_col] = tracker_df.loc[condition_3, bonus_payout_cols[2]]
    
    # Debug: Print detailed calculations for troubleshooting
    sample_accounts = tracker_df[tracker_df[final_col] > 0].head(3)
    if not sample_accounts.empty:
        print(f"       🔍 DEBUG: Sample {final_col} calculations:")
        for idx, row in sample_accounts.iterrows():
            print(f"         Account {row['credit_account']}: {final_col} = {row[final_col]:.2f}")
            # Check each bonus condition
            for i in range(1, 4):
                bonus_achieved = row.get(f'Percent_Bonus_Achieved_{i}{suffix}', 0)
                bonus_payout = row.get(f'Bonus_Payout_{i}{suffix}', 0)
                print(f"           Bonus_{i}: {bonus_achieved:.1f}% (>=100%: {bonus_achieved >= 100}) → {bonus_payout:.2f}")
            
            # Check phasing payouts
            phasing_sum = 0
            for i in range(1, 4):
                phasing_payout = row.get(f'Phasing_Payout_{i}{suffix}', 0)
                phasing_sum += phasing_payout
                print(f"           Phasing_{i}: {phasing_payout:.2f}")
            print(f"           Phasing Sum: {phasing_sum:.2f}")
            
            # Determine expected result
            if row.get(f'Percent_Bonus_Achieved_1{suffix}', 0) >= 100:
                expected = row.get(f'Bonus_Payout_1{suffix}', 0)
                print(f"           EXPECTED: Bonus_1 (highest priority) = {expected:.2f}")
            elif row.get(f'Percent_Bonus_Achieved_2{suffix}', 0) >= 100:
                expected = row.get(f'Bonus_Payout_2{suffix}', 0)
                print(f"           EXPECTED: Bonus_2 (second priority) = {expected:.2f}")
            elif row.get(f'Percent_Bonus_Achieved_3{suffix}', 0) >= 100:
                expected = row.get(f'Bonus_Payout_3{suffix}', 0)
                print(f"           EXPECTED: Bonus_3 (third priority) = {expected:.2f}")
            else:
                expected = phasing_sum
                print(f"           EXPECTED: Phasing Sum (fallback) = {expected:.2f}")
            
            print(f"           ACTUAL: {row[final_col]:.2f}")
            print(f"           MATCH: {abs(row[final_col] - expected) < 0.01}")
    
    return tracker_df
