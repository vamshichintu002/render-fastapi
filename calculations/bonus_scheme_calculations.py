"""
Bonus Scheme Calculations Module
Handles bonus scheme calculations for main scheme only (not additional schemes)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime


def calculate_bonus_scheme_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                  sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate all bonus scheme-related columns for main scheme only
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing bonus schemes
        sales_df: Sales data for bonus calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with bonus scheme columns
    """
    print("🎯 Calculating bonus scheme columns...")
    
    # Get configuration manager to check bonus scheme configuration
    config_manager = structured_data.get('config_manager')
    
    # Only calculate bonus schemes for main scheme if enabled
    if config_manager and config_manager.is_main_feature_enabled('bonusSchemes'):
        print("   🎯 Main scheme bonus schemes: ENABLED - calculating...")
        tracker_df = _add_main_scheme_bonus_columns(tracker_df, structured_data, sales_df, scheme_config)
    else:
        print("   ⚡ Main scheme bonus schemes: DISABLED - skipping for performance")
    
    print("✅ Bonus scheme calculations completed")
    return tracker_df


def _add_main_scheme_bonus_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                  sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add bonus scheme columns for main scheme"""
    print("   📊 Adding main scheme bonus columns...")
    
    # Get bonus schemes data for main scheme
    bonus_df = structured_data['bonus']
    main_bonus = bonus_df[bonus_df['scheme_type'] == 'main_scheme'].copy()
    
    if main_bonus.empty:
        print("   ℹ️ No main scheme bonus data found - skipping")
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
    
    # Sort bonus schemes by bonus_id
    main_bonus = main_bonus.sort_values('bonus_id')
    
    # Apply bonus scheme calculations for each bonus scheme
    for _, bonus_row in main_bonus.iterrows():
        bonus_id = int(bonus_row['bonus_id'])
        print(f"       📍 Processing bonus scheme {bonus_id}")
        
        tracker_df = _add_bonus_scheme_calculations(
            tracker_df, bonus_row, bonus_id, structured_data, sales_df, 
            scheme_config, is_volume_based
        )
    
    print(f"   ✅ Bonus scheme calculations completed for main scheme")
    return tracker_df


def _add_bonus_scheme_calculations(tracker_df: pd.DataFrame, bonus_row: pd.Series, 
                                  bonus_id: int, structured_data: Dict, sales_df: pd.DataFrame,
                                  scheme_config: Dict, is_volume_based: bool) -> pd.DataFrame:
    """Add bonus scheme calculations for a specific bonus scheme"""
    
    # Extract bonus configuration values
    bonus_name = bonus_row['bonus_name']
    main_scheme_target_percent = float(bonus_row['main_scheme_target_percent']) if pd.notna(bonus_row['main_scheme_target_percent']) else 0.0
    minimum_target = float(bonus_row['minimum_target']) if pd.notna(bonus_row['minimum_target']) else 0.0
    mandatory_product_target_percent = float(bonus_row['mandatory_product_target_percent']) if pd.notna(bonus_row['mandatory_product_target_percent']) else 0.0
    minimum_mandatory_product_target = float(bonus_row['minimum_mandatory_product_target']) if pd.notna(bonus_row['minimum_mandatory_product_target']) else 0.0
    reward_on_total_percent = float(bonus_row['reward_on_total_percent']) if pd.notna(bonus_row['reward_on_total_percent']) else 0.0
    reward_on_mandatory_product_percent = float(bonus_row['reward_on_mandatory_product_percent']) if pd.notna(bonus_row['reward_on_mandatory_product_percent']) else 0.0
    
    # Bonus period dates
    bonus_period_from = pd.to_datetime(bonus_row['bonus_period_from']).tz_localize(None)
    bonus_period_to = pd.to_datetime(bonus_row['bonus_period_to']).tz_localize(None)
    bonus_payout_from = pd.to_datetime(bonus_row['bonus_payout_from']).tz_localize(None)
    bonus_payout_to = pd.to_datetime(bonus_row['bonus_payout_to']).tz_localize(None)
    
    # 3️⃣ Bonus Scheme Configuration Columns
    tracker_df[f'Bonus_Scheme_No_{bonus_id}'] = bonus_name
    tracker_df[f'Bonus_Scheme_{bonus_id}%_Main_Scheme_Target'] = main_scheme_target_percent
    tracker_df[f'Bonus_Scheme_{bonus_id}_Minimum_Target'] = minimum_target
    tracker_df[f'Mandatory_Product_Target_%_Bonus_Scheme_{bonus_id}'] = mandatory_product_target_percent
    tracker_df[f'Minimum_Mandatory_Product_Target_Bonus_Scheme_{bonus_id}'] = minimum_mandatory_product_target
    tracker_df[f'%_Reward_on_Total_Bonus_Scheme_{bonus_id}'] = reward_on_total_percent
    tracker_df[f'Reward_on_Mandatory_Product_%_Bonus_Scheme_{bonus_id}'] = reward_on_mandatory_product_percent
    
    # 4️⃣ Main Scheme Bonus Targets - Updated formula with MAX logic
    # Main_Scheme_Bonus_Target_Volume_n = max(Bonus_Scheme_n_Minimum_Target, (Bonus_Scheme_n%_Main_Scheme_Target * target_volume))
    # Main_Scheme_Bonus_Target_Value_n = max(Bonus_Scheme_n_Minimum_Target, (Bonus_Scheme_n%_Main_Scheme_Target * target_value))
    
    if is_volume_based:
        period_target_col = 'target_volume'
        if period_target_col in tracker_df.columns:
            # Calculate percentage-based target using main_scheme_target_percent (NOT mandatory_product_target_percent)
            percentage_based_target = (
                main_scheme_target_percent / 100.0 * tracker_df[period_target_col].astype(float)
            )
            # Apply MAX logic with minimum target
            tracker_df[f'Main_Scheme_Bonus_Target_Volume_{bonus_id}'] = np.maximum(
                minimum_target,
                percentage_based_target
            )
        else:
            tracker_df[f'Main_Scheme_Bonus_Target_Volume_{bonus_id}'] = minimum_target
            
        tracker_df[f'Main_Scheme_Bonus_Target_Value_{bonus_id}'] = 0.0  # Not applicable for volume-based
    else:
        period_target_col = 'target_value'
        if period_target_col in tracker_df.columns:
            # Calculate percentage-based target using main_scheme_target_percent (NOT mandatory_product_target_percent)
            percentage_based_target = (
                main_scheme_target_percent / 100.0 * tracker_df[period_target_col].astype(float)
            )
            # Apply MAX logic with minimum target
            tracker_df[f'Main_Scheme_Bonus_Target_Value_{bonus_id}'] = np.maximum(
                minimum_target,
                percentage_based_target
            )
        else:
            tracker_df[f'Main_Scheme_Bonus_Target_Value_{bonus_id}'] = minimum_target
            
        tracker_df[f'Main_Scheme_Bonus_Target_Volume_{bonus_id}'] = 0.0  # Not applicable for value-based
    
    # 5️⃣ Actual Bonus Achievements
    # Calculate actual bonus sales for bonus period
    bonus_actuals = _calculate_bonus_period_sales(
        sales_df, bonus_period_from, bonus_period_to, tracker_df['credit_account'], structured_data
    )
    
    tracker_df[f'Actual_Bonus_Volume_{bonus_id}'] = bonus_actuals['volume']
    tracker_df[f'Actual_Bonus_Value_{bonus_id}'] = bonus_actuals['value']
    
    # Calculate bonus scheme percentage achieved
    if is_volume_based:
        target_col = f'Main_Scheme_Bonus_Target_Volume_{bonus_id}'
        actual_col = f'Actual_Bonus_Volume_{bonus_id}'
    else:
        target_col = f'Main_Scheme_Bonus_Target_Value_{bonus_id}'
        actual_col = f'Actual_Bonus_Value_{bonus_id}'
    
    tracker_df[f'Bonus_Scheme_%_Achieved_{bonus_id}'] = np.where(
        tracker_df[target_col] > 0,
        tracker_df[actual_col] / tracker_df[target_col],
        0.0
    )
    
    # 6️⃣ Bonus Payout Period
    # Calculate actual bonus payout period sales
    bonus_payout_actuals = _calculate_bonus_period_sales(
        sales_df, bonus_payout_from, bonus_payout_to, tracker_df['credit_account'], structured_data
    )
    
    tracker_df[f'Actual_Bonus_Payout_Period_Volume_{bonus_id}'] = bonus_payout_actuals['volume']
    tracker_df[f'Actual_Bonus_Payout_Period_Value_{bonus_id}'] = bonus_payout_actuals['value']
    
    # 7️⃣ Mandatory Product Bonus
    # Calculate mandatory product bonus targets
    _calculate_mandatory_product_bonus_targets(
        tracker_df, bonus_id, mandatory_product_target_percent, minimum_mandatory_product_target, is_volume_based
    )
    
    # Calculate actual bonus MP sales for bonus period
    bonus_mp_actuals = _calculate_bonus_mp_period_sales(
        sales_df, bonus_period_from, bonus_period_to, tracker_df['credit_account'], structured_data
    )
    
    tracker_df[f'Actual_Bonus_MP_Volume_{bonus_id}'] = bonus_mp_actuals['volume']
    tracker_df[f'Actual_Bonus_MP_Value_{bonus_id}'] = bonus_mp_actuals['value']
    
    # Calculate MP percentage achieved
    if is_volume_based:
        mp_target_col = f'Mandatory_Product_Bonus_Target_Volume_{bonus_id}'
        mp_actual_col = f'Actual_Bonus_MP_Volume_{bonus_id}'
    else:
        mp_target_col = f'Mandatory_Product_Bonus_Target_Value_{bonus_id}'
        mp_actual_col = f'Actual_Bonus_MP_Value_{bonus_id}'
    
    # Fixed Formula: %_MP_Achieved_n should return decimal values (not percentage)
    # If scheme type = volume: %_MP_Achieved_n = (Actual_Bonus_MP_Volume_n / Mandatory_Product_Bonus_Target_Volume_n)
    # If scheme type = value: %_MP_Achieved_n = (Actual_Bonus_MP_Value_n / Mandatory_Product_Bonus_Target_Value_n)
    tracker_df[f'%_MP_Achieved_{bonus_id}'] = np.where(
        tracker_df[mp_target_col] > 0,
        tracker_df[mp_actual_col] / tracker_df[mp_target_col],
        0.0
    )
    
    # 8️⃣ Final Bonus Payouts
    _calculate_final_bonus_payouts(
        tracker_df, bonus_id, reward_on_total_percent, reward_on_mandatory_product_percent, 
        is_volume_based, structured_data, sales_df, bonus_payout_from, bonus_payout_to
    )
    
    return tracker_df


def _calculate_bonus_period_sales(sales_df: pd.DataFrame, date_from: datetime, date_to: datetime, 
                                 credit_accounts: pd.Series, structured_data: Dict) -> Dict[str, pd.Series]:
    """Calculate sales volume and value for bonus period using eligible SKUs"""
    
    # Get eligible products for main scheme
    products_df = structured_data['products']
    main_products = products_df[
        (products_df['scheme_type'] == 'main_scheme') & 
        (products_df['is_product_data'] == True)
    ]
    
    if main_products.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Get eligible materials
    eligible_materials = set(main_products['material_code'].tolist())
    
    # Filter sales data for the period and eligible products
    sales_temp = sales_df.copy()
    sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date']).dt.tz_localize(None)
    
    period_sales = sales_temp[
        (sales_temp['sale_date'] >= date_from) &
        (sales_temp['sale_date'] <= date_to) &
        (sales_temp['material'].isin(eligible_materials))
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


def _calculate_bonus_mp_period_sales(sales_df: pd.DataFrame, date_from: datetime, date_to: datetime, 
                                    credit_accounts: pd.Series, structured_data: Dict) -> Dict[str, pd.Series]:
    """Calculate mandatory product sales for bonus period"""
    
    # Get mandatory products for main scheme
    products_df = structured_data['products']
    mandatory_products = products_df[
        (products_df['scheme_type'] == 'main_scheme') & 
        (products_df['is_mandatory_product'] == True)
    ]
    
    if mandatory_products.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Get mandatory product materials
    mandatory_materials = set(mandatory_products['material_code'].tolist())
    
    # Filter sales data for the period and mandatory products
    sales_temp = sales_df.copy()
    sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date']).dt.tz_localize(None)
    
    period_mp_sales = sales_temp[
        (sales_temp['sale_date'] >= date_from) &
        (sales_temp['sale_date'] <= date_to) &
        (sales_temp['material'].isin(mandatory_materials))
    ].copy()
    
    if period_mp_sales.empty:
        # Return zeros for all accounts
        return {
            'volume': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index),
            'value': pd.Series([0.0] * len(credit_accounts), index=credit_accounts.index)
        }
    
    # Aggregate by credit_account
    mp_actuals = period_mp_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    mp_actuals['credit_account'] = mp_actuals['credit_account'].astype(str)
    credit_accounts_str = credit_accounts.astype(str)
    
    # Create a DataFrame to merge
    result_df = pd.DataFrame({'credit_account': credit_accounts_str})
    result_df = result_df.merge(
        mp_actuals, on='credit_account', how='left'
    ).fillna(0.0)
    
    return {
        'volume': result_df['volume'].astype(float),
        'value': result_df['value'].astype(float)
    }


def _calculate_mandatory_product_bonus_targets(tracker_df: pd.DataFrame, bonus_id: int, 
                                              mandatory_product_target_percent: float, 
                                              minimum_mandatory_product_target: float, 
                                              is_volume_based: bool) -> None:
    """Calculate mandatory product bonus targets using corrected formulas"""
    
    # Updated formulas:
    # Mandatory_Product_Bonus_Target_Volume_n = max(Minimum_Mandatory_Product_Target_Bonus_Scheme_n, (Mandatory_Product_Target_%_Bonus_Scheme_n * MP_FINAL_TARGET))
    # Mandatory_Product_Bonus_Target_Value_n = max(Minimum_Mandatory_Product_Target_Bonus_Scheme_n, (Mandatory_Product_Target_%_Bonus_Scheme_n * MP_FINAL_TARGET))
    
    if is_volume_based:
        # Use MP_FINAL_TARGET for volume-based calculation
        mp_final_target_col = 'MP_FINAL_TARGET'
        
        if mp_final_target_col in tracker_df.columns:
            # Calculate percentage-based target using MP_FINAL_TARGET
            percentage_based_target = (
                mandatory_product_target_percent / 100.0 * tracker_df[mp_final_target_col].astype(float)
            )
            
            # Apply MAX logic with minimum target
            tracker_df[f'Mandatory_Product_Bonus_Target_Volume_{bonus_id}'] = np.maximum(
                minimum_mandatory_product_target,
                percentage_based_target
            )
        else:
            # Fallback if MP_FINAL_TARGET not available
            tracker_df[f'Mandatory_Product_Bonus_Target_Volume_{bonus_id}'] = minimum_mandatory_product_target
        
        tracker_df[f'Mandatory_Product_Bonus_Target_Value_{bonus_id}'] = 0.0  # Not applicable for volume-based
        
    else:
        # Value-based calculation - use MP_FINAL_TARGET
        mp_final_target_col = 'MP_FINAL_TARGET'
        
        if mp_final_target_col in tracker_df.columns:
            # Calculate percentage-based target using MP_FINAL_TARGET
            percentage_based_target = (
                mandatory_product_target_percent / 100.0 * tracker_df[mp_final_target_col].astype(float)
            )
            
            # Apply MAX logic with minimum target
            tracker_df[f'Mandatory_Product_Bonus_Target_Value_{bonus_id}'] = np.maximum(
                minimum_mandatory_product_target,
                percentage_based_target
            )
        else:
            # Fallback if MP_FINAL_TARGET not available
            tracker_df[f'Mandatory_Product_Bonus_Target_Value_{bonus_id}'] = minimum_mandatory_product_target
        
        tracker_df[f'Mandatory_Product_Bonus_Target_Volume_{bonus_id}'] = 0.0  # Not applicable for value-based


def _calculate_final_bonus_payouts(tracker_df: pd.DataFrame, bonus_id: int, 
                                  reward_on_total_percent: float, reward_on_mandatory_product_percent: float,
                                  is_volume_based: bool, structured_data: Dict, sales_df: pd.DataFrame,
                                  bonus_payout_from: datetime, bonus_payout_to: datetime) -> None:
    """Calculate final bonus payouts with achievement thresholds"""
    
    # 8️⃣ Final Bonus Payouts
    # Main scheme bonus payout
    achievement_col = f'Bonus_Scheme_%_Achieved_{bonus_id}'
    
    if is_volume_based:
        payout_period_col = f'Actual_Bonus_Payout_Period_Volume_{bonus_id}'
    else:
        payout_period_col = f'Actual_Bonus_Payout_Period_Value_{bonus_id}'
    
    # Bonus Scheme n Bonus Payout = IF(Bonus Scheme % Achieved n >= 100%, rewardOnTotalPercent/100 * Actual Bonus Payout Period Volume/Value n, 0)
    tracker_df[f'Bonus_Scheme_{bonus_id}_Bonus_Payout'] = np.where(
        tracker_df[achievement_col] >= 1.0,
        (reward_on_total_percent / 100.0) * tracker_df[payout_period_col].astype(float),
        0.0
    )
    
    # Calculate MP payout period actuals (for MP bonus payout calculation)
    mp_payout_actuals = _calculate_bonus_mp_period_sales(
        sales_df, bonus_payout_from, bonus_payout_to, tracker_df['credit_account'], structured_data
    )
    
    if is_volume_based:
        mp_payout_col = f'Actual_Bonus_Payout_Period_MP_Volume_{bonus_id}'
        tracker_df[mp_payout_col] = mp_payout_actuals['volume']
    else:
        mp_payout_col = f'Actual_Bonus_Payout_Period_MP_Value_{bonus_id}'
        tracker_df[mp_payout_col] = mp_payout_actuals['value']
    
    # MP achievement column
    mp_achievement_col = f'%_MP_Achieved_{bonus_id}'
    
    # Bonus Scheme n MP Payout = IF(% MP Achieved n >= 100%, rewardOnMandatoryProductPercent/100 * Actual Bonus Payout Period MP Volume/Value n, 0)
    tracker_df[f'Bonus_Scheme_{bonus_id}_MP_Payout'] = np.where(
        tracker_df[mp_achievement_col] >= 1.0,
        (reward_on_mandatory_product_percent / 100.0) * tracker_df[mp_payout_col].astype(float),
        0.0
    )
