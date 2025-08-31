"""
Estimated Calculations Module

This module handles all estimated calculations based on qualification rates.
It creates estimated payout columns for both volume and value schemes.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional


def calculate_estimated_columns(tracker_df: pd.DataFrame, 
                              structured_data: Dict[str, Any],
                              scheme_type: str = 'volume',
                              scheme_config: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Calculate all estimated columns based on qualification rates.
    
    Args:
        tracker_df: The main tracker DataFrame
        structured_data: Structured data containing slabs and other scheme info
        scheme_type: 'volume' or 'value' scheme type
    
    Returns:
        DataFrame with all estimated columns added
    """
    
    print(f"🧮 Starting estimated calculations for {scheme_type} scheme...")
    
    # Step 1: Extract qualification rates from slabs
    qualification_rates = _extract_qualification_rates(structured_data, scheme_type)
    
    # Step 2: Calculate ASP (Average Selling Price)
    tracker_df = _calculate_asp(tracker_df)
    
    # Step 3: Apply qualification rates to tracker
    tracker_df = _apply_qualification_rates(tracker_df, qualification_rates, scheme_type)
    
    # Step 4: Calculate estimated columns based on scheme type
    if scheme_type.lower() == 'volume':
        tracker_df = _calculate_volume_estimated_columns(tracker_df)
    else:
        tracker_df = _calculate_value_estimated_columns(tracker_df)
    
    # Step 5: Calculate HO Scheme bonus estimates (if applicable)
    tracker_df = _calculate_ho_scheme_bonus_estimates(tracker_df, structured_data, scheme_type, scheme_config)
    
    # Step 6: Clean infinity values in estimated volume fields
    tracker_df = _clean_infinity_values(tracker_df)
    
    # Step 7: Calculate final estimated scheme payout
    tracker_df = _calculate_final_estimated_payout(tracker_df)
    
    print(f"✅ Estimated calculations completed for {scheme_type} scheme")
    return tracker_df


def _extract_qualification_rates(structured_data: Dict[str, Any], 
                               scheme_type: str) -> Dict[str, float]:
    """
    Extract qualification rates from slabs for main and additional schemes.
    
    Args:
        structured_data: Structured data containing slabs
        scheme_type: 'volume' or 'value' scheme type
    
    Returns:
        Dictionary with qualification rates for main and additional schemes
    """
    
    qualification_rates = {}
    
    try:
        # Extract from main scheme slabs
        if 'slabs' in structured_data:
            main_slabs = structured_data['slabs']
            if not main_slabs.empty and 'dealer_qualify_percent' in main_slabs.columns:
                # Get first slab's qualification rate as fallback
                first_slab_rate = main_slabs.iloc[0]['dealer_qualify_percent'] / 100.0
                qualification_rates['main'] = first_slab_rate
                print(f"   📊 Main scheme qualification rate: {first_slab_rate:.2%}")
        
        # Extract from additional schemes slabs
        if 'slabs' in structured_data:
            slabs_df = structured_data['slabs']
            if not slabs_df.empty and 'dealer_qualify_percent' in slabs_df.columns:
                # Filter for additional scheme slabs
                additional_slabs = slabs_df[slabs_df['scheme_type'] == 'additional_scheme']
                if not additional_slabs.empty:
                    # Group by scheme_id to get qualification rates for each additional scheme
                    unique_scheme_ids = additional_slabs['scheme_id'].unique()
                    for scheme_id in unique_scheme_ids:
                        if pd.notna(scheme_id):  # Check if scheme_id is not NaN
                            scheme_slabs = additional_slabs[additional_slabs['scheme_id'] == scheme_id]
                            if not scheme_slabs.empty:
                                first_slab_rate = scheme_slabs.iloc[0]['dealer_qualify_percent'] / 100.0
                                # Extract scheme number from scheme_id (e.g., 'additional_1' -> '1')
                                if isinstance(scheme_id, str) and '_' in scheme_id:
                                    scheme_num = scheme_id.split('_')[-1]
                                else:
                                    scheme_num = '1'  # Default to 1 if can't parse
                                qualification_rates[f'additional_{scheme_num}'] = first_slab_rate
                                print(f"   📊 Additional scheme {scheme_num} qualification rate: {first_slab_rate:.2%}")
        
        # If no qualification rates found, use default
        if not qualification_rates:
            qualification_rates['main'] = 0.7  # 70% default
            print(f"   ⚠️ No qualification rates found, using default: 70%")
            
    except Exception as e:
        print(f"   ⚠️ Error extracting qualification rates: {e}")
        qualification_rates['main'] = 0.7  # 70% default
    
    return qualification_rates


def _calculate_asp(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate ASP (Average Selling Price) = total_value / total_volume
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with ASP column added
    """
    
    print(f"   💰 Calculating ASP (Average Selling Price)...")
    
    # Calculate ASP for main scheme
    if 'total_value' in tracker_df.columns and 'total_volume' in tracker_df.columns:
        tracker_df['ASP'] = np.where(
            (tracker_df['total_volume'] > 0) & (tracker_df['total_value'] > 0),
            tracker_df['total_value'] / tracker_df['total_volume'],
            0.0
        )
        print(f"   ✅ ASP calculated for main scheme")
    
    # Calculate ASP for additional schemes (p1, p2, etc.)
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        value_col = f'total_value{suffix}'
        volume_col = f'total_volume{suffix}'
        
        if value_col in tracker_df.columns and volume_col in tracker_df.columns:
            asp_col = f'ASP{suffix}'
            tracker_df[asp_col] = np.where(
                (tracker_df[volume_col] > 0) & (tracker_df[value_col] > 0),
                tracker_df[value_col] / tracker_df[volume_col],
                0.0
            )
            print(f"   ✅ ASP{suffix} calculated for additional scheme {i}")
    
    return tracker_df


def _apply_qualification_rates(tracker_df: pd.DataFrame, 
                             qualification_rates: Dict[str, float],
                             scheme_type: str) -> pd.DataFrame:
    """
    Apply qualification rates to the tracker DataFrame.
    
    Args:
        tracker_df: The main tracker DataFrame
        qualification_rates: Dictionary of qualification rates
        scheme_type: 'volume' or 'value' scheme type
    
    Returns:
        DataFrame with qualification rates applied
    """
    
    print(f"   📊 Applying qualification rates...")
    
    # Apply main scheme qualification rate
    main_rate = qualification_rates.get('main', 0.7)
    tracker_df['qualification_rate'] = main_rate
    print(f"   ✅ Main scheme qualification rate: {main_rate:.2%}")
    
    # Apply additional scheme qualification rates
    for scheme_key, rate in qualification_rates.items():
        if scheme_key.startswith('additional_'):
            scheme_num = scheme_key.split('_')[1]
            suffix = f'_p{scheme_num}'
            rate_col = f'qualification_rate{suffix}'
            tracker_df[rate_col] = rate
            print(f"   ✅ Additional scheme {scheme_num} qualification rate: {rate:.2%}")
    
    return tracker_df


def _calculate_volume_estimated_columns(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate estimated columns for volume-based schemes.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with volume-based estimated columns added
    """
    
    print(f"   📊 Calculating volume-based estimated columns...")
    
    # Main scheme estimated calculations
    if 'target_volume' in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
        # Estimated Qualifiers
        tracker_df['Estimated_Qualifiers'] = 1 * tracker_df['qualification_rate']
        
        # Estimated Volume
        tracker_df['Estimated_Volume'] = tracker_df['target_volume'] * tracker_df['qualification_rate']
        
        # Estimated Value
        if 'ASP' in tracker_df.columns:
            tracker_df['Estimated_Value'] = tracker_df['ASP'] * tracker_df['Estimated_Volume']
        
        # Estimated Basic Payout
        if 'Rebate_per_Litre' in tracker_df.columns:
            tracker_df['Est.basic_payout'] = tracker_df['Estimated_Volume'] * tracker_df['Rebate_per_Litre']
        
        # Estimated Additional Payout
        if 'Additional_Rebate_on_Growth_per_Litre' in tracker_df.columns:
            tracker_df['Est.additional_payout'] = tracker_df['Estimated_Volume'] * tracker_df['Additional_Rebate_on_Growth_per_Litre']
        
        # Estimated Total Payout
        basic_col = 'Est.basic_payout' if 'Est.basic_payout' in tracker_df.columns else None
        additional_col = 'Est.additional_payout' if 'Est.additional_payout' in tracker_df.columns else None
        
        if basic_col and additional_col:
            tracker_df['Est.total_payout'] = tracker_df[basic_col] + tracker_df[additional_col]
        elif basic_col:
            tracker_df['Est.total_payout'] = tracker_df[basic_col]
        else:
            tracker_df['Est.total_payout'] = 0.0
        
        # Estimated MP Final Payout - Conditional formula
        if 'Mandatory_Product_Rebate' in tracker_df.columns and 'Estimated_Volume' in tracker_df.columns:
            tracker_df['Est.MP_Final_Payout'] = np.where(
                tracker_df['Mandatory_Product_Rebate'] > 0,
                tracker_df['Estimated_Volume'] * tracker_df['Mandatory_Product_Rebate'],
                0.0
            )
        
        print(f"   ✅ Main scheme volume estimated columns calculated")
    
    # Phasing estimated calculations
    tracker_df = _calculate_volume_phasing_estimates(tracker_df)
    
    # Additional schemes estimated calculations
    tracker_df = _calculate_volume_additional_estimates(tracker_df)
    
    return tracker_df


def _calculate_volume_phasing_estimates(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate volume-based phasing estimated columns.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with phasing estimated columns added
    """
    
    # Phasing Volume estimates
    for period in [1, 2, 3]:
        target_col = f'Phasing_Target_Volume_{period}'
        if target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
            est_volume_col = f'Est.Phasing_Volume_{period}'
            tracker_df[est_volume_col] = tracker_df[target_col] * tracker_df['qualification_rate']
    
    # Phasing Payout estimates - Conditional formulas
    for period in [1, 2, 3]:
        est_volume_col = f'Est.Phasing_Volume_{period}'
        rebate_col = f'Phasing_Period_Rebate_{period}'
        
        if est_volume_col in tracker_df.columns and rebate_col in tracker_df.columns:
            if period == 1:
                # Period 1: Direct calculation
                tracker_df[f'Est.Phasing_Payout_{period}'] = np.where(
                    tracker_df[rebate_col] > 0,
                    tracker_df[est_volume_col] * tracker_df[rebate_col],
                    0.0
                )
            else:
                # Period 2 & 3: Cumulative calculation
                prev_est_volume_col = f'Est.Phasing_Volume_{period-1}'
                if prev_est_volume_col in tracker_df.columns:
                    tracker_df[f'Est.Phasing_Payout_{period}'] = np.where(
                        tracker_df[rebate_col] > 0,
                        (tracker_df[est_volume_col] - tracker_df[prev_est_volume_col]) * tracker_df[rebate_col],
                        0.0
                    )
    
    # Final Phasing Payout
    payout_cols = [f'Est.Phasing_Payout_{period}' for period in [1, 2, 3]]
    existing_cols = [col for col in payout_cols if col in tracker_df.columns]
    
    if existing_cols:
        tracker_df['Est.FINAL_PHASING_PAYOUT'] = tracker_df[existing_cols].sum(axis=1)
    
    return tracker_df


def _calculate_volume_additional_estimates(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate volume-based additional scheme estimated columns.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with additional scheme estimated columns added
    """
    
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        
        # Check if this additional scheme exists
        target_col = f'target_volume{suffix}'
        rate_col = f'qualification_rate{suffix}'
        
        if target_col in tracker_df.columns and rate_col in tracker_df.columns:
            # Estimated Volume for additional scheme
            tracker_df[f'Estimated_Volume{suffix}'] = tracker_df[target_col] * tracker_df[rate_col]
            
            # Estimated Value for additional scheme
            asp_col = f'ASP{suffix}'
            if asp_col in tracker_df.columns:
                tracker_df[f'Estimated_Value{suffix}'] = tracker_df[f'Estimated_Volume{suffix}'] * tracker_df[asp_col]
            
            # Estimated Basic Payout for additional scheme
            rebate_col = f'Rebate_per_Litre{suffix}'
            if rebate_col in tracker_df.columns:
                tracker_df[f'Est.basic_payout{suffix}'] = tracker_df[f'Estimated_Volume{suffix}'] * tracker_df[rebate_col]
            
            # Estimated Additional Payout for additional scheme
            additional_rebate_col = f'Additional_Rebate_on_Growth_per_Litre{suffix}'
            if additional_rebate_col in tracker_df.columns:
                tracker_df[f'Est.additional_payout{suffix}'] = tracker_df[f'Estimated_Volume{suffix}'] * tracker_df[additional_rebate_col]
            
            # Estimated Total Payout for additional scheme
            basic_col = f'Est.basic_payout{suffix}'
            additional_col = f'Est.additional_payout{suffix}'
            
            if basic_col in tracker_df.columns and additional_col in tracker_df.columns:
                tracker_df[f'Est.total_Payout{suffix}'] = tracker_df[basic_col] + tracker_df[additional_col]
            elif basic_col in tracker_df.columns:
                tracker_df[f'Est.total_Payout{suffix}'] = tracker_df[basic_col]
            else:
                tracker_df[f'Est.total_Payout{suffix}'] = 0.0
            
            # Estimated MP Final Payout for additional scheme - Conditional formula
            mp_rebate_col = f'Mandatory_Product_Rebate{suffix}'
            est_volume_col = f'Estimated_Volume{suffix}'
            if mp_rebate_col in tracker_df.columns and est_volume_col in tracker_df.columns:
                tracker_df[f'Est.MP_Final_Payout{suffix}'] = np.where(
                    tracker_df[mp_rebate_col] > 0,
                    tracker_df[est_volume_col] * tracker_df[mp_rebate_col],
                    0.0
                )
    
    return tracker_df


def _calculate_value_estimated_columns(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate estimated columns for value-based schemes.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with value-based estimated columns added
    """
    
    print(f"   📊 Calculating value-based estimated columns...")
    
    # Main scheme estimated calculations
    if 'target_value' in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
        # Estimated Value
        tracker_df['Estimated_Value'] = tracker_df['target_value'] * tracker_df['qualification_rate']
        
        # Estimated Volume
        if 'ASP' in tracker_df.columns:
            tracker_df['Estimated_Volume'] = np.where(
                (tracker_df['ASP'] > 0) & (tracker_df['Estimated_Value'] > 0),
                tracker_df['Estimated_Value'] / tracker_df['ASP'],
                0.0
            )
        
        # Estimated Basic Payout
        if 'Rebate_percent' in tracker_df.columns:
            tracker_df['Est.basic_payout'] = tracker_df['Estimated_Value'] * tracker_df['Rebate_percent']
        
        # Estimated Total Payout (same as basic for value schemes)
        if 'Est.basic_payout' in tracker_df.columns:
            tracker_df['Est.total_payout'] = tracker_df['Est.basic_payout']
        
        # Estimated MP Final Payout - Conditional formula
        if 'MP_Rebate_percent' in tracker_df.columns and 'Estimated_Value' in tracker_df.columns:
            tracker_df['Est.MP_Final_Payout'] = np.where(
                tracker_df['MP_Rebate_percent'] > 0,
                tracker_df['Estimated_Value'] * tracker_df['MP_Rebate_percent'],
                0.0
            )
        
        print(f"   ✅ Main scheme value estimated columns calculated")
    
    # Phasing estimated calculations
    tracker_df = _calculate_value_phasing_estimates(tracker_df)
    
    # Additional schemes estimated calculations
    tracker_df = _calculate_value_additional_estimates(tracker_df)
    
    # Divide all estimated payout fields by 100 for value schemes
    tracker_df = _divide_estimated_payouts_by_100(tracker_df)
    
    return tracker_df


def _calculate_value_phasing_estimates(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate value-based phasing estimated columns.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with phasing estimated columns added
    """
    
    # Phasing Value estimates
    for period in [1, 2, 3]:
        target_col = f'Phasing_Target_Value_{period}'
        if target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
            est_value_col = f'Est.Phasing_Value_{period}'
            tracker_df[est_value_col] = tracker_df[target_col] * tracker_df['qualification_rate']
    
    # Phasing Payout estimates - Conditional formulas
    for period in [1, 2, 3]:
        est_value_col = f'Est.Phasing_Value_{period}'
        rebate_percent_col = f'Phasing_Period_Rebate_Percent_{period}'
        
        if est_value_col in tracker_df.columns and rebate_percent_col in tracker_df.columns:
            if period == 1:
                # Period 1: Direct calculation
                tracker_df[f'Est.Phasing_Payout_{period}'] = np.where(
                    tracker_df[rebate_percent_col] > 0,
                    tracker_df[est_value_col] * tracker_df[rebate_percent_col],
                    0.0
                )
            else:
                # Period 2 & 3: Cumulative calculation
                prev_est_value_col = f'Est.Phasing_Value_{period-1}'
                if prev_est_value_col in tracker_df.columns:
                    tracker_df[f'Est.Phasing_Payout_{period}'] = np.where(
                        tracker_df[rebate_percent_col] > 0,
                        (tracker_df[est_value_col] - tracker_df[prev_est_value_col]) * tracker_df[rebate_percent_col],
                        0.0
                    )
    
    # Final Phasing Payout
    payout_cols = [f'Est.Phasing_Payout_{period}' for period in [1, 2, 3]]
    existing_cols = [col for col in payout_cols if col in tracker_df.columns]
    
    if existing_cols:
        tracker_df['Est.FINAL_PHASING_PAYOUT'] = tracker_df[existing_cols].sum(axis=1)
    
    return tracker_df


def _calculate_value_additional_estimates(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate value-based additional scheme estimated columns.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with additional scheme estimated columns added
    """
    
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        
        # Check if this additional scheme exists
        target_col = f'target_value{suffix}'
        rate_col = f'qualification_rate{suffix}'
        
        if target_col in tracker_df.columns and rate_col in tracker_df.columns:
            # Estimated Value for additional scheme
            tracker_df[f'Estimated_Value{suffix}'] = tracker_df[target_col] * tracker_df[rate_col]
            
            # Estimated Volume for additional scheme
            asp_col = f'ASP{suffix}'
            if asp_col in tracker_df.columns:
                tracker_df[f'Estimated_Volume{suffix}'] = np.where(
                    (tracker_df[asp_col] > 0) & (tracker_df[f'Estimated_Value{suffix}'] > 0),
                    tracker_df[f'Estimated_Value{suffix}'] / tracker_df[asp_col],
                    0.0
                )
            
            # Estimated Basic Payout for additional scheme
            rebate_col = f'Rebate_percent{suffix}'
            if rebate_col in tracker_df.columns:
                tracker_df[f'Est.basic_payout{suffix}'] = tracker_df[f'Estimated_Value{suffix}'] * tracker_df[rebate_col]
            
            # Estimated Total Payout for additional scheme (same as basic for value schemes)
            basic_col = f'Est.basic_payout{suffix}'
            if basic_col in tracker_df.columns:
                tracker_df[f'Est.Total_Payout{suffix}'] = tracker_df[basic_col]
            
            # Estimated MP Final Payout for additional scheme - Conditional formula
            mp_rebate_col = f'MP_Rebate_percent{suffix}'
            est_value_col = f'Estimated_Value{suffix}'
            if mp_rebate_col in tracker_df.columns and est_value_col in tracker_df.columns:
                tracker_df[f'Est.MP_Final_Payout{suffix}'] = np.where(
                    tracker_df[mp_rebate_col] > 0,
                    tracker_df[est_value_col] * tracker_df[mp_rebate_col],
                    0.0
                )
    
    return tracker_df


def _calculate_final_estimated_payout(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the final estimated scheme payout.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with final estimated payout column added
    """
    
    print(f"   💰 Calculating final estimated scheme payout...")
    
    # Components for final payout
    components = []
    
    # Main scheme components
    main_components = [
        'Est.total_payout',
        'Est.MP_Final_Payout',
        'Est.FINAL_PHASING_PAYOUT',
        'Est.Total_HO_Bonus_Payout'
    ]
    
    for component in main_components:
        if component in tracker_df.columns:
            components.append(component)
    
    # Additional scheme components
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        additional_components = [
            f'Est.total_Payout{suffix}',
            f'Est.MP_Final_Payout{suffix}'
        ]
        
        for component in additional_components:
            if component in tracker_df.columns:
                components.append(component)
    
    # Calculate final payout
    if components:
        tracker_df['Est.Final_Scheme_Payout'] = tracker_df[components].sum(axis=1)
        print(f"   ✅ Final estimated payout calculated using {len(components)} components")
    else:
        tracker_df['Est.Final_Scheme_Payout'] = 0.0
        print(f"   ⚠️ No components found for final estimated payout")
    
    return tracker_df


def _clean_infinity_values(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean infinity values in estimated volume fields and replace with 0.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with infinity values replaced by 0
    """
    
    print(f"   🔧 Cleaning infinity values in estimated volume fields...")
    
    # List of estimated volume fields to check for infinity
    volume_fields = ['Estimated_Volume']
    
    # Add additional scheme volume fields
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        volume_fields.append(f'Estimated_Volume{suffix}')
    
    # Replace infinity values with 0
    cleaned_count = 0
    for field in volume_fields:
        if field in tracker_df.columns:
            # Check for infinity values
            inf_mask = np.isinf(tracker_df[field])
            if inf_mask.any():
                tracker_df.loc[inf_mask, field] = 0.0
                cleaned_count += inf_mask.sum()
                print(f"     - Cleaned {inf_mask.sum()} infinity values in '{field}'")
    
    if cleaned_count > 0:
        print(f"   ✅ Cleaned {cleaned_count} infinity values total")
    else:
        print(f"   ✅ No infinity values found")
    
    return tracker_df


def _divide_estimated_payouts_by_100(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    Divide all estimated payout fields by 100 for value schemes.
    
    Args:
        tracker_df: The main tracker DataFrame
    
    Returns:
        DataFrame with estimated payout fields divided by 100
    """
    
    print(f"   🔢 Dividing estimated payout fields by 100 for value scheme...")
    
    # List of estimated payout fields to divide by 100
    payout_fields = [
        'Est.Phasing_Payout_1',
        'Est.Phasing_Payout_2', 
        'Est.Phasing_Payout_3',
        'Est.basic_payout',
        'Est.total_payout',
        'Est.MP_Final_Payout',
        'Est.basic_payout_Pn',
        'Est.total_payout_Pn',
        'Est.MP_Final_Payout_Pn',
        'Est.FINAL_PHASING_PAYOUT',
        'Est.Total_Payout_pn',
        'Est.Final_Scheme_Payout',
        'Est.Total_HO_Bonus_Payout'
    ]
    
    # HO Scheme bonus fields
    for period in [1, 2, 3, 4]:
        ho_bonus_fields = [
            f'Est.Main_Scheme_Bonus_Value_{period}',
            f'Est.Main_Scheme_MP_Bonus_Value_{period}',
            f'Est.Main_Scheme_Bonus_Volume_{period}',
            f'Est.Main_Scheme_MP_Bonus_Volume_{period}',
            f'Est.Main_Scheme_{period}_Bonus_Payout',
            f'Est.Main_Scheme_{period}_MP_Bonus_Payout'
        ]
        payout_fields.extend(ho_bonus_fields)
    
    # HO Scheme bonus reward percentage fields (divide by 100)
    for period in [1, 2, 3, 4]:
        ho_reward_fields = [
            f'%_Reward_on_Total_Bonus_Scheme_{period}',
            f'Reward_on_Mandatory_Product_%_Bonus_Scheme_{period}'
        ]
        payout_fields.extend(ho_reward_fields)
    
    # Additional scheme fields
    for i in range(1, 6):  # Support up to 5 additional schemes
        suffix = f'_p{i}'
        additional_fields = [
            f'Est.basic_payout{suffix}',
            f'Est.total_payout{suffix}',
            f'Est.MP_Final_Payout{suffix}',
            f'Est.Total_Payout{suffix}'
        ]
        payout_fields.extend(additional_fields)
    
    # Divide each field by 100 if it exists
    divided_count = 0
    for field in payout_fields:
        if field in tracker_df.columns:
            tracker_df[field] = tracker_df[field] / 100.0
            divided_count += 1
    
    print(f"   ✅ Divided {divided_count} estimated payout fields by 100")
    
    return tracker_df


def _calculate_ho_scheme_bonus_estimates(tracker_df: pd.DataFrame, 
                                       structured_data: Dict[str, Any],
                                       scheme_type: str,
                                       scheme_config: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Calculate HO Scheme bonus estimated columns.
    
    Args:
        tracker_df: The main tracker DataFrame
        structured_data: Structured data containing configuration
        scheme_type: 'volume' or 'value' scheme type
    
    Returns:
        DataFrame with HO Scheme bonus estimated columns added
    """
    
    # Check if HO Scheme conditions are met
    if 'Scheme Category' not in tracker_df.columns:
        return tracker_df
    
    # Check if it's HO Scheme (case insensitive) - handle both "ho-scheme" and "ho scheme"
    scheme_categories = tracker_df['Scheme Category'].unique()
    ho_scheme_found = any('ho' in str(cat).lower().replace('-', ' ') for cat in scheme_categories)
    if not ho_scheme_found:
        return tracker_df
    
    # Check if bonus schemes are enabled in configuration
    if scheme_config is None:
        return tracker_df
    
    # Try to find bonusSchemes flag in different places
    bonus_enabled = False
    
    # First try scheme_config
    if 'bonusSchemes' in scheme_config:
        bonus_enabled = scheme_config.get('bonusSchemes', False)
    
    # If not found, try structured_data
    if not bonus_enabled and 'scheme_config' in structured_data:
        struct_scheme_config = structured_data['scheme_config']
        if 'bonusSchemes' in struct_scheme_config:
            bonus_enabled = struct_scheme_config.get('bonusSchemes', False)
    
    # If still not found, try config_manager in structured_data
    if not bonus_enabled and 'config_manager' in structured_data:
        config_manager = structured_data['config_manager']
        
        # Try main_config
        if hasattr(config_manager, 'main_config'):
            if 'bonusSchemes' in config_manager.main_config:
                bonus_enabled = config_manager.main_config.get('bonusSchemes', False)
        
        # Try json_data
        if not bonus_enabled and hasattr(config_manager, 'json_data'):
            if isinstance(config_manager.json_data, dict) and 'bonusSchemes' in config_manager.json_data:
                bonus_enabled = config_manager.json_data.get('bonusSchemes', False)
        
        # Try using the is_main_feature_enabled method
        if not bonus_enabled and hasattr(config_manager, 'is_main_feature_enabled'):
            try:
                bonus_enabled = config_manager.is_main_feature_enabled('bonusSchemes')
            except Exception:
                bonus_enabled = False
    
    if not bonus_enabled:
        return tracker_df
    
    print(f"   🏢 Calculating HO Scheme bonus estimates for {scheme_type} scheme...")
    
    # Calculate estimated bonus values/volumes based on scheme type
    if scheme_type.lower() == 'value':
        # Value scheme formulas
        for period in [1, 2, 3, 4]:
            # Main scheme bonus values
            target_col = f'Main_Scheme_Bonus_Target_Value_{period}'
            if target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
                est_col = f'Est.Main_Scheme_Bonus_Value_{period}'
                tracker_df[est_col] = tracker_df[target_col] * tracker_df['qualification_rate']
                print(f"      ✅ Calculated {est_col}")
            
            # Mandatory product bonus values
            mp_target_col = f'Mandatory_Product_Bonus_Target_Value_{period}'
            if mp_target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
                est_mp_col = f'Est.Main_Scheme_MP_Bonus_Value_{period}'
                tracker_df[est_mp_col] = tracker_df[mp_target_col] * tracker_df['qualification_rate']
                print(f"      ✅ Calculated {est_mp_col}")
            else:
                print(f"      ⚠️ Missing {mp_target_col} for period {period}")
    
    else:  # Volume scheme
        # Volume scheme formulas
        for period in [1, 2, 3, 4]:
            # Main scheme bonus volumes
            target_col = f'Main_Scheme_Bonus_Target_Volume_{period}'
            if target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
                est_col = f'Est.Main_Scheme_Bonus_Volume_{period}'
                tracker_df[est_col] = tracker_df[target_col] * tracker_df['qualification_rate']
            
            # Mandatory product bonus volumes
            mp_target_col = f'Mandatory_Product_Bonus_Target_Volume_{period}'
            if mp_target_col in tracker_df.columns and 'qualification_rate' in tracker_df.columns:
                est_mp_col = f'Est.Main_Scheme_MP_Bonus_Volume_{period}'
                tracker_df[est_mp_col] = tracker_df[mp_target_col] * tracker_df['qualification_rate']
    
    # Calculate bonus payouts
    for period in [1, 2, 3, 4]:
        # Main scheme bonus payouts
        if scheme_type.lower() == 'value':
            value_col = f'Est.Main_Scheme_Bonus_Value_{period}'
        else:
            value_col = f'Est.Main_Scheme_Bonus_Value_{period}'
        
        reward_col = f'%_Reward_on_Total_Bonus_Scheme_{period}'
        payout_col = f'Est.Main_Scheme_{period}_Bonus_Payout'
        
        if value_col in tracker_df.columns and reward_col in tracker_df.columns:
            # FIXED: Remove division by 100 - use reward percentage directly
            tracker_df[payout_col] = tracker_df[value_col] * tracker_df[reward_col]
            print(f"      ✅ Calculated {payout_col}")
        else:
            print(f"      ⚠️ Missing columns for {payout_col}")
        
        # Mandatory product bonus payouts
        if scheme_type.lower() == 'value':
            mp_volume_col = f'Est.Main_Scheme_MP_Bonus_Value_{period}'
            mp_value_col = f'Est.Main_Scheme_MP_Bonus_Value_{period}'
        else:
            mp_volume_col = f'Est.Main_Scheme_MP_Bonus_Volume_{period}'
            mp_value_col = f'Est.Main_Scheme_MP_Bonus_Value_{period}'
        
        mp_reward_col = f'Reward_on_Mandatory_Product_%_Bonus_Scheme_{period}'
        mp_payout_col = f'Est.Main_Scheme_{period}_MP_Bonus_Payout'
        
        if mp_volume_col in tracker_df.columns and mp_value_col in tracker_df.columns and mp_reward_col in tracker_df.columns:
            # FIXED: For value schemes, only use value component; for volume schemes, use both
            if scheme_type.lower() == 'value':
                tracker_df[mp_payout_col] = tracker_df[mp_value_col] * tracker_df[mp_reward_col]
            else:
                tracker_df[mp_payout_col] = (tracker_df[mp_volume_col] * tracker_df[mp_reward_col]) + (tracker_df[mp_value_col] * tracker_df[mp_reward_col])
            print(f"      ✅ Calculated {mp_payout_col}")
        else:
            print(f"      ⚠️ Missing columns for {mp_payout_col}")
    
    # Calculate total HO bonus payout
    bonus_payout_cols = []
    for period in [1, 2, 3, 4]:
        bonus_payout_cols.extend([
            f'Est.Main_Scheme_{period}_Bonus_Payout',
            f'Est.Main_Scheme_{period}_MP_Bonus_Payout'
        ])
    
    existing_cols = [col for col in bonus_payout_cols if col in tracker_df.columns]
    if existing_cols:
        tracker_df['Est.Total_HO_Bonus_Payout'] = tracker_df[existing_cols].sum(axis=1)
        print(f"   ✅ HO Scheme bonus calculations completed with {len(existing_cols)} payout components")
    else:
        tracker_df['Est.Total_HO_Bonus_Payout'] = 0.0
        print(f"   ⚠️ No HO bonus payout components found")
    
    return tracker_df
