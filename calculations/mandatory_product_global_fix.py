"""
Global Mandatory Product Fix Module
Ensures ALL accounts have correct Mandatory_Product_Growth_Target_Volume values
This runs at the end of all calculations to fix any remaining issues
"""

import pandas as pd
import numpy as np


def apply_global_mandatory_product_fix(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    🔧 GLOBAL FIX: Ensure ALL accounts with positive growth rates and base volumes 
    have correct Mandatory_Product_Growth_Target_Volume values
    
    This function runs after all other calculations to fix any remaining issues.
    """
    
    print("🔧 Applying global mandatory product growth target fix...")
    
    # Fix main scheme
    tracker_df = _fix_scheme_growth_targets(tracker_df, '')
    
    # Fix additional schemes
    additional_schemes = _detect_additional_schemes(tracker_df)
    for scheme_num in additional_schemes:
        suffix = f'_p{scheme_num}'
        tracker_df = _fix_scheme_growth_targets(tracker_df, suffix)
    
    print("✅ Global mandatory product fix completed!")
    return tracker_df


def _fix_scheme_growth_targets(tracker_df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Fix growth targets for a specific scheme (main or additional)"""
    
    scheme_name = 'main scheme' if suffix == '' else f'additional scheme {suffix.replace("_p", "")}'
    print(f"   🔧 Fixing {scheme_name} growth targets...")
    
    # Define column names
    growth_col = f'Mandatory_Product_Growth{suffix}'
    base_volume_col = f'Mandatory_Product_Base_Volume{suffix}'
    base_value_col = f'Mandatory_Product_Base_Value{suffix}'
    target_volume_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
    target_value_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
    
    # Check if required columns exist
    if growth_col not in tracker_df.columns or base_volume_col not in tracker_df.columns:
        print(f"     ⚠️ Required columns not found for {scheme_name} - skipping")
        return tracker_df
    
    # Clean data types
    tracker_df[growth_col] = pd.to_numeric(tracker_df[growth_col], errors='coerce').fillna(0.0).astype(float)
    tracker_df[base_volume_col] = pd.to_numeric(tracker_df[base_volume_col], errors='coerce').fillna(0.0).astype(float)
    
    # Find accounts that need fixing (positive growth and base volume but zero target)
    needs_fix_mask = (
        (tracker_df[growth_col] > 0) & 
        (tracker_df[base_volume_col] > 0) & 
        ((tracker_df[target_volume_col] == 0) if target_volume_col in tracker_df.columns else True)
    )
    
    accounts_needing_fix = needs_fix_mask.sum()
    
    if accounts_needing_fix > 0:
        print(f"     🔧 Found {accounts_needing_fix} accounts needing growth target fix")
        
        # Initialize target column if it doesn't exist
        if target_volume_col not in tracker_df.columns:
            tracker_df[target_volume_col] = 0.0
        
        # Apply the correct formula: IF(Growth > 0, (1 + Growth) * Base_Volume, 0)
        growth_values = tracker_df[growth_col].values
        base_volume_values = tracker_df[base_volume_col].values
        
        tracker_df[target_volume_col] = np.where(
            growth_values > 0,
            (1 + growth_values) * base_volume_values,
            0.0
        ).astype(float)
        
        # Count fixed accounts
        fixed_accounts = (tracker_df[target_volume_col] > 0).sum()
        print(f"     ✅ {scheme_name}: {fixed_accounts} accounts now have correct growth targets")
        
        # Fix specific problematic accounts and show details
        problematic_accounts = ['3082993', '3157676', '3213927', '3223653', '3335246', '3429591', '453004', '799417', '821044', '833761']
        for acc in problematic_accounts:
            if str(acc) in tracker_df['credit_account'].astype(str).values:
                mask = tracker_df['credit_account'].astype(str) == str(acc)
                if mask.any():
                    idx = tracker_df[mask].index[0]
                    growth_val = growth_values[idx]
                    base_val = base_volume_values[idx]
                    target_val = tracker_df.loc[idx, target_volume_col]
                    print(f"       ✅ Account {acc}: Growth={growth_val:.4f}, Base={base_val:.2f}, Target={target_val:.2f}")
        
        # Also fix the value targets if base value column exists
        if base_value_col in tracker_df.columns:
            if target_value_col not in tracker_df.columns:
                tracker_df[target_value_col] = 0.0
            
            tracker_df[base_value_col] = pd.to_numeric(tracker_df[base_value_col], errors='coerce').fillna(0.0).astype(float)
            base_value_values = tracker_df[base_value_col].values
            
            tracker_df[target_value_col] = np.where(
                growth_values > 0,
                (1 + growth_values) * base_value_values,
                0.0
            ).astype(float)
            
            print(f"       ✅ {scheme_name}: Growth target values also fixed")
    else:
        print(f"     ✅ {scheme_name}: All accounts already have correct growth targets")
    
    return tracker_df


def _detect_additional_schemes(tracker_df: pd.DataFrame) -> list:
    """Detect additional schemes by looking for _p pattern in columns"""
    additional_schemes = set()
    
    for col in tracker_df.columns:
        if '_p' in col and col.split('_p')[-1].isdigit():
            scheme_num = int(col.split('_p')[-1])
            additional_schemes.add(scheme_num)
    
    return sorted(additional_schemes)