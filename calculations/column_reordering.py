"""
Column Reordering Module

This module handles the reordering of columns in the costing sheet output
according to the exact specification provided by the user.
"""

import pandas as pd
from typing import List, Dict, Set
import re

def reorder_tracker_columns(df: pd.DataFrame, json_data: Dict = None, scheme_config: Dict = None) -> pd.DataFrame:
    """
    Reorder columns according to the exact specification:
    1. Main Scheme Fields (first)
    2. Additional Scheme Fields (middle) - with _pN suffix
    3. Final Fields (last) - always at the end
    """
    print("📋 Reordering columns based on exact specification...")

    # Define the exact column order in three groups
    main_scheme_fields = [
        'Scheme Category',
        'Scheme ID',
        'Scheme Type',
        'region',
        'state_name',
        'so_name',
        'credit_account',
        'customer_name',
        'Scheme Name',
        'Scheme Period From',
        'Scheme Period To',
        'Mandatory Qualify',
        'Base 1 Volume Final',
        'Base 1 Value Final',
        'Base 2 Volume Final',
        'Base 2 Value Final',
        'total_volume',
        'total_value',
        'growth_rate',
        'target_volume',
        'target_value',
        'Rebate per Litre',
        'Rebate_per_Litre',
        'Additional_Rebate_on_Growth_per_Litre',
        'Rebate_percent',
        'Fixed_Rebate',
        'qualification_rate',
        'ASP',
        'Estimated_Qualifiers',
        'Estimated_Volume',
        'Estimated_Value',
        'Est.basic_payout',
        'Est.additional_payout',
        'Est.total_payout',
        'Est.MP_Final_Payout',
        'Mandatory_Product_Base_Volume',
        'Mandatory_Product_Base_Value',
        'Mandatory_Min_Shades_PPI',
        'Mandatory_Product_Growth',
        'Mandatory_Product_Growth_Target_Volume',
        'Mandatory_Product_Growth_Target_Value',
        'Mandatory_Product_Fixed_Target',
        'Mandatory_Product_pct_Target_to_Actual_Sales',
        'Mandatory_Product_pct_to_Actual_Target_Volume',
        'Mandatory_Product_pct_to_Actual_Target_Value',
        'Mandatory_Product_Rebate',
        'MP_Rebate_percent',
        'MP_FINAL_TARGET',
        'Phasing_Period_No_1',
        'Phasing_Target_Percent_1',
        'Phasing_Target_Volume_1',
        'Phasing_Target_Value_1',
        'Phasing_Period_Rebate_1',
        'Phasing_Period_Rebate_Percent_1',
        'Est.Phasing_Volume_1',
        'Est.Phasing_Value_1',
        'Est.Phasing_Payout_1',
        'Phasing_Period_No_2',
        'Phasing_Target_Percent_2',
        'Phasing_Target_Volume_2',
        'Phasing_Target_Value_2',
        'Phasing_Period_Rebate_2',
        'Phasing_Period_Rebate_Percent_2',
        'Est.Phasing_Volume_2',
        'Est.Phasing_Value_2',
        'Est.Phasing_Payout_2',
        'Phasing_Period_No_3',
        'Phasing_Target_Percent_3',
        'Phasing_Target_Volume_3',
        'Phasing_Target_Value_3',
        'Phasing_Period_Rebate_3',
        'Phasing_Period_Rebate_Percent_3',
        'Est.Phasing_Volume_3',
        'Est.Phasing_Value_3',
        'Est.Phasing_Payout_3',
        'Est.FINAL_PHASING_PAYOUT',
        'Is_Bonus_1',
        'Bonus_Rebate_Value_1',
        'Bonus_Phasing_Period_Rebate_Percent_1',
        'Bonus_Phasing_Target_Percent_1',
        'Bonus_Target_Volume_1',
        'Bonus_Target_Value_1',
        'Is_Bonus_2',
        'Bonus_Rebate_Value_2',
        'Bonus_Phasing_Period_Rebate_Percent_2',
        'Bonus_Phasing_Target_Percent_2',
        'Bonus_Target_Volume_2',
        'Bonus_Target_Value_2',
        'Is_Bonus_3',
        'Bonus_Rebate_Value_3',
        'Bonus_Phasing_Period_Rebate_Percent_3',
        'Bonus_Phasing_Target_Percent_3',
        'Bonus_Target_Volume_3',
        'Bonus_Target_Value_3',
        'Bonus_Scheme_No_1',
        'Bonus_Scheme_1%_Main_Scheme_Target',
        'Bonus_Scheme_1_Minimum_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_1',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_1',
        '%_Reward_on_Total_Bonus_Scheme_1',
        'Reward_on_Mandatory_Product_%_Bonus_Scheme_1',
        'Main_Scheme_Bonus_Target_Volume_1',
        'Main_Scheme_Bonus_Target_Value_1',
        'Mandatory_Product_Bonus_Target_Volume_1',
        'Mandatory_Product_Bonus_Target_Value_1',
        'Est.Main_Scheme_Bonus_Volume_1',
        'Est.Main_Scheme_Bonus_Value_1',
        'Est.Main_Scheme_MP_Bonus_Volume_1',
        'Est.Main_Scheme_MP_Bonus_Value_1',
        'Est.Main_Scheme_1_Bonus_Payout',
        'Est.Main_Scheme_1_MP_Bonus_Payout',
        'Bonus_Scheme_No_2',
        'Bonus_Scheme_2%_Main_Scheme_Target',
        'Bonus_Scheme_2_Minimum_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_2',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_2',
        '%_Reward_on_Total_Bonus_Scheme_2',
        'Reward_on_Mandatory_Product_%_Bonus_Scheme_2',
        'Main_Scheme_Bonus_Target_Volume_2',
        'Main_Scheme_Bonus_Target_Value_2',
        'Mandatory_Product_Bonus_Target_Volume_2',
        'Mandatory_Product_Bonus_Target_Value_2',
        'Est.Main_Scheme_Bonus_Volume_2',
        'Est.Main_Scheme_Bonus_Value_2',
        'Est.Main_Scheme_MP_Bonus_Volume_2',
        'Est.Main_Scheme_MP_Bonus_Value_2',
        'Est.Main_Scheme_2_Bonus_Payout',
        'Est.Main_Scheme_2_MP_Bonus_Payout',
        'Bonus_Scheme_No_3',
        'Bonus_Scheme_3%_Main_Scheme_Target',
        'Bonus_Scheme_3_Minimum_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_3',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_3',
        '%_Reward_on_Total_Bonus_Scheme_3',
        'Reward_on_Mandatory_Product_%_Bonus_Scheme_3',
        'Main_Scheme_Bonus_Target_Volume_3',
        'Main_Scheme_Bonus_Target_Value_3',
        'Mandatory_Product_Bonus_Target_Volume_3',
        'Mandatory_Product_Bonus_Target_Value_3',
        'Est.Main_Scheme_Bonus_Volume_3',
        'Est.Main_Scheme_Bonus_Value_3',
        'Est.Main_Scheme_MP_Bonus_Volume_3',
        'Est.Main_Scheme_MP_Bonus_Value_3',
        'Est.Main_Scheme_3_Bonus_Payout',
        'Est.Main_Scheme_3_MP_Bonus_Payout',
        'Bonus_Scheme_No_4',
        'Bonus_Scheme_4%_Main_Scheme_Target',
        'Bonus_Scheme_4_Minimum_Target',
        'Mandatory_Product_Target_%_Bonus_Scheme_4',
        'Minimum_Mandatory_Product_Target_Bonus_Scheme_4',
        '%_Reward_on_Total_Bonus_Scheme_4',
        'Reward_on_Mandatory_Product_%_Bonus_Scheme_4',
        'Main_Scheme_Bonus_Target_Volume_4',
        'Main_Scheme_Bonus_Target_Value_4',
        'Mandatory_Product_Bonus_Target_Volume_4',
        'Mandatory_Product_Bonus_Target_Value_4',
        'Est.Main_Scheme_Bonus_Volume_4',
        'Est.Main_Scheme_Bonus_Value_4',
        'Est.Main_Scheme_MP_Bonus_Volume_4',
        'Est.Main_Scheme_MP_Bonus_Value_4',
        'Est.Main_Scheme_4_Bonus_Payout',
        'Est.Main_Scheme_4_MP_Bonus_Payout',
        'Est.Total_HO_Bonus_Payout'
    ]

    # Additional scheme field templates (will be expanded with actual scheme numbers)
    additional_scheme_field_templates = [
        'Scheme Name_pN',
        'Scheme Type_pN',
        'Mandatory Qualify_pN',
        'Base 1 Volume Final_pN',
        'Base 1 Value Final_pN',
        'Base 2 Volume Final_pN',
        'Base 2 Value Final_pN',
        'total_volume_pN',
        'total_value_pN',
        'growth_rate_pN',
        'target_volume_pN',
        'target_value_pN',
        'Rebate_per_Litre_pN',
        'Additional_Rebate_on_Growth_per_Litre_pN',
        'Rebate_percent_pn',
        'Estimated_Volume_Pn',
        'Estimated_Value_Pn',
        'Est.basic_payout_pn',
        'Est.additional_payout_pn',
        'Est.Total_Payout_pn',
        'Est.MP_Final_Payout_pn',
        'Mandatory_Product_Base_Volume_pN',
        'Mandatory_Product_Base_Value_pN',
        'Mandatory_Min_Shades_PPI_pN',
        'Mandatory_Product_Growth_pN',
        'Mandatory_Product_Growth_Target_Volume_pN',
        'Mandatory_Product_Growth_Target_Value_pN',
        'Mandatory_Product_Fixed_Target_pN',
        'Mandatory_Product_pct_Target_to_Actual_Sales_pN',
        'Mandatory_Product_pct_to_Actual_Target_Volume_pN',
        'Mandatory_Product_pct_to_Actual_Target_Value_pN',
        'Mandatory_Product_Rebate_pN',
        'MP_Rebate_percent_pN',
        'MP_FINAL_TARGET_pN'
    ]

    # Final fields (always at the end)
    final_fields = [
        'Est.Total_Payout_pn',
        'Est.Final_Scheme_Payout'
    ]

    # Build the complete desired order
    desired_order = []
    
    # 1. Add main scheme fields
    desired_order.extend(main_scheme_fields)
    
    # 2. Add additional scheme fields (detect actual scheme numbers from DataFrame)
    additional_scheme_numbers = _detect_additional_scheme_numbers(df)
    print(f"   🔍 Detected {len(additional_scheme_numbers)} additional schemes: {additional_scheme_numbers}")
    
    for scheme_num in additional_scheme_numbers:
        for template in additional_scheme_field_templates:
            # Replace _pN with actual scheme number
            actual_field = template.replace('_pN', f'_p{scheme_num}').replace('_pn', f'_p{scheme_num}')
            if actual_field in df.columns:
                desired_order.append(actual_field)
    
    # Build the final column list
    final_columns = []
    
    # Add columns in desired order if they exist in DataFrame
    for col in desired_order:
        if col in df.columns:
            final_columns.append(col)
    
    # Add any remaining columns that are in DataFrame but not in desired order
    # EXCEPT final fields - they should always be at the very end
    remaining_columns = [col for col in df.columns if col not in final_columns and col not in final_fields]
    final_columns.extend(remaining_columns)
    
    # 3. Add final fields (always at the very end)
    for col in final_fields:
        if col in df.columns:
            final_columns.append(col)
    
    print(f"   📊 Column ordering: {len(final_columns)} total columns")
    print(f"   📋 Main scheme fields: {len([col for col in final_columns if col in main_scheme_fields])}")
    print(f"   📋 Additional scheme fields: {len([col for col in final_columns if any(f'_p{num}' in col for num in additional_scheme_numbers)])}")
    print(f"   📋 Final fields: {len([col for col in final_columns if col in final_fields])}")
    print(f"   📋 Other fields: {len(remaining_columns)}")
    
    return df[final_columns]

def _detect_additional_scheme_numbers(df: pd.DataFrame) -> List[int]:
    """
    Detect additional scheme numbers from DataFrame columns.
    Looks for patterns like _p1, _p2, _p3, etc.
    """
    additional_scheme_numbers = set()
    
    for col in df.columns:
        # Look for patterns like _p1, _p2, _p3, etc.
        matches = re.findall(r'_p(\d+)', col)
        for match in matches:
            try:
                scheme_num = int(match)
                additional_scheme_numbers.add(scheme_num)
            except ValueError:
                continue
    
    return sorted(list(additional_scheme_numbers))


def apply_column_reordering_to_tracker_file(file_path: str, json_data: Dict = None, scheme_config: Dict = None) -> str:
    """
    Apply column reordering to an existing Excel/CSV tracker file
    
    Args:
        file_path: Path to the tracker file
        json_data: Optional JSON data for scheme configuration
        scheme_config: Optional scheme configuration
    
    Returns:
        Path to the reordered file
    """
    print(f"📋 Applying column reordering to {file_path}")
    
    try:
        # Read the file
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        # Apply column reordering
        df_reordered = reorder_tracker_columns(df, json_data, scheme_config)
        
        # Save the reordered file
        output_path = file_path.replace('.xlsx', '_reordered.xlsx').replace('.csv', '_reordered.csv')
        if output_path.endswith('.xlsx'):
            df_reordered.to_excel(output_path, index=False)
        else:
            df_reordered.to_csv(output_path, index=False)
        
        print(f"✅ Column reordering applied successfully: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"❌ Error applying column reordering: {e}")
        return file_path


# Legacy functions for backward compatibility
def _build_column_order_with_conditional_visibility(additional_scheme_count: int, base_periods_count: int) -> List[str]:
    """Legacy function - use _build_exact_column_order instead"""
    return _build_exact_column_order(additional_scheme_count)


def _build_column_order(additional_scheme_count: int) -> List[str]:
    """Legacy function - use _build_exact_column_order instead"""
    return _build_exact_column_order(additional_scheme_count)