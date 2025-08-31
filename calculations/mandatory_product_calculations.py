"""
Mandatory Product Calculations Module
Handles mandatory product calculations for main and additional schemes
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def calculate_mandatory_product_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                       sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate all mandatory product-related columns for main and additional schemes
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing slabs, products, etc.
        sales_df: Sales data for mandatory product calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with mandatory product columns
    """
    print("🧮 Calculating mandatory product columns...")
    
    # Add main scheme mandatory product columns
    print("   🔍 DEBUG: _add_main_scheme_mandatory_columns called from mandatory_product_calculations.py line 27")
    tracker_df = _add_main_scheme_mandatory_columns(tracker_df, structured_data, sales_df, scheme_config)
    
    # Add additional scheme mandatory product columns
    tracker_df = _add_additional_scheme_mandatory_columns(tracker_df, structured_data, sales_df, scheme_config)
    
    # Apply mandatory product tracker fixes
    print("🔧 Applying mandatory product tracker fixes...")
    try:
        from calculations.mandatory_product_tracker_fixes import apply_mandatory_product_tracker_fixes
        
        # Get JSON data from config manager
        config_manager = structured_data.get('config_manager')
        json_data = getattr(config_manager, 'json_data', None) if config_manager else None
        
        if json_data:
            tracker_df = apply_mandatory_product_tracker_fixes(
                tracker_df, structured_data, json_data, scheme_config
            )
        else:
            print("   ⚠️ No JSON data available for tracker fixes")
    except Exception as e:
        print(f"   ⚠️ Error applying mandatory product tracker fixes: {e}")
        # Continue without fixes
    
    print("✅ Mandatory product calculations completed")
    return tracker_df


def _add_main_scheme_mandatory_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                     sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add mandatory product columns for main scheme"""
    print("   📊 Adding main scheme mandatory product columns...")
    
    # Get main scheme slabs for mandatory product data
    slabs_df = structured_data['slabs']
    main_slabs = slabs_df[slabs_df['scheme_type'] == 'main_scheme'].copy()
    
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
    
    # Calculate mandatory product metrics P1-P5
    tracker_df = _calculate_mandatory_product_metrics_p1_to_p5(
        tracker_df, structured_data, sales_df, scheme_config, 'main_scheme', ''
    )
    
    # Fetch Mandatory_Min_Shades_PPI from JSON and update P5 calculation
    tracker_df = _fetch_and_update_mandatory_min_shades_ppi(tracker_df, structured_data, 'main_scheme', '')
    
    # Apply mandatory product slab calculations
    tracker_df = _apply_mandatory_product_calculations(
        tracker_df, main_slabs, '', structured_data, is_volume_based, 'main_scheme'
    )
    
    return tracker_df


def _add_additional_scheme_mandatory_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                           sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """Add mandatory product columns for additional schemes"""
    print("   📊 Adding additional scheme mandatory product columns...")
    
    # Get unique additional schemes
    scheme_info = structured_data['scheme_info']
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found - skipping additional mandatory calculations")
        return tracker_df
    
    # Get additional scheme slabs
    slabs_df = structured_data['slabs']
    additional_slabs = slabs_df[slabs_df['scheme_type'] == 'additional_scheme'].copy()
    
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        # Extract scheme number
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📋 Processing mandatory products for additional scheme {scheme_num}")
        
        # Determine if this additional scheme is volume or value based
        volume_value_based = scheme_row['volume_value_based'].lower()
        is_volume_based = volume_value_based == 'volume'
        
        print(f"   📊 Additional scheme {scheme_num} type: {'Volume' if is_volume_based else 'Value'}")
        
        # Calculate mandatory product metrics P1-P5
        tracker_df = _calculate_mandatory_product_metrics_p1_to_p5(
            tracker_df, structured_data, sales_df, scheme_config, f'additional_scheme_{scheme_num}', suffix
        )
        
        # Fetch Mandatory_Min_Shades_PPI from JSON and update P5 calculation
        tracker_df = _fetch_and_update_mandatory_min_shades_ppi(tracker_df, structured_data, f'additional_scheme_{scheme_num}', suffix)
        
        # Get slabs for this specific additional scheme
        scheme_slabs = additional_slabs[
            additional_slabs['scheme_name'].str.contains(f'Additional Scheme {scheme_num}', na=False)
        ].copy()
        
        if not scheme_slabs.empty:
            # Apply mandatory product calculations
            tracker_df = _apply_mandatory_product_calculations(
                tracker_df, scheme_slabs, suffix, structured_data, is_volume_based, f'additional_scheme_{scheme_num}'
            )
        else:
            print(f"   ⚠️ No slabs found for additional scheme {scheme_num}")
            # Initialize columns with zeros
            _initialize_mandatory_columns_with_zeros(tracker_df, suffix)
    
    return tracker_df


def _calculate_mandatory_product_metrics_p1_to_p5(tracker_df: pd.DataFrame, structured_data: Dict, 
                                                 sales_df: pd.DataFrame, scheme_config: Dict, 
                                                 scheme_type: str, suffix: str) -> pd.DataFrame:
    """Calculate P1-P5 mandatory product metrics with correct formulas"""
    
    print(f"     📊 Calculating mandatory product metrics P1-P5 for {scheme_type}")
    
    # Get mandatory products for this scheme
    products_df = structured_data['products']
    
    if scheme_type == 'main_scheme':
        mandatory_products = products_df[
            (products_df['scheme_type'] == 'main_scheme') & 
            (products_df['is_mandatory_product'] == True)
        ]
    else:
        # For additional schemes, extract scheme number
        scheme_num = suffix.replace('_p', '') if suffix else '1'
        mandatory_products = products_df[
            (products_df['scheme_type'] == 'additional_scheme') & 
            (products_df['scheme_name'] == f'Additional Scheme {scheme_num}') &
            (products_df['is_mandatory_product'] == True)
        ]
    
    print(f"     📋 Found {len(mandatory_products)} mandatory products for {scheme_type}")
    
    if mandatory_products.empty:
        print(f"     ℹ️ No mandatory products for {scheme_type}")
        # Initialize columns with zeros
        _initialize_mandatory_metrics_with_zeros(tracker_df, suffix)
        return tracker_df
    
    # Get mandatory product materials list
    mandatory_materials = set()
    for _, product_row in mandatory_products.iterrows():
        mandatory_materials.add(product_row['material_code'])
    
    print(f"     📦 Mandatory materials: {len(mandatory_materials)} unique materials")
    
    # Get base periods from scheme_config
    base_periods = scheme_config.get('base_periods', [])
    if not base_periods:
        print("     ⚠️ No base periods found in scheme config")
        _initialize_mandatory_metrics_with_zeros(tracker_df, suffix)
        return tracker_df
    
    # Calculate P1 & P2: Base period metrics for mandatory products
    tracker_df = _calculate_mandatory_base_metrics(tracker_df, sales_df, mandatory_materials, base_periods, suffix)
    
    # Calculate P3, P4, P5: Scheme period metrics for mandatory products
    tracker_df = _calculate_mandatory_scheme_metrics(tracker_df, sales_df, mandatory_materials, scheme_config, suffix)
    
    return tracker_df


def _calculate_mandatory_base_metrics(tracker_df: pd.DataFrame, sales_df: pd.DataFrame, 
                                    mandatory_materials: set, base_periods: list, suffix: str) -> pd.DataFrame:
    """Calculate P1 & P2: Mandatory_Product_Base_Volume and Mandatory_Product_Base_Value"""
    
    print(f"     📊 Calculating P1 & P2: Base period metrics for mandatory products")
    
    # Initialize base columns
    tracker_df[f'Mandatory_Product_Base_Volume{suffix}'] = 0.0
    tracker_df[f'Mandatory_Product_Base_Value{suffix}'] = 0.0
    
    if not base_periods:
        return tracker_df
    
    # Use the first base period for mandatory product calculations
    # In case of multiple base periods, we'll use the same logic as main base calculations
    base_period = base_periods[0]  # Primary base period
    
    from_date = pd.to_datetime(base_period['from_date'])
    to_date = pd.to_datetime(base_period['to_date'])
    sum_avg_method = base_period.get('sum_avg_method', 'sum')
    
    print(f"     📅 Base period: {from_date.date()} to {to_date.date()} ({sum_avg_method})")
    
    # Filter sales data for base period and mandatory materials only
    if 'sale_date' in sales_df.columns:
        sales_temp = sales_df.copy()
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        base_mandatory_sales = sales_temp[
            (sales_temp['sale_date'] >= from_date) &
            (sales_temp['sale_date'] <= to_date) &
            (sales_temp['material'].isin(mandatory_materials))
        ].copy()
    else:
        print("     ⚠️ No sale_date column found")
        return tracker_df
    
    print(f"     📊 Base period mandatory sales: {len(base_mandatory_sales)} records")
    
    if base_mandatory_sales.empty:
        print("     ℹ️ No mandatory product sales in base period")
        return tracker_df
    
    # Aggregate by credit_account for mandatory products in base period
    base_mandatory_actuals = base_mandatory_sales.groupby('credit_account').agg({
        'volume': 'sum',
        'value': 'sum'
    }).reset_index()
    
    # Apply SUM/AVG logic
    if sum_avg_method.lower() == 'average':
        # Calculate number of months in base period
        num_months = ((to_date.year - from_date.year) * 12 + to_date.month - from_date.month) + 1
        if num_months > 0:
            base_mandatory_actuals['volume'] = base_mandatory_actuals['volume'] / num_months
            base_mandatory_actuals['value'] = base_mandatory_actuals['value'] / num_months
            print(f"     📊 Applied averaging over {num_months} months")
    
    base_mandatory_actuals['credit_account'] = base_mandatory_actuals['credit_account'].astype(str)
    
    print(f"     📈 Base mandatory actuals calculated for {len(base_mandatory_actuals)} accounts")
    
    # Merge with tracker_df
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    tracker_df = tracker_df.merge(
        base_mandatory_actuals, 
        on='credit_account', 
        how='left', 
        suffixes=('', f'_base_mandatory{suffix}')
    )
    
    # Fill NaN values with 0 and assign to final columns
    tracker_df[f'Mandatory_Product_Base_Volume{suffix}'] = tracker_df['volume'].fillna(0.0)
    tracker_df[f'Mandatory_Product_Base_Value{suffix}'] = tracker_df['value'].fillna(0.0)
    
    # Clean up temporary columns
    tracker_df = tracker_df.drop(['volume', 'value'], axis=1, errors='ignore')
    
    print(f"     ✅ P1 & P2: Base metrics calculated")
    
    return tracker_df


def _calculate_mandatory_scheme_metrics(tracker_df: pd.DataFrame, sales_df: pd.DataFrame, 
                                      mandatory_materials: set, scheme_config: dict, suffix: str) -> pd.DataFrame:
    """Calculate P3, P4, P5: Scheme period metrics for mandatory products"""
    
    print(f"     📊 Calculating P3, P4, P5: Scheme period metrics for mandatory products")
    
    # Get scheme period dates
    scheme_from = pd.to_datetime(scheme_config['scheme_from'])
    scheme_to = pd.to_datetime(scheme_config['scheme_to'])
    
    print(f"     📅 Scheme period: {scheme_from.date()} to {scheme_to.date()}")
    
    # Filter sales data for scheme period and mandatory materials only
    if 'sale_date' in sales_df.columns:
        sales_temp = sales_df.copy()
        sales_temp['sale_date'] = pd.to_datetime(sales_temp['sale_date'])
        scheme_mandatory_sales = sales_temp[
            (sales_temp['sale_date'] >= scheme_from) &
            (sales_temp['sale_date'] <= scheme_to) &
            (sales_temp['material'].isin(mandatory_materials))
        ].copy()
    else:
        print("     ⚠️ No sale_date column found")
        # Initialize with zeros
        tracker_df[f'Mandatory_product_actual_value{suffix}'] = 0.0
        tracker_df[f'Mandatory_product_actual_PPI{suffix}'] = 0.0
        tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = 0.0
        return tracker_df
    
    print(f"     📊 Scheme period mandatory sales: {len(scheme_mandatory_sales)} records")
    
    # Initialize columns
    tracker_df[f'Mandatory_product_actual_value{suffix}'] = 0.0
    tracker_df[f'Mandatory_product_actual_PPI{suffix}'] = 0.0
    tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = 0.0
    
    if scheme_mandatory_sales.empty:
        print("     ℹ️ No mandatory product sales in scheme period")
        return tracker_df
    
    # P3: Calculate Mandatory_product_actual_value (sum of values)
    mandatory_actual_values = scheme_mandatory_sales.groupby('credit_account').agg({
        'value': 'sum'  # Total actual sales value for mandatory products
    }).reset_index()
    mandatory_actual_values['credit_account'] = mandatory_actual_values['credit_account'].astype(str)
    
    # P4: Calculate Mandatory_product_actual_PPI (distinct material count)
    # Group by credit_account and count distinct material codes
    mandatory_ppi_counts = scheme_mandatory_sales.groupby('credit_account')['material'].nunique().reset_index()
    mandatory_ppi_counts.columns = ['credit_account', 'distinct_material_count']
    mandatory_ppi_counts['credit_account'] = mandatory_ppi_counts['credit_account'].astype(str)
    
    print(f"     📈 P3: Actual values calculated for {len(mandatory_actual_values)} accounts")
    print(f"     📈 P4: Distinct material counts calculated for {len(mandatory_ppi_counts)} accounts")
    
    # Merge P3 results
    tracker_df['credit_account'] = tracker_df['credit_account'].astype(str)
    tracker_df = tracker_df.merge(
        mandatory_actual_values, 
        on='credit_account', 
        how='left', 
        suffixes=('', f'_p3_temp{suffix}')
    )
    tracker_df[f'Mandatory_product_actual_value{suffix}'] = tracker_df['value'].fillna(0.0)
    tracker_df = tracker_df.drop(['value'], axis=1, errors='ignore')
    
    # Merge P4 results and apply the conditional logic
    print(f"     🔍 Before merge: tracker_df has {len(tracker_df)} rows")
    print(f"     🔍 PPI counts to merge: {len(mandatory_ppi_counts)} rows")
    
    tracker_df = tracker_df.merge(
        mandatory_ppi_counts, 
        on='credit_account', 
        how='left', 
        suffixes=('', f'_p4_temp{suffix}')
    )
    
    print(f"     🔍 After merge: tracker_df has {len(tracker_df)} rows")
    print(f"     🔍 distinct_material_count column exists: {'distinct_material_count' in tracker_df.columns}")
    
    # P4 Formula: if Mandatory_product_actual_value = 0 → 0, else → COUNT(distinct mandatory material codes)
    # First, ensure distinct_material_count is properly filled
    if 'distinct_material_count' in tracker_df.columns:
        distinct_counts = tracker_df['distinct_material_count'].fillna(0.0)
        print(f"     🔍 Raw distinct counts > 0: {(distinct_counts > 0).sum()} accounts")
    else:
        print(f"     ⚠️ distinct_material_count column not found after merge!")
        distinct_counts = pd.Series([0.0] * len(tracker_df))
    
    actual_values = tracker_df[f'Mandatory_product_actual_value{suffix}']
    
    tracker_df[f'Mandatory_product_actual_PPI{suffix}'] = np.where(
        actual_values == 0,
        0.0,
        distinct_counts
    )
    
    print(f"     🔍 P4 Debug: {(distinct_counts > 0).sum()} accounts have distinct material counts > 0")
    print(f"     🔍 P4 Debug: {(actual_values > 0).sum()} accounts have actual values > 0")
    print(f"     🔍 P4 Final: {(tracker_df[f'Mandatory_product_actual_PPI{suffix}'] > 0).sum()} accounts have P4 > 0")
    tracker_df = tracker_df.drop(['distinct_material_count'], axis=1, errors='ignore')
    
    # Initialize Mandatory_Min_Shades_PPI for P5 calculation
    tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = 0.0
    
    # P5: Calculate Mandatory_Product_PPI_Achievement
    # Formula: if Mandatory_min_shades_PPI = 0 → 0, else → Mandatory_product_actual_PPI / Mandatory_min_shades_PPI
    tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = np.where(
        tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] == 0,
        0.0,
        tracker_df[f'Mandatory_product_actual_PPI{suffix}'] / tracker_df[f'Mandatory_Min_Shades_PPI{suffix}']
    )
    
    print(f"     ✅ P3, P4, P5: Scheme period metrics calculated")
    
    return tracker_df


def _fetch_and_update_mandatory_min_shades_ppi(tracker_df: pd.DataFrame, structured_data: Dict, 
                                              scheme_type: str, suffix: str) -> pd.DataFrame:
    """🔧 ENHANCED: Fetch Mandatory_Min_Shades_PPI from JSON slab data with proper extraction"""
    
    print(f"     📋 Fetching Mandatory_Min_Shades_PPI for {scheme_type}")
    
    # Initialize with default value
    tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = 0.0
    
    # Extract from JSON scheme data based on scheme type
    if structured_data:
        # Get the raw JSON data from structured_data if available
        json_data = getattr(structured_data.get('config_manager'), 'json_data', None) if structured_data.get('config_manager') else None
        
        if json_data:
            success = _extract_mandatory_fields_from_slabs(tracker_df, json_data, scheme_type, suffix)
            if not success:
                print(f"     ⚠️ Falling back to schemeData extraction for {scheme_type}")
                _extract_mandatory_fields_from_scheme_data(tracker_df, json_data, scheme_type, suffix)
        else:
            print(f"     ⚠️ JSON data not available from config manager")
    else:
        print(f"     ⚠️ Structured data not available")
    
    # Recalculate P5: Mandatory_Product_PPI_Achievement with updated Mandatory_Min_Shades_PPI
    if f'Mandatory_product_actual_PPI{suffix}' in tracker_df.columns:
        tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = np.where(
            tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] == 0,
            0.0,
            tracker_df[f'Mandatory_product_actual_PPI{suffix}'] / tracker_df[f'Mandatory_Min_Shades_PPI{suffix}']
        )
        print(f"     ✅ P5: Mandatory_Product_PPI_Achievement{suffix} recalculated")
    
    return tracker_df


def _extract_mandatory_fields_from_slabs(tracker_df: pd.DataFrame, json_data: Dict, 
                                        scheme_type: str, suffix: str) -> bool:
    """🔧 NEW: Extract mandatory fields from slab data based on account volumes"""
    
    print(f"     🎯 Extracting mandatory fields from slabs for {scheme_type}")
    
    try:
        # Get base column for slab matching
        if suffix:
            scheme_num = suffix.replace('_p', '')
            base_column = f'total_volume_p{scheme_num}'
        else:
            base_column = 'total_volume'
        
        if base_column not in tracker_df.columns:
            print(f"     ⚠️ Base column {base_column} not found")
            return False
        
        # Get slabs from JSON
        if scheme_type == 'main_scheme':
            main_scheme = json_data.get('mainScheme', {})
            slab_data = main_scheme.get('slabData', {})
            slabs = slab_data.get('slabs', [])
        else:
            # For additional schemes, extract scheme number
            scheme_num = suffix.replace('_p', '') if suffix else '1'
            additional_schemes = json_data.get('additionalSchemes', [])
            
            if isinstance(additional_schemes, list) and len(additional_schemes) >= int(scheme_num):
                additional_scheme = additional_schemes[int(scheme_num) - 1]
                slab_data = additional_scheme.get('slabData', {})
                
                # Try both possible structures
                main_scheme_slab = slab_data.get('mainScheme', {})
                if main_scheme_slab and 'slabs' in main_scheme_slab:
                    # Structure: additionalSchemes[N].slabData.mainScheme.slabs
                    slabs = main_scheme_slab.get('slabs', [])
                    print(f"     🔍 Using slabData.mainScheme.slabs structure for additional scheme {scheme_num}")
                elif 'slabs' in slab_data:
                    # Structure: additionalSchemes[N].slabData.slabs
                    slabs = slab_data.get('slabs', [])
                    print(f"     🔍 Using slabData.slabs structure for additional scheme {scheme_num}")
                else:
                    slabs = []
                    print(f"     ⚠️ No valid slab structure found for additional scheme {scheme_num}")
            else:
                slabs = []
        
        if not slabs:
            print(f"     ⚠️ No slabs found for {scheme_type}")
            return False
        
        print(f"     📊 Processing {len(slabs)} slabs for mandatory field extraction")
        
        # 🔧 NEW: Get first slab values as fallback for accounts below minimum slab range
        first_slab_min_shades = 0.0
        first_slab_product_target = 0.0
        first_slab_target_to_actual = 0.0
        
        if slabs:
            first_slab = slabs[0]  # Get first slab for fallback
            
            # Extract first slab values
            first_slab_min_shades_raw = first_slab.get('mandatoryMinShadesPPI', '')
            first_slab_product_target_raw = first_slab.get('mandatoryProductTarget', '')
            first_slab_target_to_actual_raw = first_slab.get('mandatoryProductTargetToActual', '')
            
            if first_slab_min_shades_raw and str(first_slab_min_shades_raw).strip():
                try:
                    first_slab_min_shades = float(first_slab_min_shades_raw)
                    print(f"     📋 First slab fallback: Mandatory_Min_Shades_PPI = {first_slab_min_shades}")
                except (ValueError, TypeError):
                    print(f"     ⚠️ Could not parse first slab mandatoryMinShadesPPI: {first_slab_min_shades_raw}")
            
            if first_slab_product_target_raw and str(first_slab_product_target_raw).strip():
                try:
                    first_slab_product_target = float(first_slab_product_target_raw)
                    print(f"     📋 First slab fallback: Mandatory_Product_Fixed_Target = {first_slab_product_target}")
                except (ValueError, TypeError):
                    print(f"     ⚠️ Could not parse first slab mandatoryProductTarget: {first_slab_product_target_raw}")
            
            if first_slab_target_to_actual_raw and str(first_slab_target_to_actual_raw).strip():
                try:
                    first_slab_target_to_actual = float(first_slab_target_to_actual_raw)
                    print(f"     📋 First slab fallback: Mandatory_Product_pct_Target_to_Actual_Sales = {first_slab_target_to_actual}")
                except (ValueError, TypeError):
                    print(f"     ⚠️ Could not parse first slab mandatoryProductTargetToActual: {first_slab_target_to_actual_raw}")
        
        # Get base values for slab matching
        base_values = tracker_df[base_column].values
        
        # 🔧 ENHANCED: Initialize fields with first slab values as default fallback
        tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = first_slab_min_shades
        tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'] = first_slab_product_target
        tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] = first_slab_target_to_actual
        
        print(f"     🔧 Applied first slab fallback to ALL accounts initially")
        
        # Process each slab
        for slab in slabs:
            try:
                slab_start = float(slab.get('slabStart', 0))
                slab_end = float(slab.get('slabEnd', float('inf')))
                
                # Create mask for values in this slab range
                mask = (base_values >= slab_start) & (base_values <= slab_end)
                
                if mask.sum() > 0:
                    # Extract mandatory fields from slab
                    mandatory_min_shades = slab.get('mandatoryMinShadesPPI', '')
                    mandatory_product_target = slab.get('mandatoryProductTarget', '')
                    mandatory_target_to_actual = slab.get('mandatoryProductTargetToActual', '')
                    
                    print(f"       📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts")
                    
                    # Apply mandatoryMinShadesPPI
                    if mandatory_min_shades and str(mandatory_min_shades).strip():
                        try:
                            min_shades_value = float(mandatory_min_shades)
                            tracker_df.loc[mask, f'Mandatory_Min_Shades_PPI{suffix}'] = min_shades_value
                            print(f"         ✅ Set Mandatory_Min_Shades_PPI{suffix} = {min_shades_value}")
                        except (ValueError, TypeError):
                            print(f"         ⚠️ Could not parse mandatoryMinShadesPPI: {mandatory_min_shades}")
                    
                    # Apply mandatoryProductTarget
                    if mandatory_product_target and str(mandatory_product_target).strip():
                        try:
                            product_target_value = float(mandatory_product_target)
                            tracker_df.loc[mask, f'Mandatory_Product_Fixed_Target{suffix}'] = product_target_value
                            print(f"         ✅ Set Mandatory_Product_Fixed_Target{suffix} = {product_target_value}")
                        except (ValueError, TypeError):
                            print(f"         ⚠️ Could not parse mandatoryProductTarget: {mandatory_product_target}")
                    
                    # Apply mandatoryProductTargetToActual
                    if mandatory_target_to_actual and str(mandatory_target_to_actual).strip():
                        try:
                            target_to_actual_value = float(mandatory_target_to_actual)
                            tracker_df.loc[mask, f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] = target_to_actual_value
                            print(f"         ✅ Set Mandatory_Product_pct_Target_to_Actual_Sales{suffix} = {target_to_actual_value}%")
                        except (ValueError, TypeError):
                            print(f"         ⚠️ Could not parse mandatoryProductTargetToActual: {mandatory_target_to_actual}")
                    
            except Exception as e:
                print(f"         ⚠️ Error processing slab: {e}")
                continue
        
        # Summary
        min_shades_non_zero = (tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] > 0).sum()
        fixed_target_non_zero = (tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'] > 0).sum()
        target_to_actual_non_zero = (tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] > 0).sum()
        
        print(f"     ✅ Slab extraction completed:")
        print(f"       • Mandatory_Min_Shades_PPI{suffix}: {min_shades_non_zero} accounts with values > 0")
        print(f"       • Mandatory_Product_Fixed_Target{suffix}: {fixed_target_non_zero} accounts with values > 0")
        print(f"       • Mandatory_Product_pct_Target_to_Actual_Sales{suffix}: {target_to_actual_non_zero} accounts with values > 0")
        
        return True
        
    except Exception as e:
        print(f"     ❌ Error in slab extraction: {e}")
        return False


def _extract_mandatory_fields_from_scheme_data(tracker_df: pd.DataFrame, json_data: Dict, 
                                              scheme_type: str, suffix: str) -> None:
    """🔧 FALLBACK: Extract mandatory fields from schemeData section"""
    
    print(f"     📋 Extracting from schemeData for {scheme_type}")
    
    try:
        if scheme_type == 'main_scheme':
            # Fetch from mainScheme.schemeData if available
            main_scheme = json_data.get('mainScheme', {})
            scheme_data = main_scheme.get('schemeData', {})
            
            # Look for mandatory min shades PPI in scheme data
            mandatory_min_shades = scheme_data.get('mandatoryMinShadesPPI', '')
            mandatory_product_target = scheme_data.get('mandatoryProductTarget', '')
            mandatory_target_to_actual = scheme_data.get('mandatoryProductTargetToActual', '')
            
        else:
            # For additional schemes, extract scheme number and look in additionalSchemes
            scheme_num = suffix.replace('_p', '') if suffix else '1'
            additional_schemes = json_data.get('additionalSchemes', [])
            
            if isinstance(additional_schemes, list) and len(additional_schemes) >= int(scheme_num):
                additional_scheme = additional_schemes[int(scheme_num) - 1]
                scheme_data = additional_scheme.get('schemeData', {})
                
                # Look for mandatory fields
                mandatory_min_shades = scheme_data.get('mandatoryMinShadesPPI', '')
                mandatory_product_target = scheme_data.get('mandatoryProductTarget', '')
                mandatory_target_to_actual = scheme_data.get('mandatoryProductTargetToActual', '')
            else:
                print(f"     ⚠️ Additional scheme {scheme_num} not found in JSON data")
                return
        
        # Set values if found
        if mandatory_min_shades and str(mandatory_min_shades).strip():
            try:
                mandatory_min_shades_value = float(mandatory_min_shades)
                tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = mandatory_min_shades_value
                print(f"     ✅ Mandatory_Min_Shades_PPI{suffix} set to {mandatory_min_shades_value}")
            except (ValueError, TypeError):
                print(f"     ⚠️ Could not parse mandatoryMinShadesPPI: {mandatory_min_shades}")
        
        if mandatory_product_target and str(mandatory_product_target).strip():
            try:
                mandatory_product_target_value = float(mandatory_product_target)
                tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'] = mandatory_product_target_value
                print(f"     ✅ Mandatory_Product_Fixed_Target{suffix} set to {mandatory_product_target_value}")
            except (ValueError, TypeError):
                print(f"     ⚠️ Could not parse mandatoryProductTarget: {mandatory_product_target}")
        
        if mandatory_target_to_actual and str(mandatory_target_to_actual).strip():
            try:
                mandatory_target_to_actual_value = float(mandatory_target_to_actual)
                tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] = mandatory_target_to_actual_value
                print(f"     ✅ Mandatory_Product_pct_Target_to_Actual_Sales{suffix} set to {mandatory_target_to_actual_value}%")
            except (ValueError, TypeError):
                print(f"     ⚠️ Could not parse mandatoryProductTargetToActual: {mandatory_target_to_actual}")
        
    except Exception as e:
        print(f"     ⚠️ Error in schemeData extraction: {e}")


def _initialize_mandatory_metrics_with_zeros(tracker_df: pd.DataFrame, suffix: str) -> None:
    """Initialize P1-P5 mandatory product metrics with zeros when no data available"""
    tracker_df[f'Mandatory_Product_Base_Volume{suffix}'] = 0.0
    tracker_df[f'Mandatory_Product_Base_Value{suffix}'] = 0.0
    tracker_df[f'Mandatory_product_actual_value{suffix}'] = 0.0
    tracker_df[f'Mandatory_product_actual_PPI{suffix}'] = 0.0
    tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = 0.0
    tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = 0.0


def _apply_mandatory_product_calculations(tracker_df: pd.DataFrame, slabs: pd.DataFrame, 
                                        suffix: str, structured_data: Dict, is_volume_based: bool,
                                        scheme_type: str) -> pd.DataFrame:
    """Apply mandatory product calculations using slab data"""
    
    print(f"     🔍 DEBUG: _apply_mandatory_product_calculations called for {scheme_type} with suffix '{suffix}'")
    print(f"     🧮 Applying mandatory product calculations for {scheme_type}")
    
    # Initialize all mandatory product columns efficiently (excluding MP_FINAL_ACHIEVEMENT_pct)
    mandatory_columns = _get_mandatory_column_names(suffix)
    new_columns = {}
    for col in mandatory_columns:
        # Skip MP_FINAL_ACHIEVEMENT_pct as it's calculated separately in enhanced_costing_tracker_fields.py
        if col == f'MP_FINAL_ACHIEVEMENT_pct{suffix}':
            continue
        if col not in tracker_df.columns:
            new_columns[col] = 0.0
    
    # Add all new columns at once to avoid fragmentation
    if new_columns:
        new_df = pd.DataFrame(new_columns, index=tracker_df.index)
        tracker_df = pd.concat([tracker_df, new_df], axis=1)
    
    # Sort slabs by slab_start for proper lookup
    slabs_sorted = slabs.sort_values('slab_start').copy()
    
    # 🔧 NEW: Get first slab's rebate values based on scheme type
    first_slab_rebate = 0.0
    first_slab_rebate_percent = 0.0
    
    if is_volume_based:
        # Volume schemes: Use mandatory_product_rebate for Mandatory_Product_Rebate
        if not slabs_sorted.empty and 'mandatory_product_rebate' in slabs_sorted.columns:
            first_slab_rebate = float(slabs_sorted.iloc[0]['mandatory_product_rebate'])
            print(f"     📋 First slab rebate (Volume scheme): {first_slab_rebate}")
    else:
        # Value schemes: Use mandatory_product_rebate_percent for MP_Rebate_percent
        if not slabs_sorted.empty and 'mandatory_product_rebate_percent' in slabs_sorted.columns:
            first_slab_rebate_percent = float(slabs_sorted.iloc[0]['mandatory_product_rebate_percent'])
            print(f"     📋 First slab rebate percent (Value scheme): {first_slab_rebate_percent}")
    
    # Get base column for slab lookup - UPDATED to use total volume/value for costing sheet
    # REBATE FIELD FIX: Changed from actual_volume/actual_value to total_volume/total_value
    if suffix:
        scheme_num = suffix.replace('_p', '')
        base_column = f'total_volume_p{scheme_num}' if is_volume_based else f'total_value_p{scheme_num}'
    else:
        base_column = 'total_volume' if is_volume_based else 'total_value'
    
    print(f"     🔧 REBATE SLAB UPDATE: Using {base_column} for mandatory product rebate calculations")
    
    if base_column not in tracker_df.columns:
        print(f"     ⚠️ Base column {base_column} not found")
        return tracker_df
    
    # 🔧 ENHANCED: Initialize rebate fields based on scheme type
    if is_volume_based:
        # Volume schemes: Set Mandatory_Product_Rebate
        tracker_df[f'Mandatory_Product_Rebate{suffix}'] = first_slab_rebate
        tracker_df[f'MP_Rebate_percent{suffix}'] = 0.0  # Not used for volume schemes
        print(f"     🔧 Applied first slab rebate fallback ({first_slab_rebate}) to Mandatory_Product_Rebate{suffix}")
    else:
        # Value schemes: Set MP_Rebate_percent
        tracker_df[f'MP_Rebate_percent{suffix}'] = first_slab_rebate_percent
        tracker_df[f'Mandatory_Product_Rebate{suffix}'] = 0.0  # Not used for value schemes
        print(f"     🔧 Applied first slab rebate percent fallback ({first_slab_rebate_percent}) to MP_Rebate_percent{suffix}")
    
    # Apply vectorized slab lookup for mandatory product values
    base_values = tracker_df[base_column].values
    
    for _, slab_row in slabs_sorted.iterrows():
        slab_start = float(slab_row['slab_start'])
        slab_end = float(slab_row['slab_end'])
        
        # Create mask for values in this slab range
        mask = (base_values >= slab_start) & (base_values <= slab_end)
        
        if mask.sum() > 0:
            print(f"       📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts")
            
            # Apply slab values to matching accounts
            if 'mandatory_product_growth_percent' in slab_row:
                # Convert from raw database value (integer × 100) to proper percentage
                # e.g., 5000 in DB becomes 50%
                raw_growth_value = float(slab_row['mandatory_product_growth_percent'])
                growth_percentage = raw_growth_value / 100.0
                tracker_df.loc[mask, f'Mandatory_Product_Growth{suffix}'] = growth_percentage
                print(f"         🔄 Applied growth: {raw_growth_value} → {growth_percentage}%")
            
            # Apply rebate values based on scheme type
            if is_volume_based:
                # Volume schemes: Apply mandatory_product_rebate to Mandatory_Product_Rebate
                if 'mandatory_product_rebate' in slab_row:
                    rebate = float(slab_row['mandatory_product_rebate'])
                    tracker_df.loc[mask, f'Mandatory_Product_Rebate{suffix}'] = rebate
                    print(f"         💰 Applied Mandatory_Product_Rebate: {rebate}")
            else:
                # Value schemes: Apply mandatory_product_rebate_percent to MP_Rebate_percent
                if 'mandatory_product_rebate_percent' in slab_row:
                    rebate_percent = float(slab_row['mandatory_product_rebate_percent'])
                    tracker_df.loc[mask, f'MP_Rebate_percent{suffix}'] = rebate_percent
                    print(f"         💰 Applied MP_Rebate_percent: {rebate_percent}")
    
    # Calculate derived mandatory product columns
    try:
        print(f"     🔍 DEBUG: About to call _calculate_derived_mandatory_columns for {scheme_type}")
        tracker_df = _calculate_derived_mandatory_columns(tracker_df, suffix, base_column, is_volume_based, structured_data, scheme_type)
        print(f"     ✅ Mandatory product calculations completed for {scheme_type}")
    except Exception as e:
        print(f"     ⚠️ Error in derived calculations for {scheme_type}: {e}")
        # Continue without derived calculations
    
    return tracker_df


def _calculate_derived_mandatory_columns(tracker_df: pd.DataFrame, suffix: str, 
                                       base_column: str, is_volume_based: bool, 
                                       structured_data: Dict = None, scheme_type: str = 'main_scheme') -> pd.DataFrame:
    """🔧 ENHANCED: Calculate derived mandatory product columns with proper field initialization"""
    
    print(f"     🔍 DEBUG: _calculate_derived_mandatory_columns called for {scheme_type} with suffix '{suffix}'")
    
    # 🔧 ENSURE MANDATORY_PRODUCT_FIXED_TARGET COLUMN EXISTS
    if f'Mandatory_Product_Fixed_Target{suffix}' not in tracker_df.columns:
        tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'] = 0.0
        print(f"     ➕ Initialized Mandatory_Product_Fixed_Target{suffix} column")
    
    # Base calculations from base period values - UPDATED to use actual volume/value for slab lookup
    # REBATE FIELD FIX: base_volume_col and base_value_col still reference total for base period calculations
    # but we'll use actual_volume/actual_value for slab lookups in rebate calculations
    if suffix:
        scheme_num = suffix.replace('_p', '')
        base_volume_col = f'total_volume_p{scheme_num}'  # Keep for base period calculations
        base_value_col = f'total_value_p{scheme_num}'    # Keep for base period calculations
        actual_volume_col = f'actual_volume_p{scheme_num}'
        actual_value_col = f'actual_value_p{scheme_num}'
    else:
        base_volume_col = 'total_volume'    # Keep for base period calculations
        base_value_col = 'total_value'      # Keep for base period calculations
        actual_volume_col = 'actual_volume'
        actual_value_col = 'actual_value'
    
    # Ensure all columns are converted to float to avoid Decimal issues
    for col in [base_volume_col, base_value_col, actual_volume_col, actual_value_col]:
        if col in tracker_df.columns:
            try:
                tracker_df[col] = pd.to_numeric(tracker_df[col], errors='coerce').fillna(0.0).astype(float)
            except Exception as e:
                print(f"     ⚠️ Warning: Could not convert {col} to float: {e}")
                tracker_df[col] = tracker_df[col].fillna(0.0)
    
    # NOTE: Mandatory_Product_Base_Volume and Mandatory_Product_Base_Value are now calculated correctly 
    # in _calculate_mandatory_base_metrics function using only mandatory product sales data
    # Do not overwrite these values with total_volume/total_value which includes all products
    # The correct calculation filters sales data for mandatory products only during base period
    
    # TASK GROUP 2: Growth target calculations - FIXED Excel formulas
    # Main Scheme: Mandatory_Product_Growth_Target_Volume = IF(Mandatory_Product_Growth > 0, ((1 + Mandatory_Product_Growth) * Mandatory_Product_Base_Volume), 0)
    # Additional Scheme: Mandatory_Product_Growth_Target_Volume_pN = IF(Mandatory_Product_Growth_pN > 0, ((1 + Mandatory_Product_Growth_pN) * Mandatory_Product_Base_Volume_pN), 0)
    
    if f'Mandatory_Product_Growth{suffix}' in tracker_df.columns:
        try:
            # 🔧 CRITICAL FIX: Ensure robust data type handling and consistent column references
            # Note: Mandatory_Product_Growth is stored as decimal ratio (e.g., 0.14 = 14%)
            # Formula expects: ((1 + Mandatory_Product_Growth) * Base_Volume)
            
            # Clean and normalize the growth rate column
            growth_col = f'Mandatory_Product_Growth{suffix}'
            tracker_df[growth_col] = pd.to_numeric(tracker_df[growth_col], errors='coerce').fillna(0.0).astype(float)
            
            # 🔧 FIXED: Use correct base columns for each scheme
            # Main scheme uses Mandatory_Product_Base_Volume, Additional schemes use Mandatory_Product_Base_Volume_pN
            mandatory_base_volume_col = f'Mandatory_Product_Base_Volume{suffix}'
            mandatory_base_value_col = f'Mandatory_Product_Base_Value{suffix}'
            
            # EXCEL FORMULA: Mandatory_Product_Growth_Target_Volume = IF(Mandatory_Product_Growth > 0, ((1 + Mandatory_Product_Growth) * Mandatory_Product_Base_Volume), 0)
            if mandatory_base_volume_col in tracker_df.columns:
                # 🔧 CRITICAL FIX: Use consistent column reference for both condition and calculation
                # Ensure base volume is also clean numeric
                tracker_df[mandatory_base_volume_col] = pd.to_numeric(tracker_df[mandatory_base_volume_col], errors='coerce').fillna(0.0).astype(float)
                
                # Use the same cleaned column for both condition and calculation
                growth_rate_clean = tracker_df[growth_col].values
                base_volume_clean = tracker_df[mandatory_base_volume_col].values
                
                tracker_df[f'Mandatory_Product_Growth_Target_Volume{suffix}'] = np.where(
                    growth_rate_clean > 0,
                    (1 + growth_rate_clean) * base_volume_clean,
                    0.0
                ).astype(float)
                
                # 🔍 DEBUG: Check results for problematic accounts
                problematic_accounts = ['3082993', '3157676', '3213927', '3223653', '3335246', '3429591', '453004', '799417', '821044', '833761']
                
                print(f"       🔍 DEBUG: growth_rate_clean min={growth_rate_clean.min():.6f}, max={growth_rate_clean.max():.6f}")
                print(f"       🔍 DEBUG: base_volume_clean min={base_volume_clean.min():.2f}, max={base_volume_clean.max():.2f}")
                
                for acc in problematic_accounts:
                    if str(acc) in tracker_df['credit_account'].astype(str).values:
                        mask = tracker_df['credit_account'].astype(str) == str(acc)
                        if mask.any():
                            idx = tracker_df[mask].index[0]
                            raw_growth = tracker_df.loc[idx, growth_col]
                            raw_base = tracker_df.loc[idx, mandatory_base_volume_col]
                            calc_target = tracker_df.loc[idx, f'Mandatory_Product_Growth_Target_Volume{suffix}']
                            
                            # Manual calculation to verify
                            manual_calc = (1 + growth_rate_clean[idx]) * base_volume_clean[idx] if growth_rate_clean[idx] > 0 else 0.0
                            condition_result = growth_rate_clean[idx] > 0
                            
                            print(f"       🔍 Account {acc}: Raw_Growth={raw_growth:.6f}, Clean_Growth={growth_rate_clean[idx]:.6f}, Raw_Base={raw_base:.2f}, Clean_Base={base_volume_clean[idx]:.2f}")
                            print(f"                     Condition>{condition_result}, Manual_Calc={manual_calc:.2f}, Actual_Target={calc_target:.2f}")
                
                print(f"       ✅ Growth Target Volume: IF(Growth > 0, (1 + Growth) * {mandatory_base_volume_col}, 0)")
                
                # 🔧 FINAL VALIDATION AND CORRECTION: Ensure ALL accounts with positive growth and base get correct targets
                # This step forces the correct calculation regardless of any other code that might override it
                validation_mask = (tracker_df[growth_col] > 0) & (tracker_df[mandatory_base_volume_col] > 0)
                invalid_targets = validation_mask & (tracker_df[f'Mandatory_Product_Growth_Target_Volume{suffix}'] == 0)
                
                if invalid_targets.any():
                    print(f"       🔧 VALIDATION FIX: Found {invalid_targets.sum()} accounts with zero targets despite positive inputs")
                    
                    # Force correct calculation for these accounts
                    for idx in tracker_df[invalid_targets].index:
                        growth_val = tracker_df.loc[idx, growth_col]
                        base_val = tracker_df.loc[idx, mandatory_base_volume_col]
                        correct_target = (1 + growth_val) * base_val
                        tracker_df.loc[idx, f'Mandatory_Product_Growth_Target_Volume{suffix}'] = correct_target
                        
                        account = tracker_df.loc[idx, 'credit_account']
                        print(f"         ✅ Fixed Account {account}: {growth_val:.4f} * {base_val:.2f} = {correct_target:.2f}")
                    
                    print(f"       ✅ All accounts now have correct growth target values")
                else:
                    print(f"       ✅ All accounts already have correct growth target values")
            
            # EXCEL FORMULA: Mandatory_Product_Growth_Target_Value = IF(Mandatory_Product_Growth > 0, ((1 + Mandatory_Product_Growth) * Mandatory_Product_Base_Value), 0)
            if mandatory_base_value_col in tracker_df.columns:
                # 🔧 CRITICAL FIX: Use consistent column reference for both condition and calculation
                # Ensure base value is also clean numeric
                tracker_df[mandatory_base_value_col] = pd.to_numeric(tracker_df[mandatory_base_value_col], errors='coerce').fillna(0.0).astype(float)
                
                # Use the same cleaned column for both condition and calculation
                growth_rate_clean = tracker_df[growth_col].values
                base_value_clean = tracker_df[mandatory_base_value_col].values
                
                tracker_df[f'Mandatory_Product_Growth_Target_Value{suffix}'] = np.where(
                    growth_rate_clean > 0,
                    (1 + growth_rate_clean) * base_value_clean,
                    0.0
                ).astype(float)
                print(f"       ✅ Growth Target Value: IF(Growth > 0, (1 + Growth) * {mandatory_base_value_col}, 0)")
        except Exception as e:
            print(f"     ⚠️ Error in growth target calculations: {e}")
    
    # Growth target achieved calculations
    if (f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns and 
        f'Mandatory_Product_Growth_Target_Volume{suffix}' in tracker_df.columns):
        try:
            # Ensure both columns are float before division
            actual_vol_col = tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float)
            target_vol_col = tracker_df[f'Mandatory_Product_Growth_Target_Volume{suffix}'].astype(float)
            tracker_df[f'Mandatory_Product_Growth_Target_Achieved{suffix}'] = np.where(
                target_vol_col > 0,
                (actual_vol_col / target_vol_col).astype(float),
                0.0
            )
        except Exception as e:
            print(f"     ⚠️ Error in growth target achieved calculation: {e}")
            tracker_df[f'Mandatory_Product_Growth_Target_Achieved{suffix}'] = 0.0
    
    # PPI calculations - Mandatory_product_actual_PPI
    # NOTE: PPI is now calculated correctly in _calculate_mandatory_scheme_metrics function
    # Do not overwrite the correct PPI values here
    # The correct calculation uses distinct material count from sales data, not volume proxy
    
    # Mandatory_Product_PPI_Achievement calculation (this will be calculated after Mandatory_Min_Shades_PPI is set)
    # Formula: if Mandatory_min_shades_PPI = 0 → 0, else → Mandatory_product_actual_PPI / Mandatory_min_shades_PPI
    
    # 🔧 CRITICAL FIX: DO NOT RESET Mandatory_Min_Shades_PPI and Mandatory_Product_Fixed_Target
    # These fields are now populated by enhanced slab extraction logic in _fetch_and_update_mandatory_min_shades_ppi
    # Only initialize if they don't exist
    if f'Mandatory_Min_Shades_PPI{suffix}' not in tracker_df.columns:
        tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'] = 0.0
        print(f"     ➕ Initialized Mandatory_Min_Shades_PPI{suffix} column")
    
    if f'Mandatory_Product_Fixed_Target{suffix}' not in tracker_df.columns:
        tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'] = 0.0
        print(f"     ➕ Initialized Mandatory_Product_Fixed_Target{suffix} column")
    
    print(f"     ✅ Preserved enhanced slab extraction values for {scheme_type}")

    
    # Calculate Mandatory_Product_PPI_Achievement after Mandatory_Min_Shades_PPI is set
    # Formula: if Mandatory_min_shades_PPI = 0 → 0, else → Mandatory_product_actual_PPI / Mandatory_min_shades_PPI
    tracker_df[f'Mandatory_Product_PPI_Achievement{suffix}'] = np.where(
        tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'].astype(float) == 0,
        0.0,
        tracker_df[f'Mandatory_product_actual_PPI{suffix}'].astype(float) / tracker_df[f'Mandatory_Min_Shades_PPI{suffix}'].astype(float)
    )
    
    # Percentage calculations to total sales using Payout_Products_Volume/Value
    # Mandatory_Product_Actual_Volume_pct_to_Total_Sales = (Mandatory_Product_Actual_Volume / Payout_Products_Volume)
    payout_volume_col = f'Payout_Products_Volume{suffix}'
    payout_value_col = f'Payout_Products_Value{suffix}'
    
    if (f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns and 
        payout_volume_col in tracker_df.columns):
        try:
            tracker_df[f'Mandatory_Product_Actual_Volume_pct_to_Total_Sales{suffix}'] = np.where(
                (tracker_df[payout_volume_col] > 0) & (tracker_df[payout_volume_col].notna()),
                (tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float) / tracker_df[payout_volume_col].astype(float)) * 100.0,
                0.0
            )
        except Exception:
            tracker_df[f'Mandatory_Product_Actual_Volume_pct_to_Total_Sales{suffix}'] = 0.0
    else:
        tracker_df[f'Mandatory_Product_Actual_Volume_pct_to_Total_Sales{suffix}'] = 0.0
    
    # Mandatory_Product_Actual_Value_pct_to_Total_Sales = (Mandatory_Product_Actual_Value / Payout_Products_Value)
    if (f'Mandatory_Product_Actual_Value{suffix}' in tracker_df.columns and 
        payout_value_col in tracker_df.columns):
        try:
            tracker_df[f'Mandatory_Product_Actual_Value_pct_to_Total_Sales{suffix}'] = np.where(
                (tracker_df[payout_value_col] > 0) & (tracker_df[payout_value_col].notna()),
                (tracker_df[f'Mandatory_Product_Actual_Value{suffix}'].astype(float) / tracker_df[payout_value_col].astype(float)) * 100.0,
                0.0
            )
        except Exception:
            tracker_df[f'Mandatory_Product_Actual_Value_pct_to_Total_Sales{suffix}'] = 0.0
    else:
        tracker_df[f'Mandatory_Product_Actual_Value_pct_to_Total_Sales{suffix}'] = 0.0
    
    # Percentage to fixed target  
    if (f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns and 
        f'Mandatory_Product_Fixed_Target{suffix}' in tracker_df.columns):
        try:
            # Ensure all columns are float before division
            actual_vol = tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float)
            actual_val = tracker_df[f'Mandatory_Product_Actual_Value{suffix}'].astype(float) if f'Mandatory_Product_Actual_Value{suffix}' in tracker_df.columns else pd.Series([0] * len(tracker_df))
            fixed_target = tracker_df[f'Mandatory_Product_Fixed_Target{suffix}'].astype(float)
            
            # NOTE: Removed Mandatory_Product_Actual_Volume_pct_to_Fixed_Target_Sales 
            # and Mandatory_Product_Actual_Value_pct_to_Fixed_Target_Sales as requested
        except Exception as e:
            print(f"     ⚠️ Error in fixed target percentage calculation: {e}")
            # Removed column initializations as columns were removed
    
    # Add Mandatory_Product_pct_Target_to_Actual_Sales - fetch from JSON scheme data
    # Extract from JSON mandatoryProductTargetToActual field in slab data
    
    # 🔧 NEW: Get first slab value as fallback for accounts below minimum slab range
    first_slab_target_to_actual = 0.0
    
    if structured_data:
        # Get the raw JSON data from structured_data
        config_manager = structured_data.get('config_manager')
        json_data = getattr(config_manager, 'json_data', None) if config_manager else None
        
        if json_data:
            # Extract slab data from JSON based on scheme type to get first slab fallback
            if scheme_type == 'main_scheme':
                main_scheme = json_data.get('mainScheme', {})
                slab_data = main_scheme.get('slabData', {})
                slabs = slab_data.get('slabs', [])
            else:
                scheme_num = suffix.replace('_p', '') if suffix else '1'
                additional_schemes = json_data.get('additionalSchemes', [])
                slabs = []
                if isinstance(additional_schemes, list) and len(additional_schemes) >= int(scheme_num):
                    additional_scheme = additional_schemes[int(scheme_num) - 1]
                    slab_data = additional_scheme.get('slabData', {})
                    
                    # Try both possible structures for slabs
                    main_scheme_slab = slab_data.get('mainScheme', {})
                    if main_scheme_slab and 'slabs' in main_scheme_slab:
                        # Structure: additionalSchemes[N].slabData.mainScheme.slabs
                        slabs = main_scheme_slab.get('slabs', [])
                        print(f"     🔍 Using slabData.mainScheme.slabs structure for MP % Target to Actual extraction")
                    elif 'slabs' in slab_data:
                        # Structure: additionalSchemes[N].slabData.slabs
                        slabs = slab_data.get('slabs', [])
                        print(f"     🔍 Using slabData.slabs structure for MP % Target to Actual extraction")
                    else:
                        slabs = []
                        print(f"     ⚠️ No valid slab structure found for MP % Target to Actual extraction")
            
            # Extract first slab value for fallback
            if slabs:
                first_slab = slabs[0]
                first_slab_target_to_actual_raw = first_slab.get('mandatoryProductTargetToActual', '')
                
                if first_slab_target_to_actual_raw and str(first_slab_target_to_actual_raw).strip():
                    try:
                        first_slab_target_to_actual = float(first_slab_target_to_actual_raw)
                        print(f"     📋 First slab fallback: Mandatory_Product_pct_Target_to_Actual_Sales = {first_slab_target_to_actual}%")
                    except (ValueError, TypeError):
                        print(f"     ⚠️ Could not parse first slab mandatoryProductTargetToActual: {first_slab_target_to_actual_raw}")
    
    # 🔧 ENHANCED: Initialize with first slab value as default fallback instead of 0.0
    tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] = first_slab_target_to_actual
    print(f"     🔧 Applied first slab fallback ({first_slab_target_to_actual}%) to ALL accounts initially")
    
    if structured_data and json_data and slabs:
        # Get base values for slab matching
        base_values = tracker_df[base_column].values
        
        print(f"     🎯 Processing {len(slabs)} slabs for {scheme_type}{suffix} MP % Target to Actual")
        
        for slab in slabs:
            slab_start = float(slab.get('slabStart', 0))
            slab_end = float(slab.get('slabEnd', float('inf')))
            
            # Create mask for values in this slab range
            mask = (base_values >= slab_start) & (base_values <= slab_end)
            
            if mask.sum() > 0:
                # Extract mandatoryProductTargetToActual from slab
                target_to_actual = slab.get('mandatoryProductTargetToActual', '')
                
                if target_to_actual and str(target_to_actual).strip():
                    try:
                        target_to_actual_pct = float(target_to_actual)
                        tracker_df.loc[mask, f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'] = target_to_actual_pct
                        print(f"       📍 Slab {slab_start}-{slab_end}: {mask.sum()} accounts, MP % Target to Actual: {target_to_actual_pct}%")
                    except (ValueError, TypeError):
                        print(f"       ⚠️ Invalid mandatoryProductTargetToActual value: {target_to_actual}")
                        continue
                else:
                    print(f"       ℹ️ Slab {slab_start}-{slab_end}: No mandatoryProductTargetToActual value, keeping fallback ({first_slab_target_to_actual}%)")
    else:
        print(f"     ⚠️ No slabs found in JSON for {scheme_type}{suffix} - using fallback ({first_slab_target_to_actual}%)")
    
    # Calculate percentage to actual target volume/value (needed for MP_FINAL_TARGET)
    # These represent targets as percentage of actual sales
    actual_volume_col = f'actual_volume{suffix}' if suffix else 'actual_volume'
    actual_value_col = f'actual_value{suffix}' if suffix else 'actual_value'
    
    # Calculate Mandatory_Product_pct_to_Actual_Target_Volume = (Mandatory_Product_pct_Target_to_Actual_Sales * actual_volume)
    if (f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}' in tracker_df.columns and 
        actual_volume_col in tracker_df.columns):
        tracker_df[f'Mandatory_Product_pct_to_Actual_Target_Volume{suffix}'] = (
            tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'].astype(float) / 100.0 * 
            tracker_df[actual_volume_col].astype(float)
        )
    else:
        tracker_df[f'Mandatory_Product_pct_to_Actual_Target_Volume{suffix}'] = 0.0
        
    # Calculate Mandatory_Product_pct_to_Actual_Target_Value = (Mandatory_Product_pct_Target_to_Actual_Sales * actual_value)
    if (f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}' in tracker_df.columns and 
        actual_value_col in tracker_df.columns):
        tracker_df[f'Mandatory_Product_pct_to_Actual_Target_Value{suffix}'] = (
            tracker_df[f'Mandatory_Product_pct_Target_to_Actual_Sales{suffix}'].astype(float) / 100.0 * 
            tracker_df[actual_value_col].astype(float)
        )
    else:
        tracker_df[f'Mandatory_Product_pct_to_Actual_Target_Value{suffix}'] = 0.0
    
    # Add percentage to total sales calculations
    # Mandatory_Product_Actual_Volume_pct_to_Total_Sales = (Mandatory_Product_Actual_Volume / Payout_Products_Volume)
    payout_volume_col = f'Payout_Products_Volume{suffix}'
    if (f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns and 
        payout_volume_col in tracker_df.columns):
        tracker_df[f'Mandatory_Product_Actual_Volume_pct_to_Total_Sales{suffix}'] = np.where(
            tracker_df[payout_volume_col] > 0,
            (tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float) / 
             tracker_df[payout_volume_col].astype(float)) * 100.0,
            0.0
        )
    else:
        tracker_df[f'Mandatory_Product_Actual_Volume_pct_to_Total_Sales{suffix}'] = 0.0
    
    # Mandatory_Product_Actual_Value_pct_to_Total_Sales = (Mandatory_Product_Actual_Value / Payout_Products_Value)
    payout_value_col = f'Payout_Products_Value{suffix}'
    if (f'Mandatory_Product_Actual_Value{suffix}' in tracker_df.columns and 
        payout_value_col in tracker_df.columns):
        tracker_df[f'Mandatory_Product_Actual_Value_pct_to_Total_Sales{suffix}'] = np.where(
            tracker_df[payout_value_col] > 0,
            (tracker_df[f'Mandatory_Product_Actual_Value{suffix}'].astype(float) / 
             tracker_df[payout_value_col].astype(float)) * 100.0,
            0.0
        )
    else:
        tracker_df[f'Mandatory_Product_Actual_Value_pct_to_Total_Sales{suffix}'] = 0.0
    
    # Rebate calculations  
    # 🔧 FIXED: DO NOT overwrite Mandatory_Product_Rebate values that were set during slab processing
    # The slab processing already correctly sets:
    # - Volume schemes: Mandatory_Product_Rebate from mandatoryProductRebate, MP_Rebate_percent = 0
    # - Value schemes: MP_Rebate_percent from mandatoryProductRebatePercent, Mandatory_Product_Rebate = 0
    
    # Only ensure data types are correct without overwriting values
    if f'MP_Rebate_percent{suffix}' in tracker_df.columns:
        tracker_df[f'MP_Rebate_percent{suffix}'] = pd.to_numeric(tracker_df[f'MP_Rebate_percent{suffix}'], errors='coerce').fillna(0.0).astype(float)
    
    if f'Mandatory_Product_Rebate{suffix}' in tracker_df.columns:
        tracker_df[f'Mandatory_Product_Rebate{suffix}'] = pd.to_numeric(tracker_df[f'Mandatory_Product_Rebate{suffix}'], errors='coerce').fillna(0.0).astype(float)
    
    # Calculate payout based on actual volume/value and the rebate (if needed)
    if f'Mandatory_Product_Payout{suffix}' not in tracker_df.columns:
        if is_volume_based and f'Mandatory_Product_Actual_Volume{suffix}' in tracker_df.columns:
            tracker_df[f'Mandatory_Product_Payout{suffix}'] = (
                tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'].astype(float) * 
                tracker_df[f'Mandatory_Product_Rebate{suffix}'].astype(float)
            )
        elif not is_volume_based and f'Mandatory_Product_Actual_Value{suffix}' in tracker_df.columns:
            tracker_df[f'Mandatory_Product_Payout{suffix}'] = (
                tracker_df[f'Mandatory_Product_Actual_Value{suffix}'].astype(float) * 
                tracker_df[f'MP_Rebate_percent{suffix}'].astype(float) / 100.0
            )
        else:
            tracker_df[f'Mandatory_Product_Payout{suffix}'] = 0.0
                
        print(f"     ✅ Preserved Mandatory_Product_Rebate{suffix} values from slab processing")
        print(f"     📊 Sample rebate values: {tracker_df[f'Mandatory_Product_Rebate{suffix}'].unique()}")
    
    # 🔧 FIXED: Final target calculation using user's updated formula (MAX instead of SUM)
    # For Volume: MP_FINAL_TARGET = MAX(Mandatory_Product_Fixed_Target, (Mandatory_Product_pct_to_Actual_Target_Volume + Mandatory_Product_Growth_Target_Volume))
    # For Value: MP_FINAL_TARGET = MAX(Mandatory_Product_Fixed_Target, (Mandatory_Product_pct_to_Actual_Target_Value + Mandatory_Product_Growth_Target_Value))
    
    # Initialize fixed target column
    fixed_target_col = f'Mandatory_Product_Fixed_Target{suffix}'
    if fixed_target_col not in tracker_df.columns:
        tracker_df[fixed_target_col] = 0.0
    else:
        tracker_df[fixed_target_col] = pd.to_numeric(tracker_df[fixed_target_col], errors='coerce').fillna(0.0).astype(float)
    
    if is_volume_based:
        pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Volume{suffix}'
        growth_target_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
    else:
        pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Value{suffix}'
        growth_target_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
    
    # Initialize target components - CRITICAL FIX: Do not overwrite already calculated values
    if pct_target_col not in tracker_df.columns:
        tracker_df[pct_target_col] = 0.0
    else:
        tracker_df[pct_target_col] = pd.to_numeric(tracker_df[pct_target_col], errors='coerce').fillna(0.0).astype(float)
    
    # 🔧 CRITICAL FIX: Do not reinitialize growth target columns if they already exist and have calculated values
    # The growth target calculation happens earlier in the function and should not be overwritten
    if growth_target_col not in tracker_df.columns:
        tracker_df[growth_target_col] = 0.0
        print(f"       ➕ Initialized {growth_target_col} column with zeros")
    else:
        # Only clean data types but preserve calculated values - do NOT use fillna(0.0)
        # This preserves the correctly calculated growth target values from earlier
        tracker_df[growth_target_col] = pd.to_numeric(tracker_df[growth_target_col], errors='coerce').astype(float)
        non_zero_count = (tracker_df[growth_target_col] > 0).sum()
        print(f"       ✅ Preserved {growth_target_col} column with {non_zero_count} calculated values")
    
    # 🔧 NEW: Add missing percentage columns based on scheme type
    # These columns calculate the ratio of actual values to fixed targets
    # Formula: (Mandatory_Product_Actual_Volume/Value / Mandatory_Product_Fixed_Target) * 100
    
        # REMOVED: Percentage to fixed target sales columns as requested by user
    # These columns are not needed and have been removed from the output
    
    # 🔧 FIXED: Calculate MP_FINAL_TARGET using MAX formula
    # MP_FINAL_TARGET = MAX(Fixed_Target, (pct_Target + Growth_Target))
    combined_target = tracker_df[pct_target_col] + tracker_df[growth_target_col]
    tracker_df[f'MP_FINAL_TARGET{suffix}'] = np.maximum(
        tracker_df[fixed_target_col],
        combined_target
    ).astype(float)
    
    # 🔧 MOVED: MP_FINAL_ACHIEVEMENT_pct calculation moved to enhanced_costing_tracker_fields.py
    # This ensures mandatory actuals are calculated before the achievement percentage
    # MP_FINAL_ACHIEVEMENT_pct = IF(Scheme Type="Volume", (Mandatory_Product_Actual_Volume/MP_FINAL_TARGET), (Mandatory_Product_Actual_Value/MP_FINAL_TARGET))
    print(f"     ✅ MP_FINAL_ACHIEVEMENT_pct{suffix} calculation moved to enhanced_costing_tracker_fields.py")
    
    # 🔧 FIXED: Don't override the achievement values - they are calculated in enhanced_costing_tracker_fields.py
    # tracker_df[f'MP_FINAL_ACHIEVEMENT_pct{suffix}'] = 0.0  # REMOVED - this was overriding correct values
    
    # 🔧 MOVED: MP_Final_Payout calculation moved to enhanced_costing_tracker_fields.py
    # This ensures MP_FINAL_ACHIEVEMENT_pct is calculated before MP_Final_Payout
    # MP_Final_Payout = IF(Scheme_Type="Volume", IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Volume*Mandatory_Product_Rebate), 0), 0)
    # MP_Final_Payout = IF(Scheme_Type="Value", IF(MP_FINAL_ACHIEVEMENT_pct>=100%, (Mandatory_Product_Actual_Value*MP_Rebate_percent), 0), 0)
    print(f"     ✅ MP_Final_Payout{suffix} calculation moved to enhanced_costing_tracker_fields.py")
    
    # Initialize the final payout column here to ensure it exists
    tracker_df[f'MP_Final_Payout{suffix}'] = 0.0
    
    return tracker_df


def _initialize_mandatory_actuals_with_zeros(tracker_df: pd.DataFrame, suffix: str) -> None:
    """Initialize mandatory product actual columns with zeros when no data available"""
    tracker_df[f'Mandatory_Product_Actual_Volume{suffix}'] = 0.0
    tracker_df[f'Mandatory_Product_Actual_Value{suffix}'] = 0.0


def _initialize_mandatory_columns_with_zeros(tracker_df: pd.DataFrame, suffix: str) -> None:
    """Initialize all mandatory product columns with zeros when no slab data available"""
    mandatory_columns = _get_mandatory_column_names(suffix)
    for col in mandatory_columns:
        if col not in tracker_df.columns:
            tracker_df[col] = 0.0


def _get_mandatory_column_names(suffix: str) -> List[str]:
    """Get list of all mandatory product column names"""
    base_names = [
        # P1-P5 Metrics (Primary)
        'Mandatory_Product_Base_Volume',
        'Mandatory_Product_Base_Value', 
        'Mandatory_product_actual_value',
        'Mandatory_product_actual_PPI',
        'Mandatory_Product_PPI_Achievement',
        'Mandatory_Min_Shades_PPI',
        # Legacy/Additional columns
        'Mandatory_Product_Growth',
        'Mandatory_Product_Growth_Target_Volume',
        'Mandatory_Product_Growth_Target_Value',
        'Mandatory_Product_Actual_Volume',
        'Mandatory_Product_Actual_Value',
        'Mandatory_Product_Growth_Target_Achieved',
        'Mandatory_Product_Fixed_Target',
        'Mandatory_Product_pct_Target_to_Actual_Sales',
        'Mandatory_Product_pct_to_Actual_Target_Volume',
        'Mandatory_Product_pct_to_Actual_Target_Value',
        # REMOVED: Percentage columns as requested by user
        # 'Mandatory_Product_Actual_Volume_pct_to_Total_Sales',
        # 'Mandatory_Product_Actual_Value_pct_to_Total_Sales',
        # 'Mandatory_Product_Actual_Volume_pct_to_Fixed_Target_Sales',
        # 'Mandatory_Product_Actual_Value_pct_to_Fixed_Target_Sales',
        'Mandatory_Product_Rebate',
        'MP_Rebate_percent',
        'Mandatory_Product_Payout',
        'MP_FINAL_TARGET',
        # 'MP_FINAL_ACHIEVEMENT_pct',  # REMOVED - calculated separately in enhanced_costing_tracker_fields.py
        'MP_Final_Payout'
    ]
    
    return [f'{name}{suffix}' for name in base_names]
