"""
Mandatory Product Actual Volume/Value Calculations Module
Handles calculation of actual mandatory product sales during scheme period
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime


def calculate_mandatory_actual_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                     sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate mandatory product actual volume and value columns
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing mandatory products
        sales_df: Sales data for mandatory product calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with mandatory actual columns
    """
    print("🧮 Calculating mandatory product actual columns...")
    
    # Get scheme period dates
    scheme_from = scheme_config.get('scheme_from')
    scheme_to = scheme_config.get('scheme_to')
    
    if not scheme_from or not scheme_to:
        print("   ⚠️ Scheme period dates not found")
        return tracker_df
    
    # Convert dates to datetime
    try:
        scheme_from_dt = pd.to_datetime(scheme_from)
        scheme_to_dt = pd.to_datetime(scheme_to)
    except Exception as e:
        print(f"   ⚠️ Error parsing scheme dates: {e}")
        return tracker_df
    
    # Get mandatory products from structured data
    mandatory_products = _get_mandatory_products(structured_data)
    
    if not mandatory_products:
        print("   ℹ️ No mandatory products found")
        return tracker_df
    
    # Filter sales data for scheme period and mandatory products
    scheme_sales = _filter_scheme_sales(sales_df, scheme_from_dt, scheme_to_dt, mandatory_products)
    
    if scheme_sales.empty:
        print("   ℹ️ No sales data found for scheme period")
        return tracker_df
    
    # Calculate mandatory actuals for main scheme
    tracker_df = _calculate_main_scheme_mandatory_actuals(tracker_df, scheme_sales)
    
    # Calculate mandatory actuals for additional schemes
    tracker_df = _calculate_additional_scheme_mandatory_actuals(tracker_df, scheme_sales, structured_data)
    
    print("✅ Mandatory actual calculations completed")
    return tracker_df


def _get_mandatory_products(structured_data: Dict) -> List[str]:
    """Get list of mandatory product codes from structured data"""
    mandatory_products = []
    
    # Get mandatory products from products table
    products_df = structured_data.get('products', pd.DataFrame())
    if not products_df.empty:
        mandatory_products = products_df[
            products_df['is_mandatory_product'] == True
        ]['material_code'].tolist()
    
    print(f"   📊 Found {len(mandatory_products)} mandatory products: {mandatory_products}")
    return mandatory_products


def _filter_scheme_sales(sales_df: pd.DataFrame, scheme_from: datetime, 
                        scheme_to: datetime, mandatory_products: List[str]) -> pd.DataFrame:
    """Filter sales data for scheme period and mandatory products"""
    
    # Convert sale_date to datetime if needed
    if 'sale_date' in sales_df.columns:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    
    # Filter by date range
    scheme_sales = sales_df[
        (sales_df['sale_date'] >= scheme_from) & 
        (sales_df['sale_date'] <= scheme_to)
    ].copy()
    
    # Filter by mandatory products
    if mandatory_products and 'material' in scheme_sales.columns:
        scheme_sales = scheme_sales[
            scheme_sales['material'].isin(mandatory_products)
        ].copy()
    elif mandatory_products and 'material_code' in scheme_sales.columns:
        scheme_sales = scheme_sales[
            scheme_sales['material_code'].isin(mandatory_products)
        ].copy()
    
    print(f"   📊 Filtered {len(scheme_sales)} sales records for scheme period")
    return scheme_sales


def _calculate_main_scheme_mandatory_actuals(tracker_df: pd.DataFrame, scheme_sales: pd.DataFrame) -> pd.DataFrame:
    """Calculate mandatory actuals for main scheme"""
    print("   📊 Calculating main scheme mandatory actuals...")
    
    # Group by credit account and sum volume/value
    if not scheme_sales.empty and 'credit_account' in scheme_sales.columns:
        mandatory_actuals = scheme_sales.groupby('credit_account').agg({
            'volume': 'sum',
            'value': 'sum'
        }).reset_index()
        
        # Rename columns
        mandatory_actuals.columns = ['credit_account', 'Mandatory_Product_Actual_Volume', 'Mandatory_Product_Actual_Value']
        
        # Merge with tracker DataFrame
        tracker_df = tracker_df.merge(
            mandatory_actuals, 
            on='credit_account', 
            how='left'
        )
        
        # Fill NaN values with 0
        tracker_df['Mandatory_Product_Actual_Volume'] = tracker_df['Mandatory_Product_Actual_Volume'].fillna(0.0)
        tracker_df['Mandatory_Product_Actual_Value'] = tracker_df['Mandatory_Product_Actual_Value'].fillna(0.0)
        
        print(f"   ✅ Main scheme mandatory actuals: {(tracker_df['Mandatory_Product_Actual_Volume'] > 0).sum()} accounts with volume")
    
    return tracker_df


def _calculate_additional_scheme_mandatory_actuals(tracker_df: pd.DataFrame, scheme_sales: pd.DataFrame, 
                                                 structured_data: Dict) -> pd.DataFrame:
    """Calculate mandatory actuals for additional schemes"""
    print("   📊 Calculating additional scheme mandatory actuals...")
    
    # Get additional schemes info
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found")
        return tracker_df
    
    # Calculate mandatory actuals for each additional scheme
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📋 Processing additional scheme {scheme_num}")
        
        # Group by credit account and sum volume/value
        if not scheme_sales.empty and 'credit_account' in scheme_sales.columns:
            mandatory_actuals = scheme_sales.groupby('credit_account').agg({
                'volume': 'sum',
                'value': 'sum'
            }).reset_index()
            
            # Rename columns with suffix
            mandatory_actuals.columns = [
                'credit_account', 
                f'Mandatory_Product_Actual_Volume{suffix}', 
                f'Mandatory_Product_Actual_Value{suffix}'
            ]
            
            # Merge with tracker DataFrame
            tracker_df = tracker_df.merge(
                mandatory_actuals, 
                on='credit_account', 
                how='left'
            )
            
            # Fill NaN values with 0
            volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
            value_col = f'Mandatory_Product_Actual_Value{suffix}'
            
            tracker_df[volume_col] = tracker_df[volume_col].fillna(0.0)
            tracker_df[value_col] = tracker_df[value_col].fillna(0.0)
            
            print(f"   ✅ Additional scheme {scheme_num} mandatory actuals: {(tracker_df[volume_col] > 0).sum()} accounts with volume")
    
    return tracker_df
