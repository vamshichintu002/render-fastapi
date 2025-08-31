"""
Payout Product Volume/Value Calculations Module
Handles calculation of actual payout product sales during scheme period
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime


def calculate_payout_product_columns(tracker_df: pd.DataFrame, structured_data: Dict, 
                                   sales_df: pd.DataFrame, scheme_config: Dict) -> pd.DataFrame:
    """
    Calculate payout product volume and value columns
    
    Args:
        tracker_df: Main calculation tracker DataFrame
        structured_data: Structured JSON data containing payout products
        sales_df: Sales data for payout product calculations
        scheme_config: Scheme configuration with period dates
    
    Returns:
        Updated tracker DataFrame with payout product columns
    """
    print("🧮 Calculating payout product columns...")
    
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
    
    # Check if payout products are enabled
    payout_products_enabled = _check_payout_products_enabled(structured_data)
    
    if not payout_products_enabled:
        print("   ℹ️ Payout products not enabled")
        return tracker_df
    
    # Get payout products from structured data
    payout_products = _get_payout_products(structured_data)
    
    if not payout_products:
        print("   ℹ️ No payout products found")
        return tracker_df
    
    # Filter sales data for scheme period and payout products
    scheme_sales = _filter_payout_sales(sales_df, scheme_from_dt, scheme_to_dt, payout_products)
    
    if scheme_sales.empty:
        print("   ℹ️ No sales data found for scheme period")
        return tracker_df
    
    # Calculate payout actuals for main scheme
    tracker_df = _calculate_main_scheme_payout_actuals(tracker_df, scheme_sales)
    
    # Calculate payout actuals for additional schemes
    tracker_df = _calculate_additional_scheme_payout_actuals(tracker_df, scheme_sales, structured_data)
    
    print("✅ Payout product calculations completed")
    return tracker_df


def _check_payout_products_enabled(structured_data: Dict) -> bool:
    """Check if payout products are enabled in the scheme"""
    # Get JSON data from config manager
    json_data = getattr(structured_data.get('config_manager'), 'json_data', None) if structured_data.get('config_manager') else None
    
    if json_data:
        # Check main scheme
        main_scheme = json_data.get('mainScheme', {})
        scheme_data = main_scheme.get('schemeData', {})
        
        # Look for payout product configuration
        payout_products_enabled = scheme_data.get('payoutProductsEnabled', False)
        
        if payout_products_enabled:
            print("   📊 Payout products enabled in main scheme")
            return True
    
    print("   📊 Payout products not enabled")
    return False


def _get_payout_products(structured_data: Dict) -> List[str]:
    """Get list of payout product codes from structured data"""
    payout_products = []
    
    # Get payout products from products table
    products_df = structured_data.get('products', pd.DataFrame())
    if not products_df.empty:
        payout_products = products_df[
            products_df['is_payout_product'] == True
        ]['material_code'].tolist()
    
    print(f"   📊 Found {len(payout_products)} payout products: {payout_products}")
    return payout_products


def _filter_payout_sales(sales_df: pd.DataFrame, scheme_from: datetime, 
                        scheme_to: datetime, payout_products: List[str]) -> pd.DataFrame:
    """Filter sales data for scheme period and payout products"""
    
    # Convert sale_date to datetime if needed
    if 'sale_date' in sales_df.columns:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    
    # Filter by date range
    scheme_sales = sales_df[
        (sales_df['sale_date'] >= scheme_from) & 
        (sales_df['sale_date'] <= scheme_to)
    ].copy()
    
    # Filter by payout products
    if payout_products and 'material_code' in scheme_sales.columns:
        scheme_sales = scheme_sales[
            scheme_sales['material_code'].isin(payout_products)
        ].copy()
    
    print(f"   📊 Filtered {len(scheme_sales)} sales records for payout products")
    return scheme_sales


def _calculate_main_scheme_payout_actuals(tracker_df: pd.DataFrame, scheme_sales: pd.DataFrame) -> pd.DataFrame:
    """Calculate payout actuals for main scheme"""
    print("   📊 Calculating main scheme payout actuals...")
    
    # Group by credit account and sum volume/value
    if not scheme_sales.empty and 'credit_account' in scheme_sales.columns:
        payout_actuals = scheme_sales.groupby('credit_account').agg({
            'volume': 'sum',
            'value': 'sum'
        }).reset_index()
        
        # Rename columns
        payout_actuals.columns = ['credit_account', 'Payout_Products_Volume', 'Payout_Products_Value']
        
        # Merge with tracker DataFrame
        tracker_df = tracker_df.merge(
            payout_actuals, 
            on='credit_account', 
            how='left'
        )
        
        # Fill NaN values with 0
        tracker_df['Payout_Products_Volume'] = tracker_df['Payout_Products_Volume'].fillna(0.0)
        tracker_df['Payout_Products_Value'] = tracker_df['Payout_Products_Value'].fillna(0.0)
        
        print(f"   ✅ Main scheme payout actuals: {(tracker_df['Payout_Products_Volume'] > 0).sum()} accounts with volume")
    
    return tracker_df


def _calculate_additional_scheme_payout_actuals(tracker_df: pd.DataFrame, scheme_sales: pd.DataFrame, 
                                              structured_data: Dict) -> pd.DataFrame:
    """Calculate payout actuals for additional schemes"""
    print("   📊 Calculating additional scheme payout actuals...")
    
    # Get additional schemes info
    scheme_info = structured_data.get('scheme_info', pd.DataFrame())
    additional_schemes = scheme_info[scheme_info['scheme_type'].str.startswith('additional_scheme')]
    
    if additional_schemes.empty:
        print("   ℹ️ No additional schemes found")
        return tracker_df
    
    # Calculate payout actuals for each additional scheme
    for _, scheme_row in additional_schemes.iterrows():
        scheme_type = scheme_row['scheme_type']
        scheme_num = scheme_type.split('_')[-1] if '_' in scheme_type else '1'
        suffix = f'_p{scheme_num}'
        
        print(f"   📋 Processing additional scheme {scheme_num}")
        
        # Group by credit account and sum volume/value
        if not scheme_sales.empty and 'credit_account' in scheme_sales.columns:
            payout_actuals = scheme_sales.groupby('credit_account').agg({
                'volume': 'sum',
                'value': 'sum'
            }).reset_index()
            
            # Rename columns with suffix
            payout_actuals.columns = [
                'credit_account', 
                f'Payout_Products_Volume{suffix}', 
                f'Payout_Products_Value{suffix}'
            ]
            
            # Merge with tracker DataFrame
            tracker_df = tracker_df.merge(
                payout_actuals, 
                on='credit_account', 
                how='left'
            )
            
            # Fill NaN values with 0
            volume_col = f'Payout_Products_Volume{suffix}'
            value_col = f'Payout_Products_Value{suffix}'
            
            tracker_df[volume_col] = tracker_df[volume_col].fillna(0.0)
            tracker_df[value_col] = tracker_df[value_col].fillna(0.0)
            
            print(f"   ✅ Additional scheme {scheme_num} payout actuals: {(tracker_df[volume_col] > 0).sum()} accounts with volume")
    
    return tracker_df
