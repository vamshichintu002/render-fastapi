"""
Strata Growth Data Fetcher
Handles fetching strata growth percentages from database
"""

import pandas as pd

def fetch_strata_growth_with_mcp():
    """
    Fetch strata growth data using MCP calls
    This will be called from main.py where MCP is available
    
    Returns:
        DataFrame with credit_account and strata_growth_percentage
    """
    # This is a placeholder - actual implementation will use MCP from main.py
    return pd.DataFrame(columns=['credit_account', 'strata_growth_percentage'])

def create_strata_growth_lookup(strata_df):
    """
    Create a fast lookup dictionary for strata growth percentages
    
    Args:
        strata_df: DataFrame with credit_account and strata_growth_percentage
    
    Returns:
        dict: {credit_account: percentage}
    """
    if strata_df is None or strata_df.empty:
        return {}
    
    return dict(zip(
        strata_df['credit_account'].astype(str), 
        strata_df['strata_growth_percentage'].astype(float)
    ))

def apply_strata_growth_vectorized(tracker_df, strata_lookup, column_prefix=""):
    """
    Apply strata growth percentages using vectorized operations
    
    Args:
        tracker_df: DataFrame with calculations
        strata_lookup: Dict mapping credit_account to growth percentage
        column_prefix: Prefix for column names (e.g., "_p1" for additional schemes)
    
    Returns:
        DataFrame with strata growth columns added
    """
    if not strata_lookup:
        # No strata data, set to zero
        tracker_df[f'Strata Growth %{column_prefix}'] = 0.0
        return tracker_df
    
    # Vectorized lookup using map
    tracker_df[f'Strata Growth %{column_prefix}'] = (
        tracker_df['credit_account'].astype(str)
        .map(strata_lookup)
        .fillna(0.0)
    )
    
    return tracker_df
