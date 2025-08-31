"""
Rewards Calculation Module

This module calculates rewards for schemes based on:
1. HO-Scheme: Uses reward slabs from database to determine rewards based on final payout
2. Non-HO-Scheme: Returns "Credit Note Rs. {final_payout}" format

Formula Summary:
- HO-Scheme: Find matching slab based on final payout amount, return scheme_reward
- Non-HO-Scheme: Return "Credit Note Rs. {final_payout}" if payout > 0, else ""
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any


def calculate_rewards(tracker_df: pd.DataFrame, structured_data: Dict[str, Any],
                     scheme_config: Dict = None) -> pd.DataFrame:
    """
    Calculate Rewards column for all accounts

    Args:
        tracker_df: Main tracker DataFrame
        structured_data: Structured scheme data
        scheme_config: Scheme configuration data

    Returns:
        Updated tracker DataFrame with 'Rewards' column
    """
    print("🏆 Calculating Rewards...")

    # Get scheme type from structured data or default
    scheme_type = _get_scheme_type(structured_data, scheme_config)
    print(f"   📊 Detected scheme type: {scheme_type}")

    # Get rewards data from structured data
    rewards_df = structured_data.get('rewards', pd.DataFrame())
    if rewards_df.empty:
        print("   ⚠️ No rewards data found in structured data")
    else:
        print(f"   📋 Found {len(rewards_df)} reward slabs")

    # Calculate rewards for each row
    rewards_list = []

    for index, row in tracker_df.iterrows():
        reward = _calculate_row_reward(row, scheme_type, rewards_df)
        rewards_list.append(reward)

    # Add the Rewards column
    tracker_df['Rewards'] = rewards_list

    # Count non-empty rewards
    non_empty_rewards = (tracker_df['Rewards'] != "").sum()
    total_accounts = len(tracker_df)

    print(f"   ✅ Rewards calculated: {non_empty_rewards} accounts with rewards")
    print(f"   📊 Total accounts processed: {total_accounts}")

    return tracker_df


def _get_scheme_type(structured_data: Dict[str, Any], scheme_config: Dict = None) -> str:
    """
    Extract scheme type from structured data

    Priority:
    1. structured_data['scheme_info'] -> scheme_base
    2. Default to 'target-based' (non-inbuilt)
    """
    try:
        # Check scheme_info dataframe
        scheme_info_df = structured_data.get('scheme_info', pd.DataFrame())
        if not scheme_info_df.empty:
            main_scheme_row = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
            if not main_scheme_row.empty:
                scheme_base = main_scheme_row['scheme_base'].iloc[0]

                # Map scheme_base to scheme type
                if scheme_base == 'inbuilt':
                    return 'inbuilt'
                elif 'ho-scheme' in str(scheme_base).lower():
                    return 'ho-scheme'
                else:
                    return 'target-based'  # Default non-inbuilt type

        # Default fallback
        return 'target-based'

    except Exception as e:
        print(f"   ⚠️ Warning: Could not determine scheme type, defaulting to 'target-based': {e}")
        return 'target-based'


def _calculate_row_reward(row: pd.Series, scheme_type: str, rewards_df: pd.DataFrame) -> str:
    """
    Calculate reward for a single row based on scheme type and final payout

    Args:
        row: DataFrame row
        scheme_type: Type of scheme ('ho-scheme', 'target-based', 'inbuilt')
        rewards_df: DataFrame containing reward slabs

    Returns:
        Reward string or empty string
    """
    # Get final payout value
    final_payout = row.get("Scheme Final Payout", 0)

    # Handle None/NaN values
    if pd.isna(final_payout) or final_payout is None:
        final_payout = 0.0
    else:
        final_payout = float(final_payout)

    # If final payout is 0 or negative, return empty string
    if final_payout <= 0:
        return ""

    # For HO-Scheme: Use reward slabs
    if scheme_type == 'ho-scheme':
        return _calculate_ho_scheme_reward(final_payout, rewards_df)

    # For Non-HO-Scheme: Use credit note format
    else:
        return _calculate_non_ho_scheme_reward(final_payout)


def _calculate_ho_scheme_reward(final_payout: float, rewards_df: pd.DataFrame) -> str:
    """
    Calculate reward for HO-Scheme using reward slabs

    Args:
        final_payout: The scheme final payout amount
        rewards_df: DataFrame containing reward slabs

    Returns:
        Reward string or empty string
    """
    # If no reward slabs available, return empty string
    if rewards_df.empty:
        return ""

    # Filter for main scheme rewards
    main_scheme_rewards = rewards_df[rewards_df['scheme_type'] == 'main_scheme']

    if main_scheme_rewards.empty:
        return ""

    # Find matching slab
    for _, slab in main_scheme_rewards.iterrows():
        slab_from = slab.get('slab_from')
        slab_to = slab.get('slab_to')
        scheme_reward = slab.get('scheme_reward', '')

        # Skip if slab boundaries are None
        if pd.isna(slab_from) or pd.isna(slab_to):
            continue

        # Check if final_payout falls within this slab
        if float(slab_from) <= final_payout <= float(slab_to):
            return str(scheme_reward)

    # No matching slab found
    return ""


def _calculate_non_ho_scheme_reward(final_payout: float) -> str:
    """
    Calculate reward for Non-HO-Scheme using credit note format

    Args:
        final_payout: The scheme final payout amount

    Returns:
        Credit note formatted string
    """
    if final_payout > 0:
        # 🔢 Updated to match rounded up integer format (no decimals)
        return f"Credit Note Rs. {int(final_payout)}"
    else:
        return ""


# Column mapping information for reference
COLUMN_MAPPING = {
    'final_payout_column': 'Scheme Final Payout',
    'rewards_column': 'Rewards',
    'reward_slab_columns': ['slab_id', 'slab_from', 'slab_to', 'scheme_reward']
}
