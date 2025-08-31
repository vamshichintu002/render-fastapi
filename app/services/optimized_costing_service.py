"""
Optimized costing calculation service that minimizes database calls
and performs calculations in Python for better performance
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import threading

from app.database_psycopg2 import database_manager
from app.models.costing_models import CostingRequest, SchemeComplexityAnalysis

logger = logging.getLogger(__name__)

@dataclass
class SchemeConfig:
    """Parsed scheme configuration"""
    scheme_id: str
    volume_value_based: str
    base_periods: List[Dict]
    scheme_period: Dict
    slabs: List[Dict]
    applicable_filters: Dict
    product_filters: Dict

@dataclass
class BaseData:
    """Cached base sales data"""
    sales_df: pd.DataFrame
    material_df: pd.DataFrame
    scheme_config: SchemeConfig

class OptimizedCostingService:
    """Optimized service for handling costing calculations with caching and parallel processing"""
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)  # Increased for better concurrency
        self._data_cache = {}  # Cache for sales data
        self._config_cache = {}  # Cache for parsed configurations
        self._cache_lock = threading.Lock()  # Thread-safe cache access
        self._max_cache_size = 5  # Limit cache size to prevent memory buildup
        self._cache_ttl = 3600  # Cache TTL in seconds (1 hour)
        
    def _cleanup_memory(self):
        """Clean up memory by clearing large DataFrames and forcing garbage collection"""
        import gc
        
        with self._cache_lock:
            if len(self._data_cache) > self._max_cache_size:
                # Remove oldest entries
                keys_to_remove = list(self._data_cache.keys())[:-self._max_cache_size]
                for key in keys_to_remove:
                    del self._data_cache[key]
        
        # Force garbage collection
        gc.collect()
    
    @lru_cache(maxsize=100)
    def _parse_scheme_config(self, scheme_id: str) -> SchemeConfig:
        """Parse and cache scheme configuration"""
        try:
            logger.info(f"Parsing scheme config for scheme_id: {scheme_id}")
            query = """
            SELECT scheme_json 
            FROM schemes_data 
            WHERE scheme_id::text = %s
            """
            result = database_manager.execute_query(query, [scheme_id])
            
            if not result:
                raise Exception(f"Scheme {scheme_id} not found")
            
            scheme_json = result[0]['scheme_json']
            main_scheme = scheme_json.get('mainScheme', {})
            logger.info(f"Retrieved main scheme data")
            
            # Parse base periods (handle up to 2 base periods)
            base_periods = []
            base_vol_sections = main_scheme.get('baseVolSections', [])
            logger.info(f"Found {len(base_vol_sections)} base volume sections")
            
            # Always try to create 2 base periods (matching original SQL logic)
            for i in range(2):
                if i < len(base_vol_sections) and base_vol_sections[i]:
                    section = base_vol_sections[i]
                    logger.info(f"Parsing base period {i}: fromDate={section.get('fromDate')}, toDate={section.get('toDate')}")
                    try:
                        from_date_parsed = self._parse_date(section['fromDate'])
                        to_date_parsed = self._parse_date(section['toDate'])
                        months_count = self._calculate_months(section['fromDate'], section['toDate'])
                        
                        base_periods.append({
                            'index': i,
                            'from_date': from_date_parsed + timedelta(days=1),
                            'to_date': to_date_parsed + timedelta(days=1),
                            'sum_avg_method': section.get('sumAvg', 'sum'),
                            'months_count': months_count
                        })
                        logger.info(f"Successfully parsed base period {i}")
                    except Exception as e:
                        logger.error(f"Error parsing base period {i}: {e}")
                        raise
                else:
                    # Create empty base period (matching SQL behavior when baseVolSections[1] is NULL)
                    logger.info(f"Base period {i} not found, creating empty period")
                    base_periods.append({
                        'index': i,
                        'from_date': None,
                        'to_date': None,
                        'sum_avg_method': 'sum',
                        'months_count': 0
                    })
            
            # Parse scheme period
            scheme_period_data = main_scheme.get('schemePeriod', {})
            logger.info(f"Parsing scheme period: fromDate={scheme_period_data.get('fromDate')}, toDate={scheme_period_data.get('toDate')}")
            try:
                scheme_period = {
                    'from_date': self._parse_date(scheme_period_data['fromDate']) + timedelta(days=1),
                    'to_date': self._parse_date(scheme_period_data['toDate']) + timedelta(days=1)
                }
                logger.info(f"Successfully parsed scheme period")
            except Exception as e:
                logger.error(f"Error parsing scheme period: {e}")
                raise
            
            # Parse slabs
            slabs = []
            slab_data = main_scheme.get('slabData', {}).get('slabs', [])
            logger.info(f"Found {len(slab_data)} slabs to parse")
            for slab in slab_data:
                if slab.get('slabStart') and slab.get('slabEnd'):
                    try:
                        # Helper function to safely convert to float
                        def safe_float(value, default=0):
                            if value == '' or value is None:
                                return default
                            try:
                                return float(value)
                            except (ValueError, TypeError):
                                return default
                        
                        slabs.append({
                            'slab_start': safe_float(slab.get('slabStart', 0)),
                            'slab_end': safe_float(slab.get('slabEnd', 0)),
                            'growth_rate': safe_float(slab.get('growthPercent', 0)) / 100.0,
                            'qualification_rate': safe_float(slab.get('dealerMayQualifyPercent', 0)) / 100.0,
                            'rebate_per_litre': safe_float(slab.get('rebatePerLitre', 0)),
                            'additional_rebate_on_growth': safe_float(slab.get('additionalRebateOnGrowth', 0)),
                            'rebate_percent': safe_float(slab.get('rebatePercent', 0)) / 100.0
                        })
                        logger.info(f"Successfully parsed slab: {slab.get('slabStart')} - {slab.get('slabEnd')}")
                    except Exception as e:
                        logger.error(f"Error parsing slab {slab}: {e}")
                        raise
            
            # Parse applicable filters
            scheme_applicable = main_scheme.get('schemeApplicable', {})
            applicable_filters = {
                'states': scheme_applicable.get('selectedStates', []),
                'regions': scheme_applicable.get('selectedRegions', []),
                'area_heads': scheme_applicable.get('selectedAreaHeads', []),
                'divisions': [str(d) for d in scheme_applicable.get('selectedDivisions', [])],
                'dealer_types': scheme_applicable.get('selectedDealerTypes', []),
                'distributors': scheme_applicable.get('selectedDistributors', [])
            }
            
            # Parse product filters
            product_data = main_scheme.get('productData', {})
            product_filters = {
                'materials': [str(m) for m in product_data.get('materials', [])],
                'categories': [str(c) for c in product_data.get('categories', [])],
                'groups': [str(g) for g in product_data.get('groups', [])],
                'wanda_groups': [str(w) for w in product_data.get('wandaGroups', [])],
                'thinner_groups': [str(t) for t in product_data.get('thinnerGroups', [])]
            }
            
            return SchemeConfig(
                scheme_id=scheme_id,
                volume_value_based=main_scheme.get('volumeValueBased', 'volume'),
                base_periods=base_periods,
                scheme_period=scheme_period,
                slabs=slabs,
                applicable_filters=applicable_filters,
                product_filters=product_filters
            )
            
        except Exception as e:
            logger.error(f"Error parsing scheme config: {e}")
            raise
    
    @lru_cache(maxsize=1000)
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string that may include time component - cached for performance"""
        try:
            # Handle ISO format with Z timezone
            if date_str.endswith('Z'):
                # Remove Z and parse as UTC, then convert to naive datetime
                return datetime.fromisoformat(date_str[:-1])
            elif 'T' in date_str:
                # Handle other ISO formats
                return datetime.fromisoformat(date_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                # Fallback to date-only format
                return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # Last resort: try common date formats
            for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d']:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date: {date_str}")
    
    def _calculate_months(self, from_date: str, to_date: str) -> int:
        """Calculate number of months between dates"""
        start = self._parse_date(from_date)
        end = self._parse_date(to_date)
        return (end.year - start.year) * 12 + end.month - start.month + 1
    
    def _generate_cache_key(self, scheme_config: SchemeConfig) -> str:
        """Generate a cache key based on scheme configuration"""
        # Create a hash based on key parameters that affect data fetching
        key_data = {
            'scheme_id': scheme_config.scheme_id,
            'date_ranges': [(p['from_date'], p['to_date']) for p in scheme_config.base_periods if p['from_date'] is not None],
            'scheme_period': (scheme_config.scheme_period['from_date'], scheme_config.scheme_period['to_date']),
            'filters': scheme_config.applicable_filters,
            'product_filters': scheme_config.product_filters
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _cleanup_cache(self):
        """Clean up expired cache entries"""
        current_time = time.time()
        with self._cache_lock:
            expired_keys = []
            for key, (data, timestamp) in self._data_cache.items():
                if current_time - timestamp > self._cache_ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._data_cache[key]
                logger.info(f"Removed expired cache entry: {key}")
            
            # Also limit cache size
            if len(self._data_cache) > self._max_cache_size:
                # Remove oldest entries
                sorted_items = sorted(self._data_cache.items(), key=lambda x: x[1][1])
                for key, _ in sorted_items[:-self._max_cache_size]:
                    del self._data_cache[key]
                    logger.info(f"Removed cache entry due to size limit: {key}")
    
    def _get_cached_data(self, cache_key: str) -> Optional[BaseData]:
        """Get cached data if available and not expired"""
        with self._cache_lock:
            if cache_key in self._data_cache:
                data, timestamp = self._data_cache[cache_key]
                if time.time() - timestamp < self._cache_ttl:
                    logger.info(f"Using cached data for key: {cache_key}")
                    return data
                else:
                    # Remove expired entry
                    del self._data_cache[cache_key]
                    logger.info(f"Cache entry expired, removed: {cache_key}")
        return None
    
    def _cache_data(self, cache_key: str, data: BaseData):
        """Cache the data with timestamp"""
        with self._cache_lock:
            self._data_cache[cache_key] = (data, time.time())
            logger.info(f"Cached data for key: {cache_key}")
            
        # Clean up cache periodically
        self._cleanup_cache()
    
    def _fetch_base_data(self, scheme_config: SchemeConfig) -> BaseData:
        """Fetch all required data in minimal queries with intelligent caching"""
        try:
            # Generate cache key
            cache_key = self._generate_cache_key(scheme_config)
            
            # Try to get cached data first
            cached_data = self._get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data
            # Calculate date range for all data needed (filter out None dates)
            all_dates = []
            for period in scheme_config.base_periods:
                if period['from_date'] is not None:
                    all_dates.append(period['from_date'])
                if period['to_date'] is not None:
                    all_dates.append(period['to_date'])
            
            # Always include scheme period dates
            all_dates.extend([scheme_config.scheme_period['from_date'], scheme_config.scheme_period['to_date']])
            
            # Filter out any remaining None values
            all_dates = [d for d in all_dates if d is not None]
            
            if not all_dates:
                raise Exception("No valid dates found in scheme configuration")
            
            min_date = min(all_dates)
            max_date = max(all_dates)
            
            # Build filter conditions for sales data
            filter_conditions = []
            params = []
            
            # Date filter
            filter_conditions.append("TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') BETWEEN %s AND %s")
            params.extend([min_date, max_date])
            
            # Applicable filters
            if scheme_config.applicable_filters['states']:
                placeholders = ','.join(['%s'] * len(scheme_config.applicable_filters['states']))
                filter_conditions.append(f"sd.state_name IN ({placeholders})")
                params.extend(scheme_config.applicable_filters['states'])
            
            if scheme_config.applicable_filters['regions']:
                placeholders = ','.join(['%s'] * len(scheme_config.applicable_filters['regions']))
                filter_conditions.append(f"sd.region_name IN ({placeholders})")
                params.extend(scheme_config.applicable_filters['regions'])
            
            if scheme_config.applicable_filters['divisions']:
                placeholders = ','.join(['%s'] * len(scheme_config.applicable_filters['divisions']))
                filter_conditions.append(f"sd.division::text IN ({placeholders})")
                params.extend(scheme_config.applicable_filters['divisions'])
            
            # Product filters
            if scheme_config.product_filters['materials']:
                placeholders = ','.join(['%s'] * len(scheme_config.product_filters['materials']))
                filter_conditions.append(f"sd.material::text IN ({placeholders})")
                params.extend(scheme_config.product_filters['materials'])
            
            # Fetch sales data with optimized query (add indexes hint and limit if needed)
            sales_query = f"""
            SELECT 
                sd.credit_account,
                sd.customer_name,
                sd.so_name,
                sd.state_name,
                sd.material,
                CAST(sd.volume AS NUMERIC) as volume,
                CAST(sd.value AS NUMERIC) as value,
                TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')::timestamp as sale_date,
                mm.category,
                mm.grp,
                mm.wanda_group,
                mm.thinner_group
            FROM sales_data sd
            JOIN material_master mm ON sd.material = mm.material
            WHERE {' AND '.join(filter_conditions)}
            ORDER BY sd.credit_account, sale_date
            """
            
            logger.info(f"Fetching sales data for scheme {scheme_config.scheme_id}")
            sales_result = database_manager.execute_query(sales_query, params)
            sales_df = pd.DataFrame(sales_result)
            
            # Ensure sale_date is properly converted to datetime and numeric columns to float
            if not sales_df.empty:
                if 'sale_date' in sales_df.columns:
                    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
                
                # Convert numeric columns from Decimal to float
                numeric_columns = ['volume', 'value']
                for col in numeric_columns:
                    if col in sales_df.columns:
                        sales_df[col] = pd.to_numeric(sales_df[col], errors='coerce').fillna(0.0)
                
                # Optimize memory usage with category dtypes for repeated strings
                categorical_columns = ['state_name', 'customer_name', 'so_name', 'category', 'grp', 'wanda_group', 'thinner_group']
                for col in categorical_columns:
                    if col in sales_df.columns:
                        sales_df[col] = sales_df[col].astype('category')
            
            logger.info(f"Fetched {len(sales_df)} sales records (material data already included via JOIN)")
            
            # Create empty material_df since data is already in sales_df from JOIN
            material_df = pd.DataFrame()
            
            # Create BaseData object
            base_data = BaseData(
                sales_df=sales_df,
                material_df=material_df,
                scheme_config=scheme_config
            )
            
            # Cache the data
            self._cache_data(cache_key, base_data)
            
            return base_data
            
        except Exception as e:
            logger.error(f"Error fetching base data: {e}")
            raise
    
    def _apply_product_filters(self, df: pd.DataFrame, scheme_config: SchemeConfig) -> pd.DataFrame:
        """Apply product-based filters to dataframe"""
        filtered_df = df.copy()
        
        # Apply category filter
        if scheme_config.product_filters['categories']:
            filtered_df = filtered_df[filtered_df['category'].astype(str).isin(scheme_config.product_filters['categories'])]
        
        # Apply group filter
        if scheme_config.product_filters['groups']:
            filtered_df = filtered_df[filtered_df['grp'].astype(str).isin(scheme_config.product_filters['groups'])]
        
        # Apply wanda group filter
        if scheme_config.product_filters['wanda_groups']:
            filtered_df = filtered_df[filtered_df['wanda_group'].astype(str).isin(scheme_config.product_filters['wanda_groups'])]
        
        # Apply thinner group filter
        if scheme_config.product_filters['thinner_groups']:
            filtered_df = filtered_df[filtered_df['thinner_group'].astype(str).isin(scheme_config.product_filters['thinner_groups'])]
        
        return filtered_df
    
    def _calculate_base_periods(self, base_data: BaseData) -> pd.DataFrame:
        """Calculate base period data for all accounts"""
        try:
            scheme_config = base_data.scheme_config
            sales_df = self._apply_product_filters(base_data.sales_df, scheme_config)
            
            # Get all unique credit accounts
            all_accounts = sales_df['credit_account'].unique()
            
            # Initialize result dataframe
            result_data = []
            
            for account in all_accounts:
                account_data = sales_df[sales_df['credit_account'] == account]
                if account_data.empty:
                    continue
                
                # Get basic account info
                account_info = {
                    'credit_account': account,
                    'customer_name': account_data['customer_name'].iloc[0],
                    'so_name': account_data['so_name'].iloc[0],
                    'state_name': account_data['state_name'].iloc[0]
                }
                
                # Calculate base periods (matching original SQL logic exactly)
                for i, period in enumerate(scheme_config.base_periods):
                    # Skip periods with no date range (empty periods)
                    if period['from_date'] is None or period['to_date'] is None:
                        account_info[f'base_{i+1}_volume'] = 0
                        account_info[f'base_{i+1}_value'] = 0
                        account_info[f'base_{i+1}_sum_avg'] = period['sum_avg_method']
                        account_info[f'base_{i+1}_months'] = period['months_count']
                        account_info[f'base_{i+1}_volume_final'] = 0
                        account_info[f'base_{i+1}_value_final'] = 0
                        continue
                    
                    period_data = account_data[
                        (account_data['sale_date'] >= period['from_date']) &
                        (account_data['sale_date'] <= period['to_date'])
                    ]
                    
                    if not period_data.empty:
                        raw_volume = period_data['volume'].sum()
                        raw_value = period_data['value'].sum()
                        
                        # Step 1: Apply sum/average logic (matching original SQL base_period_X_sales)
                        if period['sum_avg_method'] == '"average"' and period['months_count'] > 0:
                            volume = round(raw_volume / period['months_count'], 2)
                            value = round(raw_value / period['months_count'], 2)
                        else:
                            volume = raw_volume
                            value = raw_value
                        
                        account_info[f'base_{i+1}_volume'] = volume
                        account_info[f'base_{i+1}_value'] = value
                        account_info[f'base_{i+1}_sum_avg'] = period['sum_avg_method']
                        account_info[f'base_{i+1}_months'] = period['months_count']
                        
                        # Step 2: Calculate finals (matching original SQL base_period_X_finals)
                        # This is where the original SQL applies additional division for average
                        cleaned_sum_avg = period['sum_avg_method'].replace('"', '')
                        if cleaned_sum_avg == 'average' and period['months_count'] > 0:
                            account_info[f'base_{i+1}_volume_final'] = round(volume / period['months_count'], 2)
                            account_info[f'base_{i+1}_value_final'] = round(value / period['months_count'], 2)
                        else:
                            account_info[f'base_{i+1}_volume_final'] = volume
                            account_info[f'base_{i+1}_value_final'] = value
                    else:
                        # No data for this period
                        account_info[f'base_{i+1}_volume'] = 0
                        account_info[f'base_{i+1}_value'] = 0
                        account_info[f'base_{i+1}_sum_avg'] = period['sum_avg_method']
                        account_info[f'base_{i+1}_months'] = period['months_count']
                        account_info[f'base_{i+1}_volume_final'] = 0
                        account_info[f'base_{i+1}_value_final'] = 0
                
                # Calculate total base (matching original SQL base_sales CTE)
                # This uses the ENTIRE base period range, not just individual periods
                base_period_data = account_data[
                    (account_data['sale_date'] >= scheme_config.base_periods[0]['from_date']) &
                    (account_data['sale_date'] <= scheme_config.base_periods[0]['to_date'])
                ] if scheme_config.base_periods[0]['from_date'] is not None else pd.DataFrame()
                
                if not base_period_data.empty:
                    total_base_volume = base_period_data['volume'].sum()
                    total_base_value = base_period_data['value'].sum()
                else:
                    total_base_volume = 0
                    total_base_value = 0
                
                # Use the total_base values (matching original SQL base_sales CTE)
                # The original SQL uses base_sales.total_volume and base_sales.total_value for final base
                account_info['final_base_volume'] = total_base_volume
                account_info['final_base_value'] = total_base_value
                
                result_data.append(account_info)
            
            return pd.DataFrame(result_data)
            
        except Exception as e:
            logger.error(f"Error calculating base periods: {e}")
            raise
    
    def _apply_slab_calculations(self, base_df: pd.DataFrame, scheme_config: SchemeConfig, is_value_based: bool = True) -> pd.DataFrame:
        """Apply slab-based calculations matching original SQL exactly"""
        try:
            result_df = base_df.copy()
            
            # Get first slab as fallback (matching original SQL first_slab CTE)
            first_slab = scheme_config.slabs[0] if scheme_config.slabs else None
            
            # Apply slab logic for each account (matching original SQL slab_applied CTE)
            for idx, row in result_df.iterrows():
                total_value = row['final_base_value']
                total_volume = row['final_base_volume']
                
                # Find matching slab based on total_value (like original SQL)
                matching_slab = None
                for slab in scheme_config.slabs:
                    if slab['slab_start'] <= total_value <= slab['slab_end']:
                        matching_slab = slab
                        break
                
                # Use slab values or fallback to first slab
                slab_to_use = matching_slab if matching_slab else first_slab
                
                if slab_to_use:
                    # Apply slab values (matching original SQL COALESCE logic)
                    result_df.at[idx, 'growth_rate'] = slab_to_use['growth_rate']
                    result_df.at[idx, 'qualification_rate'] = slab_to_use['qualification_rate']
                    result_df.at[idx, 'rebate_per_litre'] = slab_to_use['rebate_per_litre']
                    result_df.at[idx, 'additional_rebate_on_growth'] = slab_to_use['additional_rebate_on_growth']
                    result_df.at[idx, 'rebate_percent'] = slab_to_use['rebate_percent']
                    
                    # Calculate target value (matching original SQL lines 365-369)
                    if total_value == 0:
                        target_value = first_slab['slab_start'] if first_slab else 0
                    else:
                        target_value = max(
                            (1 + slab_to_use['growth_rate']) * total_value,
                            slab_to_use['slab_start']
                        )
                    
                    # Calculate estimated qualifiers (matching original SQL lines 370-376)
                    if total_value == 0:
                        estimated_qualifiers = first_slab['qualification_rate'] if first_slab else 0
                    else:
                        estimated_qualifiers = slab_to_use['qualification_rate']
                    
                    # Calculate estimated value (matching original SQL lines 377-382)
                    if total_value == 0:
                        estimated_value = first_slab['qualification_rate'] * first_slab['slab_start'] if first_slab else 0
                    else:
                        estimated_value = slab_to_use['qualification_rate'] * target_value
                    
                    # Calculate estimated volume (matching original SQL lines 383-392)
                    if total_volume == 0 or total_value == 0:
                        estimated_volume = 0
                    else:
                        price_per_unit = total_value / total_volume
                        if price_per_unit > 0:
                            estimated_volume = estimated_value / price_per_unit
                        else:
                            estimated_volume = 0
                    
                    # Calculate basic payout (matching original SQL lines 393-399)
                    if slab_to_use['rebate_per_litre'] > 0:
                        basic_payout = slab_to_use['rebate_per_litre'] * total_volume
                    else:
                        basic_payout = slab_to_use['rebate_percent'] * total_value
                    
                    # Calculate estimated basic payout (matching original SQL lines 400-423)
                    if slab_to_use['rebate_per_litre'] > 0:
                        # Per litre calculation
                        rebate_total = slab_to_use['additional_rebate_on_growth'] + slab_to_use['rebate_per_litre']
                        if total_volume == 0:
                            estimated_volume_for_payout = first_slab['qualification_rate'] * first_slab['slab_start'] if first_slab else 0
                        else:
                            estimated_volume_for_payout = slab_to_use['qualification_rate'] * max(
                                (1 + slab_to_use['growth_rate']) * total_volume,
                                slab_to_use['slab_start']
                            )
                        estimated_basic_payout = rebate_total * estimated_volume_for_payout
                    else:
                        # Percentage calculation
                        estimated_basic_payout = slab_to_use['rebate_percent'] * estimated_value
                    
                    # Set calculated values
                    result_df.at[idx, 'target_value'] = target_value
                    result_df.at[idx, 'estimated_value'] = estimated_value
                    result_df.at[idx, 'estimated_volume'] = estimated_volume
                    result_df.at[idx, 'estimated_qualifiers'] = estimated_qualifiers
                    result_df.at[idx, 'basic_payout'] = basic_payout
                    result_df.at[idx, 'estimated_base_payout'] = estimated_basic_payout
                    
                    # Calculate derived fields (matching original SQL lines 461-467)
                    result_df.at[idx, 'spent_per_value'] = estimated_basic_payout / estimated_value if estimated_value > 0 else 0
                    result_df.at[idx, 'estimated_value_final'] = estimated_value if total_value > 0 else 0
                    result_df.at[idx, 'percent_growth_planned'] = (estimated_value / total_value - 1) if total_value > 0 else 0
                    result_df.at[idx, 'percent_spent'] = estimated_basic_payout / estimated_value if estimated_value > 0 else 0
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error applying slab calculations: {e}")
            raise
    
    def _add_grand_total(self, df: pd.DataFrame, is_value_based: bool = True) -> pd.DataFrame:
        """Add grand total row"""
        if df.empty:
            return df
        
        # Calculate totals
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        totals = df[numeric_columns].sum()
        
        # Create grand total row
        grand_total = pd.Series(index=df.columns)
        grand_total['credit_account'] = 'GRAND TOTAL'
        grand_total['customer_name'] = None
        grand_total['so_name'] = None
        grand_total['state_name'] = None
        
        # Fill numeric totals
        for col in numeric_columns:
            if col not in ['growth_rate', 'qualification_rate']:  # Skip rates for totals
                grand_total[col] = totals[col]
        
        # Recalculate percentage fields for grand total
        if is_value_based:
            if totals['estimated_value'] > 0:
                grand_total['spent_per_value'] = totals['estimated_base_payout'] / totals['estimated_value']
                grand_total['percent_spent'] = totals['estimated_base_payout'] / totals['estimated_value']
        else:
            if totals['estimated_volume'] > 0:
                grand_total['spent_per_liter'] = totals['estimated_base_payout'] / totals['estimated_volume']
        
        # Add grand total row
        result_df = pd.concat([df, grand_total.to_frame().T], ignore_index=True)
        return result_df
    
    def _execute_calculation_sync(self, scheme_id: str, calculation_type: str, scheme_index: int = None) -> Dict[str, Any]:
        """Execute calculation synchronously"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting optimized {calculation_type} calculation for scheme {scheme_id}")
            
            # Parse scheme configuration
            scheme_config = self._parse_scheme_config(scheme_id)
            
            # Fetch base data
            base_data = self._fetch_base_data(scheme_config)
            
            # Calculate base periods
            base_df = self._calculate_base_periods(base_data)
            
            # Determine calculation type
            is_value_based = 'value' in calculation_type or scheme_config.volume_value_based == 'value'
            
            # Apply slab calculations
            result_df = self._apply_slab_calculations(base_df, scheme_config, is_value_based)
            
            # Add grand total if not summary
            if 'summary' not in calculation_type:
                result_df = self._add_grand_total(result_df, is_value_based)
            
            # Rename columns to match original SQL output exactly
            column_mapping = {
                'base_1_volume': 'Base 1 Volume',
                'base_1_value': 'Base 1 Value',
                'base_1_sum_avg': 'Base 1 SumAvg',
                'base_1_months': 'Base 1 Months',
                'base_1_volume_final': 'Base 1 Volume Final',
                'base_1_value_final': 'Base 1 Value Final',
                'base_2_volume': 'Base 2 Volume',
                'base_2_value': 'Base 2 Value',
                'base_2_sum_avg': 'Base 2 SumAvg',
                'base_2_months': 'Base 2 Months',
                'base_2_volume_final': 'Base 2 Volume Final',
                'base_2_value_final': 'Base 2 Value Final',
                'final_base_volume': 'Final Base Volume',
                'final_base_value': 'Final Base Value',
                'target_value': 'Target Value',
                'estimated_value': 'Estimated Value',
                'estimated_volume': 'Estimated Volume',
                'estimated_qualifiers': 'Estimated Qualifiers',
                'estimated_base_payout': 'Estimated Base Payout',
                'rebate_per_litre': 'Rebate per Litre',
                'additional_rebate_on_growth': 'Additional Rebate on Growth per Litre',
                'rebate_percent': 'Rebate Percent'
            }
            
            result_df = result_df.rename(columns=column_mapping)
            
            # Convert to list of dictionaries
            result_data = result_df.fillna(0).to_dict('records')
            
            execution_time = time.time() - start_time
            
            logger.info(f"Optimized {calculation_type} calculation completed in {execution_time:.2f}s, returned {len(result_data)} rows")
            
            return {
                'data': result_data,
                'execution_time': execution_time,
                'calculation_type': calculation_type,
                'scheme_id': scheme_id,
                'record_count': len(result_data)
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in optimized {calculation_type}: {e}")
            raise Exception(f"Optimized {calculation_type} failed after {execution_time:.2f}s: {str(e)}")
    
    # Async wrapper methods
    async def calculate_main_scheme_value_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme costing (optimized)"""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_calculation_sync, 
            scheme_id, 
            'main_value'
        )
        return result
    
    async def calculate_main_scheme_volume_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme costing (optimized)"""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_calculation_sync, 
            scheme_id, 
            'main_volume'
        )
        return result
    
    async def calculate_additional_scheme_value_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme costing (optimized)"""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_calculation_sync, 
            scheme_id, 
            'additional_value',
            scheme_index
        )
        return result
    
    async def calculate_additional_scheme_volume_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme costing (optimized)"""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_calculation_sync, 
            scheme_id, 
            'additional_volume',
            scheme_index
        )
        return result
    
    # Summary methods (return aggregated data)
    async def calculate_main_scheme_value_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme summary (optimized)"""
        result = await self.calculate_main_scheme_value_costing(scheme_id)
        # Return only grand total row for summary
        data = result['data']
        summary_data = [row for row in data if row.get('credit_account') == 'GRAND TOTAL']
        result['data'] = summary_data
        return result
    
    async def calculate_main_scheme_volume_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme summary (optimized)"""
        result = await self.calculate_main_scheme_volume_costing(scheme_id)
        # Return only grand total row for summary
        data = result['data']
        summary_data = [row for row in data if row.get('credit_account') == 'GRAND TOTAL']
        result['data'] = summary_data
        return result
    
    async def calculate_additional_scheme_value_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme summary (optimized)"""
        result = await self.calculate_additional_scheme_value_costing(scheme_id, scheme_index)
        # Return only grand total row for summary
        data = result['data']
        summary_data = [row for row in data if row.get('credit_account') == 'GRAND TOTAL']
        result['data'] = summary_data
        return result
    
    async def calculate_additional_scheme_volume_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme summary (optimized)"""
        result = await self.calculate_additional_scheme_volume_costing(scheme_id, scheme_index)
        # Return only grand total row for summary
        data = result['data']
        summary_data = [row for row in data if row.get('credit_account') == 'GRAND TOTAL']
        result['data'] = summary_data
        return result
    
    # Utility methods from original service
    async def get_additional_schemes(self, scheme_id: str) -> List[Dict[str, Any]]:
        """Get additional schemes information for a given scheme ID"""
        try:
            logger.info(f"Fetching additional schemes for scheme_id: {scheme_id}")
            schemes = database_manager.execute_function('get_additional_schemes_simple', [scheme_id])
            if not schemes:
                logger.warning(f"No additional schemes found for scheme_id: {scheme_id}")
                return []
            logger.info(f"Found {len(schemes)} additional schemes for scheme_id: {scheme_id}")
            return schemes
        except Exception as e:
            logger.error(f"Error getting additional schemes: {e}")
            raise e

    async def check_scheme_exists(self, scheme_id: str) -> bool:
        """Check if a scheme exists in the database"""
        try:
            logger.info(f"Checking if scheme exists: {scheme_id}")
            result = database_manager.execute_query(
                "SELECT COUNT(*) as count FROM schemes_data WHERE scheme_id = %s",
                [scheme_id]
            )
            if result and len(result) > 0:
                count = result[0].get('count', 0)
                exists = count > 0
                logger.info(f"Scheme {scheme_id} exists: {exists}")
                return exists
            logger.warning(f"Could not determine if scheme {scheme_id} exists")
            return False
        except Exception as e:
            logger.error(f"Error checking scheme existence: {e}")
            raise e
    
    async def analyze_scheme_complexity(self, scheme_id: str) -> SchemeComplexityAnalysis:
        """Analyze scheme complexity to predict processing time (optimized version will be much faster)"""
        try:
            query = """
            SELECT 
                scheme_json->'mainScheme'->>'volumeValueBased' as volume_value_based,
                scheme_json->'mainScheme'->>'schemeBase' as scheme_base,
                jsonb_array_length(COALESCE(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates', '[]'::jsonb)) as state_count,
                jsonb_array_length(COALESCE(scheme_json->'mainScheme'->'productData'->'materials', '[]'::jsonb)) as material_count
            FROM schemes_data 
            WHERE scheme_id = %s
            """
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                database_manager.execute_query,
                query,
                [int(scheme_id)]
            )
            
            if not result:
                raise Exception(f"Scheme {scheme_id} not found")
            
            data = result[0]
            volume_value_based = data.get('volume_value_based', 'volume')
            scheme_base = data.get('scheme_base', 'target-based')
            material_count = data.get('material_count', 0)
            state_count = data.get('state_count', 0)
            
            # Optimized version is much faster
            complexity_level = 'low'
            estimated_time = '< 2 seconds'
            recommendations = ['Using optimized calculation engine']
            
            if volume_value_based == 'value':
                if material_count > 2000:
                    complexity_level = 'medium'
                    estimated_time = '2-5 seconds'
                elif material_count > 5000:
                    complexity_level = 'high'
                    estimated_time = '5-10 seconds'
                    recommendations.append('Large product selection - consider filtering')
            
            if state_count > 15:
                recommendations.append('Multiple states selected - processing optimized')
            
            return SchemeComplexityAnalysis(
                scheme_id=scheme_id,
                volume_value_based=volume_value_based,
                scheme_base=scheme_base,
                material_count=material_count,
                state_count=state_count,
                complexity_level=complexity_level,
                estimated_processing_time=estimated_time,
                recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Error analyzing scheme complexity: {e}")
            raise Exception(f"Failed to analyze scheme complexity: {str(e)}")
    
    async def process_batch_calculations(self, requests: List[CostingRequest]) -> List[Dict[str, Any]]:
        """Process multiple costing calculations in batch with intelligent optimization"""
        logger.info(f"Processing optimized batch of {len(requests)} costing calculations")
        
        # Group requests by scheme_id to optimize data fetching
        scheme_groups = {}
        for request in requests:
            if request.scheme_id not in scheme_groups:
                scheme_groups[request.scheme_id] = []
            scheme_groups[request.scheme_id].append(request)
        
        logger.info(f"Grouped {len(requests)} requests into {len(scheme_groups)} scheme groups")
        
        async def process_scheme_group(scheme_id: str, group_requests: List[CostingRequest]) -> List[Dict[str, Any]]:
            """Process all requests for a single scheme (data will be cached after first request)"""
            results = []
            
            for request in group_requests:
                try:
                    if request.calculation_type == 'main_value':
                        result = await self.calculate_main_scheme_value_costing(request.scheme_id)
                    elif request.calculation_type == 'main_volume':
                        result = await self.calculate_main_scheme_volume_costing(request.scheme_id)
                    elif request.calculation_type == 'additional_value':
                        result = await self.calculate_additional_scheme_value_costing(request.scheme_id, request.scheme_index)
                    elif request.calculation_type == 'additional_volume':
                        result = await self.calculate_additional_scheme_volume_costing(request.scheme_id, request.scheme_index)
                    elif request.calculation_type == 'summary_main_value':
                        result = await self.calculate_main_scheme_value_summary(request.scheme_id)
                    elif request.calculation_type == 'summary_main_volume':
                        result = await self.calculate_main_scheme_volume_summary(request.scheme_id)
                    elif request.calculation_type == 'summary_additional_value':
                        result = await self.calculate_additional_scheme_value_summary(request.scheme_id, request.scheme_index)
                    elif request.calculation_type == 'summary_additional_volume':
                        result = await self.calculate_additional_scheme_volume_summary(request.scheme_id, request.scheme_index)
                    else:
                        raise Exception(f"Unknown calculation type: {request.calculation_type}")
                    
                    results.append({
                        'success': True,
                        'request': request.dict(),
                        'result': result
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing request {request.dict()}: {e}")
                    results.append({
                        'success': False,
                        'request': request.dict(),
                        'error': str(e)
                    })
            
            return results
        
        # Process scheme groups in parallel (each group processes sequentially to benefit from caching)
        tasks = [process_scheme_group(scheme_id, group_requests) 
                for scheme_id, group_requests in scheme_groups.items()]
        
        group_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results
        processed_results = []
        for group_result in group_results:
            if isinstance(group_result, Exception):
                processed_results.append({
                    'success': False,
                    'error': str(group_result)
                })
            else:
                processed_results.extend(group_result)
        
        return processed_results
    
    async def process_parallel_calculations(self, requests: List[CostingRequest], max_parallel: int = 8) -> List[Dict[str, Any]]:
        """Process calculations with intelligent parallelism and caching optimization"""
        logger.info(f"Processing {len(requests)} calculations with max {max_parallel} parallel workers")
        
        # Group requests by scheme_id for optimal cache usage
        scheme_groups = {}
        for request in requests:
            if request.scheme_id not in scheme_groups:
                scheme_groups[request.scheme_id] = []
            scheme_groups[request.scheme_id].append(request)
        
        logger.info(f"Grouped requests into {len(scheme_groups)} scheme groups for cache optimization")
        
        async def process_single_request(request: CostingRequest) -> Dict[str, Any]:
            try:
                start_time = time.time()
                
                if request.calculation_type == 'main_value':
                    result = await self.calculate_main_scheme_value_costing(request.scheme_id)
                elif request.calculation_type == 'main_volume':
                    result = await self.calculate_main_scheme_volume_costing(request.scheme_id)
                elif request.calculation_type == 'additional_value':
                    result = await self.calculate_additional_scheme_value_costing(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'additional_volume':
                    result = await self.calculate_additional_scheme_volume_costing(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'summary_main_value':
                    result = await self.calculate_main_scheme_value_summary(request.scheme_id)
                elif request.calculation_type == 'summary_main_volume':
                    result = await self.calculate_main_scheme_volume_summary(request.scheme_id)
                elif request.calculation_type == 'summary_additional_value':
                    result = await self.calculate_additional_scheme_value_summary(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'summary_additional_volume':
                    result = await self.calculate_additional_scheme_volume_summary(request.scheme_id, request.scheme_index)
                else:
                    raise Exception(f"Unknown calculation type: {request.calculation_type}")
                
                execution_time = time.time() - start_time
                
                return {
                    'success': True,
                    'request': request.dict(),
                    'result': result,
                    'execution_time': execution_time,
                    'cache_used': execution_time < 1.0  # Assume cache was used if very fast
                }
                
            except Exception as e:
                logger.error(f"Error processing request {request.dict()}: {e}")
                return {
                    'success': False,
                    'request': request.dict(),
                    'error': str(e),
                    'execution_time': time.time() - start_time if 'start_time' in locals() else 0
                }
        
        # Process scheme groups with intelligent batching
        # Same-scheme requests are processed sequentially to maximize cache hits
        # Different-scheme groups are processed in parallel
        all_results = []
        
        async def process_scheme_group(scheme_id: str, group_requests: List[CostingRequest]) -> List[Dict[str, Any]]:
            """Process all requests for a single scheme to maximize cache benefits"""
            logger.info(f"Processing {len(group_requests)} requests for scheme {scheme_id}")
            group_results = []
            
            # Process requests in the group sequentially to benefit from caching
            for request in group_requests:
                result = await process_single_request(request)
                group_results.append(result)
                
                # Log cache utilization
                if result.get('cache_used', False):
                    logger.info(f"Cache hit for {request.calculation_type} on scheme {scheme_id}")
            
            return group_results
        
        # Process different scheme groups in parallel
        group_tasks = [
            process_scheme_group(scheme_id, group_requests) 
            for scheme_id, group_requests in scheme_groups.items()
        ]
        
        # Limit concurrent scheme groups to avoid overwhelming the system
        for i in range(0, len(group_tasks), max_parallel):
            batch_tasks = group_tasks[i:i + max_parallel]
            logger.info(f"Processing scheme group batch {i//max_parallel + 1} with {len(batch_tasks)} scheme groups")
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for group_result in batch_results:
                if isinstance(group_result, Exception):
                    all_results.append({
                        'success': False,
                        'error': str(group_result)
                    })
                else:
                    all_results.extend(group_result)
        
        # Log cache utilization statistics
        cache_hits = sum(1 for r in all_results if r.get('cache_used', False))
        total_successful = sum(1 for r in all_results if r.get('success', False))
        
        if total_successful > 0:
            cache_hit_rate = (cache_hits / total_successful) * 100
            logger.info(f"Cache utilization: {cache_hits}/{total_successful} requests ({cache_hit_rate:.1f}% hit rate)")
        
        return all_results
