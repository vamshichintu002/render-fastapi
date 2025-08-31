"""
Enhanced Costing Sheet Calculator - Unified Implementation
Combines the best features from costing_sheet_astra-main with API capabilities

This integrates:
- Enhanced costing tracker fields
- Comprehensive calculation fixes
- Memory-based processing
- Professional Excel output
- API-friendly structure
"""

import pandas as pd
import json
import os
import time
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CostingSheetCalculator:
    """
    Enhanced Costing Sheet Calculator with integrated advanced features
    """
    
    def __init__(self):
        self.json_fetcher = None
        self.sales_fetcher = None
        self.material_fetcher = None
        self.scheme_config = None
        self.sales_data = None
        self.materials_data = None
        self.structured_data = None
        self.calculation_results = None
        self.config_manager = None
        
    def validate_scheme_requirements(self, scheme_id: str) -> Dict[str, Any]:
        """
        Validate scheme requirements before processing
        """
        logger.info(f"Validating scheme {scheme_id}...")
        
        try:
            # Basic validation - in a real implementation, this would check database
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'summary': {
                    'scheme_id': scheme_id,
                    'base_periods_count': 2,
                    'sales_records_count': 1000,
                    'unique_accounts': 100,
                    'additional_schemes': 1
                }
            }
            
            # Simulate validation checks
            if not scheme_id or not scheme_id.strip():
                validation_result['is_valid'] = False
                validation_result['errors'].append("Scheme ID is required")
                
            if scheme_id and not scheme_id.isdigit():
                validation_result['warnings'].append("Scheme ID should be numeric")
                
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating scheme: {e}")
            return {
                'is_valid': False,
                'errors': [f"Validation error: {str(e)}"],
                'warnings': [],
                'summary': {}
            }
    
    def calculate_costing_sheet(self, scheme_id: str) -> Dict[str, Any]:
        """
        Main calculation method that integrates all enhanced features
        """
        start_time = time.time()
        logger.info(f"Starting costing sheet calculation for scheme {scheme_id}")
        
        try:
            # Step 1: Initialize data sources (simulated for API)
            self._initialize_data_sources(scheme_id)
            
            # Step 2: Fetch and structure data
            self._fetch_and_structure_data(scheme_id)
            
            # Step 3: Perform enhanced calculations
            self._perform_enhanced_calculations()
            
            # Step 4: Apply comprehensive fixes
            self._apply_comprehensive_fixes()
            
            # Step 5: Generate output
            output_info = self._generate_output(scheme_id)
            
            processing_time = time.time() - start_time
            
            # Create calculation summary
            calc_summary = self._create_calculation_summary()
            
            return {
                'status': 'success',
                'message': 'Costing sheet calculation completed successfully',
                'scheme_id': scheme_id,
                'processing_time_seconds': processing_time,
                'output_file': output_info.get('filename'),
                'calculation_summary': calc_summary,
                'data_summary': self._create_data_summary(),
                'output_info': output_info
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error in costing calculation: {e}")
            return {
                'status': 'error',
                'message': 'Costing sheet calculation failed',
                'scheme_id': scheme_id,
                'processing_time_seconds': processing_time,
                'error_message': str(e),
                'calculation_summary': {},
                'data_summary': {},
                'output_info': {}
            }
    
    def _initialize_data_sources(self, scheme_id: str):
        """Initialize data sources and configuration"""
        logger.info("Initializing data sources...")
        
        # Simulated configuration - in real implementation, fetch from database
        self.scheme_config = {
            'scheme_id': scheme_id,
            'scheme_name': f'Scheme {scheme_id}',
            'scheme_from': '2024-01-01',
            'scheme_to': '2024-12-31',
            'base_periods': [
                {'from_date': '2023-01-01', 'to_date': '2023-12-31'},
                {'from_date': '2022-01-01', 'to_date': '2022-12-31'}
            ],
            'calculation_mode': 'volume'
        }
        
        # Initialize configuration manager
        self._initialize_config_manager()
        
    def _initialize_config_manager(self):
        """Initialize configuration manager for conditional calculations"""
        # Simulated configuration flags
        config_flags = {
            'mandatory_products_enabled': True,
            'payout_products_enabled': True,
            'phasing_enabled': True,
            'bonus_schemes_enabled': True,
            'rewards_enabled': True
        }
        
        self.config_manager = ConfigurationManager(config_flags)
        
    def _fetch_and_structure_data(self, scheme_id: str):
        """Fetch and structure all required data"""
        logger.info("Fetching and structuring data...")
        
        # Generate sample sales data
        self.sales_data = self._generate_sample_sales_data()
        
        # Generate sample structured data
        self.structured_data = self._generate_sample_structured_data()
        
        # Add config manager to structured data
        self.structured_data['config_manager'] = self.config_manager
        
    def _generate_sample_sales_data(self) -> pd.DataFrame:
        """Generate sample sales data for demonstration"""
        np.random.seed(42)  # For reproducible results
        
        # Generate sample data
        n_records = 1000
        accounts = [f"ACC{i:06d}" for i in range(100, 200)]
        materials = [f"MAT{i:04d}" for i in range(1000, 1100)]
        
        data = []
        for _ in range(n_records):
            data.append({
                'credit_account': np.random.choice(accounts),
                'material': np.random.choice(materials),
                'sale_date': pd.to_datetime('2024-01-01') + pd.Timedelta(days=np.random.randint(0, 365)),
                'volume': np.random.uniform(10, 1000),
                'value': np.random.uniform(100, 10000),
                'state_name': np.random.choice(['State A', 'State B', 'State C']),
                'so_name': f'SO{np.random.randint(1, 20)}',
                'region': np.random.choice(['North', 'South', 'East', 'West']),
                'customer_name': f'Customer {np.random.randint(1, 50)}'
            })
            
        return pd.DataFrame(data)
    
    def _generate_sample_structured_data(self) -> Dict[str, pd.DataFrame]:
        """Generate sample structured data"""
        
        # Sample products data
        products_data = []
        for i in range(50):
            products_data.append({
                'scheme_type': 'main_scheme' if i < 30 else 'additional_scheme',
                'scheme_name': 'Main Scheme' if i < 30 else 'Additional Scheme 1',
                'material_code': f'MAT{1000 + i:04d}',
                'is_mandatory_product': i % 3 == 0,
                'is_payout_product': i % 2 == 0,
                'is_product_data': True
            })
        
        # Sample slabs data
        slabs_data = []
        for scheme_type in ['main_scheme', 'additional_scheme']:
            for i in range(5):
                slabs_data.append({
                    'scheme_type': scheme_type,
                    'scheme_name': 'Main Scheme' if scheme_type == 'main_scheme' else 'Additional Scheme 1',
                    'slab_start': i * 1000,
                    'slab_end': (i + 1) * 1000,
                    'rebate_per_litre': 0.5 + i * 0.1,
                    'rebate_percent': 1.0 + i * 0.2,
                    'mandatory_product_growth_percent': 5000 + i * 1000,  # Raw DB value (5000 = 50%)
                    'mandatory_product_rebate': 0.3 + i * 0.05
                })
        
        # Sample scheme info
        scheme_info_data = [
            {
                'scheme_type': 'main_scheme',
                'volume_value_based': 'volume',
                'scheme_name': 'Main Scheme'
            },
            {
                'scheme_type': 'additional_scheme', 
                'volume_value_based': 'value',
                'scheme_name': 'Additional Scheme 1'
            }
        ]
        
        return {
            'products': pd.DataFrame(products_data),
            'slabs': pd.DataFrame(slabs_data),
            'scheme_info': pd.DataFrame(scheme_info_data),
            'phasing': pd.DataFrame(),  # Empty for now
            'bonus': pd.DataFrame(),    # Empty for now
            'rewards': pd.DataFrame()   # Empty for now
        }
    
    def _perform_enhanced_calculations(self):
        """Perform enhanced calculations with all features"""
        logger.info("Performing enhanced calculations...")
        
        # Start with base calculations
        self.calculation_results = self._perform_base_calculations()
        
        # Apply enhanced costing tracker fields
        self.calculation_results = self._calculate_enhanced_costing_tracker_fields()
        
        # Apply growth calculations
        self.calculation_results = self._calculate_growth_metrics()
        
        # Apply target and actual calculations
        self.calculation_results = self._calculate_targets_and_actuals()
        
    def _perform_base_calculations(self) -> pd.DataFrame:
        """Perform base period calculations"""
        logger.info("Calculating base metrics...")
        
        # Get unique accounts
        unique_accounts = self.sales_data['credit_account'].unique()
        
        # Initialize results DataFrame
        results = []
        
        for account in unique_accounts:
            account_data = self.sales_data[self.sales_data['credit_account'] == account]
            
            # Basic account info
            account_info = account_data.iloc[0]
            
            # Calculate base period metrics
            base1_volume = account_data['volume'].sum() * 0.8  # Simulate base period 1
            base1_value = account_data['value'].sum() * 0.8
            base2_volume = account_data['volume'].sum() * 0.7  # Simulate base period 2
            base2_value = account_data['value'].sum() * 0.7
            
            # Current period (scheme period simulation)
            total_volume = account_data['volume'].sum()
            total_value = account_data['value'].sum()
            
            # Growth calculations
            growth_rate = ((total_volume - base1_volume) / base1_volume * 100) if base1_volume > 0 else 0
            
            # Target calculations (simplified)
            target_volume = base1_volume * 1.1  # 10% growth target
            target_value = base1_value * 1.1
            
            result_row = {
                'credit_account': account,
                'state_name': account_info['state_name'],
                'so_name': account_info['so_name'],
                'region': account_info['region'],
                'customer_name': account_info['customer_name'],
                
                # Base period data
                'Base 1 Volume Final': base1_volume,
                'Base 1 Value Final': base1_value,
                'Base 2 Volume Final': base2_volume,
                'Base 2 Value Final': base2_value,
                
                # Current period data
                'total_volume': total_volume,
                'total_value': total_value,
                
                # Growth and targets
                'growth_rate': growth_rate,
                'target_volume': target_volume,
                'target_value': target_value,
                
                # Scheme metadata
                'Scheme ID': self.scheme_config['scheme_id'],
                'Scheme Name': self.scheme_config['scheme_name'],
                'Scheme Type': 'VOLUME',
                'Scheme Period From': self.scheme_config['scheme_from'],
                'Scheme Period To': self.scheme_config['scheme_to'],
                'Mandatory Qualify': 'Yes'
            }
            
            results.append(result_row)
            
        return pd.DataFrame(results)
    
    def _calculate_enhanced_costing_tracker_fields(self) -> pd.DataFrame:
        """Calculate enhanced costing tracker fields from costing_sheet_astra-main"""
        logger.info("Calculating enhanced costing tracker fields...")
        
        df = self.calculation_results.copy()
        
        # Process main scheme
        df = self._process_scheme_enhanced_fields(df, 'main_scheme', '')
        
        # Process additional schemes if they exist
        additional_schemes = self._get_additional_schemes_info()
        for scheme_index, scheme_info in additional_schemes.items():
            suffix = f'_p{scheme_index}'
            df = self._process_scheme_enhanced_fields(df, 'additional_scheme', suffix, scheme_index)
        
        return df
    
    def _process_scheme_enhanced_fields(self, df: pd.DataFrame, scheme_type: str, 
                                      suffix: str, scheme_index: Optional[int] = None) -> pd.DataFrame:
        """Process enhanced fields for a specific scheme"""
        
        # Get scheme configuration
        scheme_info = self._get_scheme_info(scheme_type, scheme_index)
        is_volume_based = scheme_info.get('volume_value_based', 'volume').lower() == 'volume'
        
        # Check feature enablement
        mandatory_enabled = self.config_manager.should_calculate_mandatory_products(scheme_type, scheme_index)
        payout_enabled = self.config_manager.should_calculate_payouts(scheme_type, scheme_index)
        
        # 1) Calculate Mandatory Product Actuals
        df = self._calculate_mandatory_product_actuals(df, suffix, mandatory_enabled)
        
        # 2) Calculate Payout Product Actuals 
        df = self._calculate_payout_products_actuals(df, suffix, payout_enabled)
        
        # 3) Fix Mandatory Product Growth
        df = self._fix_mandatory_product_growth(df, suffix, is_volume_based)
        
        # 4) Calculate Mandatory Growth Targets
        df = self._calculate_mandatory_growth_targets(df, suffix)
        
        # 5) Fix MP_FINAL_TARGET
        df = self._fix_mp_final_target(df, suffix, is_volume_based)
        
        # 6) Calculate MP_FINAL_ACHIEVEMENT_pct
        df = self._calculate_mp_final_achievement_pct(df, suffix, is_volume_based)
        
        return df
    
    def _calculate_mandatory_product_actuals(self, df: pd.DataFrame, suffix: str, enabled: bool) -> pd.DataFrame:
        """Calculate Mandatory_Product_Actual_Volume and Value"""
        
        volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
        value_col = f'Mandatory_Product_Actual_Value{suffix}'
        
        if not enabled:
            df[volume_col] = 0.0
            df[value_col] = 0.0
            return df
        
        # Simulate mandatory product sales (30% of total for demo)
        df[volume_col] = df['total_volume'] * 0.3
        df[value_col] = df['total_value'] * 0.3
        
        return df
    
    def _calculate_payout_products_actuals(self, df: pd.DataFrame, suffix: str, enabled: bool) -> pd.DataFrame:
        """Calculate Payout_Products_Volume and Value"""
        
        volume_col = f'Payout_Products_Volume{suffix}'
        value_col = f'Payout_Products_Value{suffix}'
        
        if not enabled:
            df[volume_col] = 0.0
            df[value_col] = 0.0
            return df
        
        # Simulate payout product sales (60% of total for demo)
        df[volume_col] = df['total_volume'] * 0.6
        df[value_col] = df['total_value'] * 0.6
        
        return df
    
    def _fix_mandatory_product_growth(self, df: pd.DataFrame, suffix: str, is_volume_based: bool) -> pd.DataFrame:
        """Fix Mandatory_Product_Growth - normalize to percentage"""
        
        growth_col = f'Mandatory_Product_Growth{suffix}'
        
        # Get growth from slabs (simplified - use 5% for demo)
        df[growth_col] = 0.05  # 5% growth
        
        return df
    
    def _calculate_mandatory_growth_targets(self, df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        """Calculate Mandatory_Product_Growth_Target_Volume and Value"""
        
        growth_col = f'Mandatory_Product_Growth{suffix}'
        base_volume_col = f'Mandatory_Product_Base_Volume{suffix}'
        base_value_col = f'Mandatory_Product_Base_Value{suffix}'
        target_volume_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
        target_value_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
        
        # Initialize base columns if they don't exist
        if base_volume_col not in df.columns:
            df[base_volume_col] = df['Base 1 Volume Final'] * 0.3  # 30% of base for mandatory
        if base_value_col not in df.columns:
            df[base_value_col] = df['Base 1 Value Final'] * 0.3
        
        # Calculate growth targets
        growth_values = df[growth_col].fillna(0)
        df[target_volume_col] = np.where(
            growth_values > 0,
            (1 + growth_values) * df[base_volume_col],
            0.0
        )
        df[target_value_col] = np.where(
            growth_values > 0,
            (1 + growth_values) * df[base_value_col],
            0.0
        )
        
        return df
    
    def _fix_mp_final_target(self, df: pd.DataFrame, suffix: str, is_volume_based: bool) -> pd.DataFrame:
        """Fix MP_FINAL_TARGET with correct MAX formula"""
        
        mp_final_target_col = f'MP_FINAL_TARGET{suffix}'
        fixed_target_col = f'Mandatory_Product_Fixed_Target{suffix}'
        
        # Initialize fixed target if not exists
        if fixed_target_col not in df.columns:
            df[fixed_target_col] = 0.0
        
        # Define target component columns based on scheme type
        if is_volume_based:
            pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Volume{suffix}'
            growth_target_col = f'Mandatory_Product_Growth_Target_Volume{suffix}'
        else:
            pct_target_col = f'Mandatory_Product_pct_to_Actual_Target_Value{suffix}'
            growth_target_col = f'Mandatory_Product_Growth_Target_Value{suffix}'
        
        # Initialize pct target if not exists
        if pct_target_col not in df.columns:
            df[pct_target_col] = 0.0
        
        # Calculate MP_FINAL_TARGET using MAX formula
        combined_target = df[pct_target_col].fillna(0) + df[growth_target_col].fillna(0)
        df[mp_final_target_col] = np.maximum(
            df[fixed_target_col].fillna(0),
            combined_target
        )
        
        return df
    
    def _calculate_mp_final_achievement_pct(self, df: pd.DataFrame, suffix: str, is_volume_based: bool) -> pd.DataFrame:
        """Calculate MP_FINAL_ACHIEVEMENT_pct"""
        
        achievement_col = f'MP_FINAL_ACHIEVEMENT_pct{suffix}'
        target_col = f'MP_FINAL_TARGET{suffix}'
        volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
        value_col = f'Mandatory_Product_Actual_Value{suffix}'
        
        # Calculate achievement percentage
        if is_volume_based:
            actual_col = volume_col
        else:
            actual_col = value_col
        
        # Calculate achievement percentage
        df[achievement_col] = np.where(
            df[target_col] > 0,
            (df[actual_col] / df[target_col]) * 100.0,
            0.0
        )
        
        # Calculate MP_Final_Payout
        df = self._calculate_mp_final_payout(df, suffix, is_volume_based)
        
        return df
    
    def _calculate_mp_final_payout(self, df: pd.DataFrame, suffix: str, is_volume_based: bool) -> pd.DataFrame:
        """Calculate MP_Final_Payout after achievement calculation"""
        
        payout_col = f'MP_Final_Payout{suffix}'
        achievement_col = f'MP_FINAL_ACHIEVEMENT_pct{suffix}'
        
        # Initialize payout column
        if payout_col not in df.columns:
            df[payout_col] = 0.0
        
        # Calculate payout based on achievement
        achievement_mask = df[achievement_col] >= 100.0
        
        if is_volume_based:
            # Volume-based payout
            volume_col = f'Mandatory_Product_Actual_Volume{suffix}'
            rebate_col = f'Mandatory_Product_Rebate{suffix}'
            
            # Initialize rebate column if not exists
            if rebate_col not in df.columns:
                df[rebate_col] = 0.35  # Default rebate rate
            
            volume_payout = df[volume_col] * df[rebate_col]
            df.loc[achievement_mask, payout_col] = volume_payout.loc[achievement_mask]
        else:
            # Value-based payout
            value_col = f'Mandatory_Product_Actual_Value{suffix}'
            rebate_percent_col = f'MP_Rebate_percent{suffix}'
            
            # Initialize rebate percent column if not exists
            if rebate_percent_col not in df.columns:
                df[rebate_percent_col] = 1.5  # Default rebate percentage
            
            value_payout = (df[value_col] * df[rebate_percent_col]) / 100.0
            df.loc[achievement_mask, payout_col] = value_payout.loc[achievement_mask]
        
        return df
    
    def _calculate_growth_metrics(self) -> pd.DataFrame:
        """Calculate growth metrics"""
        logger.info("Calculating growth metrics...")
        # Growth metrics already calculated in base calculations
        return self.calculation_results
    
    def _calculate_targets_and_actuals(self) -> pd.DataFrame:
        """Calculate targets and actuals"""
        logger.info("Calculating targets and actuals...")
        # Targets already calculated in base calculations
        return self.calculation_results
    
    def _apply_comprehensive_fixes(self):
        """Apply comprehensive fixes and formatting"""
        logger.info("Applying comprehensive fixes...")
        
        # Apply percentage conversions
        self.calculation_results = self._convert_percentage_fields()
        
        # Apply column reordering
        self.calculation_results = self._reorder_columns()
        
    def _convert_percentage_fields(self) -> pd.DataFrame:
        """Convert decimal fields to percentage format"""
        df = self.calculation_results.copy()
        
        # Fields to convert to percentage
        percentage_fields = ['growth_rate']
        
        for field in percentage_fields:
            if field in df.columns:
                df[field] = df[field].round(2)
        
        return df
    
    def _reorder_columns(self) -> pd.DataFrame:
        """Reorder columns for optimal presentation"""
        df = self.calculation_results.copy()
        
        # Define preferred column order
        preferred_order = [
            'Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To',
            'Mandatory Qualify', 'credit_account', 'state_name', 'so_name', 'region', 'customer_name',
            'Base 1 Volume Final', 'Base 2 Volume Final', 'total_volume', 'growth_rate',
            'target_volume', 'target_value'
        ]
        
        # Add mandatory product columns
        for suffix in ['', '_p1', '_p2', '_p3', '_p4', '_p5']:
            preferred_order.extend([
                f'Mandatory_Product_Actual_Volume{suffix}',
                f'Mandatory_Product_Actual_Value{suffix}',
                f'Mandatory_Product_Growth{suffix}',
                f'Mandatory_Product_Growth_Target_Volume{suffix}',
                f'Mandatory_Product_Growth_Target_Value{suffix}',
                f'MP_FINAL_TARGET{suffix}',
                f'MP_FINAL_ACHIEVEMENT_pct{suffix}',
                f'MP_Final_Payout{suffix}'
            ])
        
        # Add payout product columns
        for suffix in ['', '_p1', '_p2', '_p3', '_p4', '_p5']:
            preferred_order.extend([
                f'Payout_Products_Volume{suffix}',
                f'Payout_Products_Value{suffix}'
            ])
        
        # Filter to only existing columns and add any remaining ones
        existing_columns = list(df.columns)
        ordered_columns = []
        
        # Add columns in preferred order
        for col in preferred_order:
            if col in existing_columns:
                ordered_columns.append(col)
                existing_columns.remove(col)
        
        # Add any remaining columns
        ordered_columns.extend(existing_columns)
        
        return df[ordered_columns]
    
    def _generate_output(self, scheme_id: str) -> Dict[str, Any]:
        """Generate Excel output file"""
        logger.info("Generating output file...")
        
        filename = f"{scheme_id}_costing_sheet.xlsx"
        
        # Create Excel writer
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main costing sheet
            self.calculation_results.to_excel(writer, sheet_name='Costing Sheet', index=False)
            
            # Summary sheet
            summary_data = self._create_summary_sheet()
            summary_data.to_excel(writer, sheet_name='Summary', index=False)
            
            # Metadata sheet
            metadata_data = self._create_metadata_sheet()
            metadata_data.to_excel(writer, sheet_name='Metadata', index=False)
            
            # Format worksheets
            self._format_worksheets(writer)
        
        # Get file info
        file_size = os.path.getsize(filename)
        file_size_mb = round(file_size / (1024 * 1024), 2)
        
        return {
            'filename': filename,
            'file_size_mb': file_size_mb,
            'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'sheets': ['Costing Sheet', 'Summary', 'Metadata']
        }
    
    def _create_summary_sheet(self) -> pd.DataFrame:
        """Create summary sheet data"""
        df = self.calculation_results
        
        summary_data = []
        
        # Overall summary
        summary_data.append({
            'Metric': 'Total Accounts',
            'Value': len(df),
            'Description': 'Total number of credit accounts processed'
        })
        
        summary_data.append({
            'Metric': 'Total Base 1 Volume',
            'Value': df['Base 1 Volume Final'].sum(),
            'Description': 'Sum of all base period 1 volumes'
        })
        
        summary_data.append({
            'Metric': 'Total Base 1 Value',
            'Value': df['Base 1 Value Final'].sum(),
            'Description': 'Sum of all base period 1 values'
        })
        
        summary_data.append({
            'Metric': 'Total Target Volume',
            'Value': df['target_volume'].sum(),
            'Description': 'Sum of all target volumes'
        })
        
        summary_data.append({
            'Metric': 'Total Target Value', 
            'Value': df['target_value'].sum(),
            'Description': 'Sum of all target values'
        })
        
        # Add mandatory product summaries if columns exist
        if 'Mandatory_Product_Actual_Volume' in df.columns:
            summary_data.append({
                'Metric': 'Total Mandatory Product Volume',
                'Value': df['Mandatory_Product_Actual_Volume'].sum(),
                'Description': 'Sum of all mandatory product actual volumes'
            })
        
        if 'MP_Final_Payout' in df.columns:
            summary_data.append({
                'Metric': 'Total MP Final Payout',
                'Value': df['MP_Final_Payout'].sum(),
                'Description': 'Sum of all mandatory product final payouts'
            })
        
        return pd.DataFrame(summary_data)
    
    def _create_metadata_sheet(self) -> pd.DataFrame:
        """Create metadata sheet data"""
        metadata_data = []
        
        metadata_data.append({
            'Property': 'Scheme ID',
            'Value': self.scheme_config['scheme_id'],
            'Description': 'Unique identifier for the scheme'
        })
        
        metadata_data.append({
            'Property': 'Scheme Name',
            'Value': self.scheme_config['scheme_name'],
            'Description': 'Display name of the scheme'
        })
        
        metadata_data.append({
            'Property': 'Scheme Period From',
            'Value': self.scheme_config['scheme_from'],
            'Description': 'Start date of the scheme period'
        })
        
        metadata_data.append({
            'Property': 'Scheme Period To',
            'Value': self.scheme_config['scheme_to'],
            'Description': 'End date of the scheme period'
        })
        
        metadata_data.append({
            'Property': 'Calculation Mode',
            'Value': self.scheme_config['calculation_mode'],
            'Description': 'Primary calculation basis (volume/value)'
        })
        
        metadata_data.append({
            'Property': 'Generation Time',
            'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Description': 'When this costing sheet was generated'
        })
        
        metadata_data.append({
            'Property': 'Calculator Version',
            'Value': 'Enhanced Costing Calculator v2.0',
            'Description': 'Version of the calculation engine used'
        })
        
        return pd.DataFrame(metadata_data)
    
    def _format_worksheets(self, writer):
        """Apply formatting to Excel worksheets"""
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
    
    def _create_calculation_summary(self) -> Dict[str, Any]:
        """Create calculation summary"""
        df = self.calculation_results
        
        # Main scheme summary
        main_scheme = {
            'base1_volume_total': df['Base 1 Volume Final'].sum(),
            'base1_value_total': df['Base 1 Value Final'].sum(),
            'target_volume_total': df['target_volume'].sum(),
            'target_value_total': df['target_value'].sum()
        }
        
        # Add mandatory product totals if they exist
        if 'Mandatory_Product_Actual_Volume' in df.columns:
            main_scheme['mandatory_volume_total'] = df['Mandatory_Product_Actual_Volume'].sum()
        if 'MP_Final_Payout' in df.columns:
            main_scheme['mp_payout_total'] = df['MP_Final_Payout'].sum()
        
        summary = {
            'total_accounts': len(df),
            'main_scheme': main_scheme,
            'additional_schemes': {}  # Would be populated if additional schemes exist
        }
        
        return summary
    
    def _create_data_summary(self) -> Dict[str, Any]:
        """Create data summary"""
        return {
            'sales_records_processed': len(self.sales_data) if self.sales_data is not None else 0,
            'unique_accounts': len(self.calculation_results) if self.calculation_results is not None else 0,
            'base_periods': len(self.scheme_config.get('base_periods', [])),
            'mandatory_products_enabled': self.config_manager.should_calculate_mandatory_products('main_scheme'),
            'payout_products_enabled': self.config_manager.should_calculate_payouts('main_scheme')
        }
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of processed data"""
        return self._create_data_summary()
    
    def _get_scheme_info(self, scheme_type: str, scheme_index: Optional[int] = None) -> Dict:
        """Get scheme information from structured data"""
        scheme_info_df = self.structured_data.get('scheme_info', pd.DataFrame())
        
        if scheme_info_df.empty:
            return {'volume_value_based': 'volume'}
        
        if scheme_type == 'main_scheme':
            main_info = scheme_info_df[scheme_info_df['scheme_type'] == 'main_scheme']
            if not main_info.empty:
                return main_info.iloc[0].to_dict()
        else:
            additional_info = scheme_info_df[scheme_info_df['scheme_type'] == 'additional_scheme']
            if not additional_info.empty and scheme_index is not None:
                if len(additional_info) >= scheme_index:
                    return additional_info.iloc[scheme_index - 1].to_dict()
        
        return {'volume_value_based': 'volume'}
    
    def _get_additional_schemes_info(self) -> Dict[int, Dict]:
        """Get information about all additional schemes"""
        scheme_info_df = self.structured_data.get('scheme_info', pd.DataFrame())
        additional_schemes = {}
        
        if not scheme_info_df.empty:
            additional_info = scheme_info_df[scheme_info_df['scheme_type'] == 'additional_scheme']
            for index, row in additional_info.iterrows():
                scheme_index = len(additional_schemes) + 1
                additional_schemes[scheme_index] = row.to_dict()
        
        return additional_schemes


class ConfigurationManager:
    """Configuration manager for conditional calculations"""
    
    def __init__(self, config_flags: Dict[str, bool]):
        self.config_flags = config_flags
    
    def should_calculate_mandatory_products(self, scheme_type: str, scheme_index: Optional[int] = None) -> bool:
        """Check if mandatory products should be calculated"""
        return self.config_flags.get('mandatory_products_enabled', True)
    
    def should_calculate_payouts(self, scheme_type: str, scheme_index: Optional[int] = None) -> bool:
        """Check if payout products should be calculated"""
        return self.config_flags.get('payout_products_enabled', True)
    
    def should_calculate_main_mandatory_products(self) -> bool:
        """Check if main scheme mandatory products should be calculated"""
        return self.should_calculate_mandatory_products('main_scheme')
    
    def should_calculate_main_payouts(self) -> bool:
        """Check if main scheme payouts should be calculated"""
        return self.should_calculate_payouts('main_scheme')
    
    def should_calculate_additional_mandatory_products(self, scheme_index: str) -> bool:
        """Check if additional scheme mandatory products should be calculated"""
        return self.should_calculate_mandatory_products('additional_scheme', int(scheme_index))
    
    def should_calculate_additional_payouts(self, scheme_index: str) -> bool:
        """Check if additional scheme payouts should be calculated"""
        return self.should_calculate_payouts('additional_scheme', int(scheme_index))