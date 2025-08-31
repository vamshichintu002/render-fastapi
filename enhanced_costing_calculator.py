"""
Enhanced Costing Sheet Calculator - API Integration
Based on costing_sheet_astra-main with API-optimized features

This integrates:
- Enhanced costing tracker fields
- Comprehensive calculation fixes
- Memory-based processing (no file generation)
- API-friendly structure
- All advanced calculation modules
"""

import pandas as pd
import json
import os
import time
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
import sys

# Add the current directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'calculations'))

# Import the enhanced calculation modules
try:
    from vectorized_calculations import calculate_base_and_scheme_metrics_vectorized
    from growth_calculations import calculate_growth_metrics_vectorized
    from target_calculations import calculate_all_targets_and_actuals
    from enhanced_costing_tracker_fields import calculate_enhanced_costing_tracker_fields
    from metadata_filler import fill_scheme_metadata_columns
    from comprehensive_tracker_fixes import apply_comprehensive_tracker_fixes
    from scheme_final_payout_calculations import calculate_scheme_final_payout
    from conditional_payout_calculations import calculate_conditional_payout_columns
    from estimated_calculations import calculate_estimated_columns
    from rewards_calculations import calculate_rewards
    from percentage_conversions import convert_decimal_fields_to_percentage
    from mandatory_product_global_fix import apply_global_mandatory_product_fix
    from comprehensive_tracker_fixes import get_comprehensive_column_order
    from column_reordering import reorder_tracker_columns
    from configuration_manager import SchemeConfigurationManager
except ImportError as e:
    logging.warning(f"Some calculation modules not available: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CostingSheetCalculator:
    """
    Enhanced Costing Sheet Calculator with integrated advanced features
    API-optimized version that processes everything in memory
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
        self.data_extractor = None
        self.applicable_data = None
        
    def validate_scheme_requirements(self, scheme_id: str) -> Dict[str, Any]:
        """
        Validate scheme requirements before processing
        """
        logger.info(f"VALIDATING SCHEME REQUIREMENTS: {scheme_id}")
        print(f"🔍 VALIDATING SCHEME REQUIREMENTS: {scheme_id}")
        print("=" * 50)
        
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
        Returns results in memory without file generation
        """
        start_time = time.time()
        logger.info(f"Starting costing sheet calculation for scheme {scheme_id}")
        print(f"💰 COSTING SHEET: Processing Scheme {scheme_id}")
        print("=" * 50)
        
        try:
            # Step 1: Initialize data sources
            self._initialize_data_sources(scheme_id)
            
            # Step 2: Fetch and structure data
            self._fetch_and_structure_data(scheme_id)
            
            # Step 3: Perform enhanced calculations
            self._perform_enhanced_calculations()
            
            # Step 4: Apply comprehensive fixes and formatting
            self._apply_final_fixes_and_formatting()
            
            # Step 5: Prepare API response data
            processing_time = time.time() - start_time
            result_data = self._prepare_api_response(scheme_id, processing_time)
            
            logger.info(f"Costing sheet calculation completed successfully in {processing_time:.2f} seconds")
            
            return {
                'status': 'success',
                'scheme_id': scheme_id,
                'processing_time_seconds': processing_time,
                'calculation_summary': self._get_calculation_summary(),
                'data_summary': self._get_data_summary(),
                'output_info': {
                    'total_records': len(self.calculation_results) if self.calculation_results is not None else 0,
                    'columns_count': len(self.calculation_results.columns) if self.calculation_results is not None else 0,
                    'calculation_status': 'completed',
                    'file_generated': False,  # API mode - no file generation
                    'memory_processed': True
                },
                'data': result_data
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Error in costing sheet calculation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            return {
                'status': 'error',
                'scheme_id': scheme_id,
                'processing_time_seconds': processing_time,
                'error_message': error_msg,
                'calculation_summary': None,
                'data_summary': None,
                'output_info': None
            }
    
    def _initialize_data_sources(self, scheme_id: str):
        """Initialize data source objects"""
        try:
            # Import the required fetcher classes
            from json_fetcher import JSONFetcher
            from sales_fetcher import SalesFetcher
            from materialfetcher import MaterialFetcher
            from schemeapplicablefetcher import SchemeApplicableFetcher
            from store import SchemeDataExtractor
            
            self.json_fetcher = JSONFetcher()
            self.sales_fetcher = SalesFetcher()
            self.material_fetcher = MaterialFetcher()
            self.data_extractor = SchemeDataExtractor()
            
            logger.info("Data source objects initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing data sources: {e}")
            raise
    
    def _fetch_and_structure_data(self, scheme_id: str):
        """Fetch and structure all required data"""
        try:
            logger.info("Fetching scheme configuration...")
            print("   📋 Fetching scheme configuration...")
            
            # Fetch scheme config
            raw_config = self.json_fetcher.fetchAndStoreJson(scheme_id)
            if isinstance(raw_config, list) and len(raw_config) >= 1:
                self.scheme_config = raw_config[0]
            else:
                self.scheme_config = raw_config
            
            # Extract filters
            self.scheme_applicable_fetcher = SchemeApplicableFetcher(self.json_fetcher.getStoredJson())
            self.applicable_data = self.scheme_applicable_fetcher.fetchSchemeApplicable()
            filters = self.scheme_applicable_fetcher.getAllFiltersForSql()
            
            # Fetch sales data
            logger.info("Fetching sales data...")
            self.sales_data = self.sales_fetcher.fetchAllSalesWithFilters(self.scheme_config, filters)
            
            # Fetch material master
            logger.info("Fetching material master data...")
            self.materials_data = self.material_fetcher.fetchAllMaterialMaster()
            
            # Extract structured data
            logger.info("Extracting structured data...")
            raw_json = self.json_fetcher.getStoredJson()
            if raw_json:
                self.structured_data = self.data_extractor.extractAllData(raw_json)
                
                # Initialize configuration manager
                self.config_manager = SchemeConfigurationManager(raw_json)
                
            logger.info("Data fetching and structuring completed")
            
        except Exception as e:
            logger.error(f"Error fetching and structuring data: {e}")
            print(f"   ❌ Error in costing data processing: {str(e)}")
            raise
    
    def _perform_enhanced_calculations(self):
        """Perform all enhanced calculations"""
        try:
            if not self.sales_data or not self.scheme_config:
                raise ValueError("Required data not available for calculations")
            
            logger.info("Starting enhanced calculations...")
            
            # Convert sales data to DataFrame
            sales_df = pd.DataFrame(self.sales_data)
            raw_json = self.json_fetcher.getStoredJson()
            
            # Base calculations
            logger.info("Performing base calculations...")
            self.calculation_results = calculate_base_and_scheme_metrics_vectorized(
                sales_df, self.scheme_config, raw_json, self.structured_data
            )
            
            # Growth calculations
            logger.info("Adding growth calculations...")
            strata_growth_df = self._fetch_strata_growth_data()
            self.calculation_results = calculate_growth_metrics_vectorized(
                self.calculation_results,
                self.structured_data,
                raw_json,
                strata_growth_df
            )
            
            # Target and actual calculations
            logger.info("Adding target and actual calculations...")
            self.calculation_results = calculate_all_targets_and_actuals(
                self.calculation_results,
                self.structured_data,
                raw_json,
                sales_df,
                self.scheme_config
            )
            
            # Enhanced costing tracker fields
            logger.info("Adding enhanced costing tracker fields...")
            self.calculation_results = calculate_enhanced_costing_tracker_fields(
                self.calculation_results,
                self.structured_data,
                sales_df,
                self.scheme_config
            )
            
            # Metadata columns
            logger.info("Filling scheme metadata columns...")
            self.calculation_results = fill_scheme_metadata_columns(
                self.calculation_results,
                self.structured_data,
                sales_df
            )
            
            # Comprehensive tracker fixes
            logger.info("Applying comprehensive tracker fixes...")
            self.calculation_results = apply_comprehensive_tracker_fixes(
                self.calculation_results,
                self.structured_data,
                raw_json,
                self.scheme_config
            )
            
            # Scheme final payout
            logger.info("Calculating scheme final payout...")
            self.calculation_results = calculate_scheme_final_payout(
                self.calculation_results,
                self.structured_data,
                self.scheme_config
            )
            
            # Conditional payout calculations
            logger.info("Adding conditional payout calculations...")
            self.calculation_results = calculate_conditional_payout_columns(
                self.calculation_results,
                self.structured_data,
                sales_df,
                self.scheme_config
            )
            
            # Estimated calculations
            logger.info("Adding estimated calculations...")
            scheme_type = self.scheme_config.get('calculation_mode', 'volume')
            self.calculation_results = calculate_estimated_columns(
                self.calculation_results,
                self.structured_data,
                scheme_type,
                self.scheme_config
            )
            
            # Rewards calculation
            logger.info("Calculating rewards...")
            self.calculation_results = calculate_rewards(
                self.calculation_results,
                self.structured_data,
                self.scheme_config
            )
            
            # Percentage conversions
            logger.info("Converting decimal fields to percentage format...")
            self.calculation_results = convert_decimal_fields_to_percentage(
                self.calculation_results
            )
            
            # Global mandatory product fix
            logger.info("Applying global mandatory product fix...")
            self.calculation_results = apply_global_mandatory_product_fix(self.calculation_results)
            
            # Final column reordering
            logger.info("Applying final column reordering...")
            ordered_columns = get_comprehensive_column_order(self.calculation_results, raw_json)
            self.calculation_results = self.calculation_results[ordered_columns]
            
            logger.info("All enhanced calculations completed successfully")
            
        except Exception as e:
            logger.error(f"Error in enhanced calculations: {e}")
            raise
    
    def _apply_final_fixes_and_formatting(self):
        """Apply final fixes and formatting"""
        try:
            if self.calculation_results is None or self.calculation_results.empty:
                return
            
            logger.info("Applying final fixes and formatting...")
            
            # Remove unwanted columns
            self.calculation_results = self._remove_unwanted_columns(self.calculation_results)
            
            # Format decimal places
            self.calculation_results = self._format_decimal_places(self.calculation_results)
            
            # Apply column reordering
            raw_json = self.json_fetcher.getStoredJson()
            self.calculation_results = reorder_tracker_columns(
                self.calculation_results, raw_json, self.scheme_config
            )
            
            logger.info("Final fixes and formatting completed")
            
        except Exception as e:
            logger.error(f"Error in final fixes and formatting: {e}")
            # Continue without failing
    
    def _fetch_strata_growth_data(self):
        """Fetch strata growth data (simulated for API)"""
        try:
            if self.calculation_results is None:
                return pd.DataFrame()
            
            # Create sample strata growth data for all accounts (5% default)
            unique_accounts = self.calculation_results['credit_account'].unique()
            sample_data = []
            
            for account in unique_accounts:
                sample_data.append({
                    'credit_account': str(account),
                    'strata_growth_percentage': 5.0
                })
            
            return pd.DataFrame(sample_data)
            
        except Exception as e:
            logger.warning(f"Error fetching strata growth: {e}")
            return pd.DataFrame()
    
    def _remove_unwanted_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove unwanted columns from the final output"""
        try:
            # List of columns to remove (not required for costing)
            columns_to_remove = [
                'actual_volume', 'actual_value', 'percentage_achieved',
                'Payout_Products_Volume', 'Payout_Products_Value',
                'basic_payout', 'additional_payout', 'total_payout', 'Total Payout',
                'Mandatory_product_actual_value', 'Mandatory_product_actual_PPI',
                'Mandatory_Product_PPI_Achievement', 'Mandatory_Product_Actual_Volume',
                'Mandatory_Product_Actual_Value', 'Mandatory_Product_Growth_Target_Achieved',
                'MP_FINAL_ACHIEVEMENT_pct', 'MP_Final_Payout',
                'Scheme Final Payout', 'Rewards'
            ]
            
            # Add additional scheme variants
            for suffix in ['_p1', '_p2', '_p3', '_p4', '_p5']:
                for col in columns_to_remove:
                    if not col.endswith('_p'):
                        new_col = col + suffix
                        columns_to_remove.append(new_col)
            
            # Remove columns that exist
            existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
            if existing_columns_to_remove:
                df = df.drop(columns=existing_columns_to_remove)
                logger.info(f"Removed {len(existing_columns_to_remove)} unwanted columns")
            
            return df
            
        except Exception as e:
            logger.error(f"Error removing unwanted columns: {e}")
            return df
    
    def _format_decimal_places(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format decimal places for numeric columns"""
        try:
            # Fields to exclude from decimal formatting
            exclude_fields = [
                'Scheme ID', 'credit_account', 'Scheme Period From', 'Scheme Period To',
                'Phasing_Period_No_1', 'Phasing_Period_No_2', 'Phasing_Period_No_3', 'Phasing_Period_No_4'
            ]
            
            # Get numeric columns to format
            numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            columns_to_format = [col for col in numeric_columns if col not in exclude_fields]
            
            for column in columns_to_format:
                if column in df.columns:
                    df[column] = df[column].round(2).apply(lambda x: f"{x:.2f}" if pd.notna(x) else x)
            
            logger.info(f"Formatted {len(columns_to_format)} numeric columns")
            return df
            
        except Exception as e:
            logger.error(f"Error formatting decimal places: {e}")
            return df
    
    def _prepare_api_response(self, scheme_id: str, processing_time: float) -> Dict[str, Any]:
        """Prepare data for API response"""
        try:
            if self.calculation_results is None:
                return {}
            
            # Convert DataFrame to JSON-serializable format
            result_data = {
                'scheme_id': scheme_id,
                'total_records': len(self.calculation_results),
                'columns': list(self.calculation_results.columns),
                'sample_data': self.calculation_results.head(10).to_dict('records'),
                'processing_time_seconds': processing_time,
                'calculation_status': 'completed'
            }
            
            return result_data
            
        except Exception as e:
            logger.error(f"Error preparing API response: {e}")
            return {}
    
    def _get_calculation_summary(self) -> Dict[str, Any]:
        """Get summary of calculation results"""
        try:
            if self.calculation_results is None or self.calculation_results.empty:
                return {"message": "No calculation results available"}
            
            df = self.calculation_results
            
            # Dynamic column checking
            base1_volume = df['Base 1 Volume Final'].sum() if 'Base 1 Volume Final' in df.columns else 0
            base1_value = df['Base 1 Value Final'].sum() if 'Base 1 Value Final' in df.columns else 0
            base2_volume = df['Base 2 Volume Final'].sum() if 'Base 2 Volume Final' in df.columns else 0
            base2_value = df['Base 2 Value Final'].sum() if 'Base 2 Value Final' in df.columns else 0
            scheme_volume = df['total_volume'].sum() if 'total_volume' in df.columns else 0
            scheme_value = df['total_value'].sum() if 'total_value' in df.columns else 0
            
            return {
                'total_accounts': len(df),
                'base1_volume_total': base1_volume,
                'base1_value_total': base1_value,
                'base2_volume_total': base2_volume,
                'base2_value_total': base2_value,
                'scheme_volume_total': scheme_volume,
                'scheme_value_total': scheme_value,
            }
            
        except Exception as e:
            logger.error(f"Error getting calculation summary: {e}")
            return {"error": str(e)}
    
    def _get_data_summary(self) -> Dict[str, Any]:
        """Get summary of data sources"""
        try:
            summary = {
                'sales_data_records': len(self.sales_data) if self.sales_data else 0,
                'materials_data_records': len(self.materials_data) if self.materials_data else 0,
                'structured_data_available': self.structured_data is not None,
                'scheme_config_available': self.scheme_config is not None
            }
            
            if self.structured_data:
                summary.update({
                    'products_count': len(self.structured_data.get('products', [])),
                    'slabs_count': len(self.structured_data.get('slabs', [])),
                    'phasing_count': len(self.structured_data.get('phasing', [])),
                    'bonus_count': len(self.structured_data.get('bonus', [])),
                    'rewards_count': len(self.structured_data.get('rewards', []))
                })
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {"error": str(e)}
    
    def get_stored_data(self) -> Dict[str, Any]:
        """Get all stored data for debugging/analysis"""
        return {
            'scheme_config': self.scheme_config,
            'scheme_applicable': self.applicable_data,
            'combined_sales_data': self.sales_data,
            'material_master_data': self.materials_data,
            'structured_data': self.structured_data,
            'data_extractor': self.data_extractor,
            'calculation_results': self.calculation_results
        }
    
    def get_calculation_results(self) -> Optional[pd.DataFrame]:
        """Get calculation results DataFrame"""
        return self.calculation_results
    
    def get_products_data(self, scheme_type: Optional[str] = None, scheme_id: Optional[str] = None, product_type: Optional[str] = None):
        """Get products data with optional filtering"""
        if not self.data_extractor:
            logger.warning("No structured data available. Process scheme first.")
            return None
            
        products_df = self.data_extractor.products_df
        
        if products_df is None or products_df.empty:
            logger.warning("No products data available")
            return None
            
        # Apply filters
        filtered_df = products_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
            
        if product_type:
            if product_type == 'mandatory':
                filtered_df = filtered_df[filtered_df['is_mandatory_product'] == True]
            elif product_type == 'payout':
                filtered_df = filtered_df[filtered_df['is_payout_product'] == True]
            elif product_type == 'product_data':
                filtered_df = filtered_df[filtered_df['is_product_data'] == True]
        
        return filtered_df
    
    def get_slabs_data(self, scheme_type: Optional[str] = None, scheme_id: Optional[str] = None):
        """Get slabs data with optional filtering"""
        if not self.data_extractor:
            logger.warning("No structured data available. Process scheme first.")
            return None
            
        slabs_df = self.data_extractor.slabs_df
        
        if slabs_df is None or slabs_df.empty:
            logger.warning("No slabs data available")
            return None
            
        # Apply filters
        filtered_df = slabs_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
        
        return filtered_df
    
    def get_phasing_data(self, scheme_type: Optional[str] = None, scheme_id: Optional[str] = None, bonus_only: bool = False):
        """Get phasing data with optional filtering"""
        if not self.data_extractor:
            logger.warning("No structured data available. Process scheme first.")
            return None
            
        phasing_df = self.data_extractor.phasing_df
        
        if phasing_df is None or phasing_df.empty:
            logger.warning("No phasing data available")
            return None
            
        # Apply filters
        filtered_df = phasing_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
            
        if bonus_only:
            filtered_df = filtered_df[filtered_df['is_bonus'] == True]
        
        return filtered_df
    
    def get_bonus_data(self, scheme_type: Optional[str] = None, scheme_id: Optional[str] = None):
        """Get bonus schemes data with optional filtering"""
        if not self.data_extractor:
            logger.warning("No structured data available. Process scheme first.")
            return None
            
        bonus_df = self.data_extractor.bonus_df
        
        if bonus_df is None or bonus_df.empty:
            logger.warning("No bonus data available")
            return None
            
        # Apply filters
        filtered_df = bonus_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
        
        return filtered_df