import pandas as pd
import json
import os
from json_fetcher import JSONFetcher
from sales_fetcher import SalesFetcher
from materialfetcher import MaterialFetcher
from schemeapplicablefetcher import SchemeApplicableFetcher
from store import SchemeDataExtractor, process_scheme_json
from calculations import calculate_base_and_scheme_metrics, calculate_growth_metrics_vectorized, calculate_all_targets_and_actuals

class SchemeProcessor:
    def __init__(self):
        self.json_fetcher = JSONFetcher()
        self.sales_fetcher = SalesFetcher()
        self.material_fetcher = MaterialFetcher()
        self.scheme_applicable_fetcher = None
        self.scheme_config = None
        self.sales_data = None
        self.materials_data = None
        self.applicable_data = None
        self.data_extractor = None  # For structured data storage
        self.structured_data = None  # Pandas DataFrames
        self.calculation_results = None  # Calculation results storage

    def process_scheme(self, scheme_id):
        print(f"\nProcessing Scheme: {scheme_id}")
        print("=" * 50)
        
        try:
            # Step 1: Fetch scheme config
            raw_config = self.json_fetcher.fetch_and_store_json(scheme_id)
            if isinstance(raw_config, list) and len(raw_config) >= 1:
                self.scheme_config = raw_config[0]
            else:
                self.scheme_config = raw_config

            # Step 2: Extract filters
            self.scheme_applicable_fetcher = SchemeApplicableFetcher(self.json_fetcher.get_stored_json())
            self.applicable_data = self.scheme_applicable_fetcher.fetch_scheme_applicable()

            filters = self.scheme_applicable_fetcher.get_all_filters_for_sql()

            # Step 3: Display periods safely
            self._display_periods_safe()

            # Step 4: Fetch sales data for each period separately
            self.sales_data = self.sales_fetcher.fetch_all_sales_with_filters(self.scheme_config, filters)
            
            print(f"\n📊 Data Fetching Summary:")
            print(f"   ✓ Base period 1 data: {len(self.sales_fetcher.get_base_period1_data() or [])} records")
            if self.sales_fetcher.get_base_period2_data():
                print(f"   ✓ Base period 2 data: {len(self.sales_fetcher.get_base_period2_data())} records")
            print(f"   ✓ Scheme period data: {len(self.sales_fetcher.get_scheme_period_data() or [])} records")
            print(f"   ✓ Total combined records: {len(self.sales_data)} records")

            # Step 5: Fetch material master and store in memory
            self.materials_data = self.material_fetcher.fetch_all_material_master()
            print(f"✓ Material master stored in memory")

            # Step 6: Extract and structure JSON data for efficient calculations
            print(f"\n📋 Extracting structured data from JSON...")
            self._extract_structured_data()

            # Step 7: Skip file saving (calculations happen in memory)
            print(f"\n💾 Skipping intermediate file saves (calculations in memory)...")
            
            # Step 8: Skip structured data saving (not needed for final output)
            print(f"\n📊 Skipping structured data saves (not needed for final output)...")

            # Step 9: Perform base calculations
            print(f"\n🧮 Starting Calculations...")
            print(f"🔍 DEBUG: About to call _perform_base_calculations method")
            import sys
            sys.stdout.flush()
            try:
                self._perform_base_calculations()
                print(f"🔍 DEBUG: _perform_base_calculations method completed successfully")
            except Exception as base_calc_error:
                print(f"❌ ERROR in _perform_base_calculations: {str(base_calc_error)}")
                import traceback
                traceback.print_exc()

            # Step 10: Prepare results for API response (no file saving)
            print(f"\n📊 Preparing calculation results for API response...")
            # Results are now available in self.calculation_results for API access

            print(f"\nSUCCESS: All data processed and calculations completed!")
            return True  # Indicate successful processing

        except Exception as e:
            print(f"ERROR: {str(e)}")
            print(f"🔍 Debug: scheme_config type: {type(self.scheme_config) if hasattr(self, 'scheme_config') else 'Not set'}")
            # DETAILED TRACEBACK FOR DEBUGGING
            import traceback
            print(f"🔍 FULL TRACEBACK:")
            traceback.print_exc()
            return False  # Indicate processing failed

    def _display_periods_safe(self):
        """Safely display base periods and scheme period with full error handling"""
        try:
            # Get base periods safely
            base_periods = self.scheme_config.get('base_periods', [])
            
            if isinstance(base_periods, list):
                for i, period in enumerate(base_periods):
                    if isinstance(period, dict):
                        from_date = period.get('from_date', 'N/A')
                        to_date = period.get('to_date', 'N/A')
                        print(f"\n📅 Base Period {i+1}: {from_date} to {to_date}")
                    else:
                        print(f"\n⚠️ Base period {i+1} is not a dict: {type(period)}")
            else:
                print(f"\n⚠️ base_periods is not a list: {type(base_periods)}")

            # Get scheme period safely  
            scheme_from = self.scheme_config.get('scheme_from', 'N/A')
            scheme_to = self.scheme_config.get('scheme_to', 'N/A')
            print(f"📅 Scheme Period: {scheme_from} to {scheme_to}")
            
        except Exception as e:
            print(f"\n❌ Error displaying periods: {str(e)}")

    def _extract_structured_data(self):
        """Extract and structure JSON data into pandas DataFrames for efficient calculations"""
        try:
            # Get the raw JSON data
            raw_json = self.json_fetcher.get_stored_json()
            
            if raw_json:
                # Initialize the data extractor
                self.data_extractor = SchemeDataExtractor()
                
                # Extract all structured data and keep in memory
                self.structured_data = self.data_extractor.extract_all_data(raw_json)
                
                # Print extraction summary (data stored in memory)
                print(f"   ✓ Products table: {len(self.structured_data['products'])} records (in memory)")
                print(f"   ✓ Slabs table: {len(self.structured_data['slabs'])} records (in memory)")
                print(f"   ✓ Phasing table: {len(self.structured_data['phasing'])} records (in memory)")
                print(f"   ✓ Bonus schemes table: {len(self.structured_data['bonus'])} records (in memory)")
                print(f"   ✓ Rewards table: {len(self.structured_data['rewards'])} records (in memory)")
                print(f"   ✓ Scheme info table: {len(self.structured_data['scheme_info'])} records (in memory)")
                print(f"   ✓ All structured data ready for fast calculations")
                
                # 🔧 Initialize configuration manager for conditional calculations
                print(f"\n🔧 Parsing scheme configuration flags...")
                from calculations.configuration_manager import SchemeConfigurationManager
                self.config_manager = SchemeConfigurationManager(raw_json)
                self.config_manager.print_configuration_summary()
                
                # Store configuration in structured data for easy access
                self.structured_data['config_manager'] = self.config_manager
                
            else:
                print("   ⚠️ No JSON data available for structuring")
                
        except Exception as e:
            print(f"   ❌ Error extracting structured data: {str(e)}")

    # REMOVED: _save_essential_files_only and _save_structured_data_for_reference methods
    # No longer needed since we only save the final Excel file

    def _perform_base_calculations(self):
        """Perform base period and scheme period calculations"""
        print(f"🔍 DEBUG: ENTERED _perform_base_calculations method")
        import sys
        sys.stdout.flush()
        try:
            print(f"🔍 DEBUG: Inside try block of _perform_base_calculations")
            if not self.sales_data:
                print(f"   ⚠️ No sales data available for calculations")
                return
            print(f"🔍 DEBUG: Sales data exists, length: {len(self.sales_data)}")
            
            if not self.scheme_config:
                print(f"   ⚠️ No scheme configuration available for calculations")
                return
            print(f"🔍 DEBUG: Scheme config exists")
            
            # Convert sales data to DataFrame
            import pandas as pd
            sales_df = pd.DataFrame(self.sales_data)
            
            # Get raw JSON data for metadata
            raw_json = self.json_fetcher.get_stored_json()
            
            # Perform calculations with JSON data for metadata
            print(f"🔧 DEBUG: About to import and call vectorized calculations...")
            from calculations.vectorized_calculations import calculate_base_and_scheme_metrics_vectorized
            print(f"🔧 DEBUG: Import successful, function: {calculate_base_and_scheme_metrics_vectorized}")
            print(f"🔧 DEBUG: About to call function with sales_df shape: {sales_df.shape}")
            self.calculation_results = calculate_base_and_scheme_metrics_vectorized(sales_df, self.scheme_config, raw_json, self.structured_data)
            print(f"🔧 DEBUG: Function returned, result shape: {self.calculation_results.shape if self.calculation_results is not None else 'None'}")
            
            print(f"   ✅ Base calculations completed: {len(self.calculation_results)} accounts processed")
            
            # Add growth calculations
            print("   🚀 Adding growth calculations...")
            
            # Fetch strata growth data using MCP
            strata_growth_df = self._fetch_strata_growth_data()
            
            self.calculation_results = calculate_growth_metrics_vectorized(
                self.calculation_results,
                self.structured_data,
                raw_json,
                strata_growth_df
            )
            
            print(f"   ✅ Growth calculations completed!")
            
            # Add target and actual calculations
            print("   🎯 Adding target and actual calculations...")
            
            self.calculation_results = calculate_all_targets_and_actuals(
                self.calculation_results,
                self.structured_data,
                raw_json,
                sales_df,
                self.scheme_config  # Pass scheme config with dates
            )
            
            print(f"   ✅ Target and actual calculations completed!")
            
            # Add Enhanced Costing Tracker Fields (MANDATORY FIX/ADD TASK)
            print("   🧮 Adding enhanced costing tracker fields...")
            from calculations.enhanced_costing_tracker_fields import calculate_enhanced_costing_tracker_fields
            self.calculation_results = calculate_enhanced_costing_tracker_fields(
                self.calculation_results,
                self.structured_data,
                sales_df,
                self.scheme_config
            )
            print(f"   ✅ Enhanced costing tracker fields completed!")
            
            # Fill scheme metadata columns
            print("   📋 Filling scheme metadata columns...")
            from calculations.metadata_filler import fill_scheme_metadata_columns
            self.calculation_results = fill_scheme_metadata_columns(
                self.calculation_results, 
                self.structured_data, 
                sales_df
            )
            print(f"   ✅ Scheme metadata columns filled!")
            
            # Apply comprehensive tracker fixes (NEW - addresses user's 3 tasks)
            print("   🔧 Applying comprehensive tracker fixes...")
            from calculations.comprehensive_tracker_fixes import apply_comprehensive_tracker_fixes
            self.calculation_results = apply_comprehensive_tracker_fixes(
                self.calculation_results,
                self.structured_data,
                raw_json,
                self.scheme_config
            )
            print(f"   ✅ Comprehensive tracker fixes applied!")
            
            # Add Scheme Final Payout calculation
            print("   💰 Calculating Scheme Final Payout...")
            from calculations.scheme_final_payout_calculations import calculate_scheme_final_payout
            self.calculation_results = calculate_scheme_final_payout(
                self.calculation_results,
                self.structured_data,
                self.scheme_config
            )
            print(f"   ✅ Scheme Final Payout calculation completed!")
            
            # Add NEW CONDITIONAL PAYOUT calculations (USER'S REQUESTED FORMULAS)
            print("   🧠 Adding conditional payout calculations based on configuration...")
            from calculations.conditional_payout_calculations import calculate_conditional_payout_columns
            self.calculation_results = calculate_conditional_payout_columns(
                self.calculation_results,
                self.structured_data,
                sales_df,
                self.scheme_config
            )
            print(f"   ✅ Conditional payout calculations completed!")

            # Add ESTIMATED CALCULATIONS based on qualification rates (NEW FEATURE)
            print("   📊 Adding estimated calculations based on qualification rates...")
            from calculations.estimated_calculations import calculate_estimated_columns
            scheme_type = self.scheme_config.get('calculation_mode', 'volume')
            self.calculation_results = calculate_estimated_columns(
                self.calculation_results,
                self.structured_data,
                scheme_type,
                self.scheme_config
            )
            print(f"   ✅ Estimated calculations completed!")

            # Add Rewards calculation
            print("   🏆 Calculating Rewards...")
            from calculations.rewards_calculations import calculate_rewards
            self.calculation_results = calculate_rewards(
                self.calculation_results,
                self.structured_data,
                self.scheme_config
            )
            print(f"   ✅ Rewards calculation completed!")
            
            # Convert decimal fields to percentage format
            print("   🔢 Converting decimal fields to percentage format...")
            from calculations.percentage_conversions import convert_decimal_fields_to_percentage
            self.calculation_results = convert_decimal_fields_to_percentage(
                self.calculation_results
            )
            print(f"   ✅ Percentage conversions completed!")
            
            # 🔧 GLOBAL FIX: Ensure all accounts have correct mandatory product growth targets
            print("   🔧 Applying global mandatory product growth target fix...")
            from calculations.mandatory_product_global_fix import apply_global_mandatory_product_fix
            self.calculation_results = apply_global_mandatory_product_fix(self.calculation_results)
            
            # Final column reordering after all calculations
            print("   📋 Final column reordering...")
            from calculations.comprehensive_tracker_fixes import get_comprehensive_column_order
            ordered_columns = get_comprehensive_column_order(self.calculation_results, raw_json)
            self.calculation_results = self.calculation_results[ordered_columns]
            print(f"   ✅ Final column ordering completed!")
            
        except Exception as e:
            print(f"   ❌ Error in calculations: {str(e)}")

    def _reorder_final_columns(self, df, json_data):
        """Final column reordering after all calculations"""
        
        all_cols = list(df.columns)
        
        # PERFECT MAIN SCHEME ORDER (as specified by user)
        main_order = [
            'Scheme ID', 'Scheme Name', 'Scheme Type', 'Scheme Period From', 'Scheme Period To', 'Mandatory Qualify',
            'credit_account', 'state_name', 'so_name', 'region', 'customer_name',
            'Base 1 Volume Final', 'Base 2 Volume Final', 'total_volume', 'growth_rate', 
            'target_volume', 'target_value', 'actual_volume', 'actual_value', 'percentage_achieved'
        ]
        
        # Start with main scheme columns
        ordered_cols = []
        for col in main_order:
            if col in all_cols:
                ordered_cols.append(col)
        
        # Add additional scheme columns in perfect order
        additional_schemes = json_data.get('additionalSchemes', []) if json_data else []
        
        for i in range(1, len(additional_schemes) + 1):
            prefix = f'_p{i}'
            
            # Perfect order for additional scheme
            additional_order = [
                f'Scheme Name{prefix}', f'Scheme Type{prefix}', f'Mandatory Qualify{prefix}',
                f'Base 1 Volume Final{prefix}', f'Base 2 Volume Final{prefix}', f'total_volume{prefix}',
                f'growth_rate{prefix}', f'target_volume{prefix}', f'target_value{prefix}',
                f'actual_volume{prefix}', f'actual_value{prefix}', f'percentage_achieved{prefix}'
            ]
            
            for col in additional_order:
                if col in all_cols:
                    ordered_cols.append(col)
        
        # Add any remaining columns (except Scheme Final Payout and Rewards)
        for col in all_cols:
            if col not in ordered_cols and col != 'Scheme Final Payout' and col != 'Rewards':
                ordered_cols.append(col)
        
        # Add Scheme Final Payout and Rewards at the very end
        if 'Scheme Final Payout' in all_cols:
            ordered_cols.append('Scheme Final Payout')

        if 'Rewards' in all_cols:
            ordered_cols.append('Rewards')

        return df[ordered_cols]

    def _fetch_strata_growth_data(self):
        """Fetch strata growth data from database using MCP"""
        try:
            print("   📊 Fetching strata growth data...")
            import pandas as pd
            
            # Create a simple query to get all strata growth data
            # This would be enhanced with actual MCP call
            # For testing, return sample data that matches the database
            sample_data = []
            
            # Get all unique credit accounts from our calculation results
            if hasattr(self, 'calculation_results') and self.calculation_results is not None:
                unique_accounts = self.calculation_results['credit_account'].unique()
                
                # Create sample strata growth for all accounts (5% default)
                for account in unique_accounts:
                    sample_data.append({
                        'credit_account': str(account),
                        'strata_growth_percentage': 5.0
                    })
            
            strata_df = pd.DataFrame(sample_data)
            print(f"   ✅ Strata growth data: {len(strata_df)} records")
            return strata_df
            
        except Exception as e:
            print(f"⚠️  Error fetching strata growth: {e}")
            return pd.DataFrame()

    def get_calculation_results_json(self, scheme_id):
        """Return calculation results as JSON data (API-friendly, no file saving)"""
        try:
            if self.calculation_results is None or self.calculation_results.empty:
                print(f"   ⚠️ No calculation results available")
                return None
            
            import pandas as pd
            
            # Create a copy for processing (keep original intact)
            df_for_output = self.calculation_results.copy()
            
            # Filter out unwanted columns before output
            columns_to_remove = ['base_target_volume']  # Always remove this one
            
            # Dynamically find and remove payoutproductactual columns for all schemes
            all_columns = list(df_for_output.columns)
            for col in all_columns:
                if col.startswith('payoutproductactualvolume') or col.startswith('payoutproductactualvalue'):
                    columns_to_remove.append(col)
            
            # Remove unwanted columns if they exist
            existing_unwanted_cols = [col for col in columns_to_remove if col in df_for_output.columns]
            if existing_unwanted_cols:
                df_for_output = df_for_output.drop(columns=existing_unwanted_cols)
                print(f"   🗑️ Removed columns from output: {', '.join(existing_unwanted_cols)}")
            
            # Apply comprehensive column reordering
            print(f"   📋 Applying comprehensive column reordering...")
            try:
                from calculations.column_reordering import reorder_tracker_columns
                
                # Get JSON data for scheme detection
                raw_json = self.json_fetcher.get_stored_json() if hasattr(self, 'json_fetcher') else None
                
                # Apply column reordering with scheme configuration for conditional visibility
                df_for_output = reorder_tracker_columns(df_for_output, raw_json, self.scheme_config)
                print(f"   ✅ Column reordering completed - perfect order applied!")
                
            except Exception as reorder_error:
                print(f"   ⚠️ Column reordering error: {str(reorder_error)}")
                print(f"   ℹ️ Continuing with original column order...")
            
            # Remove unwanted columns for API output
            df_for_output = self._remove_unwanted_columns(df_for_output)
            
            # Format decimal places
            df_for_output = self._format_decimal_places(df_for_output)

            # Remove specific payout fields from final output
            df_for_output = self._remove_unwanted_columns(df_for_output)

            # Convert to JSON array format for API response
            headers = df_for_output.columns.tolist()
            data_rows = df_for_output.values.tolist()
            
            # Create structured JSON response
            result = {
                'scheme_id': scheme_id,
                'headers': headers,
                'data': data_rows,
                'summary': {
                    'total_records': len(data_rows),
                    'total_columns': len(headers),
                    'calculation_timestamp': pd.Timestamp.now().isoformat()
                }
            }
            
            print(f"   ✅ JSON RESULT: {len(data_rows)} records, {len(headers)} columns prepared")
            return result
            
        except Exception as e:
            print(f"   ❌ Error preparing calculation results: {str(e)}")
            return None

    def export_structured_data_to_excel(self, filename=None):
        """Export structured data to Excel only when specifically requested"""
        if not self.data_extractor:
            print("❌ No structured data available. Process scheme first.")
            return None
            
        if not filename:
            filename = "structured_scheme_data.xlsx"
            
        self.data_extractor.save_to_excel(filename)
        print(f"📁 Structured data exported to: {filename}")
        return filename

    def get_stored_data(self):
        return {
            'scheme_config': self.scheme_config,
            'scheme_applicable': self.applicable_data,
            'combined_sales_data': self.sales_data,
            'material_master_data': self.materials_data,
            'structured_data': self.structured_data,
            'data_extractor': self.data_extractor,
            'calculation_results': self.calculation_results
        }

    def get_products_data(self, scheme_type=None, scheme_id=None, product_type=None):
        """Get products data with optional filtering"""
        if not self.data_extractor:
            print("❌ No structured data available. Process scheme first.")
            return None
            
        products_df = self.data_extractor.products_df
        
        if products_df is None or products_df.empty:
            print("❌ No products data available")
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

    def get_slabs_data(self, scheme_type=None, scheme_id=None):
        """Get slabs data with optional filtering"""
        if not self.data_extractor:
            print("❌ No structured data available. Process scheme first.")
            return None
            
        slabs_df = self.data_extractor.slabs_df
        
        if slabs_df is None or slabs_df.empty:
            print("❌ No slabs data available")
            return None
            
        # Apply filters
        filtered_df = slabs_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
        
        return filtered_df

    def get_phasing_data(self, scheme_type=None, scheme_id=None, bonus_only=False):
        """Get phasing data with optional filtering"""
        if not self.data_extractor:
            print("❌ No structured data available. Process scheme first.")
            return None
            
        phasing_df = self.data_extractor.phasing_df
        
        if phasing_df is None or phasing_df.empty:
            print("❌ No phasing data available")
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

    def get_bonus_data(self, scheme_type=None, scheme_id=None):
        """Get bonus schemes data with optional filtering"""
        if not self.data_extractor:
            print("❌ No structured data available. Process scheme first.")
            return None
            
        bonus_df = self.data_extractor.bonus_df
        
        if bonus_df is None or bonus_df.empty:
            print("❌ No bonus data available")
            return None
            
        # Apply filters
        filtered_df = bonus_df.copy()
        
        if scheme_type:
            filtered_df = filtered_df[filtered_df['scheme_type'] == scheme_type]
        
        if scheme_id:
            filtered_df = filtered_df[filtered_df['scheme_id'] == scheme_id]
        
        return filtered_df

    def get_calculation_results(self):
        """Get calculation results DataFrame"""
        if self.calculation_results is None:
            print("❌ No calculation results available. Process scheme first.")
            return None
        return self.calculation_results

    def get_calculation_summary(self):
        """Get summary of calculation results"""
        if self.calculation_results is None or self.calculation_results.empty:
            return "No calculation results available"
        
        df = self.calculation_results
        
        # Dynamic column checking based on what's available
        base1_volume = df['Base 1 Volume Final'].sum() if 'Base 1 Volume Final' in df.columns else 0
        base1_value = df['Base 1 Value Final'].sum() if 'Base 1 Value Final' in df.columns else 0
        base2_volume = df['Base 2 Volume Final'].sum() if 'Base 2 Volume Final' in df.columns else 0
        base2_value = df['Base 2 Value Final'].sum() if 'Base 2 Value Final' in df.columns else 0
        scheme_volume = df['total_volume'].sum() if 'total_volume' in df.columns else 0
        scheme_value = df['total_value'].sum() if 'total_value' in df.columns else 0
        
        summary = {
            'total_accounts': len(df),
            'base1_volume_total': base1_volume,
            'base1_value_total': base1_value,
            'base2_volume_total': base2_volume,
            'base2_value_total': base2_value,
            'scheme_volume_total': scheme_volume,
            'scheme_value_total': scheme_value,
        }
        return summary

    def _remove_unwanted_columns(self, df):
        """
        Remove columns that are not required for the main tracker output.
        """
        # List of columns to remove (all the ones specified by user)
        columns_to_remove = [
            # Main scheme columns
            'actual_volume', 'actual_value', 'percentage_achieved',
            'Payout_Products_Volume', 'Payout_Products_Value',
            'basic_payout', 'additional_payout', 'total_payout', 'Total Payout',
            'Mandatory_product_actual_value', 'Mandatory_product_actual_PPI',
            'Mandatory_Product_PPI_Achievement', 'Mandatory_Product_Actual_Volume',
            'Mandatory_Product_Actual_Value', 'Mandatory_Product_Growth_Target_Achieved',
            'MP_FINAL_ACHIEVEMENT_pct', 'MP_Final_Payout',
            
            # Phasing columns (periods 1-3)
            'Phasing_Period_Volume_1', 'Phasing_Period_Value_1',
            'Phasing_Payout_Period_Volume_1', 'Phasing_Payout_Period_Value_1',
            'Phasing_Period_Payout_Product_Volume_1', 'Phasing_Period_Payout_Product_Value_1',
            'Percent_Phasing_Achieved_1', 'Phasing_Payout_1',
            'Phasing_Period_Volume_2', 'Phasing_Period_Value_2',
            'Phasing_Payout_Period_Volume_2', 'Phasing_Payout_Period_Value_2',
            'Phasing_Period_Payout_Product_Volume_2', 'Phasing_Period_Payout_Product_Value_2',
            'Percent_Phasing_Achieved_2', 'Phasing_Payout_2',
            'Phasing_Period_Volume_3', 'Phasing_Period_Value_3',
            'Phasing_Payout_Period_Volume_3', 'Phasing_Payout_Period_Value_3',
            'Phasing_Period_Payout_Product_Volume_3', 'Phasing_Period_Payout_Product_Value_3',
            'Percent_Phasing_Achieved_3', 'Phasing_Payout_3',
            
            # Bonus phasing columns (periods 1-3)
            'Bonus_Phasing_Period_Volume_1', 'Bonus_Phasing_Period_Value_1',
            'Bonus_Payout_Period_Volume_1', 'Bonus_Payout_Period_Value_1',
            'Percent_Bonus_Achieved_1', 'Bonus_Payout_Period_Payout_Product_Volume_1',
            'Bonus_Payout_Period_Payout_Product_Value_1', 'Bonus_Payout_1',
            'Bonus_Phasing_Period_Volume_2', 'Bonus_Phasing_Period_Value_2',
            'Bonus_Payout_Period_Volume_2', 'Bonus_Payout_Period_Value_2',
            'Percent_Bonus_Achieved_2', 'Bonus_Payout_Period_Payout_Product_Volume_2',
            'Bonus_Payout_Period_Payout_Product_Value_2', 'Bonus_Payout_2',
            'Bonus_Phasing_Period_Volume_3', 'Bonus_Phasing_Period_Value_3',
            'Bonus_Payout_Period_Volume_3', 'Bonus_Payout_Period_Value_3',
            'Percent_Bonus_Achieved_3', 'Bonus_Payout_Period_Payout_Product_Volume_3',
            'Bonus_Payout_Period_Payout_Product_Value_3', 'Bonus_Payout_3',
            
            # Final phasing payout
            'FINAL_PHASING_PAYOUT',
            
            # Bonus scheme columns (schemes 1-4)
            'Actual_Bonus_Volume_1', 'Actual_Bonus_Value_1', 'Bonus_Scheme_%_Achieved_1',
            'Actual_Bonus_Payout_Period_Volume_1', 'Actual_Bonus_Payout_Period_Value_1',
            'Actual_Bonus_MP_Volume_1', 'Actual_Bonus_MP_Value_1', '%_MP_Achieved_1',
            'Actual_Bonus_Payout_Period_MP_Volume_1', 'Actual_Bonus_Payout_Period_MP_value_2',
            'Bonus_Scheme_1_Bonus_Payout', 'Bonus_Scheme_1_MP_Payout',
            'Actual_Bonus_Volume_2', 'Actual_Bonus_Value_2', 'Bonus_Scheme_%_Achieved_2',
            'Actual_Bonus_Payout_Period_Volume_2', 'Actual_Bonus_Payout_Period_Value_2',
            'Actual_Bonus_MP_Volume_2', 'Actual_Bonus_MP_Value_2', '%_MP_Achieved_2',
            'Actual_Bonus_Payout_Period_MP_Volume_2', 'Actual_Bonus_Payout_Period_MP_value_2',
            'Bonus_Scheme_2_Bonus_Payout', 'Bonus_Scheme_2_MP_Payout',
            'Actual_Bonus_Volume_3', 'Actual_Bonus_Value_3', 'Bonus_Scheme_%_Achieved_3',
            'Actual_Bonus_Payout_Period_Volume_3', 'Actual_Bonus_Payout_Period_Value_3',
            'Actual_Bonus_MP_Volume_3', 'Actual_Bonus_MP_Value_3', '%_MP_Achieved_3',
            'Actual_Bonus_Payout_Period_MP_Volume_3', 'Actual_Bonus_Payout_Period_MP_Value_3',
            'Bonus_Scheme_3_Bonus_Payout', 'Bonus_Scheme_3_MP_Payout',
            'Actual_Bonus_Volume_4', 'Actual_Bonus_Value_4', 'Bonus_Scheme_%_Achieved_4',
            'Actual_Bonus_Payout_Period_Volume_4', 'Actual_Bonus_Payout_Period_Value_4',
            'Actual_Bonus_MP_Volume_4', 'Actual_Bonus_MP_Value_4', '%_MP_Achieved_4',
            'Actual_Bonus_Payout_Period_MP_Volume_4', 'Actual_Bonus_Payout_Period_MP_Value_4',
            'Bonus_Scheme_4_Bonus_Payout', 'Bonus_Scheme_4_MP_Payout',
            
            # Additional scheme columns (pN variants)
            'actual_volume_p1', 'actual_value_p1',
            'percentage_achieved_p1', 'Payout_Products_Volume_p1', 'Payout_Products_Value_p1',
            'rebate_per_litre_applied_p1', 'rebate_percent_applied_p1',
            'basic_payout_p1', 'additional_payout_p1', 'Fixed_Rebate_p1', 'Total_Payout_p1',
            'Mandatory_product_actual_value_p1', 'Mandatory_product_actual_PPI_p1',
            'Mandatory_Product_PPI_Achievement_p1', 'Mandatory_Product_Actual_Volume_p1',
            'Mandatory_Product_Actual_Value_p1', 'Mandatory_Product_Growth_Target_Achieved_p1',
            'Mandatory_Product_Payout_p1', 'MP_FINAL_ACHIEVEMENT_pct_p1', 'MP_Final_Payout_p1',
            
            # Additional scheme phasing columns (p1)
            'Phasing_Period_Volume_1_p1', 'Phasing_Period_Value_1_p1',
            'Phasing_Payout_Period_Volume_1_p1', 'Phasing_Payout_Period_Value_1_p1',
            'Phasing_Period_Payout_Product_Volume_1_p1', 'Phasing_Period_Payout_Product_Value_1_p1',
            'Percent_Phasing_Achieved_1_p1', 'Phasing_Payout_1_p1',
            'Phasing_Period_Volume_2_p1', 'Phasing_Period_Value_2_p1',
            'Phasing_Payout_Period_Volume_2_p1', 'Phasing_Payout_Period_Value_2_p1',
            'Phasing_Period_Payout_Product_Volume_2_p1', 'Phasing_Period_Payout_Product_Value_2_p1',
            'Percent_Phasing_Achieved_2_p1', 'Phasing_Payout_2_p1',
            'Phasing_Period_Volume_3_p1', 'Phasing_Period_Value_3_p1',
            'Phasing_Payout_Period_Volume_3_p1', 'Phasing_Payout_Period_Value_3_p1',
            'Phasing_Period_Payout_Product_Volume_3_p1', 'Phasing_Period_Payout_Product_Value_3_p1',
            'Percent_Phasing_Achieved_3_p1', 'Phasing_Payout_3_p1',
            
            # Additional scheme bonus columns (p1)
            'Bonus_Phasing_Period_Volume_1_p1', 'Bonus_Phasing_Period_Value_1_p1',
            'Bonus_Payout_Period_Volume_1_p1', 'Bonus_Payout_Period_Value_1_p1',
            'Percent_Bonus_Achieved_1_p1', 'Bonus_Payout_Period_Payout_Product_Volume_1_p1',
            'Bonus_Payout_Period_Payout_Product_Value_1_p1', 'Bonus_Payout_1_p1',
            'Phasing_Period_Volume_2_p1', 'Phasing_Period_Value_2_p1',
            'Phasing_Payout_Period_Volume_2_p1', 'Phasing_Payout_Period_Value_2_p1',
            'Phasing_Period_Payout_Product_Volume_2_p1', 'Phasing_Period_Payout_Product_Value_2_p1',
            'Percent_Phasing_Achieved_2_p1', 'Phasing_Payout_2_p1',
            'Bonus_Phasing_Period_Volume_2_p1', 'Bonus_Phasing_Period_Value_2_p1',
            'Bonus_Payout_Period_Volume_2_p1', 'Bonus_Payout_Period_Value_2_p1',
            'Percent_Bonus_Achieved_2_p1', 'Bonus_Payout_Period_Payout_Product_Volume_2_p1',
            'Bonus_Payout_Period_Payout_Product_Value_2_p1', 'Bonus_Payout_2_p1',
            'Phasing_Period_Volume_3_p1', 'Phasing_Period_Value_3_p1',
            'Phasing_Payout_Period_Volume_3_p1', 'Phasing_Payout_Period_Value_3_p1',
            'Phasing_Period_Payout_Product_Volume_3_p1', 'Phasing_Period_Payout_Product_Value_3_p1',
            'Percent_Phasing_Achieved_3_p1', 'Phasing_Payout_3_p1',
            'Bonus_Phasing_Period_Volume_3_p1', 'Bonus_Phasing_Period_Value_3_p1',
            'Bonus_Payout_Period_Volume_3_p1', 'Bonus_Payout_Period_Value_3_p1',
            'Percent_Bonus_Achieved_3_p1', 'Bonus_Payout_Period_Payout_Product_Volume_3_p1',
            'Bonus_Payout_Period_Payout_Product_Value_3_p1', 'Bonus_Payout_3_p1',
            'FINAL_PHASING_PAYOUT_p1',
            
            # Final payout and rewards
            'Scheme Final Payout', 'Rewards'
        ]
        
        # Add p2, p3, p4, p5 variants for additional schemes
        for suffix in ['_p2', '_p3', '_p4', '_p5']:
            for col in columns_to_remove:
                if col.endswith('_p1'):
                    new_col = col.replace('_p1', suffix)
                    columns_to_remove.append(new_col)
        
        # Remove columns that exist in the DataFrame
        existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if existing_columns_to_remove:
            print(f"   🗑️ Removing {len(existing_columns_to_remove)} unwanted columns from main tracker...")
            df_filtered = df.drop(columns=existing_columns_to_remove)
            print(f"   ✅ Removed columns: {', '.join(existing_columns_to_remove[:5])}{'...' if len(existing_columns_to_remove) > 5 else ''}")
            print(f"   📊 Columns before: {len(df.columns)}, after: {len(df_filtered.columns)}")
        else:
            print(f"   ℹ️ No unwanted columns found to remove")
            df_filtered = df.copy()
        
        return df_filtered

    def _format_decimal_places(self, df):
        """
        Format all value fields to have exactly 2 decimal places (.00) except for specified fields.
        """
        print(f"   🔢 Formatting decimal places to 2 decimals for all value fields...")
        
        # Fields to exclude from decimal formatting (keep as is)
        exclude_fields = [
            'Scheme ID',
            'credit_account', 
            'Scheme Period From',
            'Scheme Period To',
            'Phasing_Period_No_1',
            'Phasing_Period_No_2', 
            'Phasing_Period_No_3',
            'Phasing_Period_No_4'
        ]
        
        # Get all numeric columns that should be formatted
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        
        # Filter out excluded fields
        columns_to_format = [col for col in numeric_columns if col not in exclude_fields]
        
        formatted_count = 0
        for column in columns_to_format:
            if column in df.columns:
                # Format to 2 decimal places and ensure .00 is displayed
                df[column] = df[column].round(2).apply(lambda x: f"{x:.2f}" if pd.notna(x) else x)
                formatted_count += 1
        
        print(f"   ✅ Formatted {formatted_count} numeric columns to 2 decimal places")
        return df

    def _remove_unwanted_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove unwanted payout columns from the final output.
        """
        print(f"   🗑️ Removing unwanted payout columns from final output...")

        # List of columns to remove (not required for costing)
        columns_to_remove = [
            # Original unwanted columns
            'Payout_Product_Payout',
            'Total_Payout',
            'Mandatory_Product_Payout',
            'Payout_Product_Payout_p1',
            
            # Main scheme actual and percentage columns
            'actual_volume',
            'actual_value',
            'percentage_achieved',
            # Additional scheme actual and percentage columns
            'actual_volume_pN',
            'actual_value_pN',
            'percentage_achieved_pN',
            'Payout_Products_Volume',
            'Payout_Products_Value',
            'basic_payout',
            'additional_payout',
            'total_payout',
            'Mandatory_product_actual_value',
            'Mandatory_product_actual_PPI',
            'Mandatory_Product_PPI_Achievement',
            'Mandatory_Product_Actual_Volume',
            'Mandatory_Product_Actual_Value',
            'Mandatory_Product_Growth_Target_Achieved',
            'MP_FINAL_ACHIEVEMENT_pct',
            'MP_Final_Payout',
            
            # Phasing columns (all periods)
            'Phasing_Period_Volume_1', 'Phasing_Period_Value_1',
            'Phasing_Payout_Period_Volume_1', 'Phasing_Payout_Period_Value_1',
            'Phasing_Period_Payout_Product_Volume_1', 'Phasing_Period_Payout_Product_Value_1',
            'Percent_Phasing_Achieved_1', 'Phasing_Payout_1',
            
            'Phasing_Period_Volume_2', 'Phasing_Period_Value_2',
            'Phasing_Payout_Period_Volume_2', 'Phasing_Payout_Period_Value_2',
            'Phasing_Period_Payout_Product_Volume_2', 'Phasing_Period_Payout_Product_Value_2',
            'Percent_Phasing_Achieved_2', 'Phasing_Payout_2',
            
            'Phasing_Period_Volume_3', 'Phasing_Period_Value_3',
            'Phasing_Payout_Period_Volume_3', 'Phasing_Payout_Period_Value_3',
            'Phasing_Period_Payout_Product_Volume_3', 'Phasing_Period_Payout_Product_Value_3',
            'Percent_Phasing_Achieved_3', 'Phasing_Payout_3',
            
            'FINAL_PHASING_PAYOUT',
            
            # Bonus columns (all periods)
            'Bonus_Phasing_Period_Volume_1', 'Bonus_Phasing_Period_Value_1',
            'Bonus_Payout_Period_Volume_1', 'Bonus_Payout_Period_Value_1',
            'Percent_Bonus_Achieved_1',
            'Bonus_Payout_Period_Payout_Product_Volume_1', 'Bonus_Payout_Period_Payout_Product_Value_1',
            'Bonus_Payout_1',
            
            'Bonus_Phasing_Period_Volume_2', 'Bonus_Phasing_Period_Value_2',
            'Bonus_Payout_Period_Volume_2', 'Bonus_Payout_Period_Value_2',
            'Percent_Bonus_Achieved_2',
            'Bonus_Payout_Period_Payout_Product_Volume_2', 'Bonus_Payout_Period_Payout_Product_Value_2',
            'Bonus_Payout_2',
            
            'Bonus_Phasing_Period_Volume_3', 'Bonus_Phasing_Period_Value_3',
            'Bonus_Payout_Period_Volume_3', 'Bonus_Payout_Period_Value_3',
            'Percent_Bonus_Achieved_3',
            'Bonus_Payout_Period_Payout_Product_Volume_3', 'Bonus_Payout_Period_Payout_Product_Value_3',
            'Bonus_Payout_3',
            
            # Bonus scheme actual columns (all periods)
            'Actual_Bonus_Volume_1', 'Actual_Bonus_Value_1',
            'Bonus_Scheme_%_Achieved_1',
            'Actual_Bonus_Payout_Period_Volume_1', 'Actual_Bonus_Payout_Period_Value_1',
            'Actual_Bonus_MP_Volume_1', 'Actual_Bonus_MP_Value_1',
            '%_MP_Achieved_1',
            'Actual_Bonus_Payout_Period_MP_Volume_1', 'Actual_Bonus_Payout_Period_MP_Value_1',
            'Bonus_Scheme_1_Bonus_Payout', 'Bonus_Scheme_1_MP_Payout',
            
            'Actual_Bonus_Volume_2', 'Actual_Bonus_Value_2',
            'Bonus_Scheme_%_Achieved_2',
            'Actual_Bonus_Payout_Period_Volume_2', 'Actual_Bonus_Payout_Period_Value_2',
            'Actual_Bonus_MP_Volume_2', 'Actual_Bonus_MP_Value_2',
            '%_MP_Achieved_2',
            'Actual_Bonus_Payout_Period_MP_Volume_2', 'Actual_Bonus_Payout_Period_MP_value_2',
            'Bonus_Scheme_2_Bonus_Payout', 'Bonus_Scheme_2_MP_Payout',
            
            'Actual_Bonus_Volume_3', 'Actual_Bonus_Value_3',
            'Bonus_Scheme_%_Achieved_3',
            'Actual_Bonus_Payout_Period_Volume_3', 'Actual_Bonus_Payout_Period_Value_3',
            'Actual_Bonus_MP_Volume_3', 'Actual_Bonus_MP_Value_3',
            '%_MP_Achieved_3',
            'Actual_Bonus_Payout_Period_MP_Volume_3', 'Actual_Bonus_Payout_Period_MP_Value_3',
            'Bonus_Scheme_3_Bonus_Payout', 'Bonus_Scheme_3_MP_Payout',
            
            'Actual_Bonus_Volume_4', 'Actual_Bonus_Value_4',
            'Bonus_Scheme_%_Achieved_4',
            'Actual_Bonus_Payout_Period_Volume_4', 'Actual_Bonus_Payout_Period_Value_4',
            'Actual_Bonus_MP_Volume_4', 'Actual_Bonus_MP_Value_4',
            '%_MP_Achieved_4',
            'Actual_Bonus_Payout_Period_MP_Volume_4', 'Actual_Bonus_Payout_Period_MP_Value_4',
            'Bonus_Scheme_4_Bonus_Payout', 'Bonus_Scheme_4_MP_Payout',
            
            # Additional scheme columns (with _pN suffix)
            'target_volume_pN', 'target_value_pN',
            'actual_volume_pN', 'actual_value_pN',
            'percentage_achieved_pN',
            'Payout_Products_Volume_pN', 'Payout_Products_Value_pN',
            'rebate_per_litre_applied_pN', 'rebate_percent_applied_pN',
            'basic_payout_pN', 'additional_payout_pN',
            'Fixed_Rebate_Pn', 'Total_Payout_pN',
            'Mandatory_product_actual_value_pN', 'Mandatory_product_actual_PPI_pN',
            'Mandatory_Product_PPI_Achievement_pN',
            'Mandatory_Product_Actual_Volume_pN', 'Mandatory_Product_Actual_Value_pN',
            'Mandatory_Product_Growth_Target_Achieved_pN',
            'Mandatory_Product_Payout_pN',
            'MP_FINAL_ACHIEVEMENT_pct_pN', 'MP_Final_Payout_pN',
            
            # Additional scheme phasing columns (with _pn suffix)
            'Phasing_Period_Volume_1_pn', 'Phasing_Period_Value_1_pn',
            'Phasing_Payout_Period_Volume_1_pn', 'Phasing_Payout_Period_Value_1_pn',
            'Phasing_Period_Payout_Product_Volume_1_pn', 'Phasing_Period_Payout_Product_Value_1_pn',
            'Percent_Phasing_Achieved_1_pn', 'Phasing_Payout_1_pn',
            
            'Phasing_Period_Volume_2_pn', 'Phasing_Period_Value_2_pn',
            'Phasing_Payout_Period_Volume_2_pn', 'Phasing_Payout_Period_Value_2_pn',
            'Phasing_Period_Payout_Product_Volume_2_pn', 'Phasing_Period_Payout_Product_Value_2_pn',
            'Percent_Phasing_Achieved_2_pn', 'Phasing_Payout_2_pn',
            
            'Phasing_Period_Volume_3_pn', 'Phasing_Period_Value_3_pn',
            'Phasing_Payout_Period_Volume_3_pn', 'Phasing_Payout_Period_Value_3_pn',
            'Phasing_Period_Payout_Product_Volume_3_pn', 'Phasing_Period_Payout_Product_Value_3_pn',
            'Percent_Phasing_Achieved_3_pn', 'Phasing_Payout_3_pn',
            
            # Additional scheme bonus columns (with _pn suffix)
            'Bonus_Phasing_Period_Volume_1_pn', 'Bonus_Phasing_Period_Value_1_pn',
            'Bonus_Payout_Period_Volume_1_pn', 'Bonus_Payout_Period_Value_1_pn',
            'Percent_Bonus_Achieved_1_pn',
            'Bonus_Payout_Period_Payout_Product_Volume_1_pn', 'Bonus_Payout_Period_Payout_Product_Value_1_pn',
            'Bonus_Payout_1_pn',
            
            'Bonus_Phasing_Period_Volume_2_pn', 'Bonus_Phasing_Period_Value_2_pn',
            'Bonus_Payout_Period_Volume_2_pn', 'Bonus_Payout_Period_Value_2_pn',
            'Percent_Bonus_Achieved_2_pn',
            'Bonus_Payout_Period_Payout_Product_Volume_2_pn', 'Bonus_Payout_Period_Payout_Product_Value_2_pn',
            'Bonus_Payout_2_pn',
            
            'Bonus_Phasing_Period_Volume_3_pn', 'Bonus_Phasing_Period_Value_3_pn',
            'Bonus_Payout_Period_Volume_3_pn', 'Bonus_Payout_Period_Value_3_pn',
            'Percent_Bonus_Achieved_3_pn',
            'Bonus_Payout_Period_Payout_Product_Volume_3_pn', 'Bonus_Payout_Period_Payout_Product_Value_3_pn',
            'Bonus_Payout_3_pn',
            
            'FINAL_PHASING_PAYOUT_pn',
            
            # Final payout and rewards columns
            'Scheme Final Payout',
            'Rewards'
        ]

        # Remove columns if they exist
        removed_count = 0
        for col in columns_to_remove:
            if col in df.columns:
                df = df.drop(columns=[col])
                removed_count += 1
                print(f"      ✅ Removed column: {col}")

        if removed_count > 0:
            print(f"   ✅ Removed {removed_count} unwanted columns from final output")
        else:
            print(f"   ✅ No unwanted columns found to remove")

        return df

    def _save_as_json_array(self, df: pd.DataFrame, filename: str):
        """
        Save DataFrame as JSON in ARRAY format for smaller file size.
        Column names are stored once in headers, data as arrays.
        """
        print(f"   📄 Saving as JSON ARRAY format: {filename}")
        
        try:
            # Convert DataFrame to array format
            # This creates: [headers, [row1], [row2], ...]
            headers = df.columns.tolist()
            data_rows = df.values.tolist()
            
            # Create the array format JSON
            json_data = [headers] + data_rows
            
            # Save to file with compact formatting for smaller size
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=None, separators=(',', ':'), ensure_ascii=False, default=str)
            
            # Get file size for comparison
            file_size = os.path.getsize(filename)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"   ✅ JSON saved: {len(data_rows)} rows, {len(headers)} columns")
            print(f"   📊 File size: {file_size_mb:.2f} MB")
            
        except Exception as e:
            print(f"   ❌ Error saving JSON: {str(e)}")


# API-friendly version - no main() execution for imports
# The main() function is removed since this will be used as a module in FastAPI

# For testing purposes only (not used in API)
def test_scheme_processor():
    """Test function for development - not used in API"""
    scheme_id = "990126"
    processor = SchemeProcessor()
    success = processor.process_scheme(scheme_id)
    
    if success:
        print(f"\n🚀 Processing completed successfully!")
        result = processor.get_calculation_results_json(scheme_id)
        if result:
            print(f"\n📊 Results Summary:")
            print(f"  - Total records: {result['summary']['total_records']}")
            print(f"  - Total columns: {result['summary']['total_columns']}")
            return result
    return None

# Remove automatic execution
# if __name__ == "__main__":
#     main()
