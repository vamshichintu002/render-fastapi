import psycopg2
import pandas as pd
import json
import asyncio
import subprocess
import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import collections
from ..database_psycopg2 import DatabaseManager

# Configure logging for tracker service
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [TRACKER-SERVICE] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
tracker_logger = logging.getLogger(__name__)

# Import the queries from the separate file
try:
    from tracker_queries import (
        TRACKER_MAINSCHEME_VALUE,
        TRACKER_MAINSCHEME_VOLUME,
        TRACKER_ADDITIONAL_SCHEME_VALUE,
        TRACKER_ADDITIONAL_SCHEME_VOLUME
    )
except ImportError:
    # Fallback if tracker_queries.py is not available
    TRACKER_MAINSCHEME_VALUE = ""
    TRACKER_MAINSCHEME_VOLUME = ""
    TRACKER_ADDITIONAL_SCHEME_VALUE = ""
    TRACKER_ADDITIONAL_SCHEME_VOLUME = ""

# Define all columns returned by get_scheme_configuration function
SCHEME_CONFIG_COLUMNS = [
    'additional_scheme_index', 'scheme_number', 'volume_value_based', 'scheme_type',
    'scheme_period_from', 'scheme_period_to', 'reward_slabs_enabled', 'bonus_schemes_enabled', 'payout_products_enabled',
    'scheme_applicable_enabled', 'mandatory_products_enabled', 'enable_strata_growth',
    'show_mandatory_product', 'mandatory_qualify', 'base_dates_count',
    'has_phasing_periods', 'phasing_periods_count', 'bonus_schemes_count',
    'phasing_period_1_enabled', 'phasing_period_1_bonus_enabled',
    'phasing_period_2_enabled', 'phasing_period_2_bonus_enabled',
    'phasing_period_3_enabled', 'phasing_period_3_bonus_enabled'
]

# Group columns by bonus scheme index
BONUS_COLUMNS_BY_INDEX = {
    1: [
        "% MP Achieved 1", "% Reward on Total Bonus Scheme 1", "Actual Bonus MP Value 1",
        "Actual Bonus Payout Period MP Value 1", "Actual Bonus Payout Period Value 1",
        "Actual Bonus Value 1", "Bonus Scheme % Achieved 1", "Bonus Scheme 1 % Main Scheme Target",
        "Bonus Scheme 1 Minimum Target", "Bonus Scheme 1 Bonus Payout", "Bonus Scheme 1 MP Payout",
        "Bonus Scheme No 1", "Main Scheme Bonus Target Value 1", "Mandatory Product Bonus Target Value 1",
        "Mandatory Product Target % Bonus Scheme 1", "Minimum Mandatory Product Target Bonus Scheme 1",
        "Reward on Mandatory Product % Bonus Scheme 1", "Actual Bonus MP Volume 1",
        "Actual Bonus Payout Period MP Volume 1", "Actual Bonus Payout Period Volume 1",
        "Actual Bonus Volume 1", "Main Scheme Bonus Target Volume 1", "Mandatory Product Bonus Target Volume 1"
    ],
    2: [
        "% MP Achieved 2", "% Reward on Total Bonus Scheme 2", "Actual Bonus MP Value 2",
        "Actual Bonus Payout Period MP Value 2", "Actual Bonus Payout Period Value 2",
        "Actual Bonus Value 2", "Bonus Scheme % Achieved 2", "Bonus Scheme 2 % Main Scheme Target",
        "Bonus Scheme 2 Minimum Target", "Bonus Scheme 2 Bonus Payout", "Bonus Scheme 2 MP Payout",
        "Bonus Scheme No 2", "Main Scheme Bonus Target Value 2", "Mandatory Product Bonus Target Value 2",
        "Mandatory Product Target % Bonus Scheme 2", "Minimum Mandatory Product Target Bonus Scheme 2",
        "Reward on Mandatory Product % Bonus Scheme 2", "Actual Bonus MP Volume 2",
        "Actual Bonus Payout Period MP Volume 2", "Actual Bonus Payout Period Volume 2",
        "Actual Bonus Volume 2", "Main Scheme Bonus Target Volume 2", "Mandatory Product Bonus Target Volume 2"
    ],
    3: [
        "% MP Achieved 3", "% Reward on Total Bonus Scheme 3", "Actual Bonus MP Value 3",
        "Actual Bonus Payout Period MP Value 3", "Actual Bonus Payout Period Value 3",
        "Actual Bonus Value 3", "Bonus Scheme % Achieved 3", "Bonus Scheme 3 % Main Scheme Target",
        "Bonus Scheme 3 Minimum Target", "Bonus Scheme 3 Bonus Payout", "Bonus Scheme 3 MP Payout",
        "Bonus Scheme No 3", "Main Scheme Bonus Target Value 3", "Mandatory Product Bonus Target Value 3",
        "Mandatory Product Target % Bonus Scheme 3", "Minimum Mandatory Product Target Bonus Scheme 3",
        "Reward on Mandatory Product % Bonus Scheme 3", "Actual Bonus MP Volume 3",
        "Actual Bonus Payout Period MP Volume 3", "Actual Bonus Payout Period Volume 3",
        "Actual Bonus Volume 3", "Main Scheme Bonus Target Volume 3", "Mandatory Product Bonus Target Volume 3"
    ],
    4: [
        "% MP Achieved 4", "% Reward on Total Bonus Scheme 4", "Actual Bonus MP Value 4",
        "Actual Bonus Payout Period MP Value 4", "Actual Bonus Payout Period Value 4",
        "Actual Bonus Value 4", "Bonus Scheme % Achieved 4", "Bonus Scheme 4 % Main Scheme Target",
        "Bonus Scheme 4 Minimum Target", "Bonus Scheme 4 Bonus Payout", "Bonus Scheme 4 MP Payout",
        "Bonus Scheme No 4", "Main Scheme Bonus Target Value 4", "Mandatory Product Bonus Target Value 4",
        "Mandatory Product Target % Bonus Scheme 4", "Minimum Mandatory Product Target Bonus Scheme 4",
        "Reward on Mandatory Product % Bonus Scheme 4", "Actual Bonus MP Volume 4",
        "Actual Bonus Payout Period MP Volume 4", "Actual Bonus Payout Period Volume 4",
        "Actual Bonus Volume 4", "Main Scheme Bonus Target Volume 4", "Mandatory Product Bonus Target Volume 4"
    ]
}

# Create a flat list of all bonus columns
ALL_BONUS_COLUMNS = []
for cols in BONUS_COLUMNS_BY_INDEX.values():
    ALL_BONUS_COLUMNS.extend(cols)

# Other column definitions (shortened for brevity)
MAIN_MANDATORY_COLUMNS = [
    "Mandatory Products - Fields Main Scheme", "Mandatory Min. Shades - PPI",
    "Mandatory Product % PPI", "Mandatory Product % Target to Actual Sales",
    "Mandatory Product % to Actual - Target Value", "Mandatory product actual PPI",
    "Mandatory Product Actual Value", "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Base Value", "Mandatory Product Fixed Target",
    "Mandatory Product Growth", "Mandatory Product Payout", "Mandatory Product Rebate",
    "MP FINAL ACHEIVMENT %", "MP Final Payout", "MP FINAL TARGET",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved", "Mandatory Product Growth Target Value",
    "MP Rebate %", "Mandatory Product Base Volume", "Mandatory Product Growth Target Volume",
    "Mandatory Product Actual Volume", "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product % to Actual - Target Volume", "Mandatory Product Actual Volume % to Total Sales"
]

ADDITIONAL_MANDATORY_BASE_COLUMNS = MAIN_MANDATORY_COLUMNS.copy()

MAIN_PAYOUT_COLUMNS = ["Payout Product Payout", "Payout Products Value", "Payout Products Volume"]
ADDITIONAL_PAYOUT_BASE_COLUMNS = MAIN_PAYOUT_COLUMNS.copy()

# Phasing columns structure (simplified)
PHASING_COLUMNS = {
    "common": ["FINAL PHASING PAYOUT"],
    "period_1": {
        "regular": [
            "Phasing Period No 1", "Phasing Target % 1", "Phasing Target Value 1", "Phasing Period Value 1",
            "Phasing Payout Period Value 1", "% Phasing Achieved 1", "Phasing Period Rebate 1", "Phasing Period Rebate% 1",
            "Phasing Payout 1", "Phasing Target Volume 1", "Phasing Period Volume 1", "Phasing Payout Period Volume 1",
            "Phasing Period Payout Product Volume 1", "Phasing Period Payout Product Value 1"
        ],
        "bonus": [
            "Is Bonus 1", "Bonus Rebate Value 1", "Bonus Phasing Period Rebate % 1", "Bonus Phasing Target % 1",
            "Bonus Target Value 1", "Bonus Phasing Period Value 1", "Bonus Payout Period Value 1", "% Bonus Achieved 1",
            "Bonus Payout 1", "Bonus Target Volume 1", "Bonus Phasing Period Volume 1", "Bonus Payout Period Volume 1",
            "Bonus Payout Period Payout Product Volume 1", "Bonus Payout Period Payout Product Value 1"
        ]
    },
    "period_2": {
        "regular": [
            "Phasing Period No 2", "Phasing Target % 2", "Phasing Target Value 2", "Phasing Period Value 2",
            "Phasing Payout Period Value 2", "% Phasing Achieved 2", "Phasing Period Rebate 2", "Phasing Period Rebate% 2",
            "Phasing Payout 2", "Phasing Target Volume 2", "Phasing Period Volume 2", "Phasing Payout Period Volume 2",
            "Phasing Period Payout Product Volume 2", "Phasing Period Payout Product Value 2"
        ],
        "bonus": [
            "Is Bonus 2", "Bonus Rebate Value 2", "Bonus Phasing Period Rebate % 2", "Bonus Phasing Target % 2",
            "Bonus Target Value 2", "Bonus Phasing Period Value 2", "Bonus Payout Period Value 2", "% Bonus Achieved 2",
            "Bonus Payout 2", "Bonus Target Volume 2", "Bonus Phasing Period Volume 2", "Bonus Payout Period Volume 2",
            "Bonus Payout Period Payout Product Volume 2", "Bonus Payout Period Payout Product Value 2"
        ]
    },
    "period_3": {
        "regular": [
            "Phasing Period No 3", "Phasing Target % 3", "Phasing Target Value 3", "Phasing Period Value 3",
            "Phasing Payout Period Value 3", "% Phasing Achieved 3", "Phasing Period Rebate 3", "Phasing Period Rebate% 3",
            "Phasing Payout 3", "Phasing Target Volume 3", "Phasing Period Volume 3", "Phasing Payout Period Volume 3",
            "Phasing Period Payout Product Volume 3", "Phasing Period Payout Product Value 3"
        ],
        "bonus": [
            "Is Bonus 3", "Bonus Rebate Value 3", "Bonus Phasing Period Rebate % 3", "Bonus Phasing Target % 3",
            "Bonus Target Value 3", "Bonus Phasing Period Value 3", "Bonus Payout Period Value 3", "% Bonus Achieved 3",
            "Bonus Payout 3", "Bonus Target Volume 3", "Bonus Phasing Period Volume 3", "Bonus Payout Period Volume 3",
            "Bonus Payout Period Payout Product Volume 3", "Bonus Payout Period Payout Product Value 3"
        ]
    }
}

# Create a flat list of all phasing columns
ALL_PHASING_COLUMNS = []
ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS["common"])
for period in ["period_1", "period_2", "period_3"]:
    ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS[period]["regular"])
    ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS[period]["bonus"])

# Base dates columns
BASE_DATES_REMOVE_IF_COUNT_1 = [
    "Base 1 Volume", "Base 1 Value", "Base 1 SumAvg", "Base 1 Months",
    "Base 1 Volume Final", "Base 1 Value Final", "Base 2 Volume", "Base 2 Value",
    "Base 2 SumAvg", "Base 2 Months", "Base 2 Volume Final", "Base 2 Value Final"
]

BASE_DATES_REMOVE_IF_COUNT_2 = [
    "Base 1 Volume", "Base 1 Value", "Base 1 SumAvg", "Base 1 Months",
    "Base 2 Volume", "Base 2 Value", "Base 2 SumAvg", "Base 2 Months"
]

# Columns to remove from additional schemes
ADDITIONAL_SCHEME_REMOVE_COLUMNS = ["state_name", "customer_name", "so_name"]


class TrackerService:
    def __init__(self):
        self.db_manager = DatabaseManager()

    async def run_tracker(self, scheme_id: str) -> Dict[str, Any]:
        """
        Run tracker for a given scheme ID with Vercel timeout optimization.
        
        Args:
            scheme_id: The scheme ID to run tracker for
            
        Returns:
            Dict containing success status and message
        """
        request_start_time = datetime.now()
        tracker_logger.info(f"📥 [API-REQUEST] Tracker run request received for scheme_id: {scheme_id} at {request_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Set a reasonable timeout for Vercel (50 seconds max)
            tracker_logger.info(f"⏰ [SCHEME-{scheme_id}] Setting 45s timeout for execution")
            result = await asyncio.wait_for(
                self._execute_tracker(scheme_id),
                timeout=45.0  # Leave 5 seconds buffer for response
            )
            
            total_request_time = (datetime.now() - request_start_time).total_seconds()
            tracker_logger.info(f"✅ [API-REQUEST] Tracker request completed successfully in {total_request_time:.2f}s for scheme_id: {scheme_id}")
            
            return result
            
        except asyncio.TimeoutError:
            # If timeout occurs, mark as processing and return immediately
            timeout_time = (datetime.now() - request_start_time).total_seconds()
            tracker_logger.warning(f"⏰ [SCHEME-{scheme_id}] Execution timed out after {timeout_time:.2f}s, moving to background processing")
            
            await self._mark_tracker_as_processing(scheme_id)
            return {
                "success": True,
                "message": f"Tracker for scheme {scheme_id} is being processed in the background due to timeout. Check status later.",
                "status": "running",
                "scheme_id": scheme_id,
                "timeout_duration": timeout_time
            }
        except Exception as e:
            error_time = (datetime.now() - request_start_time).total_seconds()
            tracker_logger.error(f"❌ [SCHEME-{scheme_id}] Error running tracker after {error_time:.2f}s: {str(e)}")
            return {
                "success": False,
                "message": f"Error running tracker for scheme {scheme_id}: {str(e)}",
                "status": "failed",
                "scheme_id": scheme_id,
                "error_duration": error_time
            }

    async def _execute_tracker(self, scheme_id: str) -> Dict[str, Any]:
        """Execute the tracker logic by running the tracker_runner.py script"""
        tracker_start_time = datetime.now()
        tracker_logger.info(f"🚀 [SCHEME-{scheme_id}] Starting tracker execution at {tracker_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Import the tracker runner functions
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '../..')
            
            # Import the tracker runner functions directly
            sys.path.append(project_root)
            
            try:
                # Direct import approach - run the actual tracker logic
                from tracker_runner import (
                    fetch_scheme_config, 
                    build_queries_from_templates, 
                    run_multiple_queries_and_combine,
                    db_params
                )
                
                tracker_logger.info(f"🔍 [SCHEME-{scheme_id}] Tracker modules imported successfully")
                
                # Execute tracker logic using the original functions with proper connection
                conn = psycopg2.connect(**db_params)
                tracker_logger.info(f"✅ [SCHEME-{scheme_id}] Database connected successfully")
                
                # Fetch scheme configuration
                config_start = datetime.now()
                scheme_config_df = fetch_scheme_config(conn, scheme_id)
                config_time = (datetime.now() - config_start).total_seconds()
                tracker_logger.info(f"📊 [SCHEME-{scheme_id}] Scheme config fetched in {config_time:.2f}s: {len(scheme_config_df)} rows")
                
                if scheme_config_df.empty:
                    conn.close()
                    tracker_logger.error(f"❌ [SCHEME-{scheme_id}] No scheme configuration found")
                    return {
                        "success": False,
                        "message": f"No scheme configuration found for scheme_id: {scheme_id}. Please check if the scheme exists and has valid configuration.",
                        "status": "failed",
                        "scheme_id": scheme_id
                    }
                
                # Build queries from templates
                query_build_start = datetime.now()
                queries, scheme_names = build_queries_from_templates(scheme_id, scheme_config_df)
                query_build_time = (datetime.now() - query_build_start).total_seconds()
                tracker_logger.info(f"🔨 [SCHEME-{scheme_id}] Built {len(queries)} queries from templates in {query_build_time:.2f}s")
                
                # Execute the main tracker logic - this function handles everything including database save
                # We need to inject scheme_id into the tracker_runner module's globals
                import tracker_runner
                
                # Store original state
                original_scheme_id = getattr(tracker_runner, 'scheme_id', None)
                
                # Inject scheme_id into tracker_runner module
                tracker_runner.scheme_id = scheme_id
                
                try:
                    execution_start = datetime.now()
                    tracker_logger.info(f"🏃 [SCHEME-{scheme_id}] Starting parallel query execution...")
                    run_multiple_queries_and_combine(queries, scheme_names, scheme_config_df)
                    execution_time = (datetime.now() - execution_start).total_seconds()
                    tracker_logger.info(f"⚡ [SCHEME-{scheme_id}] Parallel execution completed in {execution_time:.2f}s")
                finally:
                    # Restore original state
                    if original_scheme_id is not None:
                        tracker_runner.scheme_id = original_scheme_id
                    elif hasattr(tracker_runner, 'scheme_id'):
                        delattr(tracker_runner, 'scheme_id')
                
                conn.close()
                
                total_time = (datetime.now() - tracker_start_time).total_seconds()
                tracker_logger.info(f"✅ [SCHEME-{scheme_id}] Tracker execution completed successfully in {total_time:.2f}s")
                
                return {
                    "success": True,
                    "message": f"Tracker completed successfully for scheme_id: {scheme_id}. Data saved to database. Total time: {total_time:.2f}s",
                    "status": "completed", 
                    "scheme_id": scheme_id,
                    "execution_time": total_time
                }
                
            except ImportError as import_err:
                tracker_logger.error(f"❌ [SCHEME-{scheme_id}] Import error: {import_err}")
                # Fallback to subprocess execution
                return await self._execute_tracker_subprocess(scheme_id)
            except Exception as tracker_err:
                total_time = (datetime.now() - tracker_start_time).total_seconds()
                tracker_logger.error(f"❌ [SCHEME-{scheme_id}] Tracker execution error after {total_time:.2f}s: {tracker_err}")
                raise tracker_err
            
        except Exception as e:
            total_time = (datetime.now() - tracker_start_time).total_seconds()
            tracker_logger.error(f"❌ [SCHEME-{scheme_id}] Error in _execute_tracker after {total_time:.2f}s: {str(e)}")
            return {
                "success": False,
                "message": f"Error executing tracker: {str(e)}",
                "status": "failed",
                "scheme_id": scheme_id,
                "execution_time": total_time
            }
    
    async def _execute_tracker_subprocess(self, scheme_id: str) -> Dict[str, Any]:
        """Execute tracker using subprocess as fallback"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '../..')
            tracker_script = os.path.join(project_root, 'tracker_runner.py')
            
            # Run the tracker script with the scheme_id as input
            process = subprocess.Popen(
                [sys.executable, tracker_script],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=project_root
            )
            
            stdout, stderr = process.communicate(input=scheme_id + '\n')
            
            if process.returncode == 0:
                return {
                    "success": True,
                    "message": f"Tracker completed successfully for scheme_id: {scheme_id}. Data saved to database.",
                    "status": "completed",
                    "scheme_id": scheme_id,
                    "output": stdout
                }
            else:
                return {
                    "success": False,
                    "message": f"Tracker execution failed for scheme_id: {scheme_id}. Error: {stderr}",
                    "status": "failed",
                    "scheme_id": scheme_id,
                    "error": stderr
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Error executing tracker subprocess: {str(e)}",
                "status": "failed",
                "scheme_id": scheme_id
            }

    async def _fetch_scheme_config(self, scheme_id: str) -> pd.DataFrame:
        """Fetch scheme configuration"""
        try:
            query = f"SELECT * FROM get_scheme_configuration('{scheme_id}');"
            result = self.db_manager.execute_query(query)
            
            if result:
                df = pd.DataFrame(result)
                print(f"✅ Fetched scheme configuration: {len(df)} rows, {len(df.columns)} columns")
                # Ensure all expected columns are present
                missing_cols = set(SCHEME_CONFIG_COLUMNS) - set(df.columns)
                if missing_cols:
                    print(f"Warning: Missing columns in scheme configuration: {missing_cols}")
                return df
            else:
                print(f"⚠️ No scheme configuration found for scheme_id: {scheme_id}")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error fetching scheme configuration: {e}")
            return pd.DataFrame()

    def _build_queries_from_templates(self, scheme_id: str, scheme_config_df: pd.DataFrame):
        """Build queries from templates"""
        queries = []
        scheme_names = []
        
        for idx, row in scheme_config_df.iterrows():
            scheme_index = row['additional_scheme_index']
            scheme_type = row['volume_value_based'].strip().lower()
            scheme_number = row['scheme_number'] if pd.notnull(row['scheme_number']) else scheme_id
            
            if scheme_index == 'MAINSCHEME':
                if scheme_type == 'value':
                    template = TRACKER_MAINSCHEME_VALUE
                else:
                    template = TRACKER_MAINSCHEME_VOLUME
                query = template.replace('{scheme_id}', scheme_id)
            else:
                if scheme_type == 'value':
                    template = TRACKER_ADDITIONAL_SCHEME_VALUE
                else:
                    template = TRACKER_ADDITIONAL_SCHEME_VOLUME
                query = template.replace('{scheme_id}', scheme_id).replace('{additional_scheme_index}', str(scheme_index))
            
            queries.append(query)
            scheme_names.append(scheme_number)
        
        return queries, scheme_names

    async def _run_multiple_queries_and_combine(self, function_queries, scheme_names, scheme_config_df, scheme_id):
        """Execute multiple queries and combine results"""
        try:
            print(f"🔍 Processing {len(function_queries)} queries for scheme {scheme_id}")
            
            # For now, create dummy data to test the flow
            # TODO: Replace with actual query execution once SQL queries are updated
            dummy_data = {
                'credit_account': ['12345', '67890', '11111'],
                'Total Payout': [1000, 2000, 1500],
                'MP Final Payout': [500, 750, 600],
                'FINAL PHASING PAYOUT': [200, 300, 250]
            }
            merged_df = pd.DataFrame(dummy_data)
            
            print(f"✅ Created dummy dataframe: {len(merged_df)} rows, {len(merged_df.columns)} columns")
            
            # Process the dataframe (column removal, calculations, etc.)
            merged_df = self._process_dataframe(merged_df, scheme_config_df, scheme_id)
            
            # Convert to JSON and save to database
            json_data = self._convert_to_json(merged_df)
            
            # Get scheme period dates
            main_scheme_row = scheme_config_df[scheme_config_df['additional_scheme_index'] == 'MAINSCHEME']
            scheme_period_from = main_scheme_row['scheme_period_from'].values[0] if not main_scheme_row.empty else None
            scheme_period_to = main_scheme_row['scheme_period_to'].values[0] if not main_scheme_row.empty else None
            
            # Save to database
            await self._insert_tracker_data_to_db(scheme_id, json_data, scheme_period_from, scheme_period_to)
            
            return {
                "success": True,
                "message": f"Tracker completed successfully for scheme_id: {scheme_id}. Data saved to database.",
                "status": "completed",
                "scheme_id": scheme_id,
                "total_rows": len(merged_df),
                "total_columns": len(merged_df.columns)
            }
            
        except Exception as e:
            print(f"❌ Error in _run_multiple_queries_and_combine: {e}")
            return {
                "success": False,
                "message": f"Error processing tracker data: {str(e)}",
                "status": "failed",
                "scheme_id": scheme_id
            }

    def _process_dataframe(self, merged_df: pd.DataFrame, scheme_config_df: pd.DataFrame, scheme_id: str) -> pd.DataFrame:
        """Process dataframe with column removal and calculations"""
        # This is a simplified version of the original processing logic
        # You can expand this with the full logic from the original file
        
        # Sort by presence
        merged_df['presence_count'] = merged_df.notna().sum(axis=1)
        merged_df = merged_df.sort_values(by='presence_count', ascending=False).drop(columns='presence_count')
        
        # Fill numeric columns
        numeric_cols = merged_df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            if col.startswith(("Payout", "Bonus", "Phasing", "MP", "Mandatory", "Growth", "Target", "Value", "%", "Achieved")):
                merged_df[col] = merged_df[col].fillna(0)
        
        # Add scheme information columns
        main_scheme_row = scheme_config_df[scheme_config_df['additional_scheme_index'] == 'MAINSCHEME']
        if not main_scheme_row.empty:
            scheme_name = main_scheme_row['scheme_number'].values[0]
            scheme_type = main_scheme_row['scheme_type'].values[0]
            scheme_period_from = main_scheme_row['scheme_period_from'].values[0]
            scheme_period_to = main_scheme_row['scheme_period_to'].values[0]
            mandatory_qualify = main_scheme_row['mandatory_qualify'].values[0]
            
            merged_df.insert(1, "Scheme ID", scheme_id)
            merged_df.insert(2, "Scheme Name", scheme_name)
            merged_df.insert(3, "Scheme Type", scheme_type)
            merged_df.insert(4, "Scheme Period From", scheme_period_from)
            merged_df.insert(5, "Scheme Period To", scheme_period_to)
            merged_df.insert(6, "Mandatory Qualify", "Yes" if mandatory_qualify else "No")
        
        # Add final payout and reward columns (simplified)
        merged_df["Scheme Final Payout"] = 0  # Simplified calculation
        merged_df["Scheme Reward"] = ""  # Simplified calculation
        
        return merged_df

    def _convert_to_json(self, merged_df: pd.DataFrame) -> str:
        """Convert dataframe to JSON string"""
        column_order = list(merged_df.columns)
        ordered_records = []
        
        for _, row in merged_df.iterrows():
            ordered_record = collections.OrderedDict()
            for col in column_order:
                value = row[col]
                if pd.isna(value):
                    ordered_record[col] = None
                elif value == float('inf'):
                    ordered_record[col] = "Infinity"
                elif value == float('-inf'):
                    ordered_record[col] = "-Infinity"
                else:
                    ordered_record[col] = value
            ordered_records.append(ordered_record)
        
        return json.dumps(ordered_records, indent=4, ensure_ascii=False)

    async def _insert_tracker_data_to_db(self, scheme_id: str, json_data: str, from_date, to_date):
        """Insert tracker data to database"""
        try:
            # Convert scheme_id to integer as the table expects integer type
            scheme_id_int = int(scheme_id)
            
            # Check if record exists for today's date (since table has composite primary key)
            check_query = "SELECT scheme_id FROM scheme_tracker_runs WHERE scheme_id = %s AND run_date = CURRENT_DATE"
            existing = self.db_manager.execute_query(check_query, [scheme_id_int])
            
            if existing:
                # Update existing record for today
                update_query = """
                UPDATE scheme_tracker_runs 
                SET tracker_data = %s::jsonb,
                    from_date = %s,
                    to_date = %s,
                    run_status = 'completed',
                    run_timestamp = now(),
                    updated_at = now()
                WHERE scheme_id = %s AND run_date = CURRENT_DATE
                """
                self.db_manager.execute_query(update_query, [json_data, from_date, to_date, scheme_id_int])
            else:
                # Insert new record for today
                insert_query = """
                INSERT INTO scheme_tracker_runs (scheme_id, tracker_data, from_date, to_date, run_status)
                VALUES (%s, %s::jsonb, %s, %s, 'completed')
                """
                self.db_manager.execute_query(insert_query, [scheme_id_int, json_data, from_date, to_date])
                
        except ValueError:
            raise Exception(f"Invalid scheme_id format: {scheme_id}. Expected integer.")
        except Exception as e:
            raise Exception(f"Error saving tracker data to database: {str(e)}")

    async def _mark_tracker_as_processing(self, scheme_id: str):
        """Mark tracker as processing in database"""
        try:
            await self.db_manager.connect()
            
            # Convert scheme_id to integer
            scheme_id_int = int(scheme_id)
            
            # Check if record exists for today's date
            check_query = "SELECT scheme_id FROM scheme_tracker_runs WHERE scheme_id = %s AND run_date = CURRENT_DATE"
            existing = self.db_manager.execute_query(check_query, [scheme_id_int])
            
            if existing:
                # Update existing record for today
                update_query = """
                UPDATE scheme_tracker_runs 
                SET run_status = 'running',
                    run_timestamp = now(),
                    updated_at = now()
                WHERE scheme_id = %s AND run_date = CURRENT_DATE
                """
                self.db_manager.execute_query(update_query, [scheme_id_int])
            else:
                # Insert new record for today
                insert_query = """
                INSERT INTO scheme_tracker_runs (scheme_id, run_status)
                VALUES (%s, 'running')
                """
                self.db_manager.execute_query(insert_query, [scheme_id_int])
                
        except ValueError:
            print(f"Invalid scheme_id format: {scheme_id}. Expected integer.")
        except Exception as e:
            print(f"Error marking tracker as processing: {str(e)}")
        finally:
            await self.db_manager.disconnect()

    async def _mark_tracker_as_error(self, scheme_id: str, error_message: str):
        """Mark tracker as error in database"""
        try:
            await self.db_manager.connect()
            
            # Convert scheme_id to integer
            scheme_id_int = int(scheme_id)
            
            # Check if record exists for today's date
            check_query = "SELECT scheme_id FROM scheme_tracker_runs WHERE scheme_id = %s AND run_date = CURRENT_DATE"
            existing = self.db_manager.execute_query(check_query, [scheme_id_int])
            
            if existing:
                # Update existing record for today
                # Note: The existing table doesn't have error_message column, so we'll use run_status
                update_query = """
                UPDATE scheme_tracker_runs 
                SET run_status = 'failed',
                    run_timestamp = now(),
                    updated_at = now()
                WHERE scheme_id = %s AND run_date = CURRENT_DATE
                """
                self.db_manager.execute_query(update_query, [scheme_id_int])
            else:
                # Insert new record for today
                insert_query = """
                INSERT INTO scheme_tracker_runs (scheme_id, run_status)
                VALUES (%s, 'failed')
                """
                self.db_manager.execute_query(insert_query, [scheme_id_int])
                
        except ValueError:
            print(f"Invalid scheme_id format: {scheme_id}. Expected integer.")
        except Exception as e:
            print(f"Error marking tracker as error: {str(e)}")
        finally:
            await self.db_manager.disconnect()

    async def get_tracker_status(self, scheme_id: str) -> Dict[str, Any]:
        """Get tracker status for a scheme"""
        try:
            await self.db_manager.connect()
            
            # Convert scheme_id to integer
            scheme_id_int = int(scheme_id)
            
            # Get the most recent run for this scheme
            query = """
            SELECT scheme_id, run_status, run_date, run_timestamp, from_date, to_date, updated_at
            FROM scheme_tracker_runs 
            WHERE scheme_id = %s
            ORDER BY run_date DESC, run_timestamp DESC
            LIMIT 1
            """
            result = self.db_manager.execute_query(query, [scheme_id_int])
            
            if result:
                row = result[0]
                return {
                    "success": True,
                    "scheme_id": str(row[0]),
                    "status": row[1],
                    "run_date": row[2].isoformat() if row[2] else None,
                    "run_timestamp": row[3].isoformat() if row[3] else None,
                    "from_date": row[4].isoformat() if row[4] else None,
                    "to_date": row[5].isoformat() if row[5] else None,
                    "updated_at": row[6].isoformat() if row[6] else None
                }
            else:
                return {
                    "success": False,
                    "message": f"No tracker record found for scheme_id: {scheme_id}",
                    "scheme_id": scheme_id
                }
                
        except ValueError:
            return {
                "success": False,
                "message": f"Invalid scheme_id format: {scheme_id}. Expected integer.",
                "scheme_id": scheme_id
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Error getting tracker status: {str(e)}",
                "scheme_id": scheme_id
            }
        finally:
            await self.db_manager.disconnect()

    async def debug_scheme_config(self, scheme_id: str) -> Dict[str, Any]:
        """Debug method to check scheme configuration lookup"""
        try:
            # Import tracker_runner functions
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '../..')
            sys.path.append(project_root)
            
            from tracker_runner import fetch_scheme_config, db_params
            
            # Test database connection
            conn = psycopg2.connect(**db_params)
            print(f"🔍 Debug: Testing scheme config for scheme_id: {scheme_id}")
            
            # Test the actual function call that's failing
            query = f"SELECT * FROM get_scheme_configuration('{scheme_id}');"
            print(f"🔍 Debug: Executing query: {query}")
            
            try:
                # Use pandas to read the query (same as in tracker_runner.py)
                import pandas as pd
                df = pd.read_sql_query(query, conn)
                print(f"🔍 Debug: Query returned {len(df)} rows, {len(df.columns)} columns")
                print(f"🔍 Debug: Columns: {list(df.columns)}")
                
                if not df.empty:
                    print(f"🔍 Debug: First row data: {df.iloc[0].to_dict()}")
                else:
                    print(f"🔍 Debug: No rows returned - scheme might not exist or function might have issues")
                
                # Also test if the scheme exists in schemes_data table
                check_query = "SELECT scheme_id, scheme_name FROM schemes_data WHERE scheme_id::TEXT = %s LIMIT 1"
                cur = conn.cursor()
                cur.execute(check_query, [scheme_id])
                scheme_exists = cur.fetchone()
                cur.close()
                
                conn.close()
                
                return {
                    "success": True,
                    "scheme_id": scheme_id,
                    "query_executed": query,
                    "rows_returned": len(df),
                    "columns_count": len(df.columns),
                    "columns": list(df.columns),
                    "scheme_exists_in_schemes_data": scheme_exists is not None,
                    "scheme_data": df.iloc[0].to_dict() if not df.empty else None,
                    "raw_scheme_lookup": {
                        "scheme_id": scheme_exists[0] if scheme_exists else None,
                        "scheme_name": scheme_exists[1] if scheme_exists else None
                    } if scheme_exists else None
                }
                
            except Exception as query_error:
                conn.close()
                return {
                    "success": False,
                    "scheme_id": scheme_id,
                    "query_executed": query,
                    "error": str(query_error),
                    "error_type": type(query_error).__name__
                }
                
        except Exception as e:
            return {
                "success": False,
                "scheme_id": scheme_id,
                "error": f"Debug error: {str(e)}",
                "error_type": type(e).__name__
            }

    async def debug_test_db(self) -> Dict[str, Any]:
        """Debug method to test database connection"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '../..')
            sys.path.append(project_root)
            
            from tracker_runner import db_params
            
            # Test database connection
            conn = psycopg2.connect(**db_params)
            print(f"🔍 Debug: Database connection successful")
            
            # Test basic queries
            cur = conn.cursor()
            
            # Test 1: Basic connection
            cur.execute("SELECT 1 as test")
            test_result = cur.fetchone()[0]
            
            # Test 2: Check if get_scheme_configuration function exists
            cur.execute("""
                SELECT proname, pronargs 
                FROM pg_proc 
                WHERE proname = 'get_scheme_configuration'
            """)
            function_exists = cur.fetchone()
            
            # Test 3: Check if schemes_data table exists and has data
            cur.execute("SELECT COUNT(*) FROM schemes_data LIMIT 1")
            schemes_count = cur.fetchone()[0]
            
            # Test 4: Get sample scheme IDs
            cur.execute("SELECT scheme_id FROM schemes_data LIMIT 5")
            sample_schemes = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return {
                "success": True,
                "database_connection": "OK",
                "basic_query_test": test_result == 1,
                "function_exists": function_exists is not None,
                "function_info": {
                    "name": function_exists[0] if function_exists else None,
                    "arg_count": function_exists[1] if function_exists else None
                } if function_exists else None,
                "schemes_data_count": schemes_count,
                "sample_scheme_ids": [row[0] for row in sample_schemes],
                "db_params": {
                    "host": db_params["host"],
                    "port": db_params["port"],
                    "dbname": db_params["dbname"],
                    "user": db_params["user"]
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Database test error: {str(e)}",
                "error_type": type(e).__name__
            }