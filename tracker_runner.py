import psycopg2
import pandas as pd
import sys
import time
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
# Import the queries from the separate file
from tracker_queries import (
    TRACKER_MAINSCHEME_VALUE,
    TRACKER_MAINSCHEME_VOLUME,
    TRACKER_ADDITIONAL_SCHEME_VALUE,
    TRACKER_ADDITIONAL_SCHEME_VOLUME
)

# Database connection parameters
db_params = {
    "dbname": "postgres",
    "user": "postgres.ozgkgkenzpngnptdqbqf",
    "password": "PwbcCdD?Yq4Jn.v",
    "host": "aws-0-ap-south-1.pooler.supabase.com",
    "port": 5432
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
        "% MP Achieved 1",
        "% Reward on Total Bonus Scheme 1",
        "Actual Bonus MP Value 1",
        "Actual Bonus Payout Period MP Value 1",
        "Actual Bonus Payout Period Value 1",
        "Actual Bonus Value 1",
        "Bonus Scheme % Achieved 1",
        "Bonus Scheme 1 % Main Scheme Target",
        "Bonus Scheme 1 Minimum Target",
        "Bonus Scheme 1 Bonus Payout",
        "Bonus Scheme 1 MP Payout",
        "Bonus Scheme No 1",
        "Main Scheme Bonus Target Value 1",
        "Mandatory Product Bonus Target Value 1",
        "Mandatory Product Target % Bonus Scheme 1",
        "Minimum Mandatory Product Target Bonus Scheme 1",
        "Reward on Mandatory Product % Bonus Scheme 1",
        # Added columns
        "Actual Bonus MP Volume 1",
        "Actual Bonus Payout Period MP Volume 1",
        "Actual Bonus Payout Period Volume 1",
        "Actual Bonus Volume 1",
        "Main Scheme Bonus Target Volume 1",
        "Mandatory Product Bonus Target Volume 1"
    ],
    2: [
        "% MP Achieved 2",
        "% Reward on Total Bonus Scheme 2",
        "Actual Bonus MP Value 2",
        "Actual Bonus Payout Period MP Value 2",
        "Actual Bonus Payout Period Value 2",
        "Actual Bonus Value 2",
        "Bonus Scheme % Achieved 2",
        "Bonus Scheme 2 % Main Scheme Target",
        "Bonus Scheme 2 Minimum Target",
        "Bonus Scheme 2 Bonus Payout",
        "Bonus Scheme 2 MP Payout",
        "Bonus Scheme No 2",
        "Main Scheme Bonus Target Value 2",
        "Mandatory Product Bonus Target Value 2",
        "Mandatory Product Target % Bonus Scheme 2",
        "Minimum Mandatory Product Target Bonus Scheme 2",
        "Reward on Mandatory Product % Bonus Scheme 2",
        # Added columns
        "Actual Bonus MP Volume 2",
        "Actual Bonus Payout Period MP Volume 2",
        "Actual Bonus Payout Period Volume 2",
        "Actual Bonus Volume 2",
        "Main Scheme Bonus Target Volume 2",
        "Mandatory Product Bonus Target Volume 2"
    ],
    3: [
        "% MP Achieved 3",
        "% Reward on Total Bonus Scheme 3",
        "Actual Bonus MP Value 3",
        "Actual Bonus Payout Period MP Value 3",
        "Actual Bonus Payout Period Value 3",
        "Actual Bonus Value 3",
        "Bonus Scheme % Achieved 3",
        "Bonus Scheme 3 % Main Scheme Target",
        "Bonus Scheme 3 Minimum Target",
        "Bonus Scheme 3 Bonus Payout",
        "Bonus Scheme 3 MP Payout",
        "Bonus Scheme No 3",
        "Main Scheme Bonus Target Value 3",
        "Mandatory Product Bonus Target Value 3",
        "Mandatory Product Target % Bonus Scheme 3",
        "Minimum Mandatory Product Target Bonus Scheme 3",
        "Reward on Mandatory Product % Bonus Scheme 3",
        # Added columns
        "Actual Bonus MP Volume 3",
        "Actual Bonus Payout Period MP Volume 3",
        "Actual Bonus Payout Period Volume 3",
        "Actual Bonus Volume 3",
        "Main Scheme Bonus Target Volume 3",
        "Mandatory Product Bonus Target Volume 3"
    ],
    4: [
        "% MP Achieved 4",
        "% Reward on Total Bonus Scheme 4",
        "Actual Bonus MP Value 4",
        "Actual Bonus Payout Period MP Value 4",
        "Actual Bonus Payout Period Value 4",
        "Actual Bonus Value 4",
        "Bonus Scheme % Achieved 4",
        "Bonus Scheme 4 % Main Scheme Target",
        "Bonus Scheme 4 Minimum Target",
        "Bonus Scheme 4 Bonus Payout",
        "Bonus Scheme 4 MP Payout",
        "Bonus Scheme No 4",
        "Main Scheme Bonus Target Value 4",
        "Mandatory Product Bonus Target Value 4",
        "Mandatory Product Target % Bonus Scheme 4",
        "Minimum Mandatory Product Target Bonus Scheme 4",
        "Reward on Mandatory Product % Bonus Scheme 4",
        # Added columns
        "Actual Bonus MP Volume 4",
        "Actual Bonus Payout Period MP Volume 4",
        "Actual Bonus Payout Period Volume 4",
        "Actual Bonus Volume 4",
        "Main Scheme Bonus Target Volume 4",
        "Mandatory Product Bonus Target Volume 4"
    ]
}

# Create a flat list of all bonus columns for easy reference when scheme_type != 'ho-scheme'
ALL_BONUS_COLUMNS = []
for cols in BONUS_COLUMNS_BY_INDEX.values():
    ALL_BONUS_COLUMNS.extend(cols)

# Mandatory product columns for main scheme
MAIN_MANDATORY_COLUMNS = [
    "Mandatory Products - Fields Main Scheme",
    "Mandatory Min. Shades - PPI",
    "Mandatory Product % PPI",
    "Mandatory Product % Target to Actual Sales",
    "Mandatory Product % to Actual - Target Value",
    "Mandatory product actual PPI",
    "Mandatory Product Actual Value",
    "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Base Value",
    "Mandatory Product Fixed Target",
    "Mandatory Product Growth",
    "Mandatory Product Payout",
    "Mandatory Product Rebate",
    "MP FINAL ACHEIVMENT %",
    "MP Final Payout",
    "MP FINAL TARGET",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product Growth Target Value",
    "MP Rebate %",
    # Added columns
    "Mandatory Product Base Value",
    "Mandatory Product Growth Target Value",
    "Mandatory Product Actual Value",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product % to Actual - Target Value",
    "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Base Volume",
    "Mandatory Product Growth Target Volume",
    "Mandatory Product Actual Volume",
    "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product % to Actual - Target Volume",
    "Mandatory Product Actual Volume % to Total Sales"
]

# Base mandatory product columns for additional schemes (without _pn suffix)
ADDITIONAL_MANDATORY_BASE_COLUMNS = [
    "Mandatory Product Actual Volume % to Total Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product Growth Target Volume",
    "Mandatory Min. Shades - PPI",
    "Mandatory Product % PPI",
    "Mandatory Product % Target to Actual Sales",
    "Mandatory Product % to Actual - Target Volume",
    "Mandatory product actual PPI",
    "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product Actual Volume",
    "Mandatory Product Base Volume",
    "Mandatory Product Fixed Target",
    "Mandatory Product Growth",
    "Mandatory Product Payout",
    "Mandatory Product Rebate",
    "MP FINAL ACHEIVMENT %",
    "MP Final Payout",
    "MP FINAL TARGET",
    "MP Rebate %",
    # Added columns
    "Mandatory Product Base Value",
    "Mandatory Product Growth Target Value",
    "Mandatory Product Actual Value",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product % to Actual - Target Value",
    "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Base Volume",
    "Mandatory Product Growth Target Volume",
    "Mandatory Product Actual Volume",
    "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product % to Actual - Target Volume",
    "Mandatory Product Actual Volume % to Total Sales"
]

# Payout product columns for main scheme
MAIN_PAYOUT_COLUMNS = [
    "Payout Product Payout",
    "Payout Products Value",
    "Payout Products Volume"
]

# Base payout product columns for additional schemes (without _pn suffix)
ADDITIONAL_PAYOUT_BASE_COLUMNS = [
    "Payout Product Payout",
    "Payout Products Volume",
    "Payout Products Value"
]

# Phasing columns for main scheme
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

# Create a flat list of all phasing columns for main scheme
ALL_PHASING_COLUMNS = []
ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS["common"])
for period in ["period_1", "period_2", "period_3"]:
    ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS[period]["regular"])
    ALL_PHASING_COLUMNS.extend(PHASING_COLUMNS[period]["bonus"])

# Base dates columns
BASE_DATES_ALL_COLUMNS = [
    "Base 1 Volume",
    "Base 1 Value",
    "Base 1 SumAvg",
    "Base 1 Months",
    "Base 1 Volume Final",
    "Base 1 Value Final",
    "Base 2 Volume",
    "Base 2 Value",
    "Base 2 SumAvg",
    "Base 2 Months",
    "Base 2 Volume Final",
    "Base 2 Value Final"
]

BASE_DATES_REMOVE_IF_COUNT_1 = [
    "Base 1 Volume",
    "Base 1 Value",
    "Base 1 SumAvg",
    "Base 1 Months",
    "Base 1 Volume Final",
    "Base 1 Value Final",
    "Base 2 Volume",
    "Base 2 Value",
    "Base 2 SumAvg",
    "Base 2 Months",
    "Base 2 Volume Final",
    "Base 2 Value Final"
]

BASE_DATES_REMOVE_IF_COUNT_2 = [
    "Base 1 Volume",
    "Base 1 Value",
    "Base 1 SumAvg",
    "Base 1 Months",
    "Base 2 Volume",
    "Base 2 Value",
    "Base 2 SumAvg",
    "Base 2 Months"
]

# Columns to remove from additional schemes
ADDITIONAL_SCHEME_REMOVE_COLUMNS = [
    "state_name",
    "customer_name",
    "so_name"
]


def execute_single_query(query_data: Tuple[int, str, str]) -> Tuple[int, pd.DataFrame, str, float]:
    """
    Execute a single query in a separate thread with timing and logging.
    
    Args:
        query_data: Tuple of (index, query, scheme_name)
        
    Returns:
        Tuple of (index, dataframe, scheme_name, execution_time)
    """
    idx, query, scheme_name = query_data
    start_time = time.time()
    
    try:
        logger.info(f"🔄 Starting Query {idx+1} execution for scheme: {scheme_name}")
        logger.info(f"📋 Query {idx+1} preview: {query[:200]}{'...' if len(query) > 200 else ''}")
        
        # Create a new database connection for this thread
        conn = psycopg2.connect(**db_params)
        logger.info(f"✅ Database connection established for Query {idx+1}")
        
        # Set timeout
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0;")
        conn.commit()
        cur.close()
        
        # Execute query
        query_start = time.time()
        df = pd.read_sql_query(query, conn)
        query_time = time.time() - query_start
        
        conn.close()
        
        execution_time = time.time() - start_time
        
        if df.empty:
            logger.warning(f"⚠️ Query {idx+1} returned no results")
        else:
            logger.info(f"✅ Query {idx+1} completed successfully: {len(df)} rows, {len(df.columns)} columns, Query time: {query_time:.2f}s, Total time: {execution_time:.2f}s")
        
        return idx, df, scheme_name, execution_time
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"❌ Query {idx+1} failed after {execution_time:.2f}s: {str(e)}")
        # Return empty dataframe on error
        return idx, pd.DataFrame(), scheme_name, execution_time

def run_multiple_queries_parallel(function_queries: List[str], scheme_names: List[str]) -> Tuple[List[pd.DataFrame], float]:
    """
    Execute multiple queries in parallel with comprehensive logging.
    
    Args:
        function_queries: List of SQL queries to execute
        scheme_names: List of scheme names corresponding to queries
        
    Returns:
        Tuple of (list of dataframes in original order, total execution time)
    """
    total_start_time = time.time()
    logger.info(f"🚀 Starting parallel execution of {len(function_queries)} queries")
    
    # Prepare query data for parallel execution
    query_data_list = [(idx, query, scheme_names[idx]) for idx, query in enumerate(function_queries)]
    
    # Use ThreadPoolExecutor for parallel execution
    max_workers = min(len(function_queries), 5)  # Limit to 5 concurrent connections
    logger.info(f"🔧 Using {max_workers} parallel workers")
    
    results = [None] * len(function_queries)  # Pre-allocate to maintain order
    execution_times = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries
        future_to_index = {executor.submit(execute_single_query, query_data): query_data[0] 
                          for query_data in query_data_list}
        
        completed_count = 0
        # Process completed queries as they finish
        for future in as_completed(future_to_index):
            try:
                idx, df, scheme_name, exec_time = future.result()
                results[idx] = df
                execution_times.append(exec_time)
                completed_count += 1
                
                logger.info(f"📊 Progress: {completed_count}/{len(function_queries)} queries completed")
                
            except Exception as e:
                idx = future_to_index[future]
                logger.error(f"❌ Unexpected error processing Query {idx+1}: {str(e)}")
                results[idx] = pd.DataFrame()  # Empty dataframe on error
    
    total_execution_time = time.time() - total_start_time
    
    # Log summary statistics
    logger.info(f"🎯 Parallel execution completed in {total_execution_time:.2f}s")
    logger.info(f"📈 Performance metrics:")
    logger.info(f"   - Total queries: {len(function_queries)}")
    logger.info(f"   - Successful queries: {sum(1 for df in results if not df.empty)}")
    logger.info(f"   - Failed queries: {sum(1 for df in results if df.empty)}")
    logger.info(f"   - Average query time: {sum(execution_times)/len(execution_times):.2f}s")
    logger.info(f"   - Fastest query: {min(execution_times):.2f}s")
    logger.info(f"   - Slowest query: {max(execution_times):.2f}s")
    logger.info(f"   - Total time (parallel): {total_execution_time:.2f}s")
    logger.info(f"   - Estimated sequential time: {sum(execution_times):.2f}s")
    logger.info(f"   - Time saved: {sum(execution_times) - total_execution_time:.2f}s ({((sum(execution_times) - total_execution_time) / sum(execution_times) * 100):.1f}%)")
    
    return results, total_execution_time

def fetch_scheme_config(conn, scheme_id):
    # Updated to use the new function
    query = f"SELECT * FROM get_scheme_configuration('{scheme_id}');"
    df = pd.read_sql_query(query, conn)
    
    # Ensure all expected columns are present
    missing_cols = set(SCHEME_CONFIG_COLUMNS) - set(df.columns)
    if missing_cols:
        print(f"Warning: Missing columns in scheme configuration: {missing_cols}")
    
    print("\n--- Schemes loaded ---")
    # Print all columns for verification
    print(df[SCHEME_CONFIG_COLUMNS])
    return df

def build_queries_from_templates(scheme_id, scheme_config_df):
    queries = []
    scheme_names = []
    
    for idx, row in scheme_config_df.iterrows():
        # Use the correct column name 'additional_scheme_index'
        scheme_index = row['additional_scheme_index']
        scheme_type = row['volume_value_based'].strip().lower()
        scheme_number = row['scheme_number'] if pd.notnull(row['scheme_number']) else scheme_id
        
        if scheme_index == 'MAINSCHEME':
            if scheme_type == 'value':
                template = TRACKER_MAINSCHEME_VALUE
            else:
                template = TRACKER_MAINSCHEME_VOLUME
            # Replace scheme_id placeholder
            query = template.replace('{scheme_id}', scheme_id)
        else:
            if scheme_type == 'value':
                template = TRACKER_ADDITIONAL_SCHEME_VALUE
            else:
                template = TRACKER_ADDITIONAL_SCHEME_VOLUME
            # Replace both placeholders
            query = template.replace('{scheme_id}', scheme_id).replace('{additional_scheme_index}', str(scheme_index))
        
        queries.append(query)
        scheme_names.append(scheme_number)
    
    return queries, scheme_names

def fetch_reward_slabs(conn, scheme_id):
    """
    Fetch reward slab data for ho-scheme type schemes.
    
    Args:
        conn: Database connection
        scheme_id: The scheme ID
        
    Returns:
        DataFrame with slab data or empty DataFrame if no data
    """
    try:
        query = """
        SELECT slab_data->>'slabId' as slab_id,
               slab_data->>'slabFrom' as slab_from,
               slab_data->>'slabTo' as slab_to,
               slab_data->>'schemeReward' as scheme_reward
        FROM schemes_data,
        LATERAL jsonb_array_elements(scheme_json->'mainScheme'->'rewardSlabData') as slab_data
        WHERE scheme_id::TEXT = %s
        ORDER BY (slab_data->>'slabId')::integer;
        """
        
        df = pd.read_sql_query(query, conn, params=[scheme_id])
        
        if not df.empty:
            # Convert slab_from and slab_to to numeric for comparison
            df['slab_from'] = pd.to_numeric(df['slab_from'], errors='coerce')
            df['slab_to'] = pd.to_numeric(df['slab_to'], errors='coerce')
            print(f"\n✅ Fetched {len(df)} reward slabs for scheme_id: {scheme_id}")
            print(df)
        else:
            print(f"\n⚠️ No reward slabs found for scheme_id: {scheme_id}")
            
        return df
        
    except Exception as e:
        print(f"❌ Error fetching reward slabs: {e}")
        return pd.DataFrame()

def calculate_scheme_reward(final_payout, reward_slabs_df):
    """
    Calculate scheme reward based on final payout and reward slabs.
    
    Args:
        final_payout: The scheme final payout amount
        reward_slabs_df: DataFrame with reward slab data
        
    Returns:
        The scheme reward string or empty string if no match
    """
    if pd.isna(final_payout) or final_payout == 0 or reward_slabs_df.empty:
        return ""
    
    # Find the matching slab
    for _, slab in reward_slabs_df.iterrows():
        slab_from = slab['slab_from']
        slab_to = slab['slab_to']
        
        # Check if final_payout falls within this slab range
        if pd.notna(slab_from) and pd.notna(slab_to):
            if slab_from <= final_payout <= slab_to:
                return slab['scheme_reward']
    
    return ""

def insert_tracker_data_to_db(scheme_id, json_data, from_date, to_date, conn):
    """
    Insert or update tracker data in the scheme_tracker_runs table.
    
    Args:
        scheme_id: The scheme ID
        json_data: JSON string of the tracker data
        from_date: Scheme period from date
        to_date: Scheme period to date
        conn: Database connection
    """
    try:
        cur = conn.cursor()
        
        # Check if record exists for this scheme_id
        check_query = """
        SELECT scheme_id FROM scheme_tracker_runs 
        WHERE scheme_id = %s
        """
        cur.execute(check_query, (scheme_id,))
        existing_record = cur.fetchone()
        
        if existing_record:
            # Update existing record
            update_query = """
            UPDATE scheme_tracker_runs 
            SET tracker_data = %s::jsonb,
                from_date = %s,
                to_date = %s,
                run_status = 'completed',
                run_date = CURRENT_DATE,
                updated_at = now()
            WHERE scheme_id = %s
            """
            cur.execute(update_query, (json_data, from_date, to_date, scheme_id))
            print(f"\n✅ Updated tracker data in database for scheme_id: {scheme_id}")
        else:
            # Insert new record
            insert_query = """
            INSERT INTO scheme_tracker_runs (scheme_id, tracker_data, from_date, to_date, run_status)
            VALUES (%s, %s::jsonb, %s, %s, 'completed')
            """
            cur.execute(insert_query, (scheme_id, json_data, from_date, to_date))
            print(f"\n✅ Inserted tracker data in database for scheme_id: {scheme_id}")
        
        conn.commit()
        cur.close()
        
    except Exception as db_error:
        print(f"❌ Error saving tracker data to database: {db_error}")
        conn.rollback()

def clean_dataframe_for_excel(df):
    """Clean the dataframe to prevent Excel corruption issues"""
    # Replace problematic characters that might cause Excel issues
    for col in df.columns:
        if df[col].dtype == object:
            # Replace non-printable characters
            df[col] = df[col].apply(lambda x: ''.join(char for char in str(x) if ord(char) >= 32 or char in ['\n', '\r', '\t']) if pd.notna(x) else x)
    
    # Convert any infinite values to NaN
    df = df.replace([float('inf'), float('-inf')], pd.NA)
    
    # Only fill NaN values for specific columns that should have 0 or empty string
    # Don't fill all columns as this might overwrite actual data
    for col in df.columns:
        # For numeric columns that should have 0 for missing values
        if col.startswith(("Payout", "Bonus", "Phasing", "MP", "Mandatory")) and df[col].dtype in ['int64', 'float64']:
            df[col] = df[col].fillna(0)
        # For text columns that should have empty string for missing values
        elif df[col].dtype == object and col not in ['credit_account']:
            df[col] = df[col].fillna('')
    
    return df

def calculate_scheme_final_payout(row, main_scheme_type, additional_scheme_config_rows, bonus_schemes_count):
    """
    Calculate the Scheme Final Payout for a credit account based on complex conditions.
    
    Args:
        row: A row from the dataframe representing a credit account
        main_scheme_type: The scheme type of the main scheme
        additional_scheme_config_rows: List of tuples (original_index, scheme_row, query_index) for additional schemes
        bonus_schemes_count: Number of bonus schemes enabled
        
    Returns:
        The calculated Scheme Final Payout value
    """
    total = 0
    
    # If main scheme type is 'inbuilt', return 0
    if main_scheme_type == 'inbuilt':
        return 0
    
    # Check mandatory schemes and their % achieved for all scheme types
    # Note: % values are in decimal format (0.85 = 85%), so we compare against 1.0 (100%)
    # For main scheme
    if row.get("Mandatory Qualify") == "Yes":
        achieved = row.get("% Achieved")
        if achieved is None or achieved < 1.0:
            return 0
    
    # For each additional scheme
    for original_idx, scheme_row, query_index in additional_scheme_config_rows:
        suffix = f"_p{query_index + 1}"
        mandatory_qualify_col = f"Mandatory Qualify{suffix}"
        if row.get(mandatory_qualify_col) == "Yes":
            achieved_col = f"% Achieved{suffix}"
            achieved = row.get(achieved_col)
            if achieved is None or achieved < 1.0:
                return 0
    
    # If we passed all mandatory checks, calculate the total payout
    
    # For ho-scheme, add bonus payouts from main scheme
    if main_scheme_type == 'ho-scheme':
        # Add bonus payouts for each enabled bonus scheme (1 to bonus_schemes_count)
        for i in range(1, bonus_schemes_count+1):
            bonus_payout_col = f"Bonus Scheme {i} Bonus Payout"
            bonus_mp_payout_col = f"Bonus Scheme {i} MP Payout"
            if bonus_payout_col in row:
                total += row[bonus_payout_col]
            if bonus_mp_payout_col in row:
                total += row[bonus_mp_payout_col]
    
    # For non-inbuilt schemes (including ho-scheme and others) we add base payouts
    if main_scheme_type != 'inbuilt':
        # Add base payouts from main scheme
        base_payout_cols = ["Total Payout", "MP Final Payout", "FINAL PHASING PAYOUT"]
        for col in base_payout_cols:
            if col in row:
                total += row[col]
        
        # Add payouts from each additional scheme
        for original_idx, scheme_row, query_index in additional_scheme_config_rows:
            suffix = f"_p{query_index + 1}"
            additional_payout_cols = [f"Total Payout{suffix}", f"MP Final Payout{suffix}", f"FINAL PHASING PAYOUT{suffix}"]
            for col in additional_payout_cols:
                if col in row:
                    total += row[col]
    
    return total

def run_multiple_queries_and_combine(function_queries, scheme_names, scheme_config_df):
    try:
        overall_start_time = time.time()
        logger.info(f"🚀 Starting tracker execution with {len(function_queries)} queries")
        
        # Execute queries in parallel
        query_results, parallel_exec_time = run_multiple_queries_parallel(function_queries, scheme_names)
        
        logger.info(f"🔄 Processing query results and combining data...")
        processing_start_time = time.time()
        
        aligned_tables = []
        all_credit_accounts = set()
        
        for idx, df in enumerate(query_results):
            scheme_name = scheme_names[idx]
            
            if df.empty:
                logger.warning(f"⚠️ Query {idx+1} returned no results, skipping")
                continue
                
            logger.info(f"📊 Processing Query {idx+1} result: {len(df)} rows, {len(df.columns)} columns")
            
            if 'credit_account' in df.columns:
                credit_accounts_count = len(df['credit_account'].dropna().unique())
                all_credit_accounts.update(df['credit_account'].dropna().unique())
                logger.info(f"   - Found {credit_accounts_count} unique credit accounts")
                
            if idx == 0:
                # Keep all columns for main scheme (we'll remove columns later based on conditions)
                logger.info(f"   - Main scheme data processed")
            else:
                # For additional schemes, rename columns first, then add scheme name
                original_columns = len(df.columns)
                df = df.rename(columns={col: f"{col}_p{idx}" if col != 'credit_account' else col for col in df.columns})
                df.insert(0, f"SCHEME_NAME_p{idx}", scheme_name)
                logger.info(f"   - Additional scheme data processed: {original_columns} columns renamed with suffix '_p{idx}'")
            
            aligned_tables.append(df)
        
        # Create merged dataframe with all credit accounts
        logger.info(f"🔄 Creating merged dataframe with {len(all_credit_accounts)} unique credit accounts")
        merge_start_time = time.time()
        merged_df = pd.DataFrame({'credit_account': list(all_credit_accounts)})
        
        # Merge all tables
        for idx, df in enumerate(aligned_tables):
            logger.info(f"   - Merging table {idx+1} with {len(df)} rows")
            merged_df = pd.merge(merged_df, df, on='credit_account', how='outer')
        
        merge_time = time.time() - merge_start_time
        logger.info(f"✅ Merge completed in {merge_time:.2f}s: Final dataframe has {len(merged_df)} rows, {len(merged_df.columns)} columns")
        
        # Sort by presence (number of non-null values)
        logger.info(f"🔄 Sorting dataframe by data presence...")
        sort_start_time = time.time()
        merged_df['presence_count'] = merged_df.notna().sum(axis=1)
        merged_df = merged_df.sort_values(by='presence_count', ascending=False).drop(columns='presence_count')
        sort_time = time.time() - sort_start_time
        logger.info(f"✅ Sorting completed in {sort_time:.2f}s")
        
        # Only fill specific columns with 0, not the entire dataframe
        # This preserves actual values while handling missing values appropriately
        logger.info(f"🔄 Processing numeric columns and filling missing values...")
        fill_start_time = time.time()
        numeric_cols = merged_df.select_dtypes(include=['int64', 'float64']).columns
        filled_cols_count = 0
        for col in numeric_cols:
            # Only fill columns that are likely to be financial/numeric metrics
            if col.startswith(("Payout", "Bonus", "Phasing", "MP", "Mandatory", "Growth", "Target", "Value", "%", "Achieved")):
                null_count_before = merged_df[col].isnull().sum()
                merged_df[col] = merged_df[col].fillna(0)
                if null_count_before > 0:
                    filled_cols_count += 1
        fill_time = time.time() - fill_start_time
        logger.info(f"✅ Filled missing values in {filled_cols_count} numeric columns in {fill_time:.2f}s")
        
        # Fill-down for additional_scheme_index_pX columns
        for col in merged_df.columns:
            if col.startswith("additional_scheme_index_p"):
                merged_df[col] = merged_df[col].replace(0, pd.NA).fillna(method='ffill')
        
        # Check main scheme type and remove columns based on conditions
        main_scheme_row = scheme_config_df[scheme_config_df['additional_scheme_index'] == 'MAINSCHEME']
        if not main_scheme_row.empty:
            main_scheme_type = main_scheme_row['scheme_type'].values[0]
            bonus_schemes_count = main_scheme_row['bonus_schemes_count'].values[0]
            mandatory_products_enabled = main_scheme_row['mandatory_products_enabled'].values[0]
            payout_products_enabled = main_scheme_row['payout_products_enabled'].values[0]
            has_phasing_periods = main_scheme_row['has_phasing_periods'].values[0]
            phasing_periods_count = main_scheme_row['phasing_periods_count'].values[0]
            phasing_period_1_bonus_enabled = main_scheme_row['phasing_period_1_bonus_enabled'].values[0]
            phasing_period_2_bonus_enabled = main_scheme_row['phasing_period_2_bonus_enabled'].values[0]
            phasing_period_3_bonus_enabled = main_scheme_row['phasing_period_3_bonus_enabled'].values[0]
            base_dates_count = main_scheme_row['base_dates_count'].values[0]
            enable_strata_growth = main_scheme_row['enable_strata_growth'].values[0]
            main_scheme_mandatory_qualify = main_scheme_row['mandatory_qualify'].values[0]
            
            cols_to_remove = []
            
            # First, handle bonus columns removal
            if main_scheme_type != 'ho-scheme':
                # Remove all bonus columns
                cols_to_remove = [col for col in ALL_BONUS_COLUMNS if col in merged_df.columns]
                if cols_to_remove:
                    print(f"\nRemoving {len(cols_to_remove)} bonus columns from main scheme (scheme_type is not 'ho-scheme')")
            else:
                # For ho-scheme, remove columns based on bonus_schemes_count
                if bonus_schemes_count == 0:
                    # Remove all bonus columns
                    cols_to_remove = [col for col in ALL_BONUS_COLUMNS if col in merged_df.columns]
                    if cols_to_remove:
                        print(f"\nRemoving {len(cols_to_remove)} bonus columns from main scheme (ho-scheme with bonus_schemes_count=0)")
                elif bonus_schemes_count == 1:
                    # Remove columns for bonus schemes 2, 3, and 4
                    for i in [2, 3, 4]:
                        cols_to_remove.extend([col for col in BONUS_COLUMNS_BY_INDEX.get(i, []) if col in merged_df.columns])
                    if cols_to_remove:
                        print(f"\nRemoving {len(cols_to_remove)} bonus columns from main scheme (ho-scheme with bonus_schemes_count=1, keeping scheme 1 columns)")
                elif bonus_schemes_count == 2:
                    # Remove columns for bonus schemes 3 and 4
                    for i in [3, 4]:
                        cols_to_remove.extend([col for col in BONUS_COLUMNS_BY_INDEX.get(i, []) if col in merged_df.columns])
                    if cols_to_remove:
                        print(f"\nRemoving {len(cols_to_remove)} bonus columns from main scheme (ho-scheme with bonus_schemes_count=2, keeping scheme 1 and 2 columns)")
                elif bonus_schemes_count == 3:
                    # Remove columns for bonus scheme 4
                    cols_to_remove = [col for col in BONUS_COLUMNS_BY_INDEX.get(4, []) if col in merged_df.columns]
                    if cols_to_remove:
                        print(f"\nRemoving {len(cols_to_remove)} bonus columns from main scheme (ho-scheme with bonus_schemes_count=3, keeping scheme 1, 2, and 3 columns)")
                elif bonus_schemes_count == 4:
                    # Don't remove any bonus columns
                    print("\nKeeping all bonus columns from main scheme (ho-scheme with bonus_schemes_count=4)")
            
            # Remove the identified bonus columns
            if cols_to_remove:
                merged_df.drop(columns=cols_to_remove, inplace=True)
            
            # Then, handle mandatory product columns removal for main scheme
            if not mandatory_products_enabled:
                main_mandatory_cols = [col for col in MAIN_MANDATORY_COLUMNS if col in merged_df.columns]
                if main_mandatory_cols:
                    print(f"\nRemoving {len(main_mandatory_cols)} mandatory product columns from main scheme (mandatory_products_enabled=false)")
                    merged_df.drop(columns=main_mandatory_cols, inplace=True)
            
            # Then, handle payout product columns removal for main scheme
            if not payout_products_enabled:
                main_payout_cols = [col for col in MAIN_PAYOUT_COLUMNS if col in merged_df.columns]
                if main_payout_cols:
                    print(f"\nRemoving {len(main_payout_cols)} payout product columns from main scheme (payout_products_enabled=false)")
                    merged_df.drop(columns=main_payout_cols, inplace=True)
            
            # Then, handle phasing columns removal for main scheme
            if not has_phasing_periods:
                # Remove all phasing columns
                main_phasing_cols = [col for col in ALL_PHASING_COLUMNS if col in merged_df.columns]
                if main_phasing_cols:
                    print(f"\nRemoving {len(main_phasing_cols)} phasing columns from main scheme (has_phasing_periods=false)")
                    merged_df.drop(columns=main_phasing_cols, inplace=True)
            else:
                # For each period, determine which columns to keep
                cols_to_keep = []
                
                # Always keep common phasing columns
                cols_to_keep.extend([col for col in PHASING_COLUMNS["common"] if col in merged_df.columns])
                
                # Period 1 - always keep regular columns, keep bonus if enabled
                cols_to_keep.extend([col for col in PHASING_COLUMNS["period_1"]["regular"] if col in merged_df.columns])
                if phasing_period_1_bonus_enabled:
                    cols_to_keep.extend([col for col in PHASING_COLUMNS["period_1"]["bonus"] if col in merged_df.columns])
                
                # Period 2 - keep if phasing_periods_count >= 2
                if phasing_periods_count >= 2:
                    cols_to_keep.extend([col for col in PHASING_COLUMNS["period_2"]["regular"] if col in merged_df.columns])
                    if phasing_period_2_bonus_enabled:
                        cols_to_keep.extend([col for col in PHASING_COLUMNS["period_2"]["bonus"] if col in merged_df.columns])
                
                # Period 3 - keep if phasing_periods_count >= 3
                if phasing_periods_count >= 3:
                    cols_to_keep.extend([col for col in PHASING_COLUMNS["period_3"]["regular"] if col in merged_df.columns])
                    if phasing_period_3_bonus_enabled:
                        cols_to_keep.extend([col for col in PHASING_COLUMNS["period_3"]["bonus"] if col in merged_df.columns])
                
                # Determine which phasing columns to remove
                all_phasing_cols_in_df = [col for col in ALL_PHASING_COLUMNS if col in merged_df.columns]
                phasing_cols_to_remove = [col for col in all_phasing_cols_in_df if col not in cols_to_keep]
                
                if phasing_cols_to_remove:
                    print(f"\nRemoving {len(phasing_cols_to_remove)} phasing columns from main scheme (has_phasing_periods=true, but some periods or bonuses disabled)")
                    merged_df.drop(columns=phasing_cols_to_remove, inplace=True)
            
            # Then, handle base dates columns removal for main scheme
            if base_dates_count == 1:
                # Remove all base dates columns
                base_dates_cols = [col for col in BASE_DATES_REMOVE_IF_COUNT_1 if col in merged_df.columns]
                if base_dates_cols:
                    print(f"\nRemoving {len(base_dates_cols)} base dates columns from main scheme (base_dates_count=1)")
                    merged_df.drop(columns=base_dates_cols, inplace=True)
            elif base_dates_count == 2:
                # Remove only specific base dates columns
                base_dates_cols = [col for col in BASE_DATES_REMOVE_IF_COUNT_2 if col in merged_df.columns]
                if base_dates_cols:
                    print(f"\nRemoving {len(base_dates_cols)} base dates columns from main scheme (base_dates_count=2, keeping Final columns)")
                    merged_df.drop(columns=base_dates_cols, inplace=True)
            
            # Then, handle strata growth columns for main scheme
            if enable_strata_growth:
                # Remove growth_rate column if it exists
                if 'growth_rate' in merged_df.columns:
                    print(f"\nRemoving growth_rate column from main scheme (enable_strata_growth=true)")
                    merged_df.drop(columns=['growth_rate'], inplace=True)
            else:
                # Remove Strata Growth % column if it exists
                if 'Strata Growth %' in merged_df.columns:
                    print(f"\nRemoving Strata Growth % column from main scheme (enable_strata_growth=false)")
                    merged_df.drop(columns=['Strata Growth %'], inplace=True)
        
        # Handle mandatory product, payout product, phasing, strata growth, and base dates columns removal for additional schemes
        additional_scheme_rows = []
        for idx, row in scheme_config_df.iterrows():
            if row['additional_scheme_index'] != 'MAINSCHEME':
                additional_scheme_rows.append((idx, row))
        
        for idx, row in additional_scheme_rows:
            # Get the position of this additional scheme in the queries (which determines the _p suffix)
            # The main scheme is at index 0, so additional schemes start at index 1
            query_index = next((i for i, (orig_idx, _) in enumerate(additional_scheme_rows) if orig_idx == idx), None)
            if query_index is not None:
                # The suffix should match what was created during dataframe processing: _p1, _p2, etc.
                suffix = f"_p{query_index + 1}"
                
                # Remove specified columns for additional schemes
                for base_col in ADDITIONAL_SCHEME_REMOVE_COLUMNS:
                    col_name = base_col + suffix
                    if col_name in merged_df.columns:
                        print(f"\nRemoving {col_name} column for additional scheme {query_index + 1}")
                        merged_df.drop(columns=[col_name], inplace=True)
                
                # Handle mandatory product columns
                if not row['mandatory_products_enabled']:
                    # Generate column names for this additional scheme
                    additional_mandatory_cols = [base_col + suffix for base_col in ADDITIONAL_MANDATORY_BASE_COLUMNS]
                    # Filter to only include columns that exist in the dataframe
                    existing_mandatory_cols = [col for col in additional_mandatory_cols if col in merged_df.columns]
                    
                    if existing_mandatory_cols:
                        print(f"\nRemoving {len(existing_mandatory_cols)} mandatory product columns for additional scheme {query_index + 1} (mandatory_products_enabled=false)")
                        merged_df.drop(columns=existing_mandatory_cols, inplace=True)
                
                # Handle payout product columns
                if not row['payout_products_enabled']:
                    # Generate column names for this additional scheme
                    additional_payout_cols = [base_col + suffix for base_col in ADDITIONAL_PAYOUT_BASE_COLUMNS]
                    # Filter to only include columns that exist in the dataframe
                    existing_payout_cols = [col for col in additional_payout_cols if col in merged_df.columns]
                    
                    if existing_payout_cols:
                        print(f"\nRemoving {len(existing_payout_cols)} payout product columns for additional scheme {query_index + 1} (payout_products_enabled=false)")
                        merged_df.drop(columns=existing_payout_cols, inplace=True)
                
                # Handle phasing columns
                has_phasing_periods = row['has_phasing_periods']
                phasing_periods_count = row['phasing_periods_count']
                phasing_period_1_bonus_enabled = row['phasing_period_1_bonus_enabled']
                phasing_period_2_bonus_enabled = row['phasing_period_2_bonus_enabled']
                phasing_period_3_bonus_enabled = row['phasing_period_3_bonus_enabled']
                
                if not has_phasing_periods:
                    # Remove all phasing columns for this additional scheme
                    additional_phasing_cols = [base_col + suffix for base_col in ALL_PHASING_COLUMNS]
                    # Filter to only include columns that exist in the dataframe
                    existing_phasing_cols = [col for col in additional_phasing_cols if col in merged_df.columns]
                    
                    if existing_phasing_cols:
                        print(f"\nRemoving {len(existing_phasing_cols)} phasing columns for additional scheme {query_index + 1} (has_phasing_periods=false)")
                        merged_df.drop(columns=existing_phasing_cols, inplace=True)
                else:
                    # For each period, determine which columns to keep
                    cols_to_keep = []
                    
                    # Always keep common phasing columns
                    cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["common"] if base_col + suffix in merged_df.columns])
                    
                    # Period 1 - always keep regular columns, keep bonus if enabled
                    cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_1"]["regular"] if base_col + suffix in merged_df.columns])
                    if phasing_period_1_bonus_enabled:
                        cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_1"]["bonus"] if base_col + suffix in merged_df.columns])
                    
                    # Period 2 - keep if phasing_periods_count >= 2
                    if phasing_periods_count >= 2:
                        cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_2"]["regular"] if base_col + suffix in merged_df.columns])
                        if phasing_period_2_bonus_enabled:
                            cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_2"]["bonus"] if base_col + suffix in merged_df.columns])
                    
                    # Period 3 - keep if phasing_periods_count >= 3
                    if phasing_periods_count >= 3:
                        cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_3"]["regular"] if base_col + suffix in merged_df.columns])
                        if phasing_period_3_bonus_enabled:
                            cols_to_keep.extend([base_col + suffix for base_col in PHASING_COLUMNS["period_3"]["bonus"] if base_col + suffix in merged_df.columns])
                    
                    # Determine which phasing columns to remove
                    all_phasing_cols_in_df = [base_col + suffix for base_col in ALL_PHASING_COLUMNS if base_col + suffix in merged_df.columns]
                    phasing_cols_to_remove = [col for col in all_phasing_cols_in_df if col not in cols_to_keep]
                    
                    if phasing_cols_to_remove:
                        print(f"\nRemoving {len(phasing_cols_to_remove)} phasing columns for additional scheme {query_index + 1} (has_phasing_periods=true, but some periods or bonuses disabled)")
                        merged_df.drop(columns=phasing_cols_to_remove, inplace=True)
                
                # Handle strata growth columns for additional schemes
                enable_strata_growth = row['enable_strata_growth']
                if enable_strata_growth:
                    # Remove growth_rate column if it exists
                    growth_rate_col = f'growth_rate{suffix}'
                    if growth_rate_col in merged_df.columns:
                        print(f"\nRemoving {growth_rate_col} column for additional scheme {query_index + 1} (enable_strata_growth=true)")
                        merged_df.drop(columns=[growth_rate_col], inplace=True)
                else:
                    # Remove Strata Growth % column if it exists
                    strata_growth_col = f'Strata Growth %{suffix}'
                    if strata_growth_col in merged_df.columns:
                        print(f"\nRemoving {strata_growth_col} column for additional scheme {query_index + 1} (enable_strata_growth=false)")
                        merged_df.drop(columns=[strata_growth_col], inplace=True)
                
                # Handle base dates columns for additional schemes
                base_dates_count = row['base_dates_count']
                if base_dates_count == 1:
                    # Remove all base dates columns for this additional scheme
                    base_dates_cols = [base_col + suffix for base_col in BASE_DATES_REMOVE_IF_COUNT_1 if base_col + suffix in merged_df.columns]
                    if base_dates_cols:
                        print(f"\nRemoving {len(base_dates_cols)} base dates columns for additional scheme {query_index + 1} (base_dates_count=1)")
                        merged_df.drop(columns=base_dates_cols, inplace=True)
                elif base_dates_count == 2:
                    # Remove only specific base dates columns for this additional scheme
                    base_dates_cols = [base_col + suffix for base_col in BASE_DATES_REMOVE_IF_COUNT_2 if base_col + suffix in merged_df.columns]
                    if base_dates_cols:
                        print(f"\nRemoving {len(base_dates_cols)} base dates columns for additional scheme {query_index + 1} (base_dates_count=2, keeping Final columns)")
                        merged_df.drop(columns=base_dates_cols, inplace=True)
        
        # Add Scheme ID, Scheme Name, Scheme Type, Period dates and Mandatory Qualify columns for main scheme
        if not main_scheme_row.empty:
            # Get scheme name and period dates from the first row
            scheme_name = main_scheme_row['scheme_number'].values[0]
            scheme_period_from = main_scheme_row['scheme_period_from'].values[0]
            scheme_period_to = main_scheme_row['scheme_period_to'].values[0]
            
            # Insert after credit_account (position 1)
            merged_df.insert(1, "Scheme ID", scheme_id)
            merged_df.insert(2, "Scheme Name", scheme_name)
            merged_df.insert(3, "Scheme Type", main_scheme_type)
            merged_df.insert(4, "Scheme Period From", scheme_period_from)
            merged_df.insert(5, "Scheme Period To", scheme_period_to)
            merged_df.insert(6, "Mandatory Qualify", "Yes" if main_scheme_mandatory_qualify else "No")
        
        # Add Scheme Type and Mandatory Qualify columns for each additional scheme
        additional_scheme_config_rows = []  # This will be used for the final calculation
        for i, (idx, row) in enumerate(additional_scheme_rows):
            # The suffix matches what was created during dataframe processing: _p1, _p2, etc.
            # i is the position in additional_scheme_rows (0, 1, 2, ...) 
            # But the actual suffix used in column names is _p{i+1} because main scheme is at index 0
            actual_query_idx = i + 1  # This matches the idx used in the main query loop
            suffix = f"_p{actual_query_idx}"
            
            # Find the position of "SCHEME_NAME_p{actual_query_idx}" column
            scheme_name_col = f"SCHEME_NAME{suffix}"
            if scheme_name_col in merged_df.columns:
                pos = merged_df.columns.get_loc(scheme_name_col)
                # Insert only Mandatory Qualify column after this position (no Scheme Type for additional schemes)
                merged_df.insert(pos+1, f"Mandatory Qualify{suffix}", "Yes" if row['mandatory_qualify'] else "No")
                
                # Add to additional_scheme_config_rows for final calculation
                # Use i as the query_index since it represents the position in the additional schemes list
                additional_scheme_config_rows.append((idx, row, i))
        
        # Add Scheme Final Payout column at the end
        merged_df["Scheme Final Payout"] = merged_df.apply(
            lambda row: calculate_scheme_final_payout(row, main_scheme_type, additional_scheme_config_rows, bonus_schemes_count),
            axis=1
        )
        
        # Add Scheme Reward column based on scheme type
        if main_scheme_type == 'ho-scheme':
            # For ho-scheme, fetch reward slabs and calculate scheme reward
            print(f"\n🔍 Fetching reward slabs for ho-scheme (scheme_id: {scheme_id})")
            reward_slabs_df = fetch_reward_slabs(conn, scheme_id)
            
            if not reward_slabs_df.empty:
                merged_df["Scheme Reward"] = merged_df["Scheme Final Payout"].apply(
                    lambda payout: calculate_scheme_reward(payout, reward_slabs_df)
                )
                print(f"✅ Added Scheme Reward column based on reward slabs")
            else:
                # If no reward slabs found, set empty string
                merged_df["Scheme Reward"] = ""
                print(f"⚠️ No reward slabs found, setting Scheme Reward to empty")
        else:
            # For non-ho-scheme, use "Credit Note Rs. " + Scheme Final Payout
            merged_df["Scheme Reward"] = merged_df["Scheme Final Payout"].apply(
                lambda payout: f"Credit Note Rs. {payout}" if pd.notna(payout) and payout != 0 else ""
            )
            print(f"✅ Added Scheme Reward column with 'Credit Note Rs.' prefix for non-ho-scheme")
        
        
        if merged_df.empty:
            print("No data returned from any queries.")
            return
            
        # Clean the dataframe to prevent Excel corruption
        merged_df = clean_dataframe_for_excel(merged_df)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"tracker_credit_accounts_{timestamp}.xlsx"
        csv_filename = f"tracker_credit_accounts_{timestamp}.csv"
        json_filename = f"tracker_credit_accounts_{timestamp}.json"
        
        try:
            # # First try to save as CSV as a backup
            # merged_df.to_csv(csv_filename, index=False)
            # print(f"\n✅ Tracker results saved to CSV: {csv_filename}")
            
            # # Then try to save as Excel with xlsxwriter engine
            # merged_df.to_excel(excel_filename, index=False, engine='xlsxwriter')
            # print(f"\n✅ Tracker results saved to Excel: {excel_filename}")
            
            # Create ordered JSON manually to preserve column order
            import collections
            
            # Get column names in exact order from dataframe
            column_order = list(merged_df.columns)
            
            # Create ordered records manually
            ordered_records = []
            for _, row in merged_df.iterrows():
                ordered_record = collections.OrderedDict()
                for col in column_order:
                    value = row[col]
                    # Handle NaN and infinity values
                    if pd.isna(value):
                        ordered_record[col] = None
                    elif value == float('inf'):
                        ordered_record[col] = "Infinity"
                    elif value == float('-inf'):
                        ordered_record[col] = "-Infinity"
                    else:
                        ordered_record[col] = value
                ordered_records.append(ordered_record)
            
            # Convert to JSON string with preserved order
            json_data = json.dumps(ordered_records, indent=4, ensure_ascii=False)
            
            # # Save JSON file
            # with open(json_filename, 'w', encoding='utf-8') as json_file:
            #     json_file.write(json_data)
            # print(f"\n✅ Tracker results saved to JSON: {json_filename}")
            
            print(f"Total rows: {len(merged_df)}")
            print(f"Total columns: {len(merged_df.columns)}")
            print(f"Column order preserved: {column_order[:5]}..." if len(column_order) > 5 else f"Column order: {column_order}")
            
            # Insert/Update tracker data in database using the same ordered JSON data
            # Get scheme period dates from main scheme configuration
            logger.info(f"🔄 Saving tracker data to database...")
            db_save_start_time = time.time()
            scheme_period_from = main_scheme_row['scheme_period_from'].values[0] if not main_scheme_row.empty else None
            scheme_period_to = main_scheme_row['scheme_period_to'].values[0] if not main_scheme_row.empty else None
            
            # Create new connection for database operations
            conn = psycopg2.connect(**db_params)
            insert_tracker_data_to_db(scheme_id, json_data, scheme_period_from, scheme_period_to, conn)
            conn.close()
            
            db_save_time = time.time() - db_save_start_time
            total_processing_time = time.time() - processing_start_time
            overall_time = time.time() - overall_start_time
            
            # Final summary logs
            logger.info(f"✅ Database save completed in {db_save_time:.2f}s")
            logger.info(f"🎯 Tracker execution completed successfully!")
            logger.info(f"📊 Final Summary:")
            logger.info(f"   - Total execution time: {overall_time:.2f}s")
            logger.info(f"   - Parallel query execution: {parallel_exec_time:.2f}s")
            logger.info(f"   - Data processing: {total_processing_time:.2f}s")
            logger.info(f"   - Database save: {db_save_time:.2f}s")
            logger.info(f"   - Final dataset: {len(merged_df)} rows, {len(merged_df.columns)} columns")
            
        except Exception as save_error:
            logger.error(f"❌ Error during processing: {save_error}")
            logger.info("Displaying first 5 rows in terminal:")
            print(merged_df.head())
        
    except Exception as e:
        overall_time = time.time() - overall_start_time
        logger.error(f"❌ Error during tracker execution after {overall_time:.2f}s: {e}")

if __name__ == "__main__":
    scheme_id = input("Enter Scheme ID: ").strip()
    try:
        conn = psycopg2.connect(**db_params)
        scheme_config_df = fetch_scheme_config(conn, scheme_id)
        conn.close()
        
        if scheme_config_df.empty:
            print("No scheme data returned for this scheme_id.")
        else:
            queries, scheme_names = build_queries_from_templates(scheme_id, scheme_config_df)
            run_multiple_queries_and_combine(queries, scheme_names, scheme_config_df)
    except Exception as e:
        print(f"❌ Error fetching scheme configuration: {e}")