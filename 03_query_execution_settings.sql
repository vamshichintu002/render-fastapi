-- =============================================================================
-- STEP 3: QUERY EXECUTION PERFORMANCE SETTINGS
-- Run these settings before executing your main query
-- These can all run inside transaction blocks
-- =============================================================================

-- =============================================================================
-- SESSION-LEVEL PERFORMANCE OPTIMIZATIONS
-- =============================================================================

-- Memory settings for complex queries
SET work_mem = '512MB';                    -- Increase memory for sorting/hashing
SET maintenance_work_mem = '1GB';          -- For index operations
SET effective_cache_size = '4GB';          -- Tell PostgreSQL about available cache
SET temp_buffers = '256MB';               -- Temporary table buffer

-- Cost settings optimized for SSD storage
SET random_page_cost = 1.1;               -- Optimize for SSD storage
SET seq_page_cost = 1.0;                  -- Sequential scan cost
SET cpu_tuple_cost = 0.01;                -- CPU processing cost
SET cpu_index_tuple_cost = 0.005;         -- Index tuple processing cost
SET cpu_operator_cost = 0.0025;           -- Operator execution cost

-- =============================================================================
-- PARALLEL PROCESSING SETTINGS
-- =============================================================================

-- Enable parallel query execution
SET max_parallel_workers_per_gather = 4;  -- Use parallel workers
SET parallel_tuple_cost = 0.1;           -- Cost of parallel tuple processing
SET parallel_setup_cost = 1000;          -- Setup cost for parallel queries
SET min_parallel_table_scan_size = '128MB'; -- Minimum table size for parallel scan
SET min_parallel_index_scan_size = '64MB';   -- Minimum index size for parallel scan

-- =============================================================================
-- JOIN AND SCAN METHOD OPTIMIZATIONS
-- =============================================================================

-- Enable optimal join methods for large datasets
SET enable_hashjoin = on;                 -- Enable hash joins (fastest for large datasets)
SET enable_mergejoin = on;                -- Enable merge joins
SET enable_nestloop = off;                -- Disable nested loops for large datasets
SET enable_material = on;                 -- Enable materialization
SET enable_sort = on;                     -- Enable sorting
SET enable_indexscan = on;                -- Enable index scans
SET enable_indexonlyscan = on;            -- Enable index-only scans
SET enable_bitmapscan = on;               -- Enable bitmap scans

-- =============================================================================
-- MEMORY SETTINGS FOR LARGE OPERATIONS
-- =============================================================================

-- Hash table memory settings
SET hash_mem_multiplier = 2.0;           -- Increase hash table memory
SET logical_decoding_work_mem = '64MB';   -- For logical operations

-- =============================================================================
-- JIT COMPILATION FOR COMPLEX EXPRESSIONS
-- =============================================================================

-- Enable Just-In-Time compilation for complex queries
SET jit = on;                             -- Enable JIT compilation
SET jit_above_cost = 100000;             -- JIT threshold
SET jit_inline_above_cost = 500000;      -- JIT inlining threshold
SET jit_optimize_above_cost = 500000;    -- JIT optimization threshold

-- =============================================================================
-- TIMEOUT SETTINGS
-- =============================================================================

-- Adjust timeouts for long-running queries
SET statement_timeout = '30min';          -- 30-minute timeout
SET lock_timeout = '5min';                -- Lock timeout
SET idle_in_transaction_session_timeout = '10min'; -- Idle transaction timeout

-- =============================================================================
-- QUERY PLANNER SETTINGS
-- =============================================================================

-- Optimize query planning for complex queries
SET from_collapse_limit = 12;            -- Increase FROM list collapse limit
SET join_collapse_limit = 12;            -- Increase JOIN collapse limit
SET geqo_threshold = 15;                 -- Genetic query optimization threshold
SET constraint_exclusion = 'partition';  -- Enable constraint exclusion

-- =============================================================================
-- MONITORING AND LOGGING SETTINGS
-- =============================================================================

-- Enable query execution tracking
SET track_activities = on;
SET track_counts = on;
SET track_functions = 'all';
SET track_io_timing = on;

-- Log slow queries for analysis (optional)
-- SET log_min_duration_statement = 60000;  -- Log queries taking more than 1 minute

-- =============================================================================
-- VERIFY CURRENT SETTINGS
-- =============================================================================

-- Check that settings are applied correctly
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN (
    'work_mem', 
    'effective_cache_size', 
    'max_parallel_workers_per_gather',
    'enable_hashjoin', 
    'enable_nestloop', 
    'jit',
    'random_page_cost',
    'hash_mem_multiplier'
)
ORDER BY name;

-- =============================================================================
-- YOUR QUERY EXECUTION READY
-- =============================================================================

-- Now you can execute your TRACKER_MAINSCHEME_VALUE query
-- The settings above will optimize its performance

-- Example:
-- Your original query here...
-- TRACKER_MAINSCHEME_VALUE = """
-- SET SESSION statement_timeout = 0;
-- WITH scheme AS (...)
-- """

-- =============================================================================
-- POST-EXECUTION: RESET SETTINGS (OPTIONAL)
-- =============================================================================

-- Uncomment these if you want to reset to defaults after query execution
/*
RESET work_mem;
RESET maintenance_work_mem;
RESET effective_cache_size;
RESET max_parallel_workers_per_gather;
RESET enable_nestloop;
RESET hash_mem_multiplier;
RESET jit;
*/