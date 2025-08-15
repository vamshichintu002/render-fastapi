-- =============================================================================
-- STEP 3: SAFE QUERY EXECUTION PERFORMANCE SETTINGS
-- Compatible with managed databases (Supabase, AWS RDS, etc.)
-- Only includes settings that don't require superuser permissions
-- =============================================================================

-- =============================================================================
-- SESSION-LEVEL PERFORMANCE OPTIMIZATIONS (Safe for all users)
-- =============================================================================

-- Memory settings for complex queries (adjust based on available memory)
SET work_mem = '256MB';                    -- Increase memory for sorting/hashing
SET temp_buffers = '128MB';               -- Temporary table buffer

-- Cost settings optimized for SSD storage
SET random_page_cost = 1.1;               -- Optimize for SSD storage
SET seq_page_cost = 1.0;                  -- Sequential scan cost
SET cpu_tuple_cost = 0.01;                -- CPU processing cost
SET cpu_index_tuple_cost = 0.005;         -- Index tuple processing cost
SET cpu_operator_cost = 0.0025;           -- Operator execution cost

-- =============================================================================
-- PARALLEL PROCESSING SETTINGS (if available)
-- =============================================================================

-- Enable parallel query execution (may be limited in managed databases)
SET max_parallel_workers_per_gather = 2;  -- Use parallel workers (conservative)
SET parallel_tuple_cost = 0.1;           -- Cost of parallel tuple processing
SET parallel_setup_cost = 1000;          -- Setup cost for parallel queries

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

-- Hash table memory settings (if available)
-- SET hash_mem_multiplier = 2.0;           -- May not be available in managed databases

-- =============================================================================
-- JIT COMPILATION FOR COMPLEX EXPRESSIONS (if available)
-- =============================================================================

-- Enable Just-In-Time compilation for complex queries
-- Note: JIT may not be available in all managed database environments
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

-- =============================================================================
-- QUERY PLANNER SETTINGS
-- =============================================================================

-- Optimize query planning for complex queries
SET from_collapse_limit = 12;            -- Increase FROM list collapse limit
SET join_collapse_limit = 12;            -- Increase JOIN collapse limit
SET geqo_threshold = 15;                 -- Genetic query optimization threshold

-- =============================================================================
-- VERIFY CURRENT SETTINGS
-- =============================================================================

-- Check that settings are applied correctly
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN (
    'work_mem', 
    'temp_buffers',
    'enable_hashjoin', 
    'enable_nestloop', 
    'jit',
    'random_page_cost',
    'max_parallel_workers_per_gather'
)
ORDER BY name;

-- =============================================================================
-- CHECK WHAT SETTINGS ARE AVAILABLE
-- =============================================================================

-- Check which settings you can modify (useful for troubleshooting)
SELECT 
    name, 
    setting, 
    context,
    CASE 
        WHEN context = 'user' THEN 'You can change this'
        WHEN context = 'superuser' THEN 'Requires superuser'
        WHEN context = 'postmaster' THEN 'Requires restart'
        ELSE context
    END as permission_level
FROM pg_settings 
WHERE name IN (
    'work_mem', 'maintenance_work_mem', 'effective_cache_size', 'temp_buffers',
    'max_parallel_workers_per_gather', 'hash_mem_multiplier', 'jit',
    'track_activities', 'track_functions', 'track_counts', 'track_io_timing'
)
ORDER BY context, name;

-- =============================================================================
-- YOUR QUERY EXECUTION READY
-- =============================================================================

-- Now you can execute your TRACKER_MAINSCHEME_VALUE query
-- The settings above will optimize its performance within permission limits

-- =============================================================================
-- ERROR HANDLING NOTES
-- =============================================================================

-- If you get "permission denied" errors, comment out those specific SET commands
-- The most important settings for performance are:
-- 1. work_mem (memory for operations)
-- 2. enable_nestloop = off (avoid slow nested loops)
-- 3. enable_hashjoin = on (fast joins)
-- 4. random_page_cost and seq_page_cost (SSD optimization)

-- =============================================================================
-- POST-EXECUTION: RESET SETTINGS (OPTIONAL)
-- =============================================================================

-- Uncomment these if you want to reset to defaults after query execution
/*
RESET work_mem;
RESET temp_buffers;
RESET enable_nestloop;
RESET random_page_cost;
RESET jit;
*/