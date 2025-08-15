-- =============================================================================
-- VOLUME QUERY OPTIMIZER FOR TRACKER_MAINSCHEME_VOLUME
-- Specialized optimization for volume-based calculations and aggregations
-- =============================================================================

-- =============================================================================
-- PRE-EXECUTION: VOLUME-SPECIFIC PERFORMANCE SETTINGS
-- =============================================================================

-- Enhanced memory allocation for volume aggregations (higher than value queries)
SET work_mem = '1.5GB';                    -- Even larger memory for volume calculations
SET temp_buffers = '768MB';               -- Larger temp buffer for volume aggregations
SET maintenance_work_mem = '2GB';         -- For index operations

-- Volume-specific hash optimizations
SET hash_mem_multiplier = 5.0;           -- 5x memory for volume hash operations (higher than value)

-- =============================================================================
-- AGGRESSIVE PARALLEL PROCESSING FOR VOLUME AGGREGATIONS
-- =============================================================================

-- Volume queries benefit more from parallel processing
SET max_parallel_workers_per_gather = 8;  -- More workers for volume aggregations
SET parallel_tuple_cost = 0.005;         -- Even lower cost for volume operations
SET parallel_setup_cost = 300;           -- Lower setup cost for volume
SET min_parallel_table_scan_size = '16MB'; -- Lower threshold for volume data
SET min_parallel_index_scan_size = '8MB';  -- Lower threshold for volume indexes
SET force_parallel_mode = 'regress';     -- Force parallel mode

-- =============================================================================
-- VOLUME AGGREGATION OPTIMIZATIONS
-- =============================================================================

-- Optimize specifically for SUM(volume) operations
SET enable_hashagg = on;                  -- Critical for GROUP BY volume operations
SET enable_sort = on;                     -- Enable sorting for volume aggregations
SET enable_material = on;                -- Force materialization of volume CTEs

-- =============================================================================
-- QUERY PLANNER: OPTIMIZED FOR VOLUME CALCULATIONS
-- =============================================================================

-- Higher limits for complex volume queries (volume queries tend to be more complex)
SET from_collapse_limit = 25;            -- Higher FROM clause limit for volume
SET join_collapse_limit = 25;            -- Higher JOIN limit for volume
SET geqo_threshold = 30;                 -- Higher genetic optimization threshold
SET geqo_effort = 10;                   -- Maximum genetic optimization effort
SET geqo_pool_size = 1500;              -- Larger pool for volume optimization
SET geqo_generations = 1500;            -- More generations for volume optimization

-- =============================================================================
-- JOIN OPTIMIZATIONS FOR VOLUME QUERIES
-- =============================================================================

-- Volume queries often have more complex joins
SET enable_hashjoin = on;                -- Enable hash joins for volume data
SET enable_mergejoin = on;               -- Enable merge joins for sorted volume data
SET enable_nestloop = off;               -- Disable nested loops (volume data is large)
SET enable_indexscan = on;               -- Enable index scans for volume
SET enable_indexonlyscan = on;           -- Enable index-only scans for volume
SET enable_bitmapscan = on;              -- Enable bitmap scans for volume

-- =============================================================================
-- ENHANCED JIT FOR VOLUME CALCULATIONS
-- =============================================================================

-- Volume calculations are compute-intensive, benefit more from JIT
SET jit = on;                            -- Enable JIT for volume arithmetic
SET jit_above_cost = 5000;              -- Lower threshold for volume queries
SET jit_inline_above_cost = 25000;      -- Inline volume calculations earlier
SET jit_optimize_above_cost = 25000;    -- Optimize volume expressions earlier

-- =============================================================================
-- VOLUME-SPECIFIC COST SETTINGS
-- =============================================================================

-- Force index usage even more aggressively for volume queries
SET enable_seqscan = off;               -- Force index usage for volume data
SET random_page_cost = 0.05;           -- Even lower cost for SSD (volume queries)
SET seq_page_cost = 15.0;              -- Higher penalty for sequential scans
SET cpu_tuple_cost = 0.005;            -- Lower CPU cost (volume data is compute-heavy)
SET cpu_index_tuple_cost = 0.002;      -- Very low index cost for volume
SET cpu_operator_cost = 0.001;         -- Very low operator cost for volume arithmetic

-- =============================================================================
-- TIMEOUT SETTINGS FOR VOLUME QUERIES
-- =============================================================================

-- Volume queries may take longer than value queries
SET statement_timeout = '3h';           -- 3 hour timeout (longer than value queries)
SET lock_timeout = '45min';             -- Longer lock timeout for volume
SET idle_in_transaction_session_timeout = '1.5h'; -- Longer idle timeout

-- =============================================================================
-- VOLUME-SPECIFIC DATE AND DATA OPTIMIZATIONS
-- =============================================================================

-- Optimize for volume data operations
SET datestyle = 'ISO, MDY';             -- Consistent date format
SET timezone = 'UTC';                   -- Consistent timezone
SET constraint_exclusion = 'on';        -- Enable constraint exclusion for volume partitions

-- Enhanced statistics for volume queries
SET default_statistics_target = 1500;   -- Higher statistics target for volume
SET cursor_tuple_fraction = 0.01;       -- Optimize for large result sets (volume aggregations)

-- =============================================================================
-- MEMORY AND RESOURCE TUNING FOR VOLUME
-- =============================================================================

-- Optimize memory usage patterns for volume data
SET shared_preload_libraries = 'pg_stat_statements'; -- Enable query tracking (if available)
SET log_min_duration_statement = 30000; -- Log volume queries taking >30 seconds

-- =============================================================================
-- VERIFY VOLUME OPTIMIZATION SETTINGS
-- =============================================================================

-- Check all volume-specific settings
SELECT 
    name, 
    setting, 
    unit,
    CASE 
        WHEN name IN ('work_mem', 'hash_mem_multiplier', 'max_parallel_workers_per_gather') 
        THEN '🚀 CRITICAL for volume queries'
        WHEN name IN ('jit', 'enable_nestloop', 'enable_seqscan', 'enable_hashagg')
        THEN '⚡ VOLUME performance critical'
        WHEN name IN ('parallel_tuple_cost', 'jit_above_cost', 'random_page_cost')
        THEN '📊 Volume calculation optimized'
        ELSE '✅ Volume optimization setting'
    END as volume_importance
FROM pg_settings 
WHERE name IN (
    'work_mem', 'temp_buffers', 'hash_mem_multiplier',
    'max_parallel_workers_per_gather', 'parallel_tuple_cost',
    'from_collapse_limit', 'join_collapse_limit', 'geqo_threshold',
    'jit', 'jit_above_cost', 'enable_nestloop', 'enable_seqscan',
    'enable_hashagg', 'random_page_cost', 'seq_page_cost', 
    'statement_timeout', 'cpu_tuple_cost', 'cpu_operator_cost'
)
ORDER BY 
    CASE 
        WHEN name IN ('work_mem', 'hash_mem_multiplier', 'max_parallel_workers_per_gather') THEN 1
        WHEN name IN ('jit', 'enable_nestloop', 'enable_seqscan', 'enable_hashagg') THEN 2
        WHEN name IN ('parallel_tuple_cost', 'jit_above_cost', 'random_page_cost') THEN 3
        ELSE 4
    END;

-- =============================================================================
-- VOLUME QUERY PERFORMANCE VALIDATION
-- =============================================================================

-- Test if volume aggregation will use optimal settings
EXPLAIN (ANALYZE false, BUFFERS, VERBOSE)
SELECT 
    credit_account,
    SUM(volume) as total_volume,
    AVG(volume) as avg_volume,
    COUNT(*) as transaction_count
FROM sales_data 
WHERE year = '2024' 
  AND month IN ('Jan', 'Feb', 'Mar')
  AND volume > 0
GROUP BY credit_account
ORDER BY total_volume DESC
LIMIT 100;

-- =============================================================================
-- VOLUME AGGREGATION TEST QUERIES
-- =============================================================================

-- Test volume filtering performance
EXPLAIN (ANALYZE false, BUFFERS)
SELECT material, SUM(volume), COUNT(*)
FROM sales_data 
WHERE volume > 100
  AND year = '2024'
GROUP BY material;

-- Test complex volume joins (similar to your main query pattern)
EXPLAIN (ANALYZE false, BUFFERS)
SELECT 
    sd.credit_account,
    mm.category,
    SUM(sd.volume) as category_volume
FROM sales_data sd
JOIN material_master mm ON sd.material = mm.material
WHERE sd.year = '2024'
  AND sd.volume > 0
GROUP BY sd.credit_account, mm.category;

-- =============================================================================
-- READY FOR VOLUME QUERY EXECUTION
-- =============================================================================

-- Your TRACKER_MAINSCHEME_VOLUME query is now optimized for maximum performance
-- Settings are configured specifically for:
-- 1. Large volume aggregations (SUM, AVG, COUNT)
-- 2. Complex volume-based calculations
-- 3. Multiple volume CTEs and joins
-- 4. Parallel volume processing

-- =============================================================================
-- EXECUTION PATTERN FOR VOLUME QUERIES
-- =============================================================================

/*
RECOMMENDED EXECUTION SEQUENCE:

1. Run this script (09_volume_query_optimizer.sql)
2. Verify settings with the SELECT statement above
3. Execute your TRACKER_MAINSCHEME_VOLUME query
4. Monitor with volume-specific monitoring queries

EXAMPLE:
-- Step 1: Apply volume optimizations
\i 09_volume_query_optimizer.sql

-- Step 2: Execute your volume query
TRACKER_MAINSCHEME_VOLUME = """
SET SESSION statement_timeout = 0;
WITH scheme AS (...)
[your volume query here]
"""

-- Step 3: Check volume aggregation performance
SELECT 
    sum(volume) as total_processed_volume,
    count(*) as total_rows_processed,
    count(distinct credit_account) as unique_accounts
FROM sales_data 
WHERE year = '2024';
*/

-- =============================================================================
-- POST-EXECUTION: RESET SETTINGS (OPTIONAL)
-- =============================================================================

-- Uncomment to reset after volume query execution
/*
RESET work_mem;
RESET temp_buffers;
RESET hash_mem_multiplier;
RESET max_parallel_workers_per_gather;
RESET enable_seqscan;
RESET random_page_cost;
RESET seq_page_cost;
RESET from_collapse_limit;
RESET join_collapse_limit;
RESET geqo_threshold;
RESET jit_above_cost;
RESET statement_timeout;
RESET cpu_tuple_cost;
RESET cpu_operator_cost;
*/