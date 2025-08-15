-- =============================================================================
-- VOLUME QUERY PERFORMANCE SETTINGS
-- Optimized settings specifically for TRACKER_MAINSCHEME_VOLUME
-- =============================================================================

-- =============================================================================
-- VOLUME QUERY SPECIFIC OPTIMIZATIONS
-- =============================================================================

-- Increase memory for volume aggregations (larger than VALUE query)
SET work_mem = '768MB';                    -- Higher memory for volume calculations
SET temp_buffers = '256MB';               -- More temp buffer for volume aggregations

-- Optimize for volume-heavy calculations
SET random_page_cost = 1.0;               -- Volume data is frequently accessed
SET seq_page_cost = 1.0;                  -- Equal cost for volume scans

-- =============================================================================
-- PARALLEL PROCESSING FOR VOLUME AGGREGATIONS
-- =============================================================================

-- Enable aggressive parallel processing for volume sums
SET max_parallel_workers_per_gather = 4;  -- More parallel workers for aggregations
SET parallel_tuple_cost = 0.05;          -- Lower cost (volume queries benefit more)
SET parallel_setup_cost = 800;           -- Lower setup cost for volume aggregations
SET min_parallel_table_scan_size = '64MB'; -- Lower threshold for volume data
SET min_parallel_index_scan_size = '32MB'; -- Lower threshold for volume indexes

-- =============================================================================
-- AGGREGATION OPTIMIZATIONS
-- =============================================================================

-- Optimize for SUM() and GROUP BY operations (critical for volume)
SET enable_hashagg = on;                  -- Enable hash aggregation
-- SET enable_groupagg = on;              -- REMOVED: This parameter doesn't exist
SET enable_sort = on;                     -- Enable sorting for aggregations

-- Hash memory for large volume aggregations
SET hash_mem_multiplier = 3.0;           -- Higher multiplier for volume data

-- =============================================================================
-- JOIN OPTIMIZATIONS FOR VOLUME QUERY
-- =============================================================================

-- Optimize joins for volume calculations
SET enable_hashjoin = on;                 -- Fast hash joins for volume data
SET enable_mergejoin = on;                -- Merge joins for sorted volume data
SET enable_nestloop = off;                -- Avoid nested loops (volume data is large)

-- =============================================================================
-- QUERY PLANNER OPTIMIZATIONS
-- =============================================================================

-- Optimize for complex volume aggregations
SET from_collapse_limit = 15;            -- Higher limit for volume queries
SET join_collapse_limit = 15;            -- Higher limit for volume joins
SET geqo_threshold = 20;                 -- Higher threshold for complex volume queries

-- =============================================================================
-- JIT FOR VOLUME CALCULATIONS
-- =============================================================================

-- Enable JIT for volume arithmetic
SET jit = on;                             -- JIT helps with volume calculations
SET jit_above_cost = 50000;              -- Lower threshold (volume queries benefit)
SET jit_inline_above_cost = 200000;      -- Inline volume calculations
SET jit_optimize_above_cost = 200000;    -- Optimize volume expressions

-- =============================================================================
-- TIMEOUT SETTINGS FOR VOLUME QUERIES
-- =============================================================================

-- Volume queries may take longer than value queries
SET statement_timeout = '45min';          -- Longer timeout for volume calculations
SET lock_timeout = '10min';               -- Higher lock timeout

-- =============================================================================
-- VERIFY VOLUME SETTINGS
-- =============================================================================

-- Check volume-optimized settings
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN (
    'work_mem', 
    'temp_buffers',
    'hash_mem_multiplier',
    'max_parallel_workers_per_gather',
    'enable_hashagg',
    'parallel_tuple_cost',
    'jit_above_cost'
)
ORDER BY name;