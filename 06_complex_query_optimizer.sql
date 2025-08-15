-- =============================================================================
-- COMPLEX QUERY OPTIMIZER FOR 2600+ LINE QUERIES
-- Optimizes massive queries without changing business logic
-- =============================================================================

-- =============================================================================
-- PRE-EXECUTION: PERFORMANCE SETTINGS (MODIFIED FOR PERMISSIONS)
-- =============================================================================

-- Memory allocation for complex queries (user-modifiable parameters)
SET work_mem = '1GB';                      -- Large memory for complex operations
SET temp_buffers = '512MB';               -- Large temp buffer
SET maintenance_work_mem = '2GB';         -- For any internal operations

-- Hash optimizations (removed max_stack_depth which requires superuser)
SET hash_mem_multiplier = 4.0;           -- 4x memory for hash operations

-- =============================================================================
-- PARALLEL PROCESSING: MAXIMUM PERFORMANCE
-- =============================================================================

-- Aggressive parallel processing
SET max_parallel_workers_per_gather = 6;  -- More workers for complex queries
SET parallel_tuple_cost = 0.01;          -- Very low cost (prioritize parallel)
SET parallel_setup_cost = 500;           -- Lower setup cost
SET min_parallel_table_scan_size = '32MB'; -- Lower threshold
SET min_parallel_index_scan_size = '16MB'; -- Lower threshold
SET force_parallel_mode = 'regress';     -- Force parallel mode when possible

-- =============================================================================
-- QUERY PLANNER: OPTIMIZED FOR COMPLEX QUERIES
-- =============================================================================

-- Increase planner limits for complex queries
SET from_collapse_limit = 20;            -- Higher FROM clause limit
SET join_collapse_limit = 20;            -- Higher JOIN limit
SET geqo_threshold = 25;                 -- Higher genetic optimization threshold
SET geqo_effort = 10;                   -- Maximum genetic optimization effort
SET geqo_pool_size = 1000;              -- Larger pool for optimization
SET geqo_generations = 1000;            -- More generations for optimization

-- =============================================================================
-- CTE AND SUBQUERY OPTIMIZATIONS
-- =============================================================================

-- Enable CTE inlining and optimization
SET enable_material = on;                -- Enable materialization
SET enable_memoize = on;                 -- Enable memoization (if available)

-- Join method preferences for large datasets
SET enable_hashjoin = on;                -- Enable hash joins
SET enable_mergejoin = on;               -- Enable merge joins
SET enable_nestloop = off;               -- Disable nested loops (slow for large data)

-- =============================================================================
-- JIT OPTIMIZATION FOR COMPLEX EXPRESSIONS
-- =============================================================================

-- Aggressive JIT for complex calculations
SET jit = on;                            -- Enable JIT
SET jit_above_cost = 10000;             -- Lower threshold
SET jit_inline_above_cost = 50000;      -- Inline complex expressions
SET jit_optimize_above_cost = 50000;    -- Optimize complex expressions

-- Note: Removed jit_tuple_deforming and jit_expressions as they may not be available
-- in all PostgreSQL versions or may require higher permissions

-- =============================================================================
-- TIMEOUT AND RESOURCE LIMITS
-- =============================================================================

-- Extended timeouts for complex queries
SET statement_timeout = '2h';            -- 2 hour timeout
SET lock_timeout = '30min';              -- 30 minute lock timeout
SET idle_in_transaction_session_timeout = '1h'; -- 1 hour idle timeout

-- =============================================================================
-- DATE FILTERING OPTIMIZATION HINTS
-- =============================================================================

-- Optimize date operations
SET datestyle = 'ISO, MDY';             -- Consistent date format
SET timezone = 'UTC';                   -- Consistent timezone

-- =============================================================================
-- SCHEMA AND TABLE SPECIFIC HINTS
-- =============================================================================

-- Force index usage for key tables
SET enable_seqscan = off;               -- Force index usage (use carefully)
SET random_page_cost = 0.1;            -- Very low cost for SSD (force index preference)
SET seq_page_cost = 10.0;              -- High cost for sequential scans

-- =============================================================================
-- STATISTICS AND ESTIMATION
-- =============================================================================

-- Better statistics for complex queries
SET default_statistics_target = 1000;   -- Higher statistics target
SET constraint_exclusion = 'on';        -- Enable constraint exclusion

-- =============================================================================
-- VERIFY QUERY SETTINGS
-- =============================================================================

-- Check all applied settings
SELECT 
    name, 
    setting, 
    unit,
    CASE 
        WHEN name IN ('work_mem', 'hash_mem_multiplier', 'max_parallel_workers_per_gather') 
        THEN '🚀 Critical for complex queries'
        WHEN name IN ('jit', 'enable_nestloop', 'enable_seqscan')
        THEN '⚡ Performance critical'
        ELSE '✅ Optimization setting'
    END as importance
FROM pg_settings 
WHERE name IN (
    'work_mem', 'temp_buffers', 'hash_mem_multiplier',
    'max_parallel_workers_per_gather', 'parallel_tuple_cost',
    'from_collapse_limit', 'join_collapse_limit', 'geqo_threshold',
    'jit', 'jit_above_cost', 'enable_nestloop', 'enable_seqscan',
    'random_page_cost', 'seq_page_cost', 'statement_timeout'
)
ORDER BY 
    CASE 
        WHEN name IN ('work_mem', 'hash_mem_multiplier', 'max_parallel_workers_per_gather') THEN 1
        WHEN name IN ('jit', 'enable_nestloop', 'enable_seqscan') THEN 2
        ELSE 3
    END;