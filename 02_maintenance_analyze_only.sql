-- =============================================================================
-- STEP 2: ANALYZE ONLY (Safe for all environments)
-- 
-- This script only runs ANALYZE commands which are safe to run
-- VACUUM commands are removed to avoid transaction block issues
-- =============================================================================

-- Update table statistics for better query planning
ANALYZE sales_data;

-- Update material master statistics
ANALYZE material_master;

-- Update schemes data statistics  
ANALYZE schemes_data;

-- =============================================================================
-- CHECK TABLE STATISTICS AFTER ANALYZE
-- =============================================================================

-- View table statistics
SELECT 
    schemaname,
    relname,           -- Changed from tablename to relname
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup,
    last_analyze
FROM pg_stat_user_tables 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data')  -- Changed from tablename to relname
ORDER BY relname;      -- Changed from tablename to relname

-- View index usage statistics
SELECT 
    schemaname,
    relname,           -- Changed from tablename to relname
    indexrelname,      -- Changed from indexname to indexrelname
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data')  -- Changed from tablename to relname
ORDER BY relname, idx_scan DESC;  -- Changed from tablename to relname