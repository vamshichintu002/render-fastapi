-- =============================================================================
-- INDIVIDUAL MAINTENANCE COMMANDS
-- Run each command separately to avoid transaction block issues
-- Copy and paste each command individually into your SQL client
-- =============================================================================

-- =============================================================================
-- STEP 1: RUN THESE COMMANDS ONE BY ONE
-- =============================================================================

-- Command 1: Analyze sales_data
ANALYZE sales_data;

-- Command 2: Analyze material_master
ANALYZE material_master;

-- Command 3: Analyze schemes_data
ANALYZE schemes_data;

-- =============================================================================
-- OPTIONAL: VACUUM COMMANDS (Run individually if you have permissions)
-- =============================================================================

-- IMPORTANT: Each of these must be run as separate statements
-- Do NOT run them in a transaction block

-- Command 4: Vacuum sales_data (run separately)
-- VACUUM ANALYZE sales_data;

-- Command 5: Vacuum material_master (run separately)
-- VACUUM ANALYZE material_master;

-- Command 6: Vacuum schemes_data (run separately)
-- VACUUM ANALYZE schemes_data;

-- =============================================================================
-- STEP 2: CHECK RESULTS
-- =============================================================================

-- Command 7: Check table statistics
SELECT 
    schemaname,
    tablename,
    n_live_tup,
    n_dead_tup,
    last_analyze,
    last_vacuum
FROM pg_stat_user_tables 
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY tablename;

-- Command 8: Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read
FROM pg_stat_user_indexes 
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY tablename, idx_scan DESC;