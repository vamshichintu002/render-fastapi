-- =============================================================================
-- STEP 2: DATABASE MAINTENANCE (VACUUM & ANALYZE)
-- 
-- IMPORTANT: These commands MUST be run outside transaction blocks
-- 
-- HOW TO RUN THIS SCRIPT:
-- 1. Connect to PostgreSQL with autocommit ON
-- 2. Run each command individually, OR
-- 3. Use psql with -c flag for each command, OR
-- 4. Set autocommit in your client before running
-- 
-- Example in psql:
-- \set AUTOCOMMIT on
-- \i 02_maintenance_vacuum.sql
-- 
-- Example using psql -c:
-- psql -d your_database -c "VACUUM ANALYZE sales_data;"
-- psql -d your_database -c "VACUUM ANALYZE material_master;"
-- psql -d your_database -c "VACUUM ANALYZE schemes_data;"
-- =============================================================================

-- Update table statistics for better query planning
ANALYZE sales_data;

-- Update material master statistics
ANALYZE material_master;

-- Update schemes data statistics  
ANALYZE schemes_data;

-- Vacuum and analyze sales_data (most important for performance)
VACUUM ANALYZE sales_data;

-- Vacuum and analyze material_master
VACUUM ANALYZE material_master;

-- Vacuum and analyze schemes_data
VACUUM ANALYZE schemes_data;

-- =============================================================================
-- OPTIONAL: REINDEX TABLES (only if indexes are fragmented)
-- Uncomment only if you suspect index fragmentation
-- =============================================================================

-- REINDEX TABLE sales_data;
-- REINDEX TABLE material_master;
-- REINDEX TABLE schemes_data;

-- =============================================================================
-- CHECK INDEX USAGE AFTER MAINTENANCE
-- =============================================================================

-- View index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY tablename, idx_scan DESC;