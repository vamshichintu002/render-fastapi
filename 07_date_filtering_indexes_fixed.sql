-- =============================================================================
-- VERIFY NEW INDEXES (FIXED)
-- =============================================================================

-- Check the new critical indexes with qualified column names
SELECT 
    pi.schemaname,
    pi.tablename,
    pi.indexname,
    pg_size_pretty(pg_relation_size(pi.indexname::regclass)) as size
FROM pg_indexes pi
JOIN pg_stat_user_indexes pui ON pi.indexname = pui.indexrelname 
    AND pi.tablename = pui.relname
    AND pi.schemaname = pui.schemaname
WHERE pi.tablename IN ('sales_data', 'material_master', 'schemes_data')
    AND (pi.indexname LIKE '%critical%' 
         OR pi.indexname LIKE '%optimized%'
         OR pi.indexname LIKE '%comprehensive%'
         OR pi.indexname LIKE '%combo%'
         OR pi.indexname LIKE '%detailed%')
ORDER BY pi.tablename, pi.indexname;