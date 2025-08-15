-- =============================================================================
-- VOLUME QUERY MONITORING AND DIAGNOSTICS
-- Real-time monitoring specifically for TRACKER_MAINSCHEME_VOLUME
-- =============================================================================

-- =============================================================================
-- REAL-TIME VOLUME QUERY MONITORING
-- =============================================================================

-- Monitor active volume queries with aggregation details
SELECT 
    pid,
    state,
    query_start,
    now() - query_start as runtime,
    state_change,
    wait_event_type,
    wait_event,
    backend_type,
    CASE 
        WHEN query LIKE '%TRACKER_MAINSCHEME_VOLUME%' THEN '🔢 Volume Query'
        WHEN query LIKE '%SUM(volume)%' OR query LIKE '%volume%' THEN '📊 Volume Operation'
        ELSE '📋 Other Query'
    END as query_type,
    left(query, 200) as query_preview
FROM pg_stat_activity 
WHERE state = 'active' 
    AND (query LIKE '%TRACKER_MAINSCHEME_VOLUME%' 
         OR query LIKE '%volume%' 
         OR query LIKE '%SUM(%'
         OR query LIKE '%GROUP BY%')
    AND query NOT LIKE '%pg_stat_activity%'
ORDER BY query_start;

-- =============================================================================
-- VOLUME AGGREGATION PERFORMANCE TRACKING
-- =============================================================================

-- Track volume aggregation operations
SELECT 
    p.pid,
    p.state,
    p.query_start,
    now() - p.query_start as current_runtime,
    p.wait_event_type,
    p.wait_event,
    CASE 
        WHEN p.query LIKE '%SUM(sd.volume)%' THEN '📊 Volume SUM'
        WHEN p.query LIKE '%COUNT%volume%' THEN '🔢 Volume COUNT'
        WHEN p.query LIKE '%AVG%volume%' THEN '📈 Volume AVG'
        WHEN p.query LIKE '%GROUP BY%' THEN '🗂️ Volume GROUP BY'
        ELSE '📋 Volume Operation'
    END as volume_operation_type,
    s.calls,
    s.total_time,
    s.mean_time,
    s.rows
FROM pg_stat_activity p
LEFT JOIN pg_stat_statements s ON s.query = p.query
WHERE p.state = 'active' 
    AND (p.query LIKE '%volume%' OR p.query LIKE '%TRACKER_MAINSCHEME_VOLUME%')
    AND p.pid != pg_backend_pid()
ORDER BY p.query_start;

-- =============================================================================
-- VOLUME INDEX USAGE MONITORING
-- =============================================================================

-- Monitor volume index usage in real-time
SELECT 
    relname as table_name,
    indexrelname as index_name,
    idx_scan as scan_count,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    CASE 
        WHEN indexrelname LIKE '%volume%' THEN '📊 Volume Index'
        WHEN indexrelname LIKE '%aggregation%' THEN '🔢 Aggregation Index'
        WHEN indexrelname LIKE '%critical%' THEN '🚀 Critical Index'
        ELSE '📋 Standard Index'
    END as index_type,
    CASE 
        WHEN idx_scan = 0 THEN '❌ Not Used'
        WHEN idx_scan < 10 THEN '⚠️ Low Usage'
        WHEN idx_scan < 100 THEN '✅ Moderate Usage'
        ELSE '🚀 High Usage'
    END as usage_status
FROM pg_stat_user_indexes 
WHERE relname = 'sales_data'
    AND (indexrelname LIKE '%volume%' 
         OR indexrelname LIKE '%aggregation%' 
         OR indexrelname LIKE '%critical%')
ORDER BY idx_scan DESC;

-- =============================================================================
-- VOLUME QUERY MEMORY AND RESOURCE USAGE
-- =============================================================================

-- Monitor memory usage for volume operations
SELECT 
    pid,
    state,
    query_start,
    backend_type,
    wait_event_type,
    wait_event,
    CASE 
        WHEN query LIKE '%TRACKER_MAINSCHEME_VOLUME%' THEN '🔢 Volume Query (Main)'
        WHEN query LIKE '%SUM(volume)%' THEN '📊 Volume Aggregation'
        WHEN query LIKE '%GROUP BY%volume%' THEN '🗂️ Volume Grouping'
        ELSE '📋 Volume Operation'
    END as volume_operation,
    left(query, 150) as operation_preview
FROM pg_stat_activity 
WHERE state != 'idle'
    AND (query LIKE '%volume%' OR query LIKE '%SUM(%' OR query LIKE '%GROUP BY%')
ORDER BY query_start;

-- =============================================================================
-- VOLUME AGGREGATION LOCKS AND BLOCKING
-- =============================================================================

-- Check for volume query locks and blocking
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocked_activity.query_start,
    now() - blocked_activity.query_start as blocked_duration,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocking_activity.state as blocking_state,
    blocked_activity.wait_event_type,
    CASE 
        WHEN blocked_activity.query LIKE '%volume%' THEN '📊 Volume Query Blocked'
        WHEN blocking_activity.query LIKE '%volume%' THEN '📊 Volume Query Blocking'
        ELSE '📋 Other Query'
    END as block_type,
    left(blocked_activity.query, 100) AS blocked_statement,
    left(blocking_activity.query, 100) AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity 
    ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity 
    ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted
    AND (blocked_activity.query LIKE '%volume%' OR blocking_activity.query LIKE '%volume%');

-- =============================================================================
-- VOLUME QUERY PERFORMANCE ANALYSIS
-- =============================================================================

-- Analyze volume query performance patterns
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    max_time,
    rows,
    CASE 
        WHEN query LIKE '%TRACKER_MAINSCHEME_VOLUME%' THEN '🔢 Main Volume Query'
        WHEN query LIKE '%SUM(volume)%' THEN '📊 Volume Aggregation'
        WHEN query LIKE '%volume%GROUP BY%' THEN '🗂️ Volume Grouping'
        ELSE '📋 Volume Operation'
    END as volume_query_type,
    100.0 * shared_blks_hit / 
        NULLIF(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
WHERE query LIKE '%volume%'
    OR query LIKE '%TRACKER_MAINSCHEME_VOLUME%'
    OR query LIKE '%SUM(%'
ORDER BY total_time DESC;

-- =============================================================================
-- VOLUME TABLE SCAN ANALYSIS
-- =============================================================================

-- Check if volume queries are using indexes efficiently
SELECT 
    schemaname,
    relname as table_name,
    seq_scan as sequential_scans,
    seq_tup_read as seq_tuples_read,
    idx_scan as index_scans,
    idx_tup_fetch as idx_tuples_fetched,
    n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
    CASE 
        WHEN seq_scan > idx_scan THEN '⚠️ More sequential than index scans'
        WHEN idx_scan > seq_scan * 10 THEN '✅ Excellent index usage'
        WHEN idx_scan > seq_scan * 2 THEN '👍 Good index usage'
        ELSE '📊 Mixed scan pattern'
    END as volume_scan_analysis,
    CASE 
        WHEN relname = 'sales_data' THEN '📊 Primary Volume Table'
        WHEN relname = 'material_master' THEN '🏷️ Material Lookup Table'
        WHEN relname = 'schemes_data' THEN '⚙️ Scheme Configuration Table'
        ELSE '📋 Other Table'
    END as table_role
FROM pg_stat_user_tables 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY relname;

-- =============================================================================
-- VOLUME TEMP FILE USAGE (MEMORY OVERFLOW DETECTION)
-- =============================================================================

-- Check if volume queries are spilling to disk
SELECT 
    datname as database_name,
    temp_files,
    temp_bytes,
    pg_size_pretty(temp_bytes) as temp_size,
    CASE 
        WHEN temp_bytes > 1073741824 THEN '🚨 High temp usage (>1GB)'
        WHEN temp_bytes > 268435456 THEN '⚠️ Moderate temp usage (>256MB)'
        WHEN temp_bytes > 0 THEN '📊 Some temp usage'
        ELSE '✅ No temp files'
    END as temp_usage_status
FROM pg_stat_database 
WHERE datname = current_database();

-- =============================================================================
-- VOLUME WAIT EVENT ANALYSIS
-- =============================================================================

-- Analyze what volume queries are waiting for
SELECT 
    wait_event_type,
    wait_event,
    count(*) as occurrence_count,
    string_agg(DISTINCT state, ', ') as states,
    CASE 
        WHEN wait_event_type = 'IO' THEN '💾 I/O Wait (Check disk performance)'
        WHEN wait_event_type = 'Lock' THEN '🔒 Lock Wait (Check blocking queries)'
        WHEN wait_event_type = 'CPU' THEN '⚡ CPU Wait (Check resource usage)'
        WHEN wait_event_type IS NULL THEN '🚀 Active Processing'
        ELSE wait_event_type
    END as wait_analysis
FROM pg_stat_activity 
WHERE (query LIKE '%volume%' OR query LIKE '%TRACKER_MAINSCHEME_VOLUME%')
    AND state = 'active'
GROUP BY wait_event_type, wait_event
ORDER BY occurrence_count DESC;

-- =============================================================================
-- VOLUME QUERY HEALTH DASHBOARD
-- =============================================================================

-- Quick volume query health check
SELECT 
    'Volume Query Status' as metric_category,
    'Active Volume Queries' as metric,
    count(*) as value,
    CASE 
        WHEN count(*) = 0 THEN '✅ No active volume queries'
        WHEN count(*) <= 2 THEN '📊 Normal volume query load'
        WHEN count(*) <= 5 THEN '⚠️ High volume query load'
        ELSE '🚨 Very high volume query load'
    END as status
FROM pg_stat_activity 
WHERE state = 'active' 
    AND (query LIKE '%volume%' OR query LIKE '%TRACKER_MAINSCHEME_VOLUME%')

UNION ALL

SELECT 
    'Index Usage' as metric_category,
    'Volume Indexes Used' as metric,
    count(*) as value,
    CASE 
        WHEN count(*) > 10 THEN '✅ Good volume index usage'
        WHEN count(*) > 5 THEN '📊 Moderate volume index usage'
        ELSE '⚠️ Low volume index usage'
    END as status
FROM pg_stat_user_indexes 
WHERE relname = 'sales_data' 
    AND indexrelname LIKE '%volume%'
    AND idx_scan > 0

UNION ALL

SELECT 
    'Memory Usage' as metric_category,
    'Temp File Usage' as metric,
    temp_files as value,
    CASE 
        WHEN temp_files = 0 THEN '✅ No temp files (good memory usage)'
        WHEN temp_files <= 10 THEN '📊 Some temp files'
        ELSE '⚠️ High temp file usage (increase work_mem)'
    END as status
FROM pg_stat_database 
WHERE datname = current_database();

-- =============================================================================
-- VOLUME QUERY MONITORING INSTRUCTIONS
-- =============================================================================

/*
USAGE INSTRUCTIONS FOR VOLUME QUERY MONITORING:

1. BEFORE RUNNING VOLUME QUERY:
   - Run the "Volume Query Health Dashboard" query above
   - Check "Volume Index Usage Monitoring" 
   - Verify no blocking locks

2. DURING VOLUME QUERY EXECUTION:
   - Monitor "Real-time Volume Query Monitoring"
   - Watch "Volume Aggregation Performance Tracking"
   - Check "Volume Temp File Usage" if query is slow

3. AFTER VOLUME QUERY COMPLETION:
   - Review "Volume Query Performance Analysis"
   - Check "Volume Table Scan Analysis" for optimization opportunities
   - Update statistics if needed: ANALYZE sales_data;

KEY MONITORING QUERIES TO RUN:
- Real-time monitoring (first query in this file)
- Volume index usage monitoring  
- Volume temp file usage monitoring
- Volume query health dashboard

TROUBLESHOOTING GUIDE:
- High temp usage: Increase work_mem
- Sequential scans: Add more specific volume indexes
- Lock waits: Check for blocking queries
- I/O waits: Check disk performance
- No index usage: Review query patterns and add targeted indexes
*/