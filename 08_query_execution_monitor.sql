-- =============================================================================
-- COMPLEX QUERY EXECUTION MONITOR
-- Monitor and debug your 2600+ line query performance in real-time
-- =============================================================================

-- =============================================================================
-- REAL-TIME QUERY MONITORING
-- =============================================================================

-- Monitor your active complex query (run in separate session)
SELECT 
    pid,
    state,
    query_start,
    now() - query_start as runtime,
    state_change,
    wait_event_type,
    wait_event,
    backend_type,
    left(query, 150) as query_preview
FROM pg_stat_activity 
WHERE state = 'active' 
    AND query LIKE '%TRACKER_MAINSCHEME%'
    AND query NOT LIKE '%pg_stat_activity%';

-- =============================================================================
-- DETAILED QUERY PROGRESS TRACKING
-- =============================================================================

-- Track query progress with more details
SELECT 
    p.pid,
    p.state,
    p.query_start,
    now() - p.query_start as current_runtime,
    p.wait_event_type,
    p.wait_event,
    s.query as current_query_part,
    s.calls,
    s.total_time,
    s.mean_time,
    s.rows
FROM pg_stat_activity p
LEFT JOIN pg_stat_statements s ON s.query = p.query
WHERE p.state = 'active' 
    AND p.query LIKE '%TRACKER_MAINSCHEME%'
    AND p.pid != pg_backend_pid();

-- =============================================================================
-- LOCK AND BLOCKING ANALYSIS
-- =============================================================================

-- Check for locks and blocking queries
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocked_activity.query_start,
    now() - blocked_activity.query_start as blocked_duration,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocking_activity.state as blocking_state,
    blocked_activity.wait_event_type,
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
WHERE NOT blocked_locks.granted;

-- =============================================================================
-- MEMORY AND RESOURCE USAGE
-- =============================================================================

-- Monitor memory usage during query execution
SELECT 
    pid,
    state,
    query_start,
    backend_type,
    CASE 
        WHEN query LIKE '%TRACKER_MAINSCHEME%' THEN 'Complex Query'
        ELSE 'Other'
    END as query_type,
    -- Memory usage (if available in your PostgreSQL version)
    wait_event_type,
    wait_event
FROM pg_stat_activity 
WHERE state != 'idle'
ORDER BY query_start;

-- =============================================================================
-- INDEX USAGE TRACKING DURING EXECUTION
-- =============================================================================

-- Track which indexes are being used (run before and after query)
CREATE OR REPLACE VIEW index_usage_snapshot AS
SELECT 
    current_timestamp as snapshot_time,
    relname,
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY relname, idx_scan DESC;

-- View current index usage
SELECT * FROM index_usage_snapshot;

-- =============================================================================
-- QUERY PERFORMANCE ANALYSIS
-- =============================================================================

-- Analyze query performance patterns
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    max_time,
    rows,
    100.0 * shared_blks_hit / 
        NULLIF(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
WHERE query LIKE '%TRACKER_MAINSCHEME%'
    OR query LIKE '%sales_data%'
ORDER BY total_time DESC;

-- =============================================================================
-- TABLE SCAN ANALYSIS
-- =============================================================================

-- Check if tables are being scanned efficiently
SELECT 
    schemaname,
    relname,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins + n_tup_upd + n_tup_del as total_modifications,
    CASE 
        WHEN seq_scan > idx_scan THEN '⚠️ More sequential than index scans'
        WHEN idx_scan > seq_scan * 10 THEN '✅ Good index usage'
        ELSE '📊 Mixed scan pattern'
    END as scan_analysis
FROM pg_stat_user_tables 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY relname;

-- =============================================================================
-- TEMP FILE USAGE (MEMORY OVERFLOW DETECTION)
-- =============================================================================

-- Check if query is spilling to disk (bad for performance)
SELECT 
    datname,
    temp_files,
    temp_bytes,
    pg_size_pretty(temp_bytes) as temp_size
FROM pg_stat_database 
WHERE datname = current_database();

-- =============================================================================
-- WAIT EVENT ANALYSIS
-- =============================================================================

-- Analyze what the query is waiting for
SELECT 
    wait_event_type,
    wait_event,
    count(*) as occurrence_count,
    string_agg(DISTINCT state, ', ') as states
FROM pg_stat_activity 
WHERE query LIKE '%TRACKER_MAINSCHEME%'
    AND wait_event IS NOT NULL
GROUP BY wait_event_type, wait_event
ORDER BY occurrence_count DESC;

-- =============================================================================
-- EXECUTION PLAN ANALYSIS HELPER
-- =============================================================================

-- Prepare for EXPLAIN ANALYZE (use carefully - this will actually run the query)
-- Run this to get execution plan without executing:
-- EXPLAIN (ANALYZE false, VERBOSE, BUFFERS, FORMAT JSON) 
-- [Your TRACKER_MAINSCHEME_VALUE query here]

-- =============================================================================
-- QUICK HEALTH CHECK
-- =============================================================================

-- Quick check of database health during complex query execution
SELECT 
    'Active Connections' as metric,
    count(*) as value
FROM pg_stat_activity 
WHERE state = 'active'
UNION ALL
SELECT 
    'Idle Connections' as metric,
    count(*) as value
FROM pg_stat_activity 
WHERE state = 'idle'
UNION ALL
SELECT 
    'Long Running Queries (>5min)' as metric,
    count(*) as value
FROM pg_stat_activity 
WHERE state = 'active' 
    AND now() - query_start > interval '5 minutes'
UNION ALL
SELECT 
    'Blocked Queries' as metric,
    count(*) as value
FROM pg_stat_activity 
WHERE wait_event_type = 'Lock';

-- =============================================================================
-- USAGE INSTRUCTIONS
-- =============================================================================

/*
USAGE:
1. Run 06_complex_query_optimizer.sql first (performance settings)
2. Run 07_date_filtering_indexes.sql (specialized indexes) 
3. Open a second terminal/connection
4. Run the monitoring queries from this file
5. In your main connection, execute your TRACKER_MAINSCHEME_VALUE query
6. Monitor progress using the queries above
7. Analyze results after completion

KEY MONITORING QUERIES TO RUN:
- Real-time monitoring (first query in this file)
- Lock analysis 
- Index usage tracking
- Memory/temp file usage

TROUBLESHOOTING:
- If temp_bytes is high: Increase work_mem
- If many sequential scans: Add more specific indexes
- If blocked queries: Check for lock contention
- If wait_event shows I/O: Check disk performance
*/