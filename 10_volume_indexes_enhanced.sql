-- =============================================================================
-- ENHANCED VOLUME INDEXES FOR TRACKER_MAINSCHEME_VOLUME
-- Specialized indexes optimized for volume calculations and aggregations
-- =============================================================================

BEGIN;

-- =============================================================================
-- VOLUME AGGREGATION CRITICAL INDEXES
-- =============================================================================

-- Primary volume aggregation index (most important for volume queries)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_aggregation_critical
ON sales_data (credit_account, volume, year, month, day, material, value, state_name, region_name);

-- Volume with material filtering (critical for volume queries)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_material_critical
ON sales_data (material, volume, credit_account, year, month, day, value, customer_name, so_name);

-- Volume aggregation with date filtering
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_date_critical
ON sales_data (year, month, day, volume, credit_account, material, value, state_name, region_name);

-- =============================================================================
-- VOLUME FILTERING SPECIALIZED INDEXES
-- =============================================================================

-- Volume filtering with geographic data (common in volume queries)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_geo_enhanced
ON sales_data (state_name, region_name, volume, credit_account, year, month, day, material, value);

-- Volume filtering with business hierarchy
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_business_enhanced
ON sales_data (division, dealer_type, distributor, volume, credit_account, material, year, month, day, value);

-- Volume filtering with area head (if used in volume calculations)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_area_enhanced
ON sales_data (area_head_name, volume, credit_account, material, year, month, day, state_name, value);

-- =============================================================================
-- VOLUME RANGE AND CONDITIONAL INDEXES
-- =============================================================================

-- Index for non-zero volume transactions (very common filter)
CREATE INDEX IF NOT EXISTS idx_sales_data_nonzero_volume_optimized
ON sales_data (credit_account, material, year, month, day)
WHERE volume > 0;

-- Index for significant volume transactions
-- Using composite index instead of INCLUDE with WHERE clause
CREATE INDEX IF NOT EXISTS idx_sales_data_significant_volume
ON sales_data (credit_account, volume, material, year, month, day, value, state_name)
WHERE volume > 10;  -- Adjust threshold based on your data

-- Index for high volume transactions
-- Using composite index instead of INCLUDE with WHERE clause
CREATE INDEX IF NOT EXISTS idx_sales_data_high_volume
ON sales_data (volume, credit_account, material, year, month, day, value)
WHERE volume > 100;  -- Adjust threshold based on your data

-- =============================================================================
-- VOLUME WITH DATE COMBINATIONS
-- =============================================================================

-- Volume aggregation by year (common grouping)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_year_grouping
ON sales_data (year, credit_account, volume, month, day, material, value);

-- Volume aggregation by month (common grouping)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_month_grouping
ON sales_data (year, month, credit_account, volume, day, material, value, state_name);

-- Volume aggregation by quarter pattern
-- Using composite index instead of INCLUDE with WHERE clause
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_quarter_pattern
ON sales_data (year, month, volume, credit_account, day, material, value)
WHERE month IN ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec');

-- =============================================================================
-- VOLUME CALCULATION SUPPORT INDEXES
-- =============================================================================

-- Support for volume calculations with customer data
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_customer_calc
ON sales_data (customer_name, volume, credit_account, material, year, month, day, value, state_name);

-- Support for volume calculations with SO data
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_so_calc
ON sales_data (so_name, volume, credit_account, material, year, month, day, value);

-- Support for volume ratio calculations (volume vs value)
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_value_ratio
ON sales_data (credit_account, volume, value, material, year, month, day);

-- =============================================================================
-- MATERIAL MASTER VOLUME OPTIMIZATION
-- =============================================================================

-- Enhanced material master index for volume queries
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_material_master_volume_enhanced
ON material_master (material, category, grp, wanda_group, thinner_group);

-- Material filtering for volume calculations
-- Using composite index instead of INCLUDE
CREATE INDEX IF NOT EXISTS idx_material_master_volume_filter_enhanced
ON material_master (category, grp, material, wanda_group, thinner_group);

-- =============================================================================
-- SCHEMES DATA VOLUME-SPECIFIC JSON INDEXES
-- =============================================================================

-- Volume-specific scheme JSON paths
CREATE INDEX IF NOT EXISTS idx_schemes_volume_base_sections
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'baseVolSections'));

CREATE INDEX IF NOT EXISTS idx_schemes_volume_mandatory_products
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'productData'->'mandatoryProducts'));

CREATE INDEX IF NOT EXISTS idx_schemes_volume_payout_products
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'productData'->'payoutProducts'));

CREATE INDEX IF NOT EXISTS idx_schemes_volume_slabs
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'slabData'));

-- =============================================================================
-- VOLUME PERFORMANCE STATISTICS
-- =============================================================================

-- Enhanced statistics for volume columns
ALTER TABLE sales_data ALTER COLUMN volume SET STATISTICS 1500;
ALTER TABLE sales_data ALTER COLUMN credit_account SET STATISTICS 1500;
ALTER TABLE sales_data ALTER COLUMN material SET STATISTICS 1500;
ALTER TABLE sales_data ALTER COLUMN year SET STATISTICS 1200;
ALTER TABLE sales_data ALTER COLUMN month SET STATISTICS 1200;
ALTER TABLE sales_data ALTER COLUMN day SET STATISTICS 1000;

-- Geographic statistics for volume filtering
ALTER TABLE sales_data ALTER COLUMN state_name SET STATISTICS 1200;
ALTER TABLE sales_data ALTER COLUMN region_name SET STATISTICS 1200;
ALTER TABLE sales_data ALTER COLUMN area_head_name SET STATISTICS 1000;

-- Business hierarchy statistics for volume
ALTER TABLE sales_data ALTER COLUMN division SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN dealer_type SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN distributor SET STATISTICS 1000;

COMMIT;

-- =============================================================================
-- ANALYZE TABLES FOR VOLUME OPTIMIZATION
-- =============================================================================

-- Update table statistics with focus on volume columns
ANALYZE sales_data (volume, credit_account, material, year, month, day);
ANALYZE material_master (material, category, grp);
ANALYZE schemes_data (scheme_id, scheme_json);

-- =============================================================================
-- VERIFY VOLUME INDEXES
-- =============================================================================

-- Check volume-specific indexes
SELECT 
    pi.schemaname,
    pi.tablename,
    pi.indexname,
    pg_size_pretty(pg_relation_size(pi.indexname::regclass)) as size,
    CASE 
        WHEN pi.indexname LIKE '%volume%' THEN '📊 Volume Index'
        WHEN pi.indexname LIKE '%critical%' THEN '🚀 Critical Index'
        WHEN pi.indexname LIKE '%enhanced%' THEN '⚡ Enhanced Index'
        ELSE '✅ Standard Index'
    END as index_type
FROM pg_indexes pi
WHERE pi.tablename IN ('sales_data', 'material_master', 'schemes_data')
    AND (pi.indexname LIKE '%volume%' 
         OR pi.indexname LIKE '%critical%'
         OR pi.indexname LIKE '%enhanced%')
ORDER BY pi.tablename, pg_relation_size(pi.indexname::regclass) DESC;

-- =============================================================================
-- VOLUME INDEX USAGE VALIDATION
-- =============================================================================

-- Test volume aggregation index usage
EXPLAIN (ANALYZE false, BUFFERS, VERBOSE)
SELECT 
    credit_account,
    SUM(volume) as total_volume,
    COUNT(*) as transaction_count
FROM sales_data 
WHERE volume > 0
  AND year = '2024'
GROUP BY credit_account
ORDER BY total_volume DESC;

-- Test volume filtering with material
EXPLAIN (ANALYZE false, BUFFERS, VERBOSE)
SELECT 
    material,
    credit_account,
    SUM(volume) as material_volume
FROM sales_data 
WHERE material IN ('MAT001', 'MAT002', 'MAT003')
  AND volume > 0
GROUP BY material, credit_account;

-- Test volume with geographic filtering
EXPLAIN (ANALYZE false, BUFFERS, VERBOSE)
SELECT 
    state_name,
    SUM(volume) as state_volume,
    AVG(volume) as avg_volume
FROM sales_data 
WHERE state_name IN ('STATE1', 'STATE2')
  AND volume > 0
  AND year = '2024'
GROUP BY state_name;

-- =============================================================================
-- VOLUME INDEX HEALTH CHECK
-- =============================================================================

-- Check volume index efficiency
SELECT 
    'Volume Indexes Health Check' as check_name,
    COUNT(*) as total_volume_indexes,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) as total_size
FROM pg_stat_user_indexes 
WHERE relname = 'sales_data'
  AND indexrelname LIKE '%volume%';

-- =============================================================================
-- VOLUME OPTIMIZATION COMPLETE
-- =============================================================================

-- Volume indexes are now optimized for:
-- 1. Volume aggregations (SUM, AVG, COUNT)
-- 2. Volume filtering (> 0, > threshold)
-- 3. Volume with date combinations
-- 4. Volume with material filtering
-- 5. Volume with geographic filtering
-- 6. Volume calculations and ratios