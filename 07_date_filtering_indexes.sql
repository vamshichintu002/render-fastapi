-- =============================================================================
-- SPECIALIZED INDEXES FOR DATE FILTERING OPTIMIZATION
-- Your query has many TO_DATE(...) operations - these indexes will help
-- =============================================================================

BEGIN;

-- =============================================================================
-- DATE FILTERING CRITICAL INDEXES
-- =============================================================================

-- The most critical index for your query's date filtering pattern
-- TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
CREATE INDEX IF NOT EXISTS idx_sales_data_date_parts_critical
ON sales_data (year, month, day, credit_account, material)
INCLUDE (value, volume, customer_name, so_name, state_name);

-- Optimized index for date range filtering with all commonly used columns
CREATE INDEX IF NOT EXISTS idx_sales_data_date_range_optimized
ON sales_data (year, month, day)
INCLUDE (credit_account, material, value, volume, state_name, region_name, 
         area_head_name, division, dealer_type, distributor);

-- Geographic filtering with date (common pattern in your query)
CREATE INDEX IF NOT EXISTS idx_sales_data_geo_date_filter
ON sales_data (state_name, region_name, year, month, day)
INCLUDE (credit_account, material, value, volume);

-- Business hierarchy with date filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_business_date_filter
ON sales_data (division, dealer_type, distributor, year, month, day)
INCLUDE (credit_account, material, value, volume);

-- =============================================================================
-- PRODUCT FILTERING WITH DATE
-- =============================================================================

-- Material filtering with date (very common in your query)
CREATE INDEX IF NOT EXISTS idx_sales_data_material_date_filter
ON sales_data (material, year, month, day)
INCLUDE (credit_account, value, volume, state_name);

-- =============================================================================
-- SCHEMES_DATA JSON OPTIMIZATION
-- =============================================================================

-- Critical JSON path indexes for scheme filtering
CREATE INDEX IF NOT EXISTS idx_schemes_mainscheme_base_sections
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'baseVolSections'));

CREATE INDEX IF NOT EXISTS idx_schemes_mainscheme_scheme_period
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'schemePeriod'));

CREATE INDEX IF NOT EXISTS idx_schemes_mainscheme_applicable
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'schemeApplicable'));

CREATE INDEX IF NOT EXISTS idx_schemes_mainscheme_product_data
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'productData'));

CREATE INDEX IF NOT EXISTS idx_schemes_mainscheme_slabs
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'slabData'));

-- =============================================================================
-- MATERIAL_MASTER OPTIMIZATION FOR JOINS
-- =============================================================================

-- Critical for the many material_master joins in your query
CREATE INDEX IF NOT EXISTS idx_material_master_comprehensive
ON material_master (material)
INCLUDE (category, grp, wanda_group, thinner_group);

-- =============================================================================
-- BONUS SCHEMES OPTIMIZATION
-- =============================================================================

-- If you have bonus scheme data, optimize those queries too
CREATE INDEX IF NOT EXISTS idx_schemes_bonus_data
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'bonusSchemeData'));

-- =============================================================================
-- PARTIAL INDEXES FOR COMMON CONDITIONS
-- =============================================================================

-- Index only for rows with actual volume/value (excludes zeros/nulls)
CREATE INDEX IF NOT EXISTS idx_sales_data_active_transactions
ON sales_data (credit_account, year, month, day, material)
WHERE value > 0 OR volume > 0;

-- Index for recent data (if most queries focus on recent periods)
CREATE INDEX IF NOT EXISTS idx_sales_data_recent_data
ON sales_data (credit_account, material, value, volume)
WHERE year::int >= EXTRACT(YEAR FROM CURRENT_DATE) - 2;  -- Last 2 years

-- =============================================================================
-- EXPRESSION INDEXES FOR DATE OPERATIONS
-- =============================================================================

-- Index on the actual date expression used in your query
-- This will speed up the TO_DATE operations significantly
CREATE INDEX IF NOT EXISTS idx_sales_data_computed_date
ON sales_data (TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD'));

-- Index with computed date and key columns
CREATE INDEX IF NOT EXISTS idx_sales_data_computed_date_comprehensive
ON sales_data (TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD'), 
               credit_account, material)
INCLUDE (value, volume);

-- =============================================================================
-- UPDATE STATISTICS FOR NEW INDEXES
-- =============================================================================

-- Update statistics for better query planning with new indexes
ALTER TABLE sales_data ALTER COLUMN year SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN month SET STATISTICS 1000; 
ALTER TABLE sales_data ALTER COLUMN day SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN credit_account SET STATISTICS 1500;
ALTER TABLE sales_data ALTER COLUMN material SET STATISTICS 1500;
ALTER TABLE sales_data ALTER COLUMN value SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN volume SET STATISTICS 1000;

-- Update material_master statistics
ALTER TABLE material_master ALTER COLUMN material SET STATISTICS 1500;
ALTER TABLE material_master ALTER COLUMN category SET STATISTICS 1000;
ALTER TABLE material_master ALTER COLUMN grp SET STATISTICS 1000;

-- Update schemes_data statistics  
ALTER TABLE schemes_data ALTER COLUMN scheme_id SET STATISTICS 1500;

COMMIT;

-- =============================================================================
-- ANALYZE TABLES AFTER INDEX CREATION
-- =============================================================================

-- Update table statistics to reflect new indexes
ANALYZE sales_data;
ANALYZE material_master;
ANALYZE schemes_data;

-- =============================================================================
-- VERIFY NEW INDEXES
-- =============================================================================

-- Check the new critical indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_indexes pi
JOIN pg_stat_user_indexes pui ON pi.indexname = pui.indexrelname
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
    AND indexname LIKE '%critical%' 
    OR indexname LIKE '%optimized%'
    OR indexname LIKE '%comprehensive%'
ORDER BY tablename, indexname;

-- Check total index usage after creation
SELECT 
    COUNT(*) as total_indexes,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) as total_size
FROM pg_stat_user_indexes 
WHERE relname IN ('sales_data', 'material_master', 'schemes_data');