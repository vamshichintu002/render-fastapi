-- =============================================================================
-- VOLUME QUERY SPECIFIC INDEXES
-- Optimizes TRACKER_MAINSCHEME_VOLUME query performance
-- =============================================================================

BEGIN;

-- =============================================================================
-- VOLUME-SPECIFIC PERFORMANCE INDEXES
-- =============================================================================

-- Volume aggregation index (critical for VOLUME query)
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_aggregation 
ON sales_data (credit_account, volume, year, month, day);

-- Volume filtering with material
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_material 
ON sales_data (material, volume, credit_account);

-- Volume with date filtering (for base period calculations)
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_date_range 
ON sales_data (year, month, day, volume) 
INCLUDE (credit_account, material);

-- Volume with geographic filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_geographic 
ON sales_data (state_name, region_name, volume) 
INCLUDE (credit_account, material);

-- Volume with business hierarchy
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_business 
ON sales_data (division, dealer_type, distributor, volume) 
INCLUDE (credit_account, material);

-- Composite volume index for complex filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_volume_composite 
ON sales_data (credit_account, material, year, month, volume) 
INCLUDE (customer_name, so_name, state_name);

-- =============================================================================
-- BONUS SCHEMES VOLUME-SPECIFIC INDEXES
-- =============================================================================

-- Volume aggregation for bonus schemes
CREATE INDEX IF NOT EXISTS idx_sales_data_bonus_volume 
ON sales_data (credit_account, year, month, day, volume, material);

-- =============================================================================
-- MATERIAL MASTER VOLUME OPTIMIZATIONS
-- =============================================================================

-- Material filtering for volume calculations
CREATE INDEX IF NOT EXISTS idx_material_master_volume_filter 
ON material_master (material, category, grp) 
INCLUDE (wanda_group, thinner_group);

-- =============================================================================
-- STATISTICS UPDATE FOR VOLUME COLUMNS
-- =============================================================================

-- Increase statistics for volume column specifically
ALTER TABLE sales_data ALTER COLUMN volume SET STATISTICS 1000;

-- Update correlation statistics between volume and other columns
ALTER TABLE sales_data ALTER COLUMN credit_account SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN material SET STATISTICS 1000;

COMMIT;

-- =============================================================================
-- VERIFY VOLUME INDEXES
-- =============================================================================

-- Check volume-specific indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'sales_data'
    AND indexname LIKE '%volume%'
ORDER BY indexname;

-- Check index usage after creation
SELECT 
    relname,
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE relname = 'sales_data'
    AND indexrelname LIKE '%volume%'
ORDER BY idx_scan DESC;