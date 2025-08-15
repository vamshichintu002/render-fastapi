-- =============================================================================
-- STEP 1: CREATE PERFORMANCE INDEXES
-- Run this script first to create all necessary indexes
-- =============================================================================

-- This script can be run as a single transaction
BEGIN;

-- =============================================================================
-- SALES_DATA TABLE INDEXES
-- =============================================================================

-- Primary filtering index (most important)
CREATE INDEX IF NOT EXISTS idx_sales_data_date_filtering 
ON sales_data (year, month, day, credit_account);

-- Material joins
CREATE INDEX IF NOT EXISTS idx_sales_data_material 
ON sales_data (material);

-- Geographic filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_geographic 
ON sales_data (state_name, region_name, area_head_name);

-- Distributor and dealer filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_business 
ON sales_data (division, dealer_type, distributor);

-- Composite index for date range queries
CREATE INDEX IF NOT EXISTS idx_sales_data_date_composite 
ON sales_data (credit_account, year, month, day, material) 
INCLUDE (value, volume, customer_name, so_name, state_name);

-- Value and volume filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_value_volume 
ON sales_data (value, volume);

-- Customer filtering
CREATE INDEX IF NOT EXISTS idx_sales_data_customer 
ON sales_data (customer_name, so_name);

-- =============================================================================
-- MATERIAL_MASTER TABLE INDEXES
-- =============================================================================

-- Primary material lookup
CREATE INDEX IF NOT EXISTS idx_material_master_material 
ON material_master (material);

-- Product category filtering
CREATE INDEX IF NOT EXISTS idx_material_master_categories 
ON material_master (category, grp, wanda_group, thinner_group);

-- Individual category indexes for better selectivity
CREATE INDEX IF NOT EXISTS idx_material_master_category 
ON material_master (category);

CREATE INDEX IF NOT EXISTS idx_material_master_grp 
ON material_master (grp);

CREATE INDEX IF NOT EXISTS idx_material_master_wanda_group 
ON material_master (wanda_group);

CREATE INDEX IF NOT EXISTS idx_material_master_thinner_group 
ON material_master (thinner_group);

-- =============================================================================
-- SCHEMES_DATA TABLE INDEXES
-- =============================================================================

-- Primary scheme lookup
CREATE INDEX IF NOT EXISTS idx_schemes_data_scheme_id 
ON schemes_data (scheme_id);

-- JSON path indexes for faster JSON extraction
CREATE INDEX IF NOT EXISTS idx_schemes_data_json_paths 
ON schemes_data USING GIN (scheme_json);

-- Specific JSON path indexes for frequent extractions
CREATE INDEX IF NOT EXISTS idx_schemes_main_scheme 
ON schemes_data USING GIN ((scheme_json->'mainScheme'));

-- JSON path indexes for specific data sections
CREATE INDEX IF NOT EXISTS idx_schemes_base_vol_sections 
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'baseVolSections'));

CREATE INDEX IF NOT EXISTS idx_schemes_product_data 
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'productData'));

CREATE INDEX IF NOT EXISTS idx_schemes_applicable 
ON schemes_data USING GIN ((scheme_json->'mainScheme'->'schemeApplicable'));

-- =============================================================================
-- SET STATISTICS FOR BETTER QUERY PLANNING
-- =============================================================================

-- Increase statistics target for critical columns
ALTER TABLE sales_data ALTER COLUMN credit_account SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN material SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN year SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN month SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN day SET STATISTICS 1000;
ALTER TABLE sales_data ALTER COLUMN value SET STATISTICS 500;
ALTER TABLE sales_data ALTER COLUMN volume SET STATISTICS 500;

ALTER TABLE material_master ALTER COLUMN material SET STATISTICS 1000;
ALTER TABLE material_master ALTER COLUMN category SET STATISTICS 500;
ALTER TABLE material_master ALTER COLUMN grp SET STATISTICS 500;

ALTER TABLE schemes_data ALTER COLUMN scheme_id SET STATISTICS 1000;

COMMIT;

-- =============================================================================
-- INDEX CREATION COMPLETE
-- =============================================================================

-- Check created indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
    AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;