# tracker_queries_optimized.py

TRACKER_MAINSCHEME_VALUE_OPTIMIZED = """
SET SESSION statement_timeout = 0;

-- Create temporary indexes for better performance
CREATE INDEX IF NOT EXISTS idx_temp_sales_data_composite ON sales_data (year, month, day, credit_account, material);
CREATE INDEX IF NOT EXISTS idx_temp_material_master_material ON material_master (material);
CREATE INDEX IF NOT EXISTS idx_temp_strata_growth_account ON strata_growth (credit_account);

WITH scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id = '{scheme_id}'
),

-- Pre-calculate all configurations and date ranges in one CTE
config_data AS (
  SELECT
    -- Base Period 1 Configuration
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS bp1_from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS bp1_to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->0->>'sumAvg')::text AS bp1_sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS bp1_months_count,
    
    -- Base Period 2 Configuration
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date AS bp2_from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date AS bp2_to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->1->>'sumAvg')::text AS bp2_sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS bp2_months_count,
    
    -- Scheme Period
    ((scheme_json->'mainScheme'->'schemePeriod'->>'fromDate')::date + INTERVAL '1 day')::date AS scheme_from_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'toDate')::date + INTERVAL '1 day')::date AS scheme_to_date,
    
    -- Configuration flags
    COALESCE((scheme_json->'mainScheme'->'slabData'->>'enableStrataGrowth')::boolean, false) AS enable_strata_growth,
    CASE 
      WHEN (scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials' IS NOT NULL
            AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials') > 0)
        OR (scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories' IS NOT NULL
            AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories') > 0)
        OR (scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps' IS NOT NULL
            AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps') > 0)
        OR (scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups' IS NOT NULL
            AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups') > 0)
        OR (scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups' IS NOT NULL
            AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups') > 0)
      THEN true
      ELSE false
    END AS payout_products_configured,
    scheme_json
  FROM scheme
),

-- Extract all reference arrays in one go for faster filtering
reference_arrays AS (
  SELECT
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates')), ARRAY[]::text[]) AS states,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedRegions')), ARRAY[]::text[]) AS regions,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedAreaHeads')), ARRAY[]::text[]) AS area_heads,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDivisions')), ARRAY[]::text[]) AS divisions,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDealerTypes')), ARRAY[]::text[]) AS dealer_types,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDistributors')), ARRAY[]::text[]) AS distributors,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'materials')), ARRAY[]::text[]) AS product_materials,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'categories')), ARRAY[]::text[]) AS product_categories,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'grps')), ARRAY[]::text[]) AS product_grps,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'wandaGroups')), ARRAY[]::text[]) AS product_wanda_groups,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'thinnerGroups')), ARRAY[]::text[]) AS product_thinner_groups,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials')), ARRAY[]::text[]) AS payout_product_materials,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories')), ARRAY[]::text[]) AS payout_product_categories,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps')), ARRAY[]::text[]) AS payout_product_grps,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups')), ARRAY[]::text[]) AS payout_product_wanda_groups,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups')), ARRAY[]::text[]) AS payout_product_thinner_groups,
    COALESCE(ARRAY(SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials')), ARRAY[]::text[]) AS mandatory_product_materials
  FROM scheme
),-
- Pre-filter and join sales data with material master once
sales_data_with_dates AS (
  SELECT 
    sd.*,
    mm.category,
    mm.grp,
    mm.wanda_group,
    mm.thinner_group,
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') AS sale_date
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN config_data cd
  WHERE TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
    BETWEEN LEAST(cd.bp1_from_date, cd.bp2_from_date) 
    AND GREATEST(cd.bp1_to_date, cd.bp2_to_date, cd.scheme_to_date)
),

-- Apply all scheme applicability filters once
eligible_sales AS (
  SELECT swd.*
  FROM sales_data_with_dates swd
  CROSS JOIN reference_arrays ra
  WHERE 
    (cardinality(ra.states) = 0 OR swd.state_name = ANY(ra.states))
    AND (cardinality(ra.regions) = 0 OR swd.region_name = ANY(ra.regions))
    AND (cardinality(ra.area_heads) = 0 OR swd.area_head_name = ANY(ra.area_heads))
    AND (cardinality(ra.divisions) = 0 OR swd.division::text = ANY(ra.divisions))
    AND (cardinality(ra.dealer_types) = 0 OR swd.dealer_type = ANY(ra.dealer_types))
    AND (cardinality(ra.distributors) = 0 OR swd.distributor = ANY(ra.distributors))
    AND (
      (cardinality(ra.product_materials) = 0 OR swd.material::text = ANY(ra.product_materials))
      OR (cardinality(ra.product_categories) = 0 OR swd.category::text = ANY(ra.product_categories))
      OR (cardinality(ra.product_grps) = 0 OR swd.grp::text = ANY(ra.product_grps))
      OR (cardinality(ra.product_wanda_groups) = 0 OR swd.wanda_group::text = ANY(ra.product_wanda_groups))
      OR (cardinality(ra.product_thinner_groups) = 0 OR swd.thinner_group::text = ANY(ra.product_thinner_groups))
    )
),

-- Extract slabs data
slabs AS (
  SELECT 
    COALESCE(NULLIF(slab->>'slabStart', ''), '0')::NUMERIC AS slab_start,
    COALESCE(NULLIF(slab->>'slabEnd', ''), '0')::NUMERIC AS slab_end,
    COALESCE(NULLIF(slab->>'growthPercent', ''), '0')::NUMERIC / 100.0 AS growth_rate,
    COALESCE(NULLIF(slab->>'dealerMayQualifyPercent', ''), '0')::NUMERIC / 100.0 AS qualification_rate,
    COALESCE(NULLIF(slab->>'rebatePerLitre', ''), '0')::NUMERIC AS rebate_per_litre,
    COALESCE(NULLIF(slab->>'rebatePercent', ''), '0')::NUMERIC AS rebate_percent,
    COALESCE(NULLIF(slab->>'additionalRebateOnGrowth', ''), '0')::NUMERIC AS additional_rebate_on_growth_per_litre,
    COALESCE(NULLIF(slab->>'fixedRebate', ''), '0')::NUMERIC AS fixed_rebate,
    COALESCE(NULLIF(slab->>'mandatoryProductTarget', ''), '0')::NUMERIC AS mandatory_product_target,
    COALESCE(NULLIF(slab->>'mandatoryProductGrowthPercent', ''), '0')::NUMERIC / 100.0 AS mandatory_product_growth_percent,
    COALESCE(NULLIF(slab->>'mandatoryProductTargetToActual', ''), '0')::NUMERIC / 100.0 AS mandatory_product_target_to_actual,
    COALESCE(NULLIF(slab->>'mandatoryProductRebate', ''), '0')::NUMERIC AS mandatory_product_rebate,
    COALESCE(NULLIF(slab->>'mandatoryProductRebatePercent', ''), '0')::NUMERIC AS mandatory_product_rebate_percent,
    COALESCE(NULLIF(slab->>'mandatoryMinShadesPPI', ''), '0')::NUMERIC AS mandatory_min_shades_ppi,
    row_number() OVER (ORDER BY COALESCE(NULLIF(slab->>'slabStart', ''), '0')::NUMERIC) AS slab_order
  FROM scheme,
  LATERAL jsonb_array_elements(scheme_json->'mainScheme'->'slabData'->'slabs') AS slab
),

first_slab AS (
  SELECT 
    slab_start, growth_rate, qualification_rate, rebate_per_litre, rebate_percent,
    additional_rebate_on_growth_per_litre, fixed_rebate,
    mandatory_product_target, mandatory_product_growth_percent,
    mandatory_product_target_to_actual, mandatory_product_rebate,
    mandatory_product_rebate_percent, mandatory_min_shades_ppi
  FROM slabs
  WHERE slab_order = 1
),

-- Extract phasing periods data
phasing_periods AS (
  SELECT 
    COALESCE(NULLIF(phasing->>'id', ''), '0')::INTEGER AS phasing_id,
    COALESCE(NULLIF(phasing->>'rebateValue', ''), '0')::NUMERIC AS rebate_value,
    COALESCE(NULLIF(phasing->>'rebatePercentage', ''), '0')::NUMERIC AS rebate_percentage,
    ((phasing->>'payoutToDate')::timestamp + INTERVAL '1 day')::date AS payout_to_date,
    ((phasing->>'phasingToDate')::timestamp + INTERVAL '1 day')::date AS phasing_to_date,
    ((phasing->>'payoutFromDate')::timestamp + INTERVAL '1 day')::date AS payout_from_date,
    ((phasing->>'phasingFromDate')::timestamp + INTERVAL '1 day')::date AS phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS phasing_bonus_target_percent,
    COALESCE((phasing->>'isBonus')::boolean, false) AS is_bonus,
    COALESCE(NULLIF(phasing->>'bonusRebateValue', ''), '0')::NUMERIC AS bonus_rebate_value,
    COALESCE(NULLIF(phasing->>'bonusRebatePercentage', ''), '0')::NUMERIC AS bonus_rebate_percentage,
    ((phasing->>'bonusPayoutToDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_to_date,
    ((phasing->>'bonusPhasingToDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_to_date,
    ((phasing->>'bonusPayoutFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_from_date,
    ((phasing->>'bonusPhasingFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS bonus_phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS bonus_phasing_target_percent_raw
  FROM scheme,
  LATERAL jsonb_array_elements(scheme_json->'mainScheme'->'phasingPeriods') AS phasing
),

-- Extract bonus schemes data
bonus_schemes AS (
  SELECT 
    COALESCE(NULLIF(bonus_scheme->>'id', ''), '0')::INTEGER AS bonus_scheme_id,
    COALESCE(NULLIF(bonus_scheme->>'name', ''), '') AS bonus_scheme_name,
    COALESCE(NULLIF(bonus_scheme->>'mainSchemeTargetPercent', ''), '0')::NUMERIC AS main_scheme_target_percent,
    COALESCE(NULLIF(bonus_scheme->>'minimumTarget', ''), '0')::NUMERIC AS minimum_target,
    COALESCE(NULLIF(bonus_scheme->>'mandatoryProductTargetPercent', ''), '0')::NUMERIC AS mandatory_product_target_percent,
    COALESCE(NULLIF(bonus_scheme->>'minimumMandatoryProductTarget', ''), '0')::NUMERIC AS minimum_mandatory_product_target,
    COALESCE(NULLIF(bonus_scheme->>'rewardOnTotalPercent', ''), '0')::NUMERIC AS reward_on_total_percent,
    COALESCE(NULLIF(bonus_scheme->>'rewardOnMandatoryProductPercent', ''), '0')::NUMERIC AS reward_on_mandatory_product_percent,
    ((bonus_scheme->>'bonusPayoutTo')::timestamp + INTERVAL '1 day')::date AS bonus_payout_to_date,
    ((bonus_scheme->>'bonusPeriodTo')::timestamp + INTERVAL '1 day')::date AS bonus_period_to_date,
    ((bonus_scheme->>'bonusPayoutFrom')::timestamp + INTERVAL '1 day')::date AS bonus_payout_from_date,
    ((bonus_scheme->>'bonusPeriodFrom')::timestamp + INTERVAL '1 day')::date AS bonus_period_from_date
  FROM scheme,
  LATERAL jsonb_array_elements(COALESCE(scheme_json->'mainScheme'->'bonusSchemeData'->'bonusSchemes', '[]'::jsonb)) AS bonus_scheme
),-- 
Consolidated sales aggregations with all periods and products in one pass
consolidated_sales_aggregations AS (
  SELECT 
    es.credit_account,
    MIN(es.customer_name) AS customer_name,
    MIN(es.so_name) AS so_name,
    MIN(es.state_name) AS state_name,
    
    -- Base Period 1 aggregations
    CASE 
      WHEN cd.bp1_sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp1_from_date AND cd.bp1_to_date THEN es.value END), 0) / NULLIF(cd.bp1_months_count, 0), 2)
      ELSE 
        COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp1_from_date AND cd.bp1_to_date THEN es.value END), 0)
    END AS base_1_value,
    CASE 
      WHEN cd.bp1_sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp1_from_date AND cd.bp1_to_date THEN es.volume END), 0) / NULLIF(cd.bp1_months_count, 0), 2)
      ELSE 
        COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp1_from_date AND cd.bp1_to_date THEN es.volume END), 0)
    END AS base_1_volume,
    REPLACE(REPLACE(cd.bp1_sum_avg_method, '"', ''), '"', '') AS base_1_sum_avg_method,
    cd.bp1_months_count AS base_1_months,
    
    -- Base Period 2 aggregations
    CASE 
      WHEN cd.bp2_sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp2_from_date AND cd.bp2_to_date THEN es.value END), 0) / NULLIF(cd.bp2_months_count, 0), 2)
      ELSE 
        COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp2_from_date AND cd.bp2_to_date THEN es.value END), 0)
    END AS base_2_value,
    CASE 
      WHEN cd.bp2_sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp2_from_date AND cd.bp2_to_date THEN es.volume END), 0) / NULLIF(cd.bp2_months_count, 0), 2)
      ELSE 
        COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.bp2_from_date AND cd.bp2_to_date THEN es.volume END), 0)
    END AS base_2_volume,
    REPLACE(REPLACE(cd.bp2_sum_avg_method, '"', ''), '"', '') AS base_2_sum_avg_method,
    cd.bp2_months_count AS base_2_months,
    
    -- Actual period aggregations
    COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date THEN es.value END), 0) AS actual_value,
    COALESCE(SUM(CASE WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date THEN es.volume END), 0) AS actual_volume,
    
    -- Mandatory product aggregations
    COALESCE(SUM(CASE 
      WHEN es.sale_date BETWEEN cd.bp1_from_date AND cd.bp1_to_date 
           AND es.material::text = ANY(ra.mandatory_product_materials)
      THEN es.value END), 0) AS mandatory_product_base_value,
    COALESCE(SUM(CASE 
      WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date 
           AND es.material::text = ANY(ra.mandatory_product_materials)
      THEN es.value END), 0) AS mandatory_product_actual_value,
    COUNT(DISTINCT CASE 
      WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date 
           AND es.material::text = ANY(ra.mandatory_product_materials)
           AND es.value > 0
      THEN es.material END) AS mandatory_product_actual_ppi,
    
    -- Payout product aggregations
    COALESCE(SUM(CASE 
      WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS payout_product_actual_value,
    COALESCE(SUM(CASE 
      WHEN es.sale_date BETWEEN cd.scheme_from_date AND cd.scheme_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.volume END), 0) AS payout_product_actual_volume,
      
    -- Phasing period aggregations for all 3 periods
    COALESCE(SUM(CASE 
      WHEN pp1.phasing_id = 1 AND es.sale_date BETWEEN pp1.phasing_from_date AND pp1.phasing_to_date 
      THEN es.value END), 0) AS phasing_period_value_1,
    COALESCE(SUM(CASE 
      WHEN pp1.phasing_id = 1 AND es.sale_date BETWEEN pp1.payout_from_date AND pp1.payout_to_date 
      THEN es.value END), 0) AS phasing_payout_period_value_1,
    COALESCE(SUM(CASE 
      WHEN pp2.phasing_id = 2 AND es.sale_date BETWEEN pp2.phasing_from_date AND pp2.phasing_to_date 
      THEN es.value END), 0) AS phasing_period_value_2,
    COALESCE(SUM(CASE 
      WHEN pp2.phasing_id = 2 AND es.sale_date BETWEEN pp2.payout_from_date AND pp2.payout_to_date 
      THEN es.value END), 0) AS phasing_payout_period_value_2,
    COALESCE(SUM(CASE 
      WHEN pp3.phasing_id = 3 AND es.sale_date BETWEEN pp3.phasing_from_date AND pp3.phasing_to_date 
      THEN es.value END), 0) AS phasing_period_value_3,
    COALESCE(SUM(CASE 
      WHEN pp3.phasing_id = 3 AND es.sale_date BETWEEN pp3.payout_from_date AND pp3.payout_to_date 
      THEN es.value END), 0) AS phasing_payout_period_value_3,
      
    -- Phasing payout product values
    COALESCE(SUM(CASE 
      WHEN pp1.phasing_id = 1 AND es.sale_date BETWEEN pp1.payout_from_date AND pp1.payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS phasing_period_payout_product_value_1,
    COALESCE(SUM(CASE 
      WHEN pp2.phasing_id = 2 AND es.sale_date BETWEEN pp2.payout_from_date AND pp2.payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS phasing_period_payout_product_value_2,
    COALESCE(SUM(CASE 
      WHEN pp3.phasing_id = 3 AND es.sale_date BETWEEN pp3.payout_from_date AND pp3.payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS phasing_period_payout_product_value_3,
      
    -- Bonus phasing aggregations
    COALESCE(SUM(CASE 
      WHEN pp1.phasing_id = 1 AND pp1.is_bonus = true AND es.sale_date BETWEEN pp1.bonus_phasing_from_date AND pp1.bonus_phasing_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_phasing_period_value_1,
    COALESCE(SUM(CASE 
      WHEN pp1.phasing_id = 1 AND pp1.is_bonus = true AND es.sale_date BETWEEN pp1.bonus_payout_from_date AND pp1.bonus_payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_payout_period_payout_product_value_1,
    COALESCE(SUM(CASE 
      WHEN pp2.phasing_id = 2 AND pp2.is_bonus = true AND es.sale_date BETWEEN pp2.bonus_phasing_from_date AND pp2.bonus_phasing_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_phasing_period_value_2,
    COALESCE(SUM(CASE 
      WHEN pp2.phasing_id = 2 AND pp2.is_bonus = true AND es.sale_date BETWEEN pp2.bonus_payout_from_date AND pp2.bonus_payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_payout_period_payout_product_value_2,
    COALESCE(SUM(CASE 
      WHEN pp3.phasing_id = 3 AND pp3.is_bonus = true AND es.sale_date BETWEEN pp3.bonus_phasing_from_date AND pp3.bonus_phasing_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_phasing_period_value_3,
    COALESCE(SUM(CASE 
      WHEN pp3.phasing_id = 3 AND pp3.is_bonus = true AND es.sale_date BETWEEN pp3.bonus_payout_from_date AND pp3.bonus_payout_to_date 
           AND (cardinality(ra.payout_product_materials) = 0 OR es.material::text = ANY(ra.payout_product_materials))
           AND (cardinality(ra.payout_product_categories) = 0 OR es.category::text = ANY(ra.payout_product_categories))
           AND (cardinality(ra.payout_product_grps) = 0 OR es.grp::text = ANY(ra.payout_product_grps))
           AND (cardinality(ra.payout_product_wanda_groups) = 0 OR es.wanda_group::text = ANY(ra.payout_product_wanda_groups))
           AND (cardinality(ra.payout_product_thinner_groups) = 0 OR es.thinner_group::text = ANY(ra.payout_product_thinner_groups))
      THEN es.value END), 0) AS bonus_payout_period_payout_product_value_3