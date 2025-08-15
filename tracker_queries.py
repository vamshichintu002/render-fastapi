# tracker_queries.py

TRACKER_MAINSCHEME_VALUE = """
SET SESSION statement_timeout = 0;

WITH scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id = '{scheme_id}'
),

-- Base Period 1 Configuration with Month Calculation
base_period_1_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->0->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

-- Base Period 2 Configuration with Month Calculation
base_period_2_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->1->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

base_dates AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'fromDate')::date + INTERVAL '1 day')::date AS scheme_from_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'toDate')::date + INTERVAL '1 day')::date AS scheme_to_date
  FROM scheme
),

-- REFERENCE CTEs
states AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates') AS state FROM scheme
),
regions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedRegions') AS region FROM scheme
),
area_heads AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedAreaHeads') AS area_head FROM scheme
),
divisions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDivisions') AS division FROM scheme
),
dealer_types AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDealerTypes') AS dealer_type FROM scheme
),
distributors AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDistributors') AS distributor FROM scheme
),
product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'materials') AS material FROM scheme
),
product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'categories') AS category FROM scheme
),
product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'grps') AS grp FROM scheme
),
product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'wandaGroups') AS wanda_group FROM scheme
),
product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'thinnerGroups') AS thinner_group FROM scheme
),

-- SEPARATE PAYOUT PRODUCTS EXTRACTION CTEs
payout_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials') AS material FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials') > 0
),
payout_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories') AS category FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories') > 0
),
payout_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps') AS grp FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps') > 0
),
payout_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups') AS wanda_group FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups') > 0
),
payout_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups') AS thinner_group FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups') > 0
),

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

-- UPDATED PHASING PERIODS WITH BONUS FIELDS
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
    -- BONUS FIELDS
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

-- NEW: BONUS SCHEMES EXTRACTION
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
),

-- BONUS SCHEME PAYOUT PERIOD VALUES (CHANGED FROM VOLUMES TO VALUES)
bonus_scheme_payout_values AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS bonus_payout_period_value
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_payout_from_date AND bs.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- BONUS SCHEME PERIOD ACTUALS (for bonus period dates) - CHANGED TO VALUES
bonus_scheme_period_actuals AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS bonus_period_value
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_period_from_date AND bs.bonus_period_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- BONUS SCHEME PAYOUT ACTUALS (alias for payout values)
bonus_scheme_payout_actuals AS (
  SELECT 
    bonus_scheme_id,
    credit_account,
    bonus_payout_period_value
  FROM bonus_scheme_payout_values
),

-- BONUS SCHEME MANDATORY PRODUCT VALUES (for bonus period dates) - CHANGED TO VALUES
bonus_scheme_mandatory_product_values AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS bonus_mandatory_product_value
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_period_from_date AND bs.bonus_period_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- NEW: BONUS SCHEME MANDATORY PRODUCT PAYOUT PERIOD VALUES - CHANGED TO VALUES
bonus_scheme_mandatory_product_payout_values AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS bonus_payout_period_value
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_payout_from_date AND bs.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- Extract enableStrataGrowth flag from JSON
scheme_config AS (
  SELECT 
    COALESCE((scheme_json->'mainScheme'->'slabData'->>'enableStrataGrowth')::boolean, false) AS enable_strata_growth,
    -- Check if payout products are configured in main scheme
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

-- Base Period 1 Sales Data - USING VALUES
base_period_1_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_1_value,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_1_volume,
    bp1.sum_avg_method AS base_1_sum_avg_method,
    bp1.months_count AS base_1_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_1_config bp1
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp1.from_date AND bp1.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp1.sum_avg_method, bp1.months_count
),

-- Base Period 1 Final Calculations
base_period_1_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_value / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_value
    END AS base_1_value_final,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_volume / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_volume
    END AS base_1_volume_final
  FROM base_period_1_sales
),

-- Base Period 2 Sales Data - USING VALUES
base_period_2_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_2_value,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_2_volume,
    bp2.sum_avg_method AS base_2_sum_avg_method,
    bp2.months_count AS base_2_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_2_config bp2
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp2.from_date AND bp2.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp2.sum_avg_method, bp2.months_count
),

-- Base Period 2 Final Calculations
base_period_2_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_value / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_value
    END AS base_2_value_final,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_volume / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_volume
    END AS base_2_volume_final
  FROM base_period_2_sales
),

base_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    COALESCE(SUM(sd.value), 0) AS total_value,
    COALESCE(SUM(sd.volume), 0) AS total_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

actuals AS (
  SELECT
    sd.credit_account,
    MIN(sd.customer_name) AS actual_customer_name,
    MIN(sd.so_name) AS actual_so_name,
    MIN(sd.state_name) AS actual_state_name,
    COALESCE(SUM(sd.value), 0) AS actual_value,
    COALESCE(SUM(sd.volume), 0) AS actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_actual_ppi AS (
  SELECT 
    sd.credit_account,
    COUNT(DISTINCT sd.material) AS mandatory_product_actual_ppi_count
  FROM sales_data sd
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND sd.material::text IN (
      SELECT mat_item FROM scheme,
      LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
      WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
        AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
    )
  GROUP BY sd.credit_account
),

-- MANDATORY PRODUCT BASE VALUE (CHANGED FROM VOLUME TO VALUE)
mandatory_product_base_value AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS mandatory_base_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY sd.credit_account
),

-- MANDATORY PRODUCT ACTUALS (CHANGED FROM VOLUME TO VALUE)
mandatory_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS mandatory_actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY sd.credit_account
),

payout_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS payout_product_actual_value,
    COALESCE(SUM(sd.volume), 0) AS payout_product_actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

all_accounts AS (
  SELECT credit_account FROM base_sales
  UNION
  SELECT credit_account FROM actuals
  UNION
  SELECT credit_account FROM mandatory_product_base_value
  UNION
  SELECT credit_account FROM mandatory_product_actuals
  UNION
  SELECT credit_account FROM mandatory_product_actual_ppi
  UNION
  SELECT credit_account FROM payout_product_actuals
  UNION
  SELECT credit_account FROM base_period_1_finals
  UNION
  SELECT credit_account FROM base_period_2_finals
),

-- Joined CTE with Updated Total Volume/Value Formulas - CHANGED TO USE VALUES AS PRIMARY
joined AS (
  SELECT 
    aa.credit_account,
    COALESCE(bs.customer_name, ac.actual_customer_name, bp1.customer_name, bp2.customer_name) AS customer_name,
    COALESCE(bs.so_name, ac.actual_so_name, bp1.so_name, bp2.so_name) AS so_name,
    COALESCE(bs.state_name, ac.actual_state_name, bp1.state_name, bp2.state_name) AS state_name,
    -- Total Value/Volume uses MAX of Base Final Values - VALUE IS PRIMARY
    GREATEST(COALESCE(bp1.base_1_value_final, 0), COALESCE(bp2.base_2_value_final, 0)) AS total_value,
    GREATEST(COALESCE(bp1.base_1_volume_final, 0), COALESCE(bp2.base_2_volume_final, 0)) AS total_volume,
    COALESCE(ac.actual_value, 0) AS actual_value,
    COALESCE(ac.actual_volume, 0) AS actual_volume,
    COALESCE(mpbv.mandatory_base_value, 0) AS mandatory_product_base_value,
    COALESCE(mpa.mandatory_actual_value, 0) AS mandatory_product_actual_value,
    CASE 
      WHEN COALESCE(mpa.mandatory_actual_value, 0) < 0 THEN 0
      ELSE COALESCE(mpappi.mandatory_product_actual_ppi_count, 0)
    END AS mandatory_product_actual_ppi,
    COALESCE(ppa.payout_product_actual_value, 0) AS payout_product_actual_value,
    COALESCE(ppa.payout_product_actual_volume, 0) AS payout_product_actual_volume,
    -- Base Period Columns
    COALESCE(bp1.base_1_value, 0) AS base_1_value,
    COALESCE(bp1.base_1_volume, 0) AS base_1_volume,
    REPLACE(REPLACE(bp1.base_1_sum_avg_method, '"', ''), '"', '') AS base_1_sum_avg_method,
    COALESCE(bp2.base_2_value, 0) AS base_2_value,
    COALESCE(bp2.base_2_volume, 0) AS base_2_volume,
    REPLACE(REPLACE(bp2.base_2_sum_avg_method, '"', ''), '"', '') AS base_2_sum_avg_method,
    -- Base Period Month Counts
    COALESCE(bp1.base_1_months_count, 0) AS base_1_months,
    COALESCE(bp2.base_2_months_count, 0) AS base_2_months,
    -- Base Period Final Values
    COALESCE(bp1.base_1_value_final, 0) AS base_1_value_final,
    COALESCE(bp1.base_1_volume_final, 0) AS base_1_volume_final,
    COALESCE(bp2.base_2_value_final, 0) AS base_2_value_final,
    COALESCE(bp2.base_2_volume_final, 0) AS base_2_volume_final
  FROM all_accounts aa
  LEFT JOIN base_sales bs ON aa.credit_account = bs.credit_account
  LEFT JOIN actuals ac ON aa.credit_account = ac.credit_account
  LEFT JOIN mandatory_product_base_value mpbv ON aa.credit_account = mpbv.credit_account
  LEFT JOIN mandatory_product_actuals mpa ON aa.credit_account = mpa.credit_account
  LEFT JOIN mandatory_product_actual_ppi mpappi ON aa.credit_account = mpappi.credit_account
  LEFT JOIN payout_product_actuals ppa ON aa.credit_account = ppa.credit_account
  LEFT JOIN base_period_1_finals bp1 ON aa.credit_account = bp1.credit_account
  LEFT JOIN base_period_2_finals bp2 ON aa.credit_account = bp2.credit_account
),

slab_applied AS (
  SELECT
    j.*,
    -- MODIFIED GROWTH RATE LOGIC WITH enableStrataGrowth CHECK
    CASE 
      WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
      THEN sg.strata_growth_percentage / 100.0
      ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
    END AS growth_rate,
    COALESCE(sl_actual.rebate_per_litre, fs.rebate_per_litre, 0) AS rebate_per_litre_applied,
    COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) AS rebate_percent_applied,
    COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) AS additional_rebate_on_growth_per_litre_applied,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_base,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_actual,
    
    CASE 
      WHEN COALESCE(sl_actual.mandatory_product_target, 0) <= 0 THEN fs.mandatory_product_target
      ELSE COALESCE(sl_actual.mandatory_product_target, 0)
    END AS mandatory_product_target,
    
    COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0) AS mandatory_product_growth_percent,
    COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0) AS mandatory_product_target_to_actual,
    COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) AS mandatory_product_rebate,
    COALESCE(sl_actual.mandatory_product_rebate_percent, fs.mandatory_product_rebate_percent, 0) AS mandatory_product_rebate_percent,
    COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) AS mandatory_min_shades_ppi,
    
    -- MANDATORY PRODUCT TARGET VALUE (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.mandatory_product_base_value IS NULL THEN 0
      ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
    END AS mandatory_product_target_value,
    
    -- UPDATED TARGET VALUE CALCULATION WITH enableStrataGrowth CHECK (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
      ELSE GREATEST(
        (1 + CASE 
               WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
               THEN sg.strata_growth_percentage / 100.0
               ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
             END) * j.total_value, 
        COALESCE(sl_base.slab_start, fs.slab_start, 0)
      )
    END AS target_value,
    
    CASE 
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      ELSE j.actual_value / 
        (CASE 
          WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
          ELSE GREATEST(
            (1 + CASE 
                   WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                   THEN sg.strata_growth_percentage / 100.0
                   ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                 END) * j.total_value, 
            COALESCE(sl_base.slab_start, fs.slab_start, 0)
          )
        END)
    END AS percent_achieved,
    
    -- BASIC PAYOUT BASED ON PAYOUT PRODUCT VALUE (CHANGED FROM VOLUME TO VALUE)
    (COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) / 100.0) * j.payout_product_actual_value AS basic_payout,
    
    -- ADDITIONAL PAYOUT BASED ON PAYOUT PRODUCT VALUE (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.actual_value >= 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END)
      THEN COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_value
      ELSE 0
    END AS additional_payout,
    
    -- MANDATORY PRODUCT PAYOUT LOGIC (USING VALUES WHERE APPLICABLE)
    CASE 
      WHEN (
        (CASE 
          WHEN COALESCE(j.actual_value, 0) = 0 THEN 0 
          ELSE COALESCE(j.mandatory_product_actual_value, 0) / NULLIF(j.actual_value, 0) 
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_actual_value < 0 THEN 0
          WHEN COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_ppi / NULLIF(COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)) = 0 THEN 0 
          ELSE j.mandatory_product_actual_value / NULLIF(j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_value / NULLIF(COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 0)
        END) >= 1.0
      )
      THEN COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) * j.payout_product_actual_value
      ELSE 0
    END AS mandatory_product_payout,
    
    CASE
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      WHEN j.actual_value / 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) >= 1
        THEN COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0)
      ELSE 0
    END AS fixed_rebate,
    
    -- PAYOUT PRODUCT PAYOUT WITH enableStrataGrowth CHECK (USING VALUES)
    ((COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) / 100.0) * j.payout_product_actual_value) +
    CASE 
      WHEN j.actual_value >= 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END)
      THEN COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_value
      ELSE 0
    END +
    CASE
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      WHEN j.actual_value / 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) >= 1
        THEN COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0)
      ELSE 0
    END +
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(CASE 
                  WHEN j.mandatory_product_base_value IS NULL THEN 0
                  ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
                END, 0), 
                COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 
                COALESCE(j.actual_value * COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0), 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(j.mandatory_product_actual_value, 0) / NULLIF(
                  GREATEST(
                    COALESCE(CASE 
                      WHEN j.mandatory_product_base_value IS NULL THEN 0
                      ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
                    END, 0), 
                    COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 
                    COALESCE(j.actual_value * COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0), 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN COALESCE(j.mandatory_product_actual_value, 0) * COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0)
      ELSE 0
    END AS payout_product_payout
    
  FROM joined j
  LEFT JOIN slabs sl_actual ON j.actual_value BETWEEN sl_actual.slab_start AND sl_actual.slab_end
  LEFT JOIN slabs sl_base ON j.total_value BETWEEN sl_base.slab_start AND sl_base.slab_end
  LEFT JOIN strata_growth sg ON j.credit_account::bigint = sg.credit_account
  CROSS JOIN first_slab fs
  CROSS JOIN scheme_config sc
),

-- PHASING ACTUALS (CHANGED FROM VOLUME TO VALUE) - USING SCHEME TOTAL PRODUCTS
phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.phasing_from_date AND pp.phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR SCHEME TOTAL PRODUCTS (MAIN PRODUCTS)
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- PHASING PAYOUTS (CHANGED FROM VOLUME TO VALUE) - USING PAYOUT PRODUCTS
phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_payout_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR SCHEME TOTAL PRODUCTS (MAIN PRODUCTS)
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING ACTUALS (CHANGED FROM VOLUME TO VALUE) - USING PAYOUT PRODUCTS
bonus_phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_phasing_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_phasing_from_date AND pp.bonus_phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUTS (CHANGED FROM VOLUME TO VALUE) - USING PAYOUT PRODUCTS
bonus_phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_phasing_payout_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),
-- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES - FOR PAYOUT CALCULATION
phasing_period_payout_product_values AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_period_payout_product_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),
-- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES - FOR BONUS PAYOUT CALCULATION
bonus_payout_period_payout_product_values AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_payout_period_payout_product_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- UPDATED PHASING AGGREGATED WITH FINAL PHASING PAYOUT COLUMN AND NEW BONUS SCHEME COLUMNS (CHANGED TO VALUES)
phasing_aggregated AS (
  SELECT
    sa.credit_account,
    sa.customer_name,
    sa.so_name,
    sa.state_name,
    sa.total_value,
    sa.total_volume,
    sa.actual_value,
    sa.actual_volume,
    sa.mandatory_product_base_value,
    sa.mandatory_product_actual_value,
    sa.mandatory_product_target_value,
    sa.mandatory_product_payout,
    sa.mandatory_product_actual_ppi,
    sa.payout_product_actual_value,
    sa.payout_product_actual_volume,
    sa.growth_rate,
    sa.target_value,
    sa.basic_payout,
    sa.additional_payout,
    sa.percent_achieved,
    sa.fixed_rebate,
    sa.rebate_per_litre_applied,
    sa.rebate_percent_applied,
    sa.additional_rebate_on_growth_per_litre_applied,
    sa.mandatory_product_target,
    sa.mandatory_product_growth_percent,
    sa.mandatory_product_target_to_actual,
    sa.mandatory_product_rebate,
    sa.mandatory_product_rebate_percent,
    sa.mandatory_min_shades_ppi,
    sa.basic_payout + sa.additional_payout + sa.fixed_rebate AS total_payout,
    sa.payout_product_payout AS payout_product_payout,
    -- Base Period Columns
    sa.base_1_value,
    sa.base_1_volume,
    sa.base_1_sum_avg_method,
    sa.base_2_value,
    sa.base_2_volume,
    sa.base_2_sum_avg_method,
    -- Base Period Month Counts
    sa.base_1_months,
    sa.base_2_months,
    -- Base Period Final Values
    sa.base_1_value_final,
    sa.base_1_volume_final,
    sa.base_2_value_final,
    sa.base_2_volume_final,
    
    sa.actual_value * sa.mandatory_product_target_to_actual AS "Mandatory Product % to Actual - Target Value",
    CASE WHEN sa.actual_value = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.actual_value END AS "Mandatory Product Actual Value % to Total Sales",
    CASE WHEN sa.mandatory_product_target = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.mandatory_product_target END AS "Mandatory Product Actual Value % to Fixed Target Sales",
    CASE WHEN sa.mandatory_product_target_value = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.mandatory_product_target_value END AS "Mandatory Product Growth Target Achieved",
    
    CASE 
      WHEN sa.mandatory_product_actual_value < 0 THEN 0
      WHEN sa.mandatory_min_shades_ppi = 0 THEN 0 
      ELSE sa.mandatory_product_actual_ppi / sa.mandatory_min_shades_ppi 
    END AS "Mandatory Product % PPI",
    
    -- PHASING PERIOD 1 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 1 THEN 1 END) AS "Phasing Period No 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.phasing_target_percent END) AS "Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_value END) AS "Phasing Period Rebate 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 1",
    
    -- 9 BONUS FIELDS FOR PERIOD 1 (CHANGED TO VALUES)
    (MAX(CASE WHEN pp.phasing_id = 1 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 1",
    
    -- PHASING PERIOD 2 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 2 THEN 2 END) AS "Phasing Period No 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.phasing_target_percent END) AS "Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_value END) AS "Phasing Period Rebate 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 2",
    
    -- 9 BONUS FIELDS FOR PERIOD 2 (CHANGED TO VALUES)
    (MAX(CASE WHEN pp.phasing_id = 2 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 2",
    
    -- PHASING PERIOD 3 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 3 THEN 3 END) AS "Phasing Period No 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.phasing_target_percent END) AS "Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_value END) AS "Phasing Period Rebate 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 3",
    
    -- 9 BONUS FIELDS FOR PERIOD 3 (CHANGED TO VALUES)
    (MAX(CASE WHEN pp.phasing_id = 3 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 3",

    -- NEW: BONUS SCHEME COLUMNS (7 columns x 4 schemes = 28 total columns)
    -- BONUS SCHEME 1
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 1 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0) AS "Bonus Scheme 1 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 1",
    
    -- BONUS SCHEME 2
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 2 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0) AS "Bonus Scheme 2 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 2",
    
    -- BONUS SCHEME 3
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 3 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0) AS "Bonus Scheme 3 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 3",
    
    -- BONUS SCHEME 4
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 4 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0) AS "Bonus Scheme 4 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 4",

    -- NEW: 2 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (8 total columns) - CHANGED TO VALUES
    -- BONUS SCHEME 1 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Value 1",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 1 THEN bspa.bonus_period_value END), 0) AS "Actual Bonus Value 1",
    
    -- BONUS SCHEME 2 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Value 2",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 2 THEN bspa.bonus_period_value END), 0) AS "Actual Bonus Value 2",
    
    -- BONUS SCHEME 3 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Value 3",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 3 THEN bspa.bonus_period_value END), 0) AS "Actual Bonus Value 3",
    
    -- BONUS SCHEME 4 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Value 4",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 4 THEN bspa.bonus_period_value END), 0) AS "Actual Bonus Value 4",

    -- NEW: 2 MORE BONUS SCHEME COLUMNS FOR EACH SCHEME (8 total columns) - CHANGED TO VALUES
    -- BONUS SCHEME 1 - % Achieved and Payout Period Value
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 1 THEN bspa.bonus_period_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 1",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 1 THEN bspay.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period Value 1",
    
    -- BONUS SCHEME 2 - % Achieved and Payout Period Value
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 2 THEN bspa.bonus_period_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 2",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 2 THEN bspay.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period Value 2",
    
    -- BONUS SCHEME 3 - % Achieved and Payout Period Value
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 3 THEN bspa.bonus_period_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 3",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 3 THEN bspay.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period Value 3",
    
    -- BONUS SCHEME 4 - % Achieved and Payout Period Value
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 4 THEN bspa.bonus_period_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_value,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 4",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 4 THEN bspay.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period Value 4",

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns) - CHANGED TO VALUES
    -- BONUS SCHEME 1 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_value, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Value 1",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 1 THEN bsmp.bonus_mandatory_product_value END), 0) AS "Actual Bonus MP Value 1",
    
    -- BONUS SCHEME 2 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_value, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Value 2",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 2 THEN bsmp.bonus_mandatory_product_value END), 0) AS "Actual Bonus MP Value 2",
    
    -- BONUS SCHEME 3 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_value, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Value 3",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 3 THEN bsmp.bonus_mandatory_product_value END), 0) AS "Actual Bonus MP Value 3",
    
    -- BONUS SCHEME 4 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_value, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Value 4",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 4 THEN bsmp.bonus_mandatory_product_value END), 0) AS "Actual Bonus MP Value 4",

    -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns) - CHANGED TO VALUES
    -- BONUS SCHEME 1 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_value, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 1 THEN bsmp.bonus_mandatory_product_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_value, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 1",
    
    -- BONUS SCHEME 2 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_value, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 2 THEN bsmp.bonus_mandatory_product_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_value, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 2",
    
    -- BONUS SCHEME 3 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_value, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 3 THEN bsmp.bonus_mandatory_product_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_value, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 3",
    
    -- BONUS SCHEME 4 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_value, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 4 THEN bsmp.bonus_mandatory_product_value END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_value, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_value * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 4",

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VALUE COLUMNS FOR EACH BONUS SCHEME (4 total columns) - CHANGED TO VALUES
    -- BONUS SCHEME 1 - Actual Bonus Payout Period MP Value
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 1 THEN bsmpv.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period MP Value 1",
    
    -- BONUS SCHEME 2 - Actual Bonus Payout Period MP Value
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 2 THEN bsmpv.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period MP Value 2",
    
    -- BONUS SCHEME 3 - Actual Bonus Payout Period MP Value
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 3 THEN bsmpv.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period MP Value 3",
    
    -- BONUS SCHEME 4 - Actual Bonus Payout Period MP Value
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 4 THEN bsmpv.bonus_payout_period_value END), 0) AS "Actual Bonus Payout Period MP Value 4",
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 1",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 2",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 3",
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 1",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 2",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 3"
    
  FROM slab_applied sa
  LEFT JOIN phasing_periods pp ON 1=1
  LEFT JOIN phasing_actuals pa ON sa.credit_account = pa.credit_account AND pp.phasing_id = pa.phasing_id
  LEFT JOIN phasing_payouts ppay ON sa.credit_account = ppay.credit_account AND pp.phasing_id = ppay.phasing_id
  LEFT JOIN bonus_phasing_actuals bpa ON sa.credit_account = bpa.credit_account AND pp.phasing_id = bpa.phasing_id
  LEFT JOIN bonus_phasing_payouts bppay ON sa.credit_account = bppay.credit_account AND pp.phasing_id = bppay.phasing_id
  LEFT JOIN bonus_schemes bs ON 1=1
  LEFT JOIN bonus_scheme_period_actuals bspa ON sa.credit_account = bspa.credit_account AND bs.bonus_scheme_id = bspa.bonus_scheme_id
  LEFT JOIN bonus_scheme_payout_actuals bspay ON sa.credit_account = bspay.credit_account AND bs.bonus_scheme_id = bspay.bonus_scheme_id
  LEFT JOIN bonus_scheme_mandatory_product_values bsmp ON sa.credit_account = bsmp.credit_account AND bs.bonus_scheme_id = bsmp.bonus_scheme_id
  LEFT JOIN bonus_scheme_mandatory_product_payout_values bsmpv ON sa.credit_account = bsmpv.credit_account AND bs.bonus_scheme_id = bsmpv.bonus_scheme_id
  LEFT JOIN phasing_period_payout_product_values pppv ON sa.credit_account = pppv.credit_account AND pp.phasing_id = pppv.phasing_id
  LEFT JOIN bonus_payout_period_payout_product_values bpppv ON sa.credit_account = bpppv.credit_account AND pp.phasing_id = bpppv.phasing_id
  GROUP BY sa.credit_account, sa.customer_name, sa.so_name, sa.state_name, 
           sa.total_value, sa.total_volume, sa.actual_value, sa.actual_volume,
           sa.mandatory_product_base_value, sa.mandatory_product_actual_value,
           sa.mandatory_product_target_value, sa.mandatory_product_actual_ppi,
           sa.payout_product_actual_value, sa.payout_product_actual_volume,
           sa.mandatory_product_payout, sa.growth_rate, sa.target_value, 
           sa.basic_payout, sa.additional_payout, sa.percent_achieved, sa.fixed_rebate, 
           sa.rebate_per_litre_applied, sa.rebate_percent_applied, sa.additional_rebate_on_growth_per_litre_applied,
           sa.mandatory_product_target, sa.mandatory_product_growth_percent,
           sa.mandatory_product_target_to_actual, sa.mandatory_product_rebate,
           sa.mandatory_product_rebate_percent, sa.mandatory_min_shades_ppi, sa.payout_product_payout,
           sa.base_1_value, sa.base_1_volume, sa.base_1_sum_avg_method,
           sa.base_2_value, sa.base_2_volume, sa.base_2_sum_avg_method,
           sa.base_1_months, sa.base_2_months,
           sa.base_1_value_final, sa.base_1_volume_final,
           sa.base_2_value_final, sa.base_2_volume_final
),

final_output AS (
  SELECT 
    credit_account,
    customer_name,
    so_name,
    state_name,
    -- Base Period Columns
    base_1_value AS "Base 1 Value",
    base_1_volume AS "Base 1 Volume", 
    base_1_sum_avg_method AS "Base 1 SumAvg",
    base_1_months AS "Base 1 Months",
    base_1_value_final AS "Base 1 Value Final",
    base_1_volume_final AS "Base 1 Volume Final",
    base_2_value AS "Base 2 Value",
    base_2_volume AS "Base 2 Volume",
    base_2_sum_avg_method AS "Base 2 SumAvg",
    base_2_months AS "Base 2 Months",
    base_2_value_final AS "Base 2 Value Final",
    base_2_volume_final AS "Base 2 Volume Final",
    total_value,
    total_volume,
    actual_value,
    actual_volume,
    mandatory_product_base_value AS "Mandatory Product Base Value",
    mandatory_product_actual_value AS "Mandatory Product Actual Value",
    mandatory_product_target_value AS "Mandatory Product Growth Target Value",
    mandatory_product_payout AS "Mandatory Product Payout",
    mandatory_product_actual_ppi AS "Mandatory product actual PPI",
    growth_rate,
    target_value,
    basic_payout,
    additional_payout,
    percent_achieved,
    fixed_rebate,
    rebate_per_litre_applied AS "Rebate per Litre",
    rebate_percent_applied AS "Rebate %",
    additional_rebate_on_growth_per_litre_applied AS "Additional Rebate on Growth per Litre",
    mandatory_product_target AS "Mandatory Product Fixed Target",
    mandatory_product_growth_percent AS "Mandatory Product Growth",
    mandatory_product_target_to_actual AS "Mandatory Product % Target to Actual Sales",
    mandatory_product_rebate AS "Mandatory Product Rebate",
    mandatory_product_rebate_percent AS "MP Rebate %",
    mandatory_min_shades_ppi AS "Mandatory Min. Shades - PPI",
    total_payout AS "Total Payout",
    payout_product_payout AS "Payout Product Payout",
    "Mandatory Product % to Actual - Target Value",
    "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product % PPI",
    payout_product_actual_value AS "Payout Products Value",
    payout_product_actual_volume AS "Payout Products Volume",
    
    -- REGULAR PHASING FIELDS
    "Phasing Period No 1", "Phasing Target % 1", "Phasing Target Value 1", "Phasing Period Value 1", 
    "Phasing Payout Period Value 1", "% Phasing Achieved 1", "Phasing Period Rebate 1", "Phasing Period Rebate% 1", "Phasing Payout 1",
    
    -- 9 BONUS FIELDS FOR PERIOD 1
    "Is Bonus 1", "Bonus Rebate Value 1", "Bonus Phasing Period Rebate % 1", "Bonus Phasing Target % 1", "Bonus Target Value 1", "Bonus Phasing Period Value 1",
    "Bonus Payout Period Value 1", "% Bonus Achieved 1", "Bonus Payout 1",
    
    "Phasing Period No 2", "Phasing Target % 2", "Phasing Target Value 2", "Phasing Period Value 2",
    "Phasing Payout Period Value 2", "% Phasing Achieved 2", "Phasing Period Rebate 2", "Phasing Period Rebate% 2", "Phasing Payout 2",
    
    -- 9 BONUS FIELDS FOR PERIOD 2
    "Is Bonus 2", "Bonus Rebate Value 2", "Bonus Phasing Period Rebate % 2", "Bonus Phasing Target % 2", "Bonus Target Value 2", "Bonus Phasing Period Value 2",
    "Bonus Payout Period Value 2", "% Bonus Achieved 2", "Bonus Payout 2",
    
    "Phasing Period No 3", "Phasing Target % 3", "Phasing Target Value 3", "Phasing Period Value 3",
    "Phasing Payout Period Value 3", "% Phasing Achieved 3", "Phasing Period Rebate 3", "Phasing Period Rebate% 3", "Phasing Payout 3",
    
    -- 9 BONUS FIELDS FOR PERIOD 3
    "Is Bonus 3", "Bonus Rebate Value 3", "Bonus Phasing Period Rebate % 3", "Bonus Phasing Target % 3", "Bonus Target Value 3", "Bonus Phasing Period Value 3",
    "Bonus Payout Period Value 3", "% Bonus Achieved 3", "Bonus Payout 3",

    -- **FINAL PHASING PAYOUT COLUMN**
    CASE 
      WHEN (COALESCE("% Bonus Achieved 1", 0) >= 1.0) OR (COALESCE("% Bonus Achieved 2", 0) >= 1.0)
      THEN GREATEST(COALESCE("Bonus Payout 1", 0), COALESCE("Bonus Payout 2", 0))
      ELSE COALESCE("Phasing Payout 1", 0) + COALESCE("Phasing Payout 2", 0) + COALESCE("Phasing Payout 3", 0)
    END AS "FINAL PHASING PAYOUT",

    -- NEW: 28 BONUS SCHEME COLUMNS (7 per scheme x 4 schemes)
    "Bonus Scheme No 1", "Bonus Scheme 1 % Main Scheme Target", "Bonus Scheme 1 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 1", "Minimum Mandatory Product Target Bonus Scheme 1",
    "% Reward on Total Bonus Scheme 1", "Reward on Mandatory Product % Bonus Scheme 1",
    
    "Bonus Scheme No 2", "Bonus Scheme 2 % Main Scheme Target", "Bonus Scheme 2 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 2", "Minimum Mandatory Product Target Bonus Scheme 2",
    "% Reward on Total Bonus Scheme 2", "Reward on Mandatory Product % Bonus Scheme 2",
    
    "Bonus Scheme No 3", "Bonus Scheme 3 % Main Scheme Target", "Bonus Scheme 3 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 3", "Minimum Mandatory Product Target Bonus Scheme 3",
    "% Reward on Total Bonus Scheme 3", "Reward on Mandatory Product % Bonus Scheme 3",
    
    "Bonus Scheme No 4", "Bonus Scheme 4 % Main Scheme Target", "Bonus Scheme 4 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 4", "Minimum Mandatory Product Target Bonus Scheme 4",
    "% Reward on Total Bonus Scheme 4", "Reward on Mandatory Product % Bonus Scheme 4",

    -- NEW: 4 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (16 total columns) - CHANGED TO VALUES
    "Main Scheme Bonus Target Value 1", "Actual Bonus Value 1",
    "Bonus Scheme % Achieved 1", "Actual Bonus Payout Period Value 1",
    "Main Scheme Bonus Target Value 2", "Actual Bonus Value 2", 
    "Bonus Scheme % Achieved 2", "Actual Bonus Payout Period Value 2",
    "Main Scheme Bonus Target Value 3", "Actual Bonus Value 3",
    "Bonus Scheme % Achieved 3", "Actual Bonus Payout Period Value 3",
    "Main Scheme Bonus Target Value 4", "Actual Bonus Value 4",
    "Bonus Scheme % Achieved 4", "Actual Bonus Payout Period Value 4",

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns) - CHANGED TO VALUES
    "Mandatory Product Bonus Target Value 1", "Actual Bonus MP Value 1",
    "Mandatory Product Bonus Target Value 2", "Actual Bonus MP Value 2", 
    "Mandatory Product Bonus Target Value 3", "Actual Bonus MP Value 3",
    "Mandatory Product Bonus Target Value 4", "Actual Bonus MP Value 4",

    -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns)
    "% MP Achieved 1", "% MP Achieved 2", "% MP Achieved 3", "% MP Achieved 4",

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VALUE COLUMNS FOR EACH BONUS SCHEME (4 total columns) - CHANGED TO VALUES
    "Actual Bonus Payout Period MP Value 1", "Actual Bonus Payout Period MP Value 2",
    "Actual Bonus Payout Period MP Value 3", "Actual Bonus Payout Period MP Value 4",
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    "Phasing Period Payout Product Value 1", "Phasing Period Payout Product Value 2", "Phasing Period Payout Product Value 3",
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    "Bonus Payout Period Payout Product Value 1", "Bonus Payout Period Payout Product Value 2", "Bonus Payout Period Payout Product Value 3",

    -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (CORRECTED) - CHANGED TO VALUES**
    GREATEST(
      COALESCE(mandatory_product_target_value, 0), 
      COALESCE(mandatory_product_target, 0), 
      COALESCE("Mandatory Product % to Actual - Target Value", 0)
    ) AS "MP FINAL TARGET",
    
    CASE 
      WHEN GREATEST(
        COALESCE(mandatory_product_target_value, 0), 
        COALESCE(mandatory_product_target, 0), 
        COALESCE("Mandatory Product % to Actual - Target Value", 0)
      ) = 0 THEN 0
      ELSE ROUND(
        COALESCE(mandatory_product_actual_value, 0) / NULLIF(
          GREATEST(
            COALESCE(mandatory_product_target_value, 0), 
            COALESCE(mandatory_product_target, 0), 
            COALESCE("Mandatory Product % to Actual - Target Value", 0)
          ), 0
        ), 6
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN: MP FINAL PAYOUT** - DYNAMIC BASED ON JSON PAYOUT PRODUCTS CONFIGURATION
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(mandatory_product_target_value, 0), 
                COALESCE(mandatory_product_target, 0), 
                COALESCE("Mandatory Product % to Actual - Target Value", 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(mandatory_product_actual_value, 0) / NULLIF(
                  GREATEST(
                    COALESCE(mandatory_product_target_value, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Value", 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN 
        CASE 
          -- Check JSON configuration directly for payout products
          WHEN (SELECT payout_products_configured FROM scheme_config) = true
          THEN COALESCE(payout_product_actual_value, 0) * (COALESCE(mandatory_product_rebate_percent, 0) / 100.0)
          ELSE COALESCE(actual_value, 0) * (COALESCE(mandatory_product_rebate_percent, 0) / 100.0)
        END
      ELSE 0
    END AS "MP Final Payout",

    -- **NEW BONUS SCHEME PAYOUT COLUMNS (2 per scheme x 4 schemes = 8 total)** - CHANGED TO VALUES
    
    -- BONUS SCHEME 1 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 1", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 1", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Value 1", 0)
      ELSE 0
    END AS "Bonus Scheme 1 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 1", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 1", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Value 1", 0)
      ELSE 0
    END AS "Bonus Scheme 1 MP Payout",

    -- BONUS SCHEME 2 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 2", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 2", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Value 2", 0)
      ELSE 0
    END AS "Bonus Scheme 2 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 2", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 2", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Value 2", 0)
      ELSE 0
    END AS "Bonus Scheme 2 MP Payout",

    -- BONUS SCHEME 3 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 3", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 3", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Value 3", 0)
      ELSE 0
    END AS "Bonus Scheme 3 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 3", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 3", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Value 3", 0)
      ELSE 0
    END AS "Bonus Scheme 3 MP Payout",

    -- BONUS SCHEME 4 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 4", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 4", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Value 4", 0)
      ELSE 0
    END AS "Bonus Scheme 4 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 4", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 4", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Value 4", 0)
      ELSE 0
    END AS "Bonus Scheme 4 MP Payout"
    
  FROM phasing_aggregated
),

unioned AS (
  SELECT *, 0 AS is_grand_total FROM final_output
  UNION ALL
  SELECT
    'GRAND TOTAL',
    NULL, NULL, NULL,
    -- Base Period Grand Totals
    COALESCE(SUM("Base 1 Value"), 0),
    COALESCE(SUM("Base 1 Volume"), 0),
    NULL, -- Base 1 SumAvg
    NULL, -- Base 1 Months (no meaningful total)
    COALESCE(SUM("Base 1 Value Final"), 0),
    COALESCE(SUM("Base 1 Volume Final"), 0),
    COALESCE(SUM("Base 2 Value"), 0),  
    COALESCE(SUM("Base 2 Volume"), 0),
    NULL, -- Base 2 SumAvg
    NULL, -- Base 2 Months (no meaningful total)
    COALESCE(SUM("Base 2 Value Final"), 0),
    COALESCE(SUM("Base 2 Volume Final"), 0),
    COALESCE(SUM(total_value), 0),
    COALESCE(SUM(total_volume), 0),
    COALESCE(SUM(actual_value), 0),
    COALESCE(SUM(actual_volume), 0),
    COALESCE(SUM("Mandatory Product Base Value"), 0),
    COALESCE(SUM("Mandatory Product Actual Value"), 0),
    COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
    COALESCE(SUM("Mandatory Product Payout"), 0),
    COALESCE(SUM("Mandatory product actual PPI"), 0),
    NULL,
    COALESCE(SUM(target_value), 0),
    COALESCE(SUM(basic_payout), 0),
    COALESCE(SUM(additional_payout), 0),
    CASE WHEN COALESCE(SUM(target_value), 0) = 0 THEN 0 ELSE COALESCE(SUM(actual_value), 0) / NULLIF(SUM(target_value), 0) END AS percent_achieved,
    COALESCE(SUM(fixed_rebate), 0),
    NULL,
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Fixed Target"), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Rebate"), 0),
    NULL, -- MP Rebate % (percentage field, no meaningful grand total)
    COALESCE(SUM("Mandatory Min. Shades - PPI"), 0),
    COALESCE(SUM("Total Payout"), 0),
    COALESCE(SUM("Payout Product Payout"), 0),
    COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0),
    CASE WHEN COALESCE(SUM(actual_value), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM(actual_value), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Fixed Target"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM("Mandatory Product Fixed Target"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Growth Target Value"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM("Mandatory Product Growth Target Value"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Min. Shades - PPI"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory product actual PPI"), 0) / NULLIF(SUM("Mandatory Min. Shades - PPI"), 0) END,
    COALESCE(SUM("Payout Products Value"), 0),
    COALESCE(SUM("Payout Products Volume"), 0),
    
    -- REGULAR PHASING GRAND TOTALS
    NULL, NULL, COALESCE(SUM("Phasing Target Value 1"), 0), COALESCE(SUM("Phasing Period Value 1"), 0),
    COALESCE(SUM("Phasing Payout Period Value 1"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 1"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 1
    NULL, NULL, NULL, NULL, COALESCE(SUM("Bonus Target Value 1"), 0), COALESCE(SUM("Bonus Phasing Period Value 1"), 0),
    COALESCE(SUM("Bonus Payout Period Value 1"), 0), NULL, COALESCE(SUM("Bonus Payout 1"), 0),
    
    NULL, NULL, COALESCE(SUM("Phasing Target Value 2"), 0), COALESCE(SUM("Phasing Period Value 2"), 0),
    COALESCE(SUM("Phasing Payout Period Value 2"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 2"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 2
    NULL, NULL, NULL, NULL, COALESCE(SUM("Bonus Target Value 2"), 0), COALESCE(SUM("Bonus Phasing Period Value 2"), 0),
    COALESCE(SUM("Bonus Payout Period Value 2"), 0), NULL, COALESCE(SUM("Bonus Payout 2"), 0),
    
    NULL, NULL, COALESCE(SUM("Phasing Target Value 3"), 0), COALESCE(SUM("Phasing Period Value 3"), 0),
    COALESCE(SUM("Phasing Payout Period Value 3"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 3"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 3
    NULL, NULL, NULL, NULL, COALESCE(SUM("Bonus Target Value 3"), 0), COALESCE(SUM("Bonus Phasing Period Value 3"), 0),
    COALESCE(SUM("Bonus Payout Period Value 3"), 0), NULL, COALESCE(SUM("Bonus Payout 3"), 0),
    
    -- **FINAL PHASING PAYOUT GRAND TOTAL**
    COALESCE(SUM("FINAL PHASING PAYOUT"), 0),

    -- BONUS SCHEME GRAND TOTALS (all set to NULL as these are configuration values)
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,

    -- NEW: 4 BONUS SCHEME PERIOD COLUMNS GRAND TOTALS (16 total columns) - CHANGED TO VALUES
    COALESCE(SUM("Main Scheme Bonus Target Value 1"), 0), COALESCE(SUM("Actual Bonus Value 1"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Value 1"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Value 2"), 0), COALESCE(SUM("Actual Bonus Value 2"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Value 2"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Value 3"), 0), COALESCE(SUM("Actual Bonus Value 3"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Value 3"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Value 4"), 0), COALESCE(SUM("Actual Bonus Value 4"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Value 4"), 0),

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS GRAND TOTALS (8 total columns) - CHANGED TO VALUES
    COALESCE(SUM("Mandatory Product Bonus Target Value 1"), 0), COALESCE(SUM("Actual Bonus MP Value 1"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Value 2"), 0), COALESCE(SUM("Actual Bonus MP Value 2"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Value 3"), 0), COALESCE(SUM("Actual Bonus MP Value 3"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Value 4"), 0), COALESCE(SUM("Actual Bonus MP Value 4"), 0),

    -- NEW: % MP ACHIEVED COLUMNS GRAND TOTALS (4 total columns)
    NULL, NULL, NULL, NULL, -- % MP Achieved columns don't have meaningful grand totals

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VALUE COLUMNS GRAND TOTALS (4 total columns) - CHANGED TO VALUES
    COALESCE(SUM("Actual Bonus Payout Period MP Value 1"), 0), COALESCE(SUM("Actual Bonus Payout Period MP Value 2"), 0),
    COALESCE(SUM("Actual Bonus Payout Period MP Value 3"), 0), COALESCE(SUM("Actual Bonus Payout Period MP Value 4"), 0),
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES GRAND TOTALS (3 total columns)
    COALESCE(SUM("Phasing Period Payout Product Value 1"), 0), COALESCE(SUM("Phasing Period Payout Product Value 2"), 0), COALESCE(SUM("Phasing Period Payout Product Value 3"), 0),
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES GRAND TOTALS (3 total columns)
    COALESCE(SUM("Bonus Payout Period Payout Product Value 1"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Value 2"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Value 3"), 0),

    -- **NEW COLUMNS GRAND TOTALS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (CORRECTED)** - CHANGED TO VALUES
    GREATEST(
      COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
      COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
      COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
    ) AS "MP FINAL TARGET",
    CASE 
      WHEN GREATEST(
        COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
        COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
        COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
      ) = 0 THEN 0
      ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(
        GREATEST(
          COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
          COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
          COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
        ), 0
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN GRAND TOTAL: MP FINAL PAYOUT** - CHANGED TO VALUES
    COALESCE(SUM("MP Final Payout"), 0) AS "MP Final Payout",

    -- **NEW BONUS SCHEME PAYOUT COLUMNS GRAND TOTALS (8 total)**
    COALESCE(SUM("Bonus Scheme 1 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 1 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 2 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 2 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 3 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 3 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 4 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 4 MP Payout"), 0),
    
    1 AS is_grand_total
  FROM final_output
)

-- FINAL SELECT WITH ALL COLUMNS INCLUDING NEW MP FINAL COLUMNS - CHANGED TO VALUES
SELECT 
  u.state_name,
  u.credit_account,
  u.customer_name,
  u.so_name,
  -- Base Period Columns
  u."Base 1 Value",
  u."Base 1 Volume", 
  u."Base 1 SumAvg",
  u."Base 1 Months",
  u."Base 1 Value Final",
  u."Base 1 Volume Final",
  u."Base 2 Value",
  u."Base 2 Volume",
  u."Base 2 SumAvg",
  u."Base 2 Months",
  u."Base 2 Value Final",
  u."Base 2 Volume Final",
  u.total_value,
  u.total_volume,
  u.growth_rate,
  sg.strata_growth_percentage AS "Strata Growth %",
  u.target_value,
  u.actual_value,
  u.actual_volume,
  u.percent_achieved AS "% Achieved",
  u."Rebate per Litre",
  u."Rebate %",
  u.basic_payout,
  u."Additional Rebate on Growth per Litre",
  u.additional_payout,
  u.fixed_rebate AS "Fixed Rebate",
  u."Total Payout",
  u."Payout Product Payout",
  u."Mandatory Product Base Value",
  u."Mandatory Product Growth",
  u."Mandatory Product Growth Target Value",
  u."Mandatory Product Actual Value",
  u."Mandatory Product Growth Target Achieved",
  u."Mandatory Min. Shades - PPI",
  u."Mandatory product actual PPI",
  u."Mandatory Product % PPI",
  u."Mandatory Product Fixed Target",
  u."Mandatory Product Actual Value % to Fixed Target Sales",
  u."Mandatory Product % Target to Actual Sales",
  u."Mandatory Product % to Actual - Target Value",
  u."Mandatory Product Actual Value % to Total Sales",
  u."Mandatory Product Rebate",
  u."MP Rebate %",
  u."Mandatory Product Payout",
  u."Payout Products Value",
  u."Payout Products Volume",
  
  -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD - CHANGED TO VALUES
  u."Phasing Period No 1", u."Phasing Target % 1", u."Phasing Target Value 1", u."Phasing Period Value 1", 
  u."Phasing Payout Period Value 1", u."% Phasing Achieved 1", u."Phasing Period Rebate 1", u."Phasing Period Rebate% 1", u."Phasing Payout 1",
  u."Is Bonus 1", u."Bonus Rebate Value 1", u."Bonus Phasing Period Rebate % 1", u."Bonus Phasing Target % 1", u."Bonus Target Value 1", u."Bonus Phasing Period Value 1",
  u."Bonus Payout Period Value 1", u."% Bonus Achieved 1", u."Bonus Payout 1",
  
  u."Phasing Period No 2", u."Phasing Target % 2", u."Phasing Target Value 2", u."Phasing Period Value 2",
  u."Phasing Payout Period Value 2", u."% Phasing Achieved 2", u."Phasing Period Rebate 2", u."Phasing Period Rebate% 2", u."Phasing Payout 2",
  u."Is Bonus 2", u."Bonus Rebate Value 2", u."Bonus Phasing Period Rebate % 2", u."Bonus Phasing Target % 2", u."Bonus Target Value 2", u."Bonus Phasing Period Value 2",
  u."Bonus Payout Period Value 2", u."% Bonus Achieved 2", u."Bonus Payout 2",
  
  u."Phasing Period No 3", u."Phasing Target % 3", u."Phasing Target Value 3", u."Phasing Period Value 3",
  u."Phasing Payout Period Value 3", u."% Phasing Achieved 3", u."Phasing Period Rebate 3", u."Phasing Period Rebate% 3", u."Phasing Payout 3",
  u."Is Bonus 3", u."Bonus Rebate Value 3", u."Bonus Phasing Period Rebate % 3", u."Bonus Phasing Target % 3", u."Bonus Target Value 3", u."Bonus Phasing Period Value 3",
  u."Bonus Payout Period Value 3", u."% Bonus Achieved 3", u."Bonus Payout 3",
  
  -- **FINAL PHASING PAYOUT COLUMN**
  u."FINAL PHASING PAYOUT",

  -- **28 BONUS SCHEME COLUMNS (7 per scheme x 4 schemes)**
  u."Bonus Scheme No 1", u."Bonus Scheme 1 % Main Scheme Target", u."Bonus Scheme 1 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 1", u."Minimum Mandatory Product Target Bonus Scheme 1",
  u."% Reward on Total Bonus Scheme 1", u."Reward on Mandatory Product % Bonus Scheme 1",
  
  u."Bonus Scheme No 2", u."Bonus Scheme 2 % Main Scheme Target", u."Bonus Scheme 2 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 2", u."Minimum Mandatory Product Target Bonus Scheme 2",
  u."% Reward on Total Bonus Scheme 2", u."Reward on Mandatory Product % Bonus Scheme 2",
  
  u."Bonus Scheme No 3", u."Bonus Scheme 3 % Main Scheme Target", u."Bonus Scheme 3 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 3", u."Minimum Mandatory Product Target Bonus Scheme 3",
  u."% Reward on Total Bonus Scheme 3", u."Reward on Mandatory Product % Bonus Scheme 3",
  
  u."Bonus Scheme No 4", u."Bonus Scheme 4 % Main Scheme Target", u."Bonus Scheme 4 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 4", u."Minimum Mandatory Product Target Bonus Scheme 4",
  u."% Reward on Total Bonus Scheme 4", u."Reward on Mandatory Product % Bonus Scheme 4",

  -- NEW: 4 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (16 total columns) - CHANGED TO VALUES
  u."Main Scheme Bonus Target Value 1", u."Actual Bonus Value 1",
  u."Bonus Scheme % Achieved 1", u."Actual Bonus Payout Period Value 1",
  u."Main Scheme Bonus Target Value 2", u."Actual Bonus Value 2",
  u."Bonus Scheme % Achieved 2", u."Actual Bonus Payout Period Value 2",
  u."Main Scheme Bonus Target Value 3", u."Actual Bonus Value 3",
  u."Bonus Scheme % Achieved 3", u."Actual Bonus Payout Period Value 3",
  u."Main Scheme Bonus Target Value 4", u."Actual Bonus Value 4",
  u."Bonus Scheme % Achieved 4", u."Actual Bonus Payout Period Value 4",

  -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns) - CHANGED TO VALUES
  u."Mandatory Product Bonus Target Value 1", u."Actual Bonus MP Value 1",
  u."Mandatory Product Bonus Target Value 2", u."Actual Bonus MP Value 2",
  u."Mandatory Product Bonus Target Value 3", u."Actual Bonus MP Value 3",
  u."Mandatory Product Bonus Target Value 4", u."Actual Bonus MP Value 4",

  -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns)
  u."% MP Achieved 1", u."% MP Achieved 2", u."% MP Achieved 3", u."% MP Achieved 4",

  -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VALUE COLUMNS FOR EACH BONUS SCHEME (4 total columns) - CHANGED TO VALUES
  u."Actual Bonus Payout Period MP Value 1", u."Actual Bonus Payout Period MP Value 2",
  u."Actual Bonus Payout Period MP Value 3", u."Actual Bonus Payout Period MP Value 4",
  -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
  u."Phasing Period Payout Product Value 1", u."Phasing Period Payout Product Value 2", u."Phasing Period Payout Product Value 3",
  -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
  u."Bonus Payout Period Payout Product Value 1", u."Bonus Payout Period Payout Product Value 2", u."Bonus Payout Period Payout Product Value 3",

  -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)** - CHANGED TO VALUES
  u."MP FINAL TARGET",
  u."MP FINAL ACHEIVMENT %",
  u."MP Final Payout",

  -- **NEW BONUS SCHEME PAYOUT COLUMNS (2 per scheme x 4 schemes = 8 total)**
  u."Bonus Scheme 1 Bonus Payout", u."Bonus Scheme 1 MP Payout",
  u."Bonus Scheme 2 Bonus Payout", u."Bonus Scheme 2 MP Payout",
  u."Bonus Scheme 3 Bonus Payout", u."Bonus Scheme 3 MP Payout",
  u."Bonus Scheme 4 Bonus Payout", u."Bonus Scheme 4 MP Payout"
  
FROM unioned u
LEFT JOIN strata_growth sg ON u.credit_account = sg.credit_account::text
ORDER BY u.is_grand_total, u.credit_account;

"""

TRACKER_MAINSCHEME_VOLUME = """
SET SESSION statement_timeout = 0;

WITH scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id = '{scheme_id}'
),

-- Base Period 1 Configuration with Month Calculation
base_period_1_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->0->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

-- Base Period 2 Configuration with Month Calculation
base_period_2_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->1->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

base_dates AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'fromDate')::date + INTERVAL '1 day')::date AS scheme_from_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'toDate')::date + INTERVAL '1 day')::date AS scheme_to_date
  FROM scheme
),

-- REFERENCE CTEs
states AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates') AS state FROM scheme
),
regions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedRegions') AS region FROM scheme
),
area_heads AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedAreaHeads') AS area_head FROM scheme
),
divisions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDivisions') AS division FROM scheme
),
dealer_types AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDealerTypes') AS dealer_type FROM scheme
),
distributors AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDistributors') AS distributor FROM scheme
),
product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'materials') AS material FROM scheme
),
product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'categories') AS category FROM scheme
),
product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'grps') AS grp FROM scheme
),
product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'wandaGroups') AS wanda_group FROM scheme
),
product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'thinnerGroups') AS thinner_group FROM scheme
),

-- SEPARATE PAYOUT PRODUCTS EXTRACTION CTEs
payout_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials') AS material FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials') > 0
),
payout_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories') AS category FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'categories') > 0
),
payout_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps') AS grp FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'grps') > 0
),
payout_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups') AS wanda_group FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'wandaGroups') > 0
),
payout_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups') AS thinner_group FROM scheme
  WHERE scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'thinnerGroups') > 0
),

slabs AS (
  SELECT 
    COALESCE(NULLIF(slab->>'slabStart', ''), '0')::NUMERIC AS slab_start,
    COALESCE(NULLIF(slab->>'slabEnd', ''), '0')::NUMERIC AS slab_end,
    COALESCE(NULLIF(slab->>'growthPercent', ''), '0')::NUMERIC / 100.0 AS growth_rate,
    COALESCE(NULLIF(slab->>'dealerMayQualifyPercent', ''), '0')::NUMERIC / 100.0 AS qualification_rate,
    COALESCE(NULLIF(slab->>'rebatePerLitre', ''), '0')::NUMERIC AS rebate_per_litre,
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
    slab_start, growth_rate, qualification_rate, rebate_per_litre, 
    additional_rebate_on_growth_per_litre, fixed_rebate,
    mandatory_product_target, mandatory_product_growth_percent,
    mandatory_product_target_to_actual, mandatory_product_rebate,
    mandatory_product_rebate_percent, mandatory_min_shades_ppi
  FROM slabs
  WHERE slab_order = 1
),

-- UPDATED PHASING PERIODS WITH BONUS FIELDS
phasing_periods AS (
  SELECT 
    COALESCE(NULLIF(phasing->>'id', ''), '0')::INTEGER AS phasing_id,
    COALESCE(NULLIF(phasing->>'rebateValue', ''), '0')::NUMERIC AS rebate_value,
    ((phasing->>'payoutToDate')::timestamp + INTERVAL '1 day')::date AS payout_to_date,
    ((phasing->>'phasingToDate')::timestamp + INTERVAL '1 day')::date AS phasing_to_date,
    ((phasing->>'payoutFromDate')::timestamp + INTERVAL '1 day')::date AS payout_from_date,
    ((phasing->>'phasingFromDate')::timestamp + INTERVAL '1 day')::date AS phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS phasing_bonus_target_percent,
    -- BONUS FIELDS
    COALESCE((phasing->>'isBonus')::boolean, false) AS is_bonus,
    COALESCE(NULLIF(phasing->>'bonusRebateValue', ''), '0')::NUMERIC AS bonus_rebate_value,
    ((phasing->>'bonusPayoutToDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_to_date,
    ((phasing->>'bonusPhasingToDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_to_date,
    ((phasing->>'bonusPayoutFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_from_date,
    ((phasing->>'bonusPhasingFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS bonus_phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS bonus_phasing_target_percent_raw
  FROM scheme,
  LATERAL jsonb_array_elements(scheme_json->'mainScheme'->'phasingPeriods') AS phasing
),

-- NEW: BONUS SCHEMES EXTRACTION
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
),

-- BONUS SCHEME PAYOUT PERIOD VOLUMES
bonus_scheme_payout_volumes AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS bonus_payout_period_volume
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_payout_from_date AND bs.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- BONUS SCHEME PERIOD ACTUALS (for bonus period dates)
bonus_scheme_period_actuals AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS bonus_period_volume
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_period_from_date AND bs.bonus_period_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- BONUS SCHEME PAYOUT ACTUALS (alias for payout volumes)
bonus_scheme_payout_actuals AS (
  SELECT 
    bonus_scheme_id,
    credit_account,
    bonus_payout_period_volume
  FROM bonus_scheme_payout_volumes
),

-- BONUS SCHEME MANDATORY PRODUCT VOLUMES (for bonus period dates)
bonus_scheme_mandatory_product_volumes AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS bonus_mandatory_product_volume
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_period_from_date AND bs.bonus_period_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- NEW: BONUS SCHEME MANDATORY PRODUCT PAYOUT PERIOD VOLUMES
bonus_scheme_mandatory_product_payout_volumes AS (
  SELECT 
    bs.bonus_scheme_id,
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS bonus_payout_period_volume
  FROM bonus_schemes bs
  CROSS JOIN sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bs.bonus_payout_from_date AND bs.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY bs.bonus_scheme_id, sd.credit_account
),

-- Extract enableStrataGrowth flag and payoutProducts configuration from JSON
scheme_config AS (
  SELECT 
    COALESCE((scheme_json->'mainScheme'->'slabData'->>'enableStrataGrowth')::boolean, false) AS enable_strata_growth,
    CASE 
      WHEN scheme_json->'configuration'->'enabledSections'->>'payoutProducts' = 'true' 
           AND scheme_json->'mainScheme'->'productData'->'payoutProducts' IS NOT NULL
           AND jsonb_typeof(scheme_json->'mainScheme'->'productData'->'payoutProducts') = 'object'
           AND jsonb_array_length(COALESCE(scheme_json->'mainScheme'->'productData'->'payoutProducts'->'materials', '[]'::jsonb)) > 0
      THEN true
      ELSE false
    END AS payout_products_enabled,
    scheme_json
  FROM scheme
),

-- Base Period 1 Sales Data
base_period_1_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_1_volume,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_1_value,
    bp1.sum_avg_method AS base_1_sum_avg_method,
    bp1.months_count AS base_1_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_1_config bp1
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp1.from_date AND bp1.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp1.sum_avg_method, bp1.months_count
),

-- Base Period 1 Final Calculations
base_period_1_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_volume / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_volume
    END AS base_1_volume_final,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_value / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_value
    END AS base_1_value_final
  FROM base_period_1_sales
),

-- Base Period 2 Sales Data
base_period_2_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_2_volume,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_2_value,
    bp2.sum_avg_method AS base_2_sum_avg_method,
    bp2.months_count AS base_2_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_2_config bp2
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp2.from_date AND bp2.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp2.sum_avg_method, bp2.months_count
),

-- Base Period 2 Final Calculations
base_period_2_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_volume / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_volume
    END AS base_2_volume_final,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_value / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_value
    END AS base_2_value_final
  FROM base_period_2_sales
),

base_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    COALESCE(SUM(sd.volume), 0) AS total_volume,
    COALESCE(SUM(sd.value), 0) AS total_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

actuals AS (
  SELECT
    sd.credit_account,
    MIN(sd.customer_name) AS actual_customer_name,
    MIN(sd.so_name) AS actual_so_name,
    MIN(sd.state_name) AS actual_state_name,
    COALESCE(SUM(sd.volume), 0) AS actual_volume,
    COALESCE(SUM(sd.value), 0) AS actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_actual_ppi AS (
  SELECT 
    sd.credit_account,
    COUNT(DISTINCT sd.material) AS mandatory_product_actual_ppi_count
  FROM sales_data sd
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND sd.material::text IN (
      SELECT mat_item FROM scheme,
      LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
      WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
        AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
    )
  GROUP BY sd.credit_account
),

mandatory_product_base_volume AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS mandatory_base_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY sd.credit_account
),

mandatory_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS mandatory_actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      sd.material::text IN (
        SELECT mat_item FROM scheme,
        LATERAL jsonb_array_elements_text(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') AS mat_item
        WHERE scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'mainScheme'->'productData'->'mandatoryProducts'->'materials') > 0
      )
    )
  GROUP BY sd.credit_account
),

payout_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS payout_product_actual_volume,
    COALESCE(SUM(sd.value), 0) AS payout_product_actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

all_accounts AS (
  SELECT credit_account FROM base_sales
  UNION
  SELECT credit_account FROM actuals
  UNION
  SELECT credit_account FROM mandatory_product_base_volume
  UNION
  SELECT credit_account FROM mandatory_product_actuals
  UNION
  SELECT credit_account FROM mandatory_product_actual_ppi
  UNION
  SELECT credit_account FROM payout_product_actuals
  UNION
  SELECT credit_account FROM base_period_1_finals
  UNION
  SELECT credit_account FROM base_period_2_finals
),

-- Joined CTE with Updated Total Volume/Value Formulas
joined AS (
  SELECT 
    aa.credit_account,
    COALESCE(bs.customer_name, ac.actual_customer_name, bp1.customer_name, bp2.customer_name) AS customer_name,
    COALESCE(bs.so_name, ac.actual_so_name, bp1.so_name, bp2.so_name) AS so_name,
    COALESCE(bs.state_name, ac.actual_state_name, bp1.state_name, bp2.state_name) AS state_name,
    -- Total Volume/Value uses MAX of Base Final Values
    GREATEST(COALESCE(bp1.base_1_volume_final, 0), COALESCE(bp2.base_2_volume_final, 0)) AS total_volume,
    GREATEST(COALESCE(bp1.base_1_value_final, 0), COALESCE(bp2.base_2_value_final, 0)) AS total_value,
    COALESCE(ac.actual_volume, 0) AS actual_volume,
    COALESCE(ac.actual_value, 0) AS actual_value,
    COALESCE(mpbv.mandatory_base_volume, 0) AS mandatory_product_base_volume,
    COALESCE(mpa.mandatory_actual_volume, 0) AS mandatory_product_actual_volume,
    CASE 
      WHEN COALESCE(mpa.mandatory_actual_volume, 0) < 0 THEN 0
      ELSE COALESCE(mpappi.mandatory_product_actual_ppi_count, 0)
    END AS mandatory_product_actual_ppi,
    COALESCE(ppa.payout_product_actual_volume, 0) AS payout_product_actual_volume,
    COALESCE(ppa.payout_product_actual_value, 0) AS payout_product_actual_value,
    -- Base Period Columns
    COALESCE(bp1.base_1_volume, 0) AS base_1_volume,
    COALESCE(bp1.base_1_value, 0) AS base_1_value,
    REPLACE(REPLACE(bp1.base_1_sum_avg_method, '"', ''), '"', '') AS base_1_sum_avg_method,
    COALESCE(bp2.base_2_volume, 0) AS base_2_volume,
    COALESCE(bp2.base_2_value, 0) AS base_2_value,
    REPLACE(REPLACE(bp2.base_2_sum_avg_method, '"', ''), '"', '') AS base_2_sum_avg_method,
    -- Base Period Month Counts
    COALESCE(bp1.base_1_months_count, 0) AS base_1_months,
    COALESCE(bp2.base_2_months_count, 0) AS base_2_months,
    -- Base Period Final Values
    COALESCE(bp1.base_1_volume_final, 0) AS base_1_volume_final,
    COALESCE(bp1.base_1_value_final, 0) AS base_1_value_final,
    COALESCE(bp2.base_2_volume_final, 0) AS base_2_volume_final,
    COALESCE(bp2.base_2_value_final, 0) AS base_2_value_final
  FROM all_accounts aa
  LEFT JOIN base_sales bs ON aa.credit_account = bs.credit_account
  LEFT JOIN actuals ac ON aa.credit_account = ac.credit_account
  LEFT JOIN mandatory_product_base_volume mpbv ON aa.credit_account = mpbv.credit_account
  LEFT JOIN mandatory_product_actuals mpa ON aa.credit_account = mpa.credit_account
  LEFT JOIN mandatory_product_actual_ppi mpappi ON aa.credit_account = mpappi.credit_account
  LEFT JOIN payout_product_actuals ppa ON aa.credit_account = ppa.credit_account
  LEFT JOIN base_period_1_finals bp1 ON aa.credit_account = bp1.credit_account
  LEFT JOIN base_period_2_finals bp2 ON aa.credit_account = bp2.credit_account
),

slab_applied AS (
  SELECT
    j.*,
    -- MODIFIED GROWTH RATE LOGIC WITH enableStrataGrowth CHECK
    CASE 
      WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
      THEN sg.strata_growth_percentage / 100.0
      ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
    END AS growth_rate,
    COALESCE(sl_actual.rebate_per_litre, fs.rebate_per_litre, 0) AS rebate_per_litre_applied,
    COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) AS additional_rebate_on_growth_per_litre_applied,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_base,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_actual,
    
    CASE 
      WHEN COALESCE(sl_actual.mandatory_product_target, 0) <= 0 THEN fs.mandatory_product_target
      ELSE COALESCE(sl_actual.mandatory_product_target, 0)
    END AS mandatory_product_target,
    
    COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0) AS mandatory_product_growth_percent,
    COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0) AS mandatory_product_target_to_actual,
    COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) AS mandatory_product_rebate,
    COALESCE(sl_actual.mandatory_product_rebate_percent, fs.mandatory_product_rebate_percent, 0) AS mandatory_product_rebate_percent,
    COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) AS mandatory_min_shades_ppi,
    
    CASE 
      WHEN j.mandatory_product_base_volume IS NULL THEN 0
      ELSE j.mandatory_product_base_volume * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
    END AS mandatory_product_target_volume,
    
    -- UPDATED TARGET VOLUME CALCULATION WITH enableStrataGrowth CHECK
    CASE 
      WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
      ELSE GREATEST(
        (1 + CASE 
               WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
               THEN sg.strata_growth_percentage / 100.0
               ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
             END) * j.total_volume, 
        COALESCE(sl_base.slab_start, fs.slab_start, 0)
      )
    END AS target_volume,
    
    CASE 
      WHEN (CASE 
              WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_volume, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      ELSE j.actual_volume / 
        (CASE 
          WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
          ELSE GREATEST(
            (1 + CASE 
                   WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                   THEN sg.strata_growth_percentage / 100.0
                   ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                 END) * j.total_volume, 
            COALESCE(sl_base.slab_start, fs.slab_start, 0)
          )
        END)
    END AS percent_achieved,
    
    -- BASIC PAYOUT: Dynamic check for payoutProducts configuration
    CASE 
      WHEN sc.payout_products_enabled = false THEN 
        COALESCE(sl_actual.rebate_per_litre, fs.rebate_per_litre, 0) * j.actual_volume
      ELSE 
        COALESCE(sl_actual.rebate_per_litre, fs.rebate_per_litre, 0) * j.payout_product_actual_volume
    END AS basic_payout,
    
    -- ADDITIONAL PAYOUT: Dynamic check for payoutProducts configuration
    CASE 
      WHEN j.actual_volume >= 
           (CASE 
              WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_volume, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END)
      THEN CASE 
             WHEN sc.payout_products_enabled = false THEN 
               COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.actual_volume
             ELSE 
               COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_volume
           END
      ELSE 0
    END AS additional_payout,
    
    CASE 
      WHEN (
        (CASE 
          WHEN COALESCE(j.actual_volume, 0) = 0 THEN 0 
          ELSE COALESCE(j.mandatory_product_actual_volume, 0) / NULLIF(j.actual_volume, 0) 
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_actual_volume < 0 THEN 0
          WHEN COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_ppi / NULLIF(COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_base_volume * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)) = 0 THEN 0 
          ELSE j.mandatory_product_actual_volume / NULLIF(j.mandatory_product_base_volume * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_volume / NULLIF(COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 0)
        END) >= 1.0
      )
      -- MP FINAL PAYOUT: Dynamic check for payoutProducts configuration
      THEN CASE 
             WHEN sc.payout_products_enabled = false THEN 
               COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) * j.actual_volume
             ELSE 
               COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) * j.payout_product_actual_volume
           END
      ELSE 0
    END AS mandatory_product_payout,
    
    CASE
      WHEN (CASE 
              WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_volume, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      WHEN j.actual_volume / 
           (CASE 
              WHEN j.total_volume = 0 OR j.total_volume IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_volume, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) >= 1
        THEN COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0)
      ELSE 0
    END AS fixed_rebate,
    
    -- PAYOUT PRODUCT PAYOUT: Dynamic check for payoutProducts configuration
    -- Based on specification: Additional Rebate on Growth per Litre * (actual_volume OR payout_product_actual_volume)
    CASE 
      WHEN sc.payout_products_enabled = false THEN 
        COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.actual_volume
      ELSE 
        COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_volume
    END AS payout_product_payout
    
  FROM joined j
  LEFT JOIN slabs sl_actual ON j.actual_volume BETWEEN sl_actual.slab_start AND sl_actual.slab_end
  LEFT JOIN slabs sl_base ON j.total_volume BETWEEN sl_base.slab_start AND sl_base.slab_end
  LEFT JOIN strata_growth sg ON j.credit_account::bigint = sg.credit_account
  CROSS JOIN first_slab fs
  CROSS JOIN scheme_config sc
),

phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.phasing_from_date AND pp.phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_payout_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- PHASING PAYOUT PERIOD PAYOUT PRODUCT VOLUMES
phasing_payout_product_volumes AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_payout_period_payout_product_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING ACTUALS
bonus_phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_phasing_from_date AND pp.bonus_phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUTS
bonus_phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_payout_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      OR (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      OR (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      OR (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUT PERIOD PAYOUT PRODUCT VOLUMES
bonus_phasing_payout_product_volumes AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_payout_period_payout_product_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),



-- UPDATED PHASING AGGREGATED WITH FINAL PHASING PAYOUT COLUMN AND NEW BONUS SCHEME COLUMNS
phasing_aggregated AS (
  SELECT
    sa.credit_account,
    sa.customer_name,
    sa.so_name,
    sa.state_name,
    sa.total_volume,
    sa.total_value,
    sa.actual_volume,
    sa.actual_value,
    sa.mandatory_product_base_volume,
    sa.mandatory_product_actual_volume,
    sa.mandatory_product_target_volume,
    sa.mandatory_product_payout,
    sa.mandatory_product_actual_ppi,
    sa.payout_product_actual_volume,
    sa.payout_product_actual_value,
    sa.growth_rate,
    sa.target_volume,
    sa.basic_payout,
    sa.additional_payout,
    sa.percent_achieved,
    sa.fixed_rebate,
    sa.rebate_per_litre_applied,
    sa.additional_rebate_on_growth_per_litre_applied,
    sa.mandatory_product_target,
    sa.mandatory_product_growth_percent,
    sa.mandatory_product_target_to_actual,
    sa.mandatory_product_rebate,
    sa.mandatory_product_rebate_percent,
    sa.mandatory_min_shades_ppi,
    sa.basic_payout + sa.additional_payout + sa.fixed_rebate + 
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(sa.mandatory_product_target_volume, 0), 
                COALESCE(sa.mandatory_product_target, 0), 
                COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(sa.mandatory_product_actual_volume, 0) / NULLIF(
                  GREATEST(
                    COALESCE(sa.mandatory_product_target_volume, 0), 
                    COALESCE(sa.mandatory_product_target, 0), 
                    COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN COALESCE(sa.mandatory_product_actual_volume, 0) * COALESCE(sa.mandatory_product_rebate, 0)
      ELSE 0
    END AS total_payout,
    sa.basic_payout + sa.additional_payout + sa.fixed_rebate + 
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(sa.mandatory_product_target_volume, 0), 
                COALESCE(sa.mandatory_product_target, 0), 
                COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(sa.mandatory_product_actual_volume, 0) / NULLIF(
                  GREATEST(
                    COALESCE(sa.mandatory_product_target_volume, 0), 
                    COALESCE(sa.mandatory_product_target, 0), 
                    COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN COALESCE(sa.mandatory_product_actual_volume, 0) * COALESCE(sa.mandatory_product_rebate, 0)
      ELSE 0
    END AS payout_product_payout,
    -- Base Period Columns
    sa.base_1_volume,
    sa.base_1_value,
    sa.base_1_sum_avg_method,
    sa.base_2_volume,
    sa.base_2_value,
    sa.base_2_sum_avg_method,
    -- Base Period Month Counts
    sa.base_1_months,
    sa.base_2_months,
    -- Base Period Final Values
    sa.base_1_volume_final,
    sa.base_1_value_final,
    sa.base_2_volume_final,
    sa.base_2_value_final,
    
    sa.actual_volume * sa.mandatory_product_target_to_actual AS "Mandatory Product % to Actual - Target Volume",
    CASE WHEN sa.actual_volume = 0 THEN 0 ELSE sa.mandatory_product_actual_volume / sa.actual_volume END AS "Mandatory Product Actual Volume % to Total Sales",
    CASE WHEN sa.mandatory_product_target = 0 THEN 0 ELSE sa.mandatory_product_actual_volume / sa.mandatory_product_target END AS "Mandatory Product Actual Volume % to Fixed Target Sales",
    CASE WHEN sa.mandatory_product_target_volume = 0 THEN 0 ELSE sa.mandatory_product_actual_volume / sa.mandatory_product_target_volume END AS "Mandatory Product Growth Target Achieved",
    
    CASE 
      WHEN sa.mandatory_product_actual_volume < 0 THEN 0
      WHEN sa.mandatory_min_shades_ppi = 0 THEN 0 
      ELSE sa.mandatory_product_actual_ppi / sa.mandatory_min_shades_ppi 
    END AS "Mandatory Product % PPI",
    
    -- PHASING PERIOD 1
    MAX(CASE WHEN pp.phasing_id = 1 THEN 1 END) AS "Phasing Period No 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.phasing_target_percent END) AS "Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.phasing_target_percent * sa.target_volume, 2) END) AS "Phasing Target Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pa.phasing_period_volume, 0) END) AS "Phasing Period Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(ppay.phasing_payout_period_volume, 0) END) AS "Phasing Payout Period Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) END) AS "Phasing Period Payout Product Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Phasing Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_value END) AS "Phasing Period Rebate 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 1",
    
    -- 8 BONUS FIELDS FOR PERIOD 1
    (MAX(CASE WHEN pp.phasing_id = 1 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_volume, 2) END) AS "Bonus Target Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpa.bonus_phasing_period_volume, 0) END) AS "Bonus Phasing Period Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bppay.bonus_phasing_payout_period_volume, 0) END) AS "Bonus Payout Period Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) END) AS "Bonus Payout Period Payout Product Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 1",
    
    -- PHASING PERIOD 2
    MAX(CASE WHEN pp.phasing_id = 2 THEN 2 END) AS "Phasing Period No 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.phasing_target_percent END) AS "Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.phasing_target_percent * sa.target_volume, 2) END) AS "Phasing Target Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pa.phasing_period_volume, 0) END) AS "Phasing Period Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(ppay.phasing_payout_period_volume, 0) END) AS "Phasing Payout Period Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) END) AS "Phasing Period Payout Product Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Phasing Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_value END) AS "Phasing Period Rebate 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 2",
    
    -- 8 BONUS FIELDS FOR PERIOD 2
    (MAX(CASE WHEN pp.phasing_id = 2 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_volume, 2) END) AS "Bonus Target Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpa.bonus_phasing_period_volume, 0) END) AS "Bonus Phasing Period Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bppay.bonus_phasing_payout_period_volume, 0) END) AS "Bonus Payout Period Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) END) AS "Bonus Payout Period Payout Product Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 2",
    
    -- PHASING PERIOD 3
    MAX(CASE WHEN pp.phasing_id = 3 THEN 3 END) AS "Phasing Period No 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.phasing_target_percent END) AS "Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.phasing_target_percent * sa.target_volume, 2) END) AS "Phasing Target Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pa.phasing_period_volume, 0) END) AS "Phasing Period Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(ppay.phasing_payout_period_volume, 0) END) AS "Phasing Payout Period Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) END) AS "Phasing Period Payout Product Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Phasing Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_value END) AS "Phasing Period Rebate 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 3",
    
    -- 8 BONUS FIELDS FOR PERIOD 3
    (MAX(CASE WHEN pp.phasing_id = 3 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_volume, 2) END) AS "Bonus Target Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpa.bonus_phasing_period_volume, 0) END) AS "Bonus Phasing Period Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bppay.bonus_phasing_payout_period_volume, 0) END) AS "Bonus Payout Period Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) END) AS "Bonus Payout Period Payout Product Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 3",

    -- NEW: BONUS SCHEME COLUMNS (7 columns x 4 schemes = 28 total columns)
    -- BONUS SCHEME 1
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 1 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0) AS "Bonus Scheme 1 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 1",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 1",
    
    -- BONUS SCHEME 2
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 2 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0) AS "Bonus Scheme 2 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 2",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 2",
    
    -- BONUS SCHEME 3
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 3 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0) AS "Bonus Scheme 3 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 3",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 3",
    
    -- BONUS SCHEME 4
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.bonus_scheme_name END), '') AS "Bonus Scheme No 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) AS "Bonus Scheme 4 % Main Scheme Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0) AS "Bonus Scheme 4 Minimum Target",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) AS "Mandatory Product Target % Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0) AS "Minimum Mandatory Product Target Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.reward_on_total_percent END), 0) AS "% Reward on Total Bonus Scheme 4",
    COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.reward_on_mandatory_product_percent END), 0) AS "Reward on Mandatory Product % Bonus Scheme 4",

    -- NEW: 2 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (8 total columns)
    -- BONUS SCHEME 1 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Volume 1",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 1 THEN bspa.bonus_period_volume END), 0) AS "Actual Bonus Volume 1",
    
    -- BONUS SCHEME 2 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Volume 2",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 2 THEN bspa.bonus_period_volume END), 0) AS "Actual Bonus Volume 2",
    
    -- BONUS SCHEME 3 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Volume 3",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 3 THEN bspa.bonus_period_volume END), 0) AS "Actual Bonus Volume 3",
    
    -- BONUS SCHEME 4 PERIOD COLUMNS
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
    ) AS "Main Scheme Bonus Target Volume 4",
    COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 4 THEN bspa.bonus_period_volume END), 0) AS "Actual Bonus Volume 4",

    -- NEW: 2 MORE BONUS SCHEME COLUMNS FOR EACH SCHEME (8 total columns)
    -- BONUS SCHEME 1 - % Achieved and Payout Period Volume
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 1 THEN bspa.bonus_period_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 1",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 1 THEN bspay.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period Volume 1",
    
    -- BONUS SCHEME 2 - % Achieved and Payout Period Volume
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 2 THEN bspa.bonus_period_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 2",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 2 THEN bspay.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period Volume 2",
    
    -- BONUS SCHEME 3 - % Achieved and Payout Period Volume
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 3 THEN bspa.bonus_period_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 3",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 3 THEN bspay.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period Volume 3",
    
    -- BONUS SCHEME 4 - % Achieved and Payout Period Volume
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bspa.bonus_scheme_id = 4 THEN bspa.bonus_period_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.main_scheme_target_percent END), 0) / 100.0) * sa.target_volume,
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_target END), 0)
        ), 0
      )
    END AS "Bonus Scheme % Achieved 4",
    COALESCE(MAX(CASE WHEN bspay.bonus_scheme_id = 4 THEN bspay.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period Volume 4",

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns)
    -- BONUS SCHEME 1 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_volume, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Volume 1",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 1 THEN bsmp.bonus_mandatory_product_volume END), 0) AS "Actual Bonus MP Volume 1",
    
    -- BONUS SCHEME 2 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_volume, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Volume 2",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 2 THEN bsmp.bonus_mandatory_product_volume END), 0) AS "Actual Bonus MP Volume 2",
    
    -- BONUS SCHEME 3 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_volume, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Volume 3",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 3 THEN bsmp.bonus_mandatory_product_volume END), 0) AS "Actual Bonus MP Volume 3",
    
    -- BONUS SCHEME 4 - Mandatory Product Columns
    GREATEST(
      (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
      GREATEST(
        COALESCE(sa.mandatory_product_target_volume, 0), 
        COALESCE(sa.mandatory_product_target, 0), 
        COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
      ),
      COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
    ) AS "Mandatory Product Bonus Target Volume 4",
    COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 4 THEN bsmp.bonus_mandatory_product_volume END), 0) AS "Actual Bonus MP Volume 4",

    -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns)
    -- BONUS SCHEME 1 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_volume, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 1 THEN bsmp.bonus_mandatory_product_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_volume, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 1 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 1",
    
    -- BONUS SCHEME 2 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_volume, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 2 THEN bsmp.bonus_mandatory_product_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_volume, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 2 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 2",
    
    -- BONUS SCHEME 3 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_volume, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 3 THEN bsmp.bonus_mandatory_product_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_volume, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 3 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 3",
    
    -- BONUS SCHEME 4 - % MP Achieved
    CASE 
      WHEN GREATEST(
        (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
        GREATEST(
          COALESCE(sa.mandatory_product_target_volume, 0), 
          COALESCE(sa.mandatory_product_target, 0), 
          COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
        ),
        COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
      ) = 0 THEN 0
      ELSE COALESCE(MAX(CASE WHEN bsmp.bonus_scheme_id = 4 THEN bsmp.bonus_mandatory_product_volume END), 0) / NULLIF(
        GREATEST(
          (COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.mandatory_product_target_percent END), 0) / 100.0) * 
          GREATEST(
            COALESCE(sa.mandatory_product_target_volume, 0), 
            COALESCE(sa.mandatory_product_target, 0), 
            COALESCE(sa.actual_volume * sa.mandatory_product_target_to_actual, 0)
          ),
          COALESCE(MAX(CASE WHEN bs.bonus_scheme_id = 4 THEN bs.minimum_mandatory_product_target END), 0)
        ), 0
      )
    END AS "% MP Achieved 4",

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VOLUME COLUMNS FOR EACH BONUS SCHEME (4 total columns)
    -- BONUS SCHEME 1 - Actual Bonus Payout Period MP Volume
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 1 THEN bsmpv.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period MP Volume 1",
    
    -- BONUS SCHEME 2 - Actual Bonus Payout Period MP Volume
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 2 THEN bsmpv.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period MP Volume 2",
    
    -- BONUS SCHEME 3 - Actual Bonus Payout Period MP Volume
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 3 THEN bsmpv.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period MP Volume 3",
    
    -- BONUS SCHEME 4 - Actual Bonus Payout Period MP Volume
    COALESCE(MAX(CASE WHEN bsmpv.bonus_scheme_id = 4 THEN bsmpv.bonus_payout_period_volume END), 0) AS "Actual Bonus Payout Period MP Volume 4",

    -- **FINAL PHASING PAYOUT COLUMN**
    CASE 
      WHEN (COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN 
        CASE 
          WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
          WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
          THEN ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
          ELSE 0
        END 
      END), 0) >= 1.0) OR 
      (COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN 
        CASE 
          WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
          WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
          THEN ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
          ELSE 0
        END 
      END), 0) >= 1.0) OR 
      (COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN 
        CASE 
          WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
          WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
          THEN ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)), 6)
          ELSE 0
        END 
      END), 0) >= 1.0)
      THEN GREATEST(
        COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN 
          CASE 
            WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
            THEN CASE 
                   WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
                   ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
                 END
            ELSE 0
          END
        END), 0),
        COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN 
          CASE 
            WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
            THEN CASE 
                   WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
                   ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
                 END
            ELSE 0
          END
        END), 0),
        COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN 
          CASE 
            WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_volume, 0)) >= 1 
            THEN CASE 
                   WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
                   ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
                 END
            ELSE 0
          END
        END), 0)
      )
      ELSE 
        COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN 
          CASE 
            WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE 0
          END
        END), 0) + 
        COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN 
          CASE 
            WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE 0
          END
        END), 0) + 
        COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN 
          CASE 
            WHEN NULLIF(pp.phasing_target_percent * sa.target_volume, 0) IS NULL THEN 0
            WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * sa.target_volume, 0)) >= 1 THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE 0
          END
        END), 0)
    END AS "FINAL PHASING PAYOUT",
    
    -- Add scheme config fields for use in final_output
    sc.payout_products_enabled
    
  FROM slab_applied sa
  CROSS JOIN scheme_config sc
  LEFT JOIN phasing_periods pp ON 1=1
  LEFT JOIN phasing_actuals pa ON sa.credit_account = pa.credit_account AND pp.phasing_id = pa.phasing_id
  LEFT JOIN phasing_payouts ppay ON sa.credit_account = ppay.credit_account AND pp.phasing_id = ppay.phasing_id
  LEFT JOIN phasing_payout_product_volumes pppv ON sa.credit_account = pppv.credit_account AND pp.phasing_id = pppv.phasing_id
  LEFT JOIN bonus_phasing_actuals bpa ON sa.credit_account = bpa.credit_account AND pp.phasing_id = bpa.phasing_id
  LEFT JOIN bonus_phasing_payouts bppay ON sa.credit_account = bppay.credit_account AND pp.phasing_id = bppay.phasing_id
  LEFT JOIN bonus_phasing_payout_product_volumes bpppv ON sa.credit_account = bpppv.credit_account AND pp.phasing_id = bpppv.phasing_id
  LEFT JOIN bonus_schemes bs ON 1=1
  LEFT JOIN bonus_scheme_period_actuals bspa ON sa.credit_account = bspa.credit_account AND bs.bonus_scheme_id = bspa.bonus_scheme_id
  LEFT JOIN bonus_scheme_payout_actuals bspay ON sa.credit_account = bspay.credit_account AND bs.bonus_scheme_id = bspay.bonus_scheme_id
  LEFT JOIN bonus_scheme_mandatory_product_volumes bsmp ON sa.credit_account = bsmp.credit_account AND bs.bonus_scheme_id = bsmp.bonus_scheme_id
  LEFT JOIN bonus_scheme_mandatory_product_payout_volumes bsmpv ON sa.credit_account = bsmpv.credit_account AND bs.bonus_scheme_id = bsmpv.bonus_scheme_id
  GROUP BY sa.credit_account, sa.customer_name, sa.so_name, sa.state_name, 
           sa.total_volume, sa.total_value, sa.actual_volume, sa.actual_value,
           sa.mandatory_product_base_volume, sa.mandatory_product_actual_volume,
           sa.mandatory_product_target_volume, sa.mandatory_product_actual_ppi,
           sa.payout_product_actual_volume, sa.payout_product_actual_value,
           sa.mandatory_product_payout, sa.growth_rate, sa.target_volume, 
           sa.basic_payout, sa.additional_payout, sa.percent_achieved, sa.fixed_rebate, 
           sa.rebate_per_litre_applied, sa.additional_rebate_on_growth_per_litre_applied,
           sa.mandatory_product_target, sa.mandatory_product_growth_percent,
           sa.mandatory_product_target_to_actual, sa.mandatory_product_rebate,
           sa.mandatory_product_rebate_percent, sa.mandatory_min_shades_ppi, sa.payout_product_payout,
           sa.base_1_volume, sa.base_1_value, sa.base_1_sum_avg_method,
           sa.base_2_volume, sa.base_2_value, sa.base_2_sum_avg_method,
           sa.base_1_months, sa.base_2_months,
           sa.base_1_volume_final, sa.base_1_value_final,
           sa.base_2_volume_final, sa.base_2_value_final,
           sc.payout_products_enabled
),

final_output AS (
  SELECT 
    credit_account,
    customer_name,
    so_name,
    state_name,
    -- Base Period Columns
    base_1_volume AS "Base 1 Volume",
    base_1_value AS "Base 1 Value", 
    base_1_sum_avg_method AS "Base 1 SumAvg",
    base_1_months AS "Base 1 Months",
    base_1_volume_final AS "Base 1 Volume Final",
    base_1_value_final AS "Base 1 Value Final",
    base_2_volume AS "Base 2 Volume",
    base_2_value AS "Base 2 Value",
    base_2_sum_avg_method AS "Base 2 SumAvg",
    base_2_months AS "Base 2 Months",
    base_2_volume_final AS "Base 2 Volume Final",
    base_2_value_final AS "Base 2 Value Final",
    total_volume,
    total_value,
    actual_volume,
    actual_value,
    mandatory_product_base_volume AS "Mandatory Product Base Volume",
    mandatory_product_actual_volume AS "Mandatory Product Actual Volume",
    mandatory_product_target_volume AS "Mandatory Product Growth Target Volume",
    mandatory_product_payout AS "Mandatory Product Payout",
    mandatory_product_actual_ppi AS "Mandatory product actual PPI",
    growth_rate,
    target_volume,
    basic_payout,
    additional_payout,
    percent_achieved,
    fixed_rebate,
    rebate_per_litre_applied AS "Rebate per Litre",
    additional_rebate_on_growth_per_litre_applied AS "Additional Rebate on Growth per Litre",
    mandatory_product_target AS "Mandatory Product Fixed Target",
    mandatory_product_growth_percent AS "Mandatory Product Growth",
    mandatory_product_target_to_actual AS "Mandatory Product % Target to Actual Sales",
    mandatory_product_rebate AS "Mandatory Product Rebate",
    mandatory_min_shades_ppi AS "Mandatory Min. Shades - PPI",
    total_payout,
    payout_product_payout AS "Payout Product Payout",
    "Mandatory Product % to Actual - Target Volume",
    "Mandatory Product Actual Volume % to Total Sales",
    "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product % PPI",
    payout_product_actual_volume AS "Payout Products Volume",
    payout_product_actual_value AS "Payout Products Value",
    
    -- REGULAR PHASING FIELDS
    "Phasing Period No 1", "Phasing Target % 1", "Phasing Target Volume 1", "Phasing Period Volume 1", 
    "Phasing Payout Period Volume 1", "Phasing Period Payout Product Volume 1", "% Phasing Achieved 1", "Phasing Period Rebate 1", "Phasing Payout 1",
    
    -- 8 BONUS FIELDS FOR PERIOD 1
    "Is Bonus 1", "Bonus Rebate Value 1", "Bonus Phasing Target % 1", "Bonus Target Volume 1", "Bonus Phasing Period Volume 1",
    "Bonus Payout Period Volume 1", "Bonus Payout Period Payout Product Volume 1", "% Bonus Achieved 1", "Bonus Payout 1",
    
    "Phasing Period No 2", "Phasing Target % 2", "Phasing Target Volume 2", "Phasing Period Volume 2",
    "Phasing Payout Period Volume 2", "Phasing Period Payout Product Volume 2", "% Phasing Achieved 2", "Phasing Period Rebate 2", "Phasing Payout 2",
    
    -- 8 BONUS FIELDS FOR PERIOD 2
    "Is Bonus 2", "Bonus Rebate Value 2", "Bonus Phasing Target % 2", "Bonus Target Volume 2", "Bonus Phasing Period Volume 2",
    "Bonus Payout Period Volume 2", "Bonus Payout Period Payout Product Volume 2", "% Bonus Achieved 2", "Bonus Payout 2",
    
    "Phasing Period No 3", "Phasing Target % 3", "Phasing Target Volume 3", "Phasing Period Volume 3",
    "Phasing Payout Period Volume 3", "Phasing Period Payout Product Volume 3", "% Phasing Achieved 3", "Phasing Period Rebate 3", "Phasing Payout 3",
    
    -- 8 BONUS FIELDS FOR PERIOD 3
    "Is Bonus 3", "Bonus Rebate Value 3", "Bonus Phasing Target % 3", "Bonus Target Volume 3", "Bonus Phasing Period Volume 3",
    "Bonus Payout Period Volume 3", "Bonus Payout Period Payout Product Volume 3", "% Bonus Achieved 3", "Bonus Payout 3",

    -- **FINAL PHASING PAYOUT COLUMN**
    CASE 
      WHEN (COALESCE("% Bonus Achieved 1", 0) >= 1.0) OR (COALESCE("% Bonus Achieved 2", 0) >= 1.0) OR (COALESCE("% Bonus Achieved 3", 0) >= 1.0)
      THEN GREATEST(COALESCE("Bonus Payout 1", 0), COALESCE("Bonus Payout 2", 0), COALESCE("Bonus Payout 3", 0))
      ELSE COALESCE("Phasing Payout 1", 0) + COALESCE("Phasing Payout 2", 0) + COALESCE("Phasing Payout 3", 0)
    END AS "FINAL PHASING PAYOUT",

    -- NEW: 28 BONUS SCHEME COLUMNS (7 per scheme x 4 schemes)
    "Bonus Scheme No 1", "Bonus Scheme 1 % Main Scheme Target", "Bonus Scheme 1 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 1", "Minimum Mandatory Product Target Bonus Scheme 1",
    "% Reward on Total Bonus Scheme 1", "Reward on Mandatory Product % Bonus Scheme 1",
    
    "Bonus Scheme No 2", "Bonus Scheme 2 % Main Scheme Target", "Bonus Scheme 2 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 2", "Minimum Mandatory Product Target Bonus Scheme 2",
    "% Reward on Total Bonus Scheme 2", "Reward on Mandatory Product % Bonus Scheme 2",
    
    "Bonus Scheme No 3", "Bonus Scheme 3 % Main Scheme Target", "Bonus Scheme 3 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 3", "Minimum Mandatory Product Target Bonus Scheme 3",
    "% Reward on Total Bonus Scheme 3", "Reward on Mandatory Product % Bonus Scheme 3",
    
    "Bonus Scheme No 4", "Bonus Scheme 4 % Main Scheme Target", "Bonus Scheme 4 Minimum Target",
    "Mandatory Product Target % Bonus Scheme 4", "Minimum Mandatory Product Target Bonus Scheme 4",
    "% Reward on Total Bonus Scheme 4", "Reward on Mandatory Product % Bonus Scheme 4",

    -- NEW: 4 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (16 total columns)
    "Main Scheme Bonus Target Volume 1", "Actual Bonus Volume 1",
    "Bonus Scheme % Achieved 1", "Actual Bonus Payout Period Volume 1",
    "Main Scheme Bonus Target Volume 2", "Actual Bonus Volume 2", 
    "Bonus Scheme % Achieved 2", "Actual Bonus Payout Period Volume 2",
    "Main Scheme Bonus Target Volume 3", "Actual Bonus Volume 3",
    "Bonus Scheme % Achieved 3", "Actual Bonus Payout Period Volume 3",
    "Main Scheme Bonus Target Volume 4", "Actual Bonus Volume 4",
    "Bonus Scheme % Achieved 4", "Actual Bonus Payout Period Volume 4",

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns)
    "Mandatory Product Bonus Target Volume 1", "Actual Bonus MP Volume 1",
    "Mandatory Product Bonus Target Volume 2", "Actual Bonus MP Volume 2", 
    "Mandatory Product Bonus Target Volume 3", "Actual Bonus MP Volume 3",
    "Mandatory Product Bonus Target Volume 4", "Actual Bonus MP Volume 4",

    -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns)
    "% MP Achieved 1", "% MP Achieved 2", "% MP Achieved 3", "% MP Achieved 4",

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VOLUME COLUMNS FOR EACH BONUS SCHEME (4 total columns)
    "Actual Bonus Payout Period MP Volume 1", "Actual Bonus Payout Period MP Volume 2",
    "Actual Bonus Payout Period MP Volume 3", "Actual Bonus Payout Period MP Volume 4",

    -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (CORRECTED)**
    GREATEST(
      COALESCE(mandatory_product_target_volume, 0), 
      COALESCE(mandatory_product_target, 0), 
      COALESCE("Mandatory Product % to Actual - Target Volume", 0)
    ) AS "MP FINAL TARGET",
    
    CASE 
      WHEN GREATEST(
        COALESCE(mandatory_product_target_volume, 0), 
        COALESCE(mandatory_product_target, 0), 
        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
      ) = 0 THEN 0
      ELSE ROUND(
        COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
          GREATEST(
            COALESCE(mandatory_product_target_volume, 0), 
            COALESCE(mandatory_product_target, 0), 
            COALESCE("Mandatory Product % to Actual - Target Volume", 0)
          ), 0
        ), 6
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN: MP FINAL PAYOUT** - Updated with payout products logic
    CASE 
      WHEN payout_products_enabled = false THEN
        CASE 
          WHEN (CASE 
                  WHEN GREATEST(
                    COALESCE(mandatory_product_target_volume, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                  ) = 0 THEN 0
                  ELSE ROUND(
                    COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
                      GREATEST(
                        COALESCE(mandatory_product_target_volume, 0), 
                        COALESCE(mandatory_product_target, 0), 
                        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                      ), 0
                    ), 6
                  )
                END) >= 1.0 
          THEN COALESCE(actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
          ELSE 0
        END
      ELSE
        CASE 
          WHEN (CASE 
                  WHEN GREATEST(
                    COALESCE(mandatory_product_target_volume, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                  ) = 0 THEN 0
                  ELSE ROUND(
                    COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
                      GREATEST(
                        COALESCE(mandatory_product_target_volume, 0), 
                        COALESCE(mandatory_product_target, 0), 
                        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                      ), 0
                    ), 6
                  )
                END) >= 1.0 
          THEN COALESCE(payout_product_actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
          ELSE 0
        END
    END AS "MP Final Payout",

    -- **NEW BONUS SCHEME PAYOUT COLUMNS (2 per scheme x 4 schemes = 8 total)**
    
    -- BONUS SCHEME 1 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 1", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 1", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Volume 1", 0)
      ELSE 0
    END AS "Bonus Scheme 1 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 1", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 1", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Volume 1", 0)
      ELSE 0
    END AS "Bonus Scheme 1 MP Payout",

    -- BONUS SCHEME 2 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 2", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 2", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Volume 2", 0)
      ELSE 0
    END AS "Bonus Scheme 2 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 2", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 2", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Volume 2", 0)
      ELSE 0
    END AS "Bonus Scheme 2 MP Payout",

    -- BONUS SCHEME 3 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 3", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 3", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Volume 3", 0)
      ELSE 0
    END AS "Bonus Scheme 3 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 3", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 3", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Volume 3", 0)
      ELSE 0
    END AS "Bonus Scheme 3 MP Payout",

    -- BONUS SCHEME 4 PAYOUTS
    CASE 
      WHEN COALESCE("Bonus Scheme % Achieved 4", 0) >= 1.0 THEN 
        COALESCE("% Reward on Total Bonus Scheme 4", 0) / 100.0 * COALESCE("Actual Bonus Payout Period Volume 4", 0)
      ELSE 0
    END AS "Bonus Scheme 4 Bonus Payout",
    
    CASE 
      WHEN COALESCE("% MP Achieved 4", 0) >= 1.0 THEN 
        COALESCE("Reward on Mandatory Product % Bonus Scheme 4", 0) / 100.0 * COALESCE("Actual Bonus Payout Period MP Volume 4", 0)
      ELSE 0
    END AS "Bonus Scheme 4 MP Payout"
    
  FROM phasing_aggregated
),

unioned AS (
  SELECT *, 0 AS is_grand_total FROM final_output
  UNION ALL
  SELECT
    'GRAND TOTAL',
    NULL, NULL, NULL,
    -- Base Period Grand Totals
    COALESCE(SUM("Base 1 Volume"), 0),
    COALESCE(SUM("Base 1 Value"), 0),
    NULL, -- Base 1 SumAvg
    NULL, -- Base 1 Months (no meaningful total)
    COALESCE(SUM("Base 1 Volume Final"), 0),
    COALESCE(SUM("Base 1 Value Final"), 0),
    COALESCE(SUM("Base 2 Volume"), 0),  
    COALESCE(SUM("Base 2 Value"), 0),
    NULL, -- Base 2 SumAvg
    NULL, -- Base 2 Months (no meaningful total)
    COALESCE(SUM("Base 2 Volume Final"), 0),
    COALESCE(SUM("Base 2 Value Final"), 0),
    COALESCE(SUM(total_volume), 0),
    COALESCE(SUM(total_value), 0),
    COALESCE(SUM(actual_volume), 0),
    COALESCE(SUM(actual_value), 0),
    COALESCE(SUM("Mandatory Product Base Volume"), 0),
    COALESCE(SUM("Mandatory Product Actual Volume"), 0),
    COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
    COALESCE(SUM("Mandatory Product Payout"), 0),
    COALESCE(SUM("Mandatory product actual PPI"), 0),
    NULL,
    COALESCE(SUM(target_volume), 0),
    COALESCE(SUM(basic_payout), 0),
    COALESCE(SUM(additional_payout), 0),
    CASE WHEN COALESCE(SUM(target_volume), 0) = 0 THEN 0 ELSE COALESCE(SUM(actual_volume), 0) / NULLIF(SUM(target_volume), 0) END AS percent_achieved,
    COALESCE(SUM(fixed_rebate), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Fixed Target"), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Rebate"), 0),
    COALESCE(SUM("Mandatory Min. Shades - PPI"), 0),
    COALESCE(SUM(total_payout), 0),
    COALESCE(SUM("Payout Product Payout"), 0),
    COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0),
    CASE WHEN COALESCE(SUM(actual_volume), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM(actual_volume), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Fixed Target"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM("Mandatory Product Fixed Target"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Growth Target Volume"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM("Mandatory Product Growth Target Volume"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Min. Shades - PPI"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory product actual PPI"), 0) / NULLIF(SUM("Mandatory Min. Shades - PPI"), 0) END,
    COALESCE(SUM("Payout Products Volume"), 0),
    COALESCE(SUM("Payout Products Value"), 0),
    
    -- REGULAR PHASING GRAND TOTALS
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 1"), 0), COALESCE(SUM("Phasing Period Volume 1"), 0),
    COALESCE(SUM("Phasing Payout Period Volume 1"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 1"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 1"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 1
    NULL, NULL, NULL, COALESCE(SUM("Bonus Target Volume 1"), 0), COALESCE(SUM("Bonus Phasing Period Volume 1"), 0),
    COALESCE(SUM("Bonus Payout Period Volume 1"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Volume 1"), 0), NULL, COALESCE(SUM("Bonus Payout 1"), 0),
    
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 2"), 0), COALESCE(SUM("Phasing Period Volume 2"), 0),
    COALESCE(SUM("Phasing Payout Period Volume 2"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 2"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 2"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 2
    NULL, NULL, NULL, COALESCE(SUM("Bonus Target Volume 2"), 0), COALESCE(SUM("Bonus Phasing Period Volume 2"), 0),
    COALESCE(SUM("Bonus Payout Period Volume 2"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Volume 2"), 0), NULL, COALESCE(SUM("Bonus Payout 2"), 0),
    
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 3"), 0), COALESCE(SUM("Phasing Period Volume 3"), 0),
    COALESCE(SUM("Phasing Payout Period Volume 3"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 3"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 3"), 0),
    
    -- BONUS PHASING GRAND TOTALS FOR PERIOD 3
    NULL, NULL, NULL, COALESCE(SUM("Bonus Target Volume 3"), 0), COALESCE(SUM("Bonus Phasing Period Volume 3"), 0),
    COALESCE(SUM("Bonus Payout Period Volume 3"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Volume 3"), 0), NULL, COALESCE(SUM("Bonus Payout 3"), 0),
    
    -- **FINAL PHASING PAYOUT GRAND TOTAL**
    COALESCE(SUM("FINAL PHASING PAYOUT"), 0),

    -- BONUS SCHEME GRAND TOTALS (all set to NULL as these are configuration values)
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,

    -- NEW: 4 BONUS SCHEME PERIOD COLUMNS GRAND TOTALS (16 total columns)
    COALESCE(SUM("Main Scheme Bonus Target Volume 1"), 0), COALESCE(SUM("Actual Bonus Volume 1"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Volume 1"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Volume 2"), 0), COALESCE(SUM("Actual Bonus Volume 2"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Volume 2"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Volume 3"), 0), COALESCE(SUM("Actual Bonus Volume 3"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Volume 3"), 0),
    COALESCE(SUM("Main Scheme Bonus Target Volume 4"), 0), COALESCE(SUM("Actual Bonus Volume 4"), 0),
    NULL, COALESCE(SUM("Actual Bonus Payout Period Volume 4"), 0),

    -- NEW: MANDATORY PRODUCT BONUS COLUMNS GRAND TOTALS (8 total columns)
    COALESCE(SUM("Mandatory Product Bonus Target Volume 1"), 0), COALESCE(SUM("Actual Bonus MP Volume 1"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Volume 2"), 0), COALESCE(SUM("Actual Bonus MP Volume 2"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Volume 3"), 0), COALESCE(SUM("Actual Bonus MP Volume 3"), 0),
    COALESCE(SUM("Mandatory Product Bonus Target Volume 4"), 0), COALESCE(SUM("Actual Bonus MP Volume 4"), 0),

    -- NEW: % MP ACHIEVED COLUMNS GRAND TOTALS (4 total columns)
    NULL, NULL, NULL, NULL, -- % MP Achieved columns don't have meaningful grand totals

    -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VOLUME COLUMNS GRAND TOTALS (4 total columns)
    COALESCE(SUM("Actual Bonus Payout Period MP Volume 1"), 0), COALESCE(SUM("Actual Bonus Payout Period MP Volume 2"), 0),
    COALESCE(SUM("Actual Bonus Payout Period MP Volume 3"), 0), COALESCE(SUM("Actual Bonus Payout Period MP Volume 4"), 0),

    -- **NEW COLUMNS GRAND TOTALS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (CORRECTED)**
    GREATEST(
      COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
      COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
      COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
    ) AS "MP FINAL TARGET",
    CASE 
      WHEN GREATEST(
        COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
        COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
        COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
      ) = 0 THEN 0
      ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(
        GREATEST(
          COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
          COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
          COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
        ), 0
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN GRAND TOTAL: MP FINAL PAYOUT**
    COALESCE(SUM("MP Final Payout"), 0) AS "MP Final Payout",

    -- **NEW BONUS SCHEME PAYOUT COLUMNS GRAND TOTALS (8 total)**
    COALESCE(SUM("Bonus Scheme 1 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 1 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 2 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 2 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 3 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 3 MP Payout"), 0),
    COALESCE(SUM("Bonus Scheme 4 Bonus Payout"), 0), COALESCE(SUM("Bonus Scheme 4 MP Payout"), 0),
    
    1 AS is_grand_total
  FROM final_output
)

-- FINAL SELECT WITH ALL COLUMNS INCLUDING NEW MP FINAL COLUMNS
SELECT 
  u.state_name,
  u.credit_account,
  u.customer_name,
  u.so_name,
  -- Base Period Columns
  u."Base 1 Volume",
  u."Base 1 Value", 
  u."Base 1 SumAvg",
  u."Base 1 Months",
  u."Base 1 Volume Final",
  u."Base 1 Value Final",
  u."Base 2 Volume",
  u."Base 2 Value",
  u."Base 2 SumAvg",
  u."Base 2 Months",
  u."Base 2 Volume Final",
  u."Base 2 Value Final",
  u.total_volume,
  u.total_value,
  u.growth_rate,
  sg.strata_growth_percentage AS "Strata Growth %",
  u.target_volume,
  u.actual_volume,
  u.actual_value,
  u.percent_achieved AS "% Achieved",
  u."Rebate per Litre",
  u.basic_payout,
  u."Additional Rebate on Growth per Litre",
  u.additional_payout,
  u.fixed_rebate AS "Fixed Rebate",
  u.total_payout AS "Total Payout",
  u."Payout Product Payout",
  u."Mandatory Product Base Volume",
  u."Mandatory Product Growth",
  u."Mandatory Product Growth Target Volume",
  u."Mandatory Product Actual Volume",
  u."Mandatory Product Growth Target Achieved",
  u."Mandatory Min. Shades - PPI",
  u."Mandatory product actual PPI",
  u."Mandatory Product % PPI",
  u."Mandatory Product Fixed Target",
  u."Mandatory Product Actual Volume % to Fixed Target Sales",
  u."Mandatory Product % Target to Actual Sales",
  u."Mandatory Product % to Actual - Target Volume",
  u."Mandatory Product Actual Volume % to Total Sales",
  u."Mandatory Product Rebate",
  u."Mandatory Product Payout",
  u."Payout Products Volume",
  u."Payout Products Value",
  
  -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD
  u."Phasing Period No 1", u."Phasing Target % 1", u."Phasing Target Volume 1", u."Phasing Period Volume 1", 
  u."Phasing Payout Period Volume 1", u."Phasing Period Payout Product Volume 1", u."% Phasing Achieved 1", u."Phasing Period Rebate 1", u."Phasing Payout 1",
  u."Is Bonus 1", u."Bonus Rebate Value 1", u."Bonus Phasing Target % 1", u."Bonus Target Volume 1", u."Bonus Phasing Period Volume 1",
  u."Bonus Payout Period Volume 1", u."Bonus Payout Period Payout Product Volume 1", u."% Bonus Achieved 1", u."Bonus Payout 1",
  
  u."Phasing Period No 2", u."Phasing Target % 2", u."Phasing Target Volume 2", u."Phasing Period Volume 2",
  u."Phasing Payout Period Volume 2", u."Phasing Period Payout Product Volume 2", u."% Phasing Achieved 2", u."Phasing Period Rebate 2", u."Phasing Payout 2",
  u."Is Bonus 2", u."Bonus Rebate Value 2", u."Bonus Phasing Target % 2", u."Bonus Target Volume 2", u."Bonus Phasing Period Volume 2",
  u."Bonus Payout Period Volume 2", u."Bonus Payout Period Payout Product Volume 2", u."% Bonus Achieved 2", u."Bonus Payout 2",
  
  u."Phasing Period No 3", u."Phasing Target % 3", u."Phasing Target Volume 3", u."Phasing Period Volume 3",
  u."Phasing Payout Period Volume 3", u."Phasing Period Payout Product Volume 3", u."% Phasing Achieved 3", u."Phasing Period Rebate 3", u."Phasing Payout 3",
  u."Is Bonus 3", u."Bonus Rebate Value 3", u."Bonus Phasing Target % 3", u."Bonus Target Volume 3", u."Bonus Phasing Period Volume 3",
  u."Bonus Payout Period Volume 3", u."Bonus Payout Period Payout Product Volume 3", u."% Bonus Achieved 3", u."Bonus Payout 3",
  
  -- **FINAL PHASING PAYOUT COLUMN**
  u."FINAL PHASING PAYOUT",

  -- **28 BONUS SCHEME COLUMNS (7 per scheme x 4 schemes)**
  u."Bonus Scheme No 1", u."Bonus Scheme 1 % Main Scheme Target", u."Bonus Scheme 1 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 1", u."Minimum Mandatory Product Target Bonus Scheme 1",
  u."% Reward on Total Bonus Scheme 1", u."Reward on Mandatory Product % Bonus Scheme 1",
  
  u."Bonus Scheme No 2", u."Bonus Scheme 2 % Main Scheme Target", u."Bonus Scheme 2 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 2", u."Minimum Mandatory Product Target Bonus Scheme 2",
  u."% Reward on Total Bonus Scheme 2", u."Reward on Mandatory Product % Bonus Scheme 2",
  
  u."Bonus Scheme No 3", u."Bonus Scheme 3 % Main Scheme Target", u."Bonus Scheme 3 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 3", u."Minimum Mandatory Product Target Bonus Scheme 3",
  u."% Reward on Total Bonus Scheme 3", u."Reward on Mandatory Product % Bonus Scheme 3",
  
  u."Bonus Scheme No 4", u."Bonus Scheme 4 % Main Scheme Target", u."Bonus Scheme 4 Minimum Target",
  u."Mandatory Product Target % Bonus Scheme 4", u."Minimum Mandatory Product Target Bonus Scheme 4",
  u."% Reward on Total Bonus Scheme 4", u."Reward on Mandatory Product % Bonus Scheme 4",

  -- NEW: 4 BONUS SCHEME PERIOD COLUMNS FOR EACH SCHEME (16 total columns)
  u."Main Scheme Bonus Target Volume 1", u."Actual Bonus Volume 1",
  u."Bonus Scheme % Achieved 1", u."Actual Bonus Payout Period Volume 1",
  u."Main Scheme Bonus Target Volume 2", u."Actual Bonus Volume 2",
  u."Bonus Scheme % Achieved 2", u."Actual Bonus Payout Period Volume 2",
  u."Main Scheme Bonus Target Volume 3", u."Actual Bonus Volume 3",
  u."Bonus Scheme % Achieved 3", u."Actual Bonus Payout Period Volume 3",
  u."Main Scheme Bonus Target Volume 4", u."Actual Bonus Volume 4",
  u."Bonus Scheme % Achieved 4", u."Actual Bonus Payout Period Volume 4",

  -- NEW: MANDATORY PRODUCT BONUS COLUMNS FOR EACH SCHEME (8 total columns)
  u."Mandatory Product Bonus Target Volume 1", u."Actual Bonus MP Volume 1",
  u."Mandatory Product Bonus Target Volume 2", u."Actual Bonus MP Volume 2",
  u."Mandatory Product Bonus Target Volume 3", u."Actual Bonus MP Volume 3",
  u."Mandatory Product Bonus Target Volume 4", u."Actual Bonus MP Volume 4",

  -- NEW: % MP ACHIEVED COLUMNS FOR EACH BONUS SCHEME (4 total columns)
  u."% MP Achieved 1", u."% MP Achieved 2", u."% MP Achieved 3", u."% MP Achieved 4",

  -- NEW: ACTUAL BONUS PAYOUT PERIOD MP VOLUME COLUMNS FOR EACH BONUS SCHEME (4 total columns)
  u."Actual Bonus Payout Period MP Volume 1", u."Actual Bonus Payout Period MP Volume 2",
  u."Actual Bonus Payout Period MP Volume 3", u."Actual Bonus Payout Period MP Volume 4",

  -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)**
  u."MP FINAL TARGET",
  u."MP FINAL ACHEIVMENT %",
  u."MP Final Payout",

  -- **NEW BONUS SCHEME PAYOUT COLUMNS (2 per scheme x 4 schemes = 8 total)**
  u."Bonus Scheme 1 Bonus Payout", u."Bonus Scheme 1 MP Payout",
  u."Bonus Scheme 2 Bonus Payout", u."Bonus Scheme 2 MP Payout",
  u."Bonus Scheme 3 Bonus Payout", u."Bonus Scheme 3 MP Payout",
  u."Bonus Scheme 4 Bonus Payout", u."Bonus Scheme 4 MP Payout"
  
FROM unioned u
LEFT JOIN strata_growth sg ON u.credit_account = sg.credit_account::text
ORDER BY u.is_grand_total, u.credit_account;
"""

TRACKER_ADDITIONAL_SCHEME_VALUE = """
SET SESSION statement_timeout = 0;

-- ADDITIONAL SCHEME INDEX PARAMETER (Change this to switch between additional schemes)
-- 0 = First additional scheme, 1 = Second additional scheme, etc.
WITH additional_scheme_index AS (
  SELECT {additional_scheme_index} AS idx
),

scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id = '{scheme_id}'
),

-- Base Period 1 Configuration with Month Calculation
base_period_1_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->0->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

-- Base Period 2 Configuration with Month Calculation
base_period_2_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->1->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

base_dates AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'fromDate')::date + INTERVAL '1 day')::date AS scheme_from_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'toDate')::date + INTERVAL '1 day')::date AS scheme_to_date
  FROM scheme
),

-- REFERENCE CTEs (FROM MAIN SCHEME - SCHEME APPLICABILITY)
states AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates') AS state FROM scheme
),
regions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedRegions') AS region FROM scheme
),
area_heads AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedAreaHeads') AS area_head FROM scheme
),
divisions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDivisions') AS division FROM scheme
),
dealer_types AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDealerTypes') AS dealer_type FROM scheme
),
distributors AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDistributors') AS distributor FROM scheme
),

-- PRODUCT DATA (FROM ADDITIONAL SCHEME - for main product filtering)
product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials') > 0
),
product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories') > 0
),
product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps') > 0
),
product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups') > 0
),
product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups') > 0
),

-- MANDATORY PRODUCTS (FROM ADDITIONAL SCHEME - separate from main products)
mandatory_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials') > 0
),
mandatory_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories') > 0
),
mandatory_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps') > 0
),
mandatory_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups') > 0
),
mandatory_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups') > 0
),

-- PAYOUT PRODUCTS (FROM ADDITIONAL SCHEME)
payout_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
),
payout_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories') > 0
),
payout_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps') > 0
),
payout_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups') > 0
),
payout_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups') > 0
),

-- SLABS (FROM ADDITIONAL SCHEME) - UPDATED WITH VALUE-BASED FIELDS
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
  FROM scheme, additional_scheme_index asi,
  LATERAL jsonb_array_elements(scheme_json->'additionalSchemes'->asi.idx->'slabData'->'mainScheme'->'slabs') AS slab
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

-- PHASING PERIODS (FROM ADDITIONAL SCHEME) - UPDATED WITH VALUE-BASED FIELDS
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
    -- BONUS FIELDS (will be false/0 for additional schemes)
    COALESCE((phasing->>'isBonus')::boolean, false) AS is_bonus,
    COALESCE(NULLIF(phasing->>'bonusRebateValue', ''), '0')::NUMERIC AS bonus_rebate_value,
    COALESCE(NULLIF(phasing->>'bonusRebatePercentage', ''), '0')::NUMERIC AS bonus_rebate_percentage,
    ((phasing->>'bonusPayoutToDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_to_date,
    ((phasing->>'bonusPhasingToDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_to_date,
    ((phasing->>'bonusPayoutFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_from_date,
    ((phasing->>'bonusPhasingFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS bonus_phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS bonus_phasing_target_percent_raw
  FROM scheme, additional_scheme_index asi,
  LATERAL jsonb_array_elements(scheme_json->'additionalSchemes'->asi.idx->'phasingPeriods') AS phasing
),

-- Extract enableStrataGrowth flag from JSON (FROM ADDITIONAL SCHEME)
scheme_config AS (
  SELECT 
    COALESCE((scheme_json->'additionalSchemes'->asi.idx->'slabData'->'mainScheme'->>'enableStrataGrowth')::boolean, false) AS enable_strata_growth,
    -- Check if payout products are configured in additional scheme
    CASE 
      WHEN (scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
            AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0)
        OR (scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories' IS NOT NULL
            AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories') > 0)
        OR (scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps' IS NOT NULL
            AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps') > 0)
        OR (scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups' IS NOT NULL
            AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups') > 0)
        OR (scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups' IS NOT NULL
            AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups') > 0)
      THEN true
      ELSE false
    END AS payout_products_configured,
    scheme_json
  FROM scheme, additional_scheme_index asi
),

-- Base Period 1 Sales Data (using additional scheme products) - CHANGED TO VALUES AS PRIMARY
base_period_1_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_1_value,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_1_volume,
    bp1.sum_avg_method AS base_1_sum_avg_method,
    bp1.months_count AS base_1_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_1_config bp1
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp1.from_date AND bp1.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp1.sum_avg_method, bp1.months_count
),

-- Base Period 1 Final Calculations
base_period_1_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_value / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_value
    END AS base_1_value_final,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_volume / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_volume
    END AS base_1_volume_final
  FROM base_period_1_sales
),

-- Base Period 2 Sales Data (using additional scheme products) - CHANGED TO VALUES AS PRIMARY
base_period_2_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_2_value,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_2_volume,
    bp2.sum_avg_method AS base_2_sum_avg_method,
    bp2.months_count AS base_2_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_2_config bp2
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp2.from_date AND bp2.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp2.sum_avg_method, bp2.months_count
),

-- Base Period 2 Final Calculations
base_period_2_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_value / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_value
    END AS base_2_value_final,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_volume / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_volume
    END AS base_2_volume_final
  FROM base_period_2_sales
),

base_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    COALESCE(SUM(sd.value), 0) AS total_value,
    COALESCE(SUM(sd.volume), 0) AS total_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

actuals AS (
  SELECT
    sd.credit_account,
    MIN(sd.customer_name) AS actual_customer_name,
    MIN(sd.so_name) AS actual_so_name,
    MIN(sd.state_name) AS actual_state_name,
    COALESCE(SUM(sd.value), 0) AS actual_value,
    COALESCE(SUM(sd.volume), 0) AS actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_actual_ppi AS (
  SELECT 
    sd.credit_account,
    COUNT(DISTINCT sd.material) AS mandatory_product_actual_ppi_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

-- MANDATORY PRODUCT BASE VALUE (CHANGED FROM VOLUME TO VALUE)
mandatory_product_base_value AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS mandatory_base_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

-- MANDATORY PRODUCT ACTUALS (CHANGED FROM VOLUME TO VALUE)
mandatory_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.value), 0) AS mandatory_actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

payout_product_actuals AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    -- Return 0 if no payout products are defined or if they overlap with main scheme
    CASE 
      WHEN NOT EXISTS (
        SELECT 1 FROM scheme, additional_scheme_index asi
        WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
      ) THEN 0
      ELSE COALESCE(SUM(sd.value), 0)
    END AS payout_product_actual_value,
    CASE 
      WHEN NOT EXISTS (
        SELECT 1 FROM scheme, additional_scheme_index asi
        WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
      ) THEN 0
      ELSE COALESCE(SUM(sd.volume), 0)
    END AS payout_product_actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- Only include products that are specifically in additional scheme payout products
    AND sd.material::text IN (
      SELECT mat_item FROM scheme, additional_scheme_index asi,
      LATERAL jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') AS mat_item
      WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
        AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
    )
  GROUP BY sd.credit_account
),

all_accounts AS (
  SELECT credit_account FROM base_sales
  UNION
  SELECT credit_account FROM actuals
  UNION
  SELECT credit_account FROM mandatory_product_base_value
  UNION
  SELECT credit_account FROM mandatory_product_actuals
  UNION
  SELECT credit_account FROM mandatory_product_actual_ppi
  UNION
  SELECT credit_account FROM payout_product_actuals
  UNION
  SELECT credit_account FROM base_period_1_finals
  UNION
  SELECT credit_account FROM base_period_2_finals
),

-- Get customer information for all credit accounts (fallback for missing data)
customer_info_fallback AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS fallback_customer_name,
    MIN(sd.so_name) AS fallback_so_name,
    MIN(sd.state_name) AS fallback_state_name
  FROM sales_data sd
  WHERE sd.credit_account IN (SELECT credit_account FROM all_accounts)
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
  GROUP BY sd.credit_account
),

-- Joined CTE with Updated Total Volume/Value Formulas - CHANGED TO USE VALUES AS PRIMARY
joined AS (
  SELECT 
    aa.credit_account,
    COALESCE(bs.customer_name, ac.actual_customer_name, bp1.customer_name, bp2.customer_name, ppa.customer_name, cif.fallback_customer_name, 'Unknown') AS customer_name,
    COALESCE(bs.so_name, ac.actual_so_name, bp1.so_name, bp2.so_name, ppa.so_name, cif.fallback_so_name, 'Unknown') AS so_name,
    COALESCE(bs.state_name, ac.actual_state_name, bp1.state_name, bp2.state_name, ppa.state_name, cif.fallback_state_name, 'Unknown') AS state_name,
    -- Total Value/Volume uses MAX of Base Final Values - VALUE IS PRIMARY
    GREATEST(COALESCE(bp1.base_1_value_final, 0), COALESCE(bp2.base_2_value_final, 0)) AS total_value,
    GREATEST(COALESCE(bp1.base_1_volume_final, 0), COALESCE(bp2.base_2_volume_final, 0)) AS total_volume,
    COALESCE(ac.actual_value, 0) AS actual_value,
    COALESCE(ac.actual_volume, 0) AS actual_volume,
    COALESCE(mpbv.mandatory_base_value, 0) AS mandatory_product_base_value,
    COALESCE(mpa.mandatory_actual_value, 0) AS mandatory_product_actual_value,
    CASE 
      WHEN COALESCE(mpa.mandatory_actual_value, 0) < 0 THEN 0
      ELSE COALESCE(mpappi.mandatory_product_actual_ppi_count, 0)
    END AS mandatory_product_actual_ppi,
    COALESCE(ppa.payout_product_actual_value, 0) AS payout_product_actual_value,
    COALESCE(ppa.payout_product_actual_volume, 0) AS payout_product_actual_volume,
    -- Base Period Columns
    COALESCE(bp1.base_1_value, 0) AS base_1_value,
    COALESCE(bp1.base_1_volume, 0) AS base_1_volume,
    REPLACE(REPLACE(bp1.base_1_sum_avg_method, '"', ''), '"', '') AS base_1_sum_avg_method,
    COALESCE(bp2.base_2_value, 0) AS base_2_value,
    COALESCE(bp2.base_2_volume, 0) AS base_2_volume,
    REPLACE(REPLACE(bp2.base_2_sum_avg_method, '"', ''), '"', '') AS base_2_sum_avg_method,
    -- Base Period Month Counts
    COALESCE(bp1.base_1_months_count, 0) AS base_1_months,
    COALESCE(bp2.base_2_months_count, 0) AS base_2_months,
    -- Base Period Final Values
    COALESCE(bp1.base_1_value_final, 0) AS base_1_value_final,
    COALESCE(bp1.base_1_volume_final, 0) AS base_1_volume_final,
    COALESCE(bp2.base_2_value_final, 0) AS base_2_value_final,
    COALESCE(bp2.base_2_volume_final, 0) AS base_2_volume_final
  FROM all_accounts aa
  LEFT JOIN base_sales bs ON aa.credit_account = bs.credit_account
  LEFT JOIN actuals ac ON aa.credit_account = ac.credit_account
  LEFT JOIN mandatory_product_base_value mpbv ON aa.credit_account = mpbv.credit_account
  LEFT JOIN mandatory_product_actuals mpa ON aa.credit_account = mpa.credit_account
  LEFT JOIN mandatory_product_actual_ppi mpappi ON aa.credit_account = mpappi.credit_account
  LEFT JOIN payout_product_actuals ppa ON aa.credit_account = ppa.credit_account
  LEFT JOIN base_period_1_finals bp1 ON aa.credit_account = bp1.credit_account
  LEFT JOIN base_period_2_finals bp2 ON aa.credit_account = bp2.credit_account
  LEFT JOIN customer_info_fallback cif ON aa.credit_account = cif.credit_account
),

slab_applied AS (
  SELECT
    j.*,
    -- MODIFIED GROWTH RATE LOGIC WITH enableStrataGrowth CHECK
    CASE 
      WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
      THEN sg.strata_growth_percentage / 100.0
      ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
    END AS growth_rate,
    COALESCE(sl_actual.rebate_per_litre, fs.rebate_per_litre, 0) AS rebate_per_litre_applied,
    COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) AS rebate_percent_applied,
    COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) AS additional_rebate_on_growth_per_litre_applied,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_base,
    COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0) AS fixed_rebate_actual,
    
    CASE 
      WHEN COALESCE(sl_actual.mandatory_product_target, 0) <= 0 THEN fs.mandatory_product_target
      ELSE COALESCE(sl_actual.mandatory_product_target, 0)
    END AS mandatory_product_target,
    
    COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0) AS mandatory_product_growth_percent,
    COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0) AS mandatory_product_target_to_actual,
    COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) AS mandatory_product_rebate,
    COALESCE(sl_actual.mandatory_product_rebate_percent, fs.mandatory_product_rebate_percent, 0) AS mandatory_product_rebate_percent,
    COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) AS mandatory_min_shades_ppi,
    
    -- MANDATORY PRODUCT TARGET VALUE (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.mandatory_product_base_value IS NULL THEN 0
      ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
    END AS mandatory_product_target_value,
    
    -- UPDATED TARGET VALUE CALCULATION WITH enableStrataGrowth CHECK (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
      ELSE GREATEST(
        (1 + CASE 
               WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
               THEN sg.strata_growth_percentage / 100.0
               ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
             END) * j.total_value, 
        COALESCE(sl_base.slab_start, fs.slab_start, 0)
      )
    END AS target_value,
    
    CASE 
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      ELSE j.actual_value / 
        (CASE 
          WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
          ELSE GREATEST(
            (1 + CASE 
                   WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                   THEN sg.strata_growth_percentage / 100.0
                   ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                 END) * j.total_value, 
            COALESCE(sl_base.slab_start, fs.slab_start, 0)
          )
        END)
    END AS percent_achieved,
    
    -- BASIC PAYOUT BASED ON PAYOUT PRODUCT VALUE (CHANGED FROM VOLUME TO VALUE)
    (COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) / 100.0) * j.payout_product_actual_value AS basic_payout,
    
    -- ADDITIONAL PAYOUT BASED ON PAYOUT PRODUCT VALUE (CHANGED FROM VOLUME TO VALUE)
    CASE 
      WHEN j.actual_value >= 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END)
      THEN COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_value
      ELSE 0
    END AS additional_payout,    -- MANDATORY PRODUCT PAYOUT LOGIC (USING VALUES WHERE APPLICABLE)
    CASE 
      WHEN (
        (CASE 
          WHEN COALESCE(j.actual_value, 0) = 0 THEN 0 
          ELSE COALESCE(j.mandatory_product_actual_value, 0) / NULLIF(j.actual_value, 0) 
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_actual_value < 0 THEN 0
          WHEN COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_ppi / NULLIF(COALESCE(sl_actual.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi, 0), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)) = 0 THEN 0 
          ELSE j.mandatory_product_actual_value / NULLIF(j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0)), 0)
        END) >= 1.0
        
        OR 
        
        (CASE 
          WHEN COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0) = 0 THEN 0 
          ELSE j.mandatory_product_actual_value / NULLIF(COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 0)
        END) >= 1.0
      )
      THEN COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0) * j.payout_product_actual_value
      ELSE 0
    END AS mandatory_product_payout,
    
    CASE
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      WHEN j.actual_value / 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) >= 1
        THEN COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0)
      ELSE 0
    END AS fixed_rebate,
    
    -- PAYOUT PRODUCT PAYOUT WITH enableStrataGrowth CHECK (USING VALUES)
    ((COALESCE(sl_actual.rebate_percent, fs.rebate_percent, 0) / 100.0) * j.payout_product_actual_value) +
    CASE 
      WHEN j.actual_value >= 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END)
      THEN COALESCE(sl_actual.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) * j.payout_product_actual_value
      ELSE 0
    END +
    CASE
      WHEN (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) = 0 THEN 0
      WHEN j.actual_value / 
           (CASE 
              WHEN j.total_value = 0 OR j.total_value IS NULL THEN COALESCE(fs.slab_start, 0)
              ELSE GREATEST(
                (1 + CASE 
                       WHEN sc.enable_strata_growth = true AND sg.strata_growth_percentage > 0 
                       THEN sg.strata_growth_percentage / 100.0
                       ELSE COALESCE(sl_base.growth_rate, fs.growth_rate, 0)
                     END) * j.total_value, 
                COALESCE(sl_base.slab_start, fs.slab_start, 0)
              )
            END) >= 1
        THEN COALESCE(sl_actual.fixed_rebate, fs.fixed_rebate, 0)
      ELSE 0
    END +
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(CASE 
                  WHEN j.mandatory_product_base_value IS NULL THEN 0
                  ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
                END, 0), 
                COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 
                COALESCE(j.actual_value * COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0), 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(j.mandatory_product_actual_value, 0) / NULLIF(
                  GREATEST(
                    COALESCE(CASE 
                      WHEN j.mandatory_product_base_value IS NULL THEN 0
                      ELSE j.mandatory_product_base_value * (1 + COALESCE(sl_base.mandatory_product_growth_percent, fs.mandatory_product_growth_percent, 0))
                    END, 0), 
                    COALESCE(sl_actual.mandatory_product_target, fs.mandatory_product_target, 0), 
                    COALESCE(j.actual_value * COALESCE(sl_actual.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual, 0), 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN COALESCE(j.mandatory_product_actual_value, 0) * COALESCE(sl_actual.mandatory_product_rebate, fs.mandatory_product_rebate, 0)
      ELSE 0
    END AS payout_product_payout
    
  FROM joined j
  LEFT JOIN slabs sl_actual ON j.actual_value BETWEEN sl_actual.slab_start AND sl_actual.slab_end
  LEFT JOIN slabs sl_base ON j.total_value BETWEEN sl_base.slab_start AND sl_base.slab_end
  LEFT JOIN strata_growth sg ON j.credit_account::bigint = sg.credit_account
  CROSS JOIN first_slab fs
  CROSS JOIN scheme_config sc
),

-- PHASING ACTUALS (CHANGED FROM VOLUME TO VALUE) - USING SCHEME TOTAL PRODUCTS
phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.phasing_from_date AND pp.phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR SCHEME TOTAL PRODUCTS (MAIN PRODUCTS)
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- PHASING PAYOUTS (CHANGED FROM VOLUME TO VALUE) - USING SCHEME TOTAL PRODUCTS
phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_payout_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR SCHEME TOTAL PRODUCTS (MAIN PRODUCTS)
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING ACTUALS (CHANGED FROM VOLUME TO VALUE) - USING PAYOUT PRODUCTS
bonus_phasing_actuals AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_phasing_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_phasing_from_date AND pp.bonus_phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUTS (CHANGED FROM VOLUME TO VALUE) - USING PAYOUT PRODUCTS
bonus_phasing_payouts AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_phasing_payout_period_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),
-- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES - FOR PAYOUT CALCULATION
phasing_period_payout_product_values AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS phasing_period_payout_product_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),
-- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES - FOR BONUS PAYOUT CALCULATION
bonus_payout_period_payout_product_values AS (
  SELECT
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.value), 0) AS bonus_payout_period_payout_product_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD') 
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- FILTER FOR PAYOUT PRODUCTS ONLY
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- UPDATED PHASING AGGREGATED WITH FINAL PHASING PAYOUT COLUMN (CHANGED TO VALUES)
phasing_aggregated AS (
  SELECT
    sa.credit_account,
    sa.customer_name,
    sa.so_name,
    sa.state_name,
    sa.total_value,
    sa.total_volume,
    sa.actual_value,
    sa.actual_volume,
    sa.mandatory_product_base_value,
    sa.mandatory_product_actual_value,
    sa.mandatory_product_target_value,
    sa.mandatory_product_payout,
    sa.mandatory_product_actual_ppi,
    sa.payout_product_actual_value,
    sa.payout_product_actual_volume,
    sa.growth_rate,
    sa.target_value,
    sa.basic_payout,
    sa.additional_payout,
    sa.percent_achieved,
    sa.fixed_rebate,
    sa.rebate_per_litre_applied,
    sa.rebate_percent_applied,
    sa.additional_rebate_on_growth_per_litre_applied,
    sa.mandatory_product_target,
    sa.mandatory_product_growth_percent,
    sa.mandatory_product_target_to_actual,
    sa.mandatory_product_rebate,
    sa.mandatory_product_rebate_percent,
    sa.mandatory_min_shades_ppi,
    sa.basic_payout + sa.additional_payout + sa.fixed_rebate AS total_payout,
    sa.payout_product_payout AS payout_product_payout,
    -- Base Period Columns
    sa.base_1_value,
    sa.base_1_volume,
    sa.base_1_sum_avg_method,
    sa.base_2_value,
    sa.base_2_volume,
    sa.base_2_sum_avg_method,
    -- Base Period Month Counts
    sa.base_1_months,
    sa.base_2_months,
    -- Base Period Final Values
    sa.base_1_value_final,
    sa.base_1_volume_final,
    sa.base_2_value_final,
    sa.base_2_volume_final,
    
    sa.actual_value * sa.mandatory_product_target_to_actual AS "Mandatory Product % to Actual - Target Value",
    CASE WHEN sa.actual_value = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.actual_value END AS "Mandatory Product Actual Value % to Total Sales",
    CASE WHEN sa.mandatory_product_target = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.mandatory_product_target END AS "Mandatory Product Actual Value % to Fixed Target Sales",
    CASE WHEN sa.mandatory_product_target_value = 0 THEN 0 ELSE sa.mandatory_product_actual_value / sa.mandatory_product_target_value END AS "Mandatory Product Growth Target Achieved",
    
    CASE 
      WHEN sa.mandatory_product_actual_value < 0 THEN 0
      WHEN sa.mandatory_min_shades_ppi = 0 THEN 0 
      ELSE sa.mandatory_product_actual_ppi / sa.mandatory_min_shades_ppi 
    END AS "Mandatory Product % PPI",
    
    -- PHASING PERIOD 1 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 1 THEN 1 END) AS "Phasing Period No 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.phasing_target_percent END) AS "Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_value END) AS "Phasing Period Rebate 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 1",
    
    -- 9 BONUS FIELDS FOR PERIOD 1 (DYNAMIC BONUS PAYOUT FOR ADDITIONAL SCHEMES)
    (MAX(CASE WHEN pp.phasing_id = 1 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 1",
    
    -- PHASING PERIOD 2 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 2 THEN 2 END) AS "Phasing Period No 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.phasing_target_percent END) AS "Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_value END) AS "Phasing Period Rebate 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 2",
    
    -- 9 BONUS FIELDS FOR PERIOD 2 (DYNAMIC BONUS PAYOUT FOR ADDITIONAL SCHEMES)
    (MAX(CASE WHEN pp.phasing_id = 2 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 2",
    
    -- PHASING PERIOD 3 (CHANGED TO VALUES)
    MAX(CASE WHEN pp.phasing_id = 3 THEN 3 END) AS "Phasing Period No 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.phasing_target_percent END) AS "Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.phasing_target_percent * sa.target_value, 2) END) AS "Phasing Target Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pa.phasing_period_value, 0) END) AS "Phasing Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(ppay.phasing_payout_period_value, 0) END) AS "Phasing Payout Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Phasing Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_value END) AS "Phasing Period Rebate 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_percentage END) AS "Phasing Period Rebate% 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_value, 0) / NULLIF(pp.phasing_target_percent * sa.target_value, 0)) >= 1 THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(pppv.phasing_period_payout_product_value, 0) * (pp.rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Phasing Payout 3",
    
    -- 9 BONUS FIELDS FOR PERIOD 3 (DYNAMIC BONUS PAYOUT FOR ADDITIONAL SCHEMES)
    (MAX(CASE WHEN pp.phasing_id = 3 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_percentage END) AS "Bonus Phasing Period Rebate % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.bonus_phasing_target_percent * sa.target_value, 2) END) AS "Bonus Target Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpa.bonus_phasing_period_value, 0) END) AS "Bonus Phasing Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bppay.bonus_phasing_payout_period_value, 0) END) AS "Bonus Payout Period Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * sa.target_value = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)), 6)
      END 
    END) AS "% Bonus Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_value, 0) / NULLIF(pp.bonus_phasing_target_percent * sa.target_value, 0)) >= 1 
        THEN 
          CASE 
            -- Dynamic payout base selection based on JSON payout products configuration
            WHEN (SELECT payout_products_configured FROM scheme_config) = true
            THEN ROUND(COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
            ELSE ROUND(COALESCE(ppay.phasing_payout_period_value, 0) * (pp.bonus_rebate_percentage / 100.0), 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 3",
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 1",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 2",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(pppv.phasing_period_payout_product_value, 0) END) AS "Phasing Period Payout Product Value 3",
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    MAX(CASE WHEN pp.phasing_id = 1 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 1",
    MAX(CASE WHEN pp.phasing_id = 2 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 2",
    MAX(CASE WHEN pp.phasing_id = 3 THEN COALESCE(bpppv.bonus_payout_period_payout_product_value, 0) END) AS "Bonus Payout Period Payout Product Value 3"
    
  FROM slab_applied sa
  LEFT JOIN phasing_periods pp ON 1=1
  LEFT JOIN phasing_actuals pa ON sa.credit_account = pa.credit_account AND pp.phasing_id = pa.phasing_id
  LEFT JOIN phasing_payouts ppay ON sa.credit_account = ppay.credit_account AND pp.phasing_id = ppay.phasing_id
  LEFT JOIN bonus_phasing_actuals bpa ON sa.credit_account = bpa.credit_account AND pp.phasing_id = bpa.phasing_id
  LEFT JOIN bonus_phasing_payouts bppay ON sa.credit_account = bppay.credit_account AND pp.phasing_id = bppay.phasing_id
  LEFT JOIN phasing_period_payout_product_values pppv ON sa.credit_account = pppv.credit_account AND pp.phasing_id = pppv.phasing_id
  LEFT JOIN bonus_payout_period_payout_product_values bpppv ON sa.credit_account = bpppv.credit_account AND pp.phasing_id = bpppv.phasing_id
  GROUP BY sa.credit_account, sa.customer_name, sa.so_name, sa.state_name, 
           sa.total_value, sa.total_volume, sa.actual_value, sa.actual_volume,
           sa.mandatory_product_base_value, sa.mandatory_product_actual_value,
           sa.mandatory_product_target_value, sa.mandatory_product_actual_ppi,
           sa.payout_product_actual_value, sa.payout_product_actual_volume,
           sa.mandatory_product_payout, sa.growth_rate, sa.target_value, 
           sa.basic_payout, sa.additional_payout, sa.percent_achieved, sa.fixed_rebate, 
           sa.rebate_per_litre_applied, sa.rebate_percent_applied, sa.additional_rebate_on_growth_per_litre_applied,
           sa.mandatory_product_target, sa.mandatory_product_growth_percent,
           sa.mandatory_product_target_to_actual, sa.mandatory_product_rebate,
           sa.mandatory_product_rebate_percent, sa.mandatory_min_shades_ppi, sa.payout_product_payout,
           sa.base_1_value, sa.base_1_volume, sa.base_1_sum_avg_method,
           sa.base_2_value, sa.base_2_volume, sa.base_2_sum_avg_method,
           sa.base_1_months, sa.base_2_months,
           sa.base_1_value_final, sa.base_1_volume_final,
           sa.base_2_value_final, sa.base_2_volume_final
),

final_output AS (
  SELECT 
    credit_account,
    customer_name,
    so_name,
    state_name,
    -- Base Period Columns
    base_1_value AS "Base 1 Value",
    base_1_volume AS "Base 1 Volume", 
    base_1_sum_avg_method AS "Base 1 SumAvg",
    base_1_months AS "Base 1 Months",
    base_1_value_final AS "Base 1 Value Final",
    base_1_volume_final AS "Base 1 Volume Final",
    base_2_value AS "Base 2 Value",
    base_2_volume AS "Base 2 Volume",
    base_2_sum_avg_method AS "Base 2 SumAvg",
    base_2_months AS "Base 2 Months",
    base_2_value_final AS "Base 2 Value Final",
    base_2_volume_final AS "Base 2 Volume Final",
    total_value,
    total_volume,
    actual_value,
    actual_volume,
    mandatory_product_base_value AS "Mandatory Product Base Value",
    mandatory_product_actual_value AS "Mandatory Product Actual Value",
    mandatory_product_target_value AS "Mandatory Product Growth Target Value",
    mandatory_product_payout AS "Mandatory Product Payout",
    mandatory_product_actual_ppi AS "Mandatory product actual PPI",
    growth_rate,
    target_value,
    basic_payout,
    additional_payout,
    percent_achieved,
    fixed_rebate,
    rebate_per_litre_applied AS "Rebate per Litre",
    rebate_percent_applied AS "Rebate %",
    additional_rebate_on_growth_per_litre_applied AS "Additional Rebate on Growth per Litre",
    mandatory_product_target AS "Mandatory Product Fixed Target",
    mandatory_product_growth_percent AS "Mandatory Product Growth",
    mandatory_product_target_to_actual AS "Mandatory Product % Target to Actual Sales",
    mandatory_product_rebate AS "Mandatory Product Rebate",
    mandatory_product_rebate_percent AS "MP Rebate %",
    mandatory_min_shades_ppi AS "Mandatory Min. Shades - PPI",
    total_payout AS "Total Payout",
    payout_product_payout AS "Payout Product Payout",
    "Mandatory Product % to Actual - Target Value",
    "Mandatory Product Actual Value % to Total Sales",
    "Mandatory Product Actual Value % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product % PPI",
    payout_product_actual_value AS "Payout Products Value",
    payout_product_actual_volume AS "Payout Products Volume", 
   -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD
    "Phasing Period No 1", "Phasing Target % 1", "Phasing Target Value 1", "Phasing Period Value 1", 
    "Phasing Payout Period Value 1", "% Phasing Achieved 1", "Phasing Period Rebate 1", "Phasing Period Rebate% 1", "Phasing Payout 1",
    "Is Bonus 1", "Bonus Rebate Value 1", "Bonus Phasing Period Rebate % 1", "Bonus Phasing Target % 1", "Bonus Target Value 1", "Bonus Phasing Period Value 1",
    "Bonus Payout Period Value 1", "% Bonus Achieved 1", "Bonus Payout 1",
    
    "Phasing Period No 2", "Phasing Target % 2", "Phasing Target Value 2", "Phasing Period Value 2",
    "Phasing Payout Period Value 2", "% Phasing Achieved 2", "Phasing Period Rebate 2", "Phasing Period Rebate% 2", "Phasing Payout 2",
    "Is Bonus 2", "Bonus Rebate Value 2", "Bonus Phasing Period Rebate % 2", "Bonus Phasing Target % 2", "Bonus Target Value 2", "Bonus Phasing Period Value 2",
    "Bonus Payout Period Value 2", "% Bonus Achieved 2", "Bonus Payout 2",
    
    "Phasing Period No 3", "Phasing Target % 3", "Phasing Target Value 3", "Phasing Period Value 3",
    "Phasing Payout Period Value 3", "% Phasing Achieved 3", "Phasing Period Rebate 3", "Phasing Period Rebate% 3", "Phasing Payout 3",
    "Is Bonus 3", "Bonus Rebate Value 3", "Bonus Phasing Period Rebate % 3", "Bonus Phasing Target % 3", "Bonus Target Value 3", "Bonus Phasing Period Value 3",
    "Bonus Payout Period Value 3", "% Bonus Achieved 3", "Bonus Payout 3",
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    "Phasing Period Payout Product Value 1", "Phasing Period Payout Product Value 2", "Phasing Period Payout Product Value 3",
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
    "Bonus Payout Period Payout Product Value 1", "Bonus Payout Period Payout Product Value 2", "Bonus Payout Period Payout Product Value 3",

    -- **FINAL PHASING PAYOUT COLUMN**
    CASE 
      WHEN (COALESCE("% Bonus Achieved 1", 0) >= 1.0) OR (COALESCE("% Bonus Achieved 2", 0) >= 1.0)
      THEN GREATEST(COALESCE("Bonus Payout 1", 0), COALESCE("Bonus Payout 2", 0))
      ELSE COALESCE("Phasing Payout 1", 0) + COALESCE("Phasing Payout 2", 0) + COALESCE("Phasing Payout 3", 0)
    END AS "FINAL PHASING PAYOUT",

    -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)** - CHANGED TO VALUES
    GREATEST(
      COALESCE(mandatory_product_target_value, 0), 
      COALESCE(mandatory_product_target, 0), 
      COALESCE("Mandatory Product % to Actual - Target Value", 0)
    ) AS "MP FINAL TARGET",
    
    CASE 
      WHEN GREATEST(
        COALESCE(mandatory_product_target_value, 0), 
        COALESCE(mandatory_product_target, 0), 
        COALESCE("Mandatory Product % to Actual - Target Value", 0)
      ) = 0 THEN 0
      ELSE ROUND(
        COALESCE(mandatory_product_actual_value, 0) / NULLIF(
          GREATEST(
            COALESCE(mandatory_product_target_value, 0), 
            COALESCE(mandatory_product_target, 0), 
            COALESCE("Mandatory Product % to Actual - Target Value", 0)
          ), 0
        ), 6
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN: MP FINAL PAYOUT** - DYNAMIC BASED ON JSON PAYOUT PRODUCTS CONFIGURATION
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(mandatory_product_target_value, 0), 
                COALESCE(mandatory_product_target, 0), 
                COALESCE("Mandatory Product % to Actual - Target Value", 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(mandatory_product_actual_value, 0) / NULLIF(
                  GREATEST(
                    COALESCE(mandatory_product_target_value, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Value", 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN 
        CASE 
          -- Check JSON configuration directly for payout products
          WHEN (SELECT payout_products_configured FROM scheme_config) = true
          THEN COALESCE(payout_product_actual_value, 0) * (COALESCE(mandatory_product_rebate_percent, 0) / 100.0)
          ELSE COALESCE(actual_value, 0) * (COALESCE(mandatory_product_rebate_percent, 0) / 100.0)
        END
      ELSE 0
    END AS "MP Final Payout"
    
  FROM phasing_aggregated
),

unioned AS (
  SELECT *, 0 AS is_grand_total FROM final_output
  UNION ALL
  SELECT
    'GRAND TOTAL',
    NULL, NULL, NULL,
    -- Base Period Grand Totals
    COALESCE(SUM("Base 1 Value"), 0),
    COALESCE(SUM("Base 1 Volume"), 0),
    NULL, -- Base 1 SumAvg
    NULL, -- Base 1 Months (no meaningful total)
    COALESCE(SUM("Base 1 Value Final"), 0),
    COALESCE(SUM("Base 1 Volume Final"), 0),
    COALESCE(SUM("Base 2 Value"), 0),  
    COALESCE(SUM("Base 2 Volume"), 0),
    NULL, -- Base 2 SumAvg
    NULL, -- Base 2 Months (no meaningful total)
    COALESCE(SUM("Base 2 Value Final"), 0),
    COALESCE(SUM("Base 2 Volume Final"), 0),
    COALESCE(SUM(total_value), 0),
    COALESCE(SUM(total_volume), 0),
    COALESCE(SUM(actual_value), 0),
    COALESCE(SUM(actual_volume), 0),
    COALESCE(SUM("Mandatory Product Base Value"), 0),
    COALESCE(SUM("Mandatory Product Actual Value"), 0),
    COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
    COALESCE(SUM("Mandatory Product Payout"), 0),
    COALESCE(SUM("Mandatory product actual PPI"), 0),
    NULL,
    COALESCE(SUM(target_value), 0),
    COALESCE(SUM(basic_payout), 0),
    COALESCE(SUM(additional_payout), 0),
    CASE WHEN COALESCE(SUM(target_value), 0) = 0 THEN 0 ELSE COALESCE(SUM(actual_value), 0) / NULLIF(SUM(target_value), 0) END AS percent_achieved,
    COALESCE(SUM(fixed_rebate), 0),
    NULL,
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Fixed Target"), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Rebate"), 0),
    NULL, -- MP Rebate % (percentage field, no meaningful grand total)
    COALESCE(SUM("Mandatory Min. Shades - PPI"), 0),
    COALESCE(SUM("Total Payout"), 0),
    COALESCE(SUM("Payout Product Payout"), 0),
    COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0),
    CASE WHEN COALESCE(SUM(actual_value), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM(actual_value), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Fixed Target"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM("Mandatory Product Fixed Target"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Growth Target Value"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(SUM("Mandatory Product Growth Target Value"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Min. Shades - PPI"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory product actual PPI"), 0) / NULLIF(SUM("Mandatory Min. Shades - PPI"), 0) END,
    COALESCE(SUM("Payout Products Value"), 0),
    COALESCE(SUM("Payout Products Volume"), 0),
    
    -- Phasing Grand Totals (all 48 phasing fields - 16 per period × 3 periods)
    -- Period 1 (16 fields)
    NULL, NULL, COALESCE(SUM("Phasing Target Value 1"), 0), COALESCE(SUM("Phasing Period Value 1"), 0), 
    COALESCE(SUM("Phasing Payout Period Value 1"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 1"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,
    -- Period 2 (16 fields)  
    NULL, NULL, COALESCE(SUM("Phasing Target Value 2"), 0), COALESCE(SUM("Phasing Period Value 2"), 0),
    COALESCE(SUM("Phasing Payout Period Value 2"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 2"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,
    -- Period 3 (16 fields)
    NULL, NULL, COALESCE(SUM("Phasing Target Value 3"), 0), COALESCE(SUM("Phasing Period Value 3"), 0),
    COALESCE(SUM("Phasing Payout Period Value 3"), 0), NULL, NULL, NULL, COALESCE(SUM("Phasing Payout 3"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,
    -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES GRAND TOTALS (3 total columns)
    COALESCE(SUM("Phasing Period Payout Product Value 1"), 0), COALESCE(SUM("Phasing Period Payout Product Value 2"), 0), COALESCE(SUM("Phasing Period Payout Product Value 3"), 0),
    -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES GRAND TOTALS (3 total columns)
    COALESCE(SUM("Bonus Payout Period Payout Product Value 1"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Value 2"), 0), COALESCE(SUM("Bonus Payout Period Payout Product Value 3"), 0),

    -- **FINAL PHASING PAYOUT GRAND TOTAL**
    COALESCE(SUM("FINAL PHASING PAYOUT"), 0),

    -- **NEW COLUMNS GRAND TOTALS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT %** - CHANGED TO VALUES
    GREATEST(
      COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
      COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
      COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
    ) AS "MP FINAL TARGET",
    CASE 
      WHEN GREATEST(
        COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
        COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
        COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
      ) = 0 THEN 0
      ELSE COALESCE(SUM("Mandatory Product Actual Value"), 0) / NULLIF(
        GREATEST(
          COALESCE(SUM("Mandatory Product Growth Target Value"), 0),
          COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
          COALESCE(SUM("Mandatory Product % to Actual - Target Value"), 0)
        ), 0
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN GRAND TOTAL: MP FINAL PAYOUT** - CHANGED TO VALUES
    COALESCE(SUM("MP Final Payout"), 0) AS "MP Final Payout",
    
    1 AS is_grand_total
  FROM final_output
)

-- FINAL SELECT WITH CLEAN STRUCTURE (NO BONUS SCHEMES) - CHANGED TO VALUES
SELECT 
  u.state_name,
  u.credit_account,
  u.customer_name,
  u.so_name,
  u."Base 1 Value", 
  u."Base 1 Volume", 
  u."Base 1 SumAvg",
  u."Base 1 Months",
  u."Base 1 Value Final",
  u."Base 1 Volume Final",
  u."Base 2 Value",
  u."Base 2 Volume",
  u."Base 2 SumAvg",
  u."Base 2 Months",
  u."Base 2 Value Final",
  u."Base 2 Volume Final",
  u.total_value,
  u.total_volume,
  u.growth_rate,
  sg.strata_growth_percentage AS "Strata Growth %",
  u.target_value,
  u.actual_value,
  u.actual_volume,
  u.percent_achieved AS "% Achieved",
  u."Rebate per Litre",
  u."Rebate %",
  u.basic_payout,
  u."Additional Rebate on Growth per Litre",
  u.additional_payout,
  u.fixed_rebate AS "Fixed Rebate",
  u."Total Payout",
  u."Payout Product Payout",
  u."Mandatory Product Base Value",
  u."Mandatory Product Growth",
  u."Mandatory Product Growth Target Value",
  u."Mandatory Product Actual Value",
  u."Mandatory Product Growth Target Achieved",
  u."Mandatory Min. Shades - PPI",
  u."Mandatory product actual PPI",
  u."Mandatory Product % PPI",
  u."Mandatory Product Fixed Target",
  u."Mandatory Product Actual Value % to Fixed Target Sales",
  u."Mandatory Product % Target to Actual Sales",
  u."Mandatory Product % to Actual - Target Value",
  u."Mandatory Product Actual Value % to Total Sales",
  u."Mandatory Product Rebate",
  u."MP Rebate %",
  u."Mandatory Product Payout",
  u."Payout Products Value",
  u."Payout Products Volume",
  
  -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD - CHANGED TO VALUES
  u."Phasing Period No 1", u."Phasing Target % 1", u."Phasing Target Value 1", u."Phasing Period Value 1", 
  u."Phasing Payout Period Value 1", u."% Phasing Achieved 1", u."Phasing Period Rebate 1", u."Phasing Period Rebate% 1", u."Phasing Payout 1",
  u."Is Bonus 1", u."Bonus Rebate Value 1", u."Bonus Phasing Period Rebate % 1", u."Bonus Phasing Target % 1", u."Bonus Target Value 1", u."Bonus Phasing Period Value 1",
  u."Bonus Payout Period Value 1", u."% Bonus Achieved 1", u."Bonus Payout 1",
  
  u."Phasing Period No 2", u."Phasing Target % 2", u."Phasing Target Value 2", u."Phasing Period Value 2",
  u."Phasing Payout Period Value 2", u."% Phasing Achieved 2", u."Phasing Period Rebate 2", u."Phasing Period Rebate% 2", u."Phasing Payout 2",
  u."Is Bonus 2", u."Bonus Rebate Value 2", u."Bonus Phasing Period Rebate % 2", u."Bonus Phasing Target % 2", u."Bonus Target Value 2", u."Bonus Phasing Period Value 2",
  u."Bonus Payout Period Value 2", u."% Bonus Achieved 2", u."Bonus Payout 2",
  
  u."Phasing Period No 3", u."Phasing Target % 3", u."Phasing Target Value 3", u."Phasing Period Value 3",
  u."Phasing Payout Period Value 3", u."% Phasing Achieved 3", u."Phasing Period Rebate 3", u."Phasing Period Rebate% 3", u."Phasing Payout 3",
  u."Is Bonus 3", u."Bonus Rebate Value 3", u."Bonus Phasing Period Rebate % 3", u."Bonus Phasing Target % 3", u."Bonus Target Value 3", u."Bonus Phasing Period Value 3",
  u."Bonus Payout Period Value 3", u."% Bonus Achieved 3", u."Bonus Payout 3",
  -- NEW: PHASING PERIOD PAYOUT PRODUCT VALUES (3 total columns)
  u."Phasing Period Payout Product Value 1", u."Phasing Period Payout Product Value 2", u."Phasing Period Payout Product Value 3",
  -- NEW: BONUS PAYOUT PERIOD PAYOUT PRODUCT VALUES (3 total columns)
  u."Bonus Payout Period Payout Product Value 1", u."Bonus Payout Period Payout Product Value 2", u."Bonus Payout Period Payout Product Value 3",
  
  -- **FINAL PHASING PAYOUT COLUMN**
  u."FINAL PHASING PAYOUT",

  -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)** - CHANGED TO VALUES
  u."MP FINAL TARGET",
  u."MP FINAL ACHEIVMENT %",
  u."MP Final Payout"
  
FROM unioned u
LEFT JOIN strata_growth sg ON u.credit_account = sg.credit_account::text
ORDER BY u.is_grand_total, u.credit_account;
"""

TRACKER_ADDITIONAL_SCHEME_VOLUME = """
SET SESSION statement_timeout = 0;

-- ADDITIONAL SCHEME INDEX PARAMETER (Change this to switch between additional schemes)
-- 0 = First additional scheme, 1 = Second additional scheme, etc.
WITH additional_scheme_index AS (
  SELECT {additional_scheme_index} AS idx
),

scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id = '{scheme_id}'
),

-- Base Period 1 Configuration with Month Calculation
base_period_1_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->0->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

-- Base Period 2 Configuration with Month Calculation
base_period_2_config AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    (scheme_json->'mainScheme'->'baseVolSections'->1->>'sumAvg')::text AS sum_avg_method,
    EXTRACT(MONTH FROM AGE(
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'toDate')::date + INTERVAL '1 day')::date,
      ((scheme_json->'mainScheme'->'baseVolSections'->1->>'fromDate')::date + INTERVAL '1 day')::date
    )) + 1 AS months_count
  FROM scheme
),

base_dates AS (
  SELECT
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'fromDate')::date + INTERVAL '1 day')::date AS from_date,
    ((scheme_json->'mainScheme'->'baseVolSections'->0->>'toDate')::date + INTERVAL '1 day')::date AS to_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'fromDate')::date + INTERVAL '1 day')::date AS scheme_from_date,
    ((scheme_json->'mainScheme'->'schemePeriod'->>'toDate')::date + INTERVAL '1 day')::date AS scheme_to_date
  FROM scheme
),

-- REFERENCE CTEs (FROM MAIN SCHEME - SCHEME APPLICABILITY)
states AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates') AS state FROM scheme
),
regions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedRegions') AS region FROM scheme
),
area_heads AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedAreaHeads') AS area_head FROM scheme
),
divisions AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDivisions') AS division FROM scheme
),
dealer_types AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDealerTypes') AS dealer_type FROM scheme
),
distributors AS (
  SELECT jsonb_array_elements_text(scheme_json->'mainScheme'->'schemeApplicable'->'selectedDistributors') AS distributor FROM scheme
),

-- PRODUCT DATA (FROM ADDITIONAL SCHEME - for main product filtering)
product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'materials') > 0
),
product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'categories') > 0
),
product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'grps') > 0
),
product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'wandaGroups') > 0
),
product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'thinnerGroups') > 0
),

-- MANDATORY PRODUCTS (FROM ADDITIONAL SCHEME - separate from main products)
mandatory_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'materials') > 0
),
mandatory_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'categories') > 0
),
mandatory_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'grps') > 0
),
mandatory_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'wandaGroups') > 0
),
mandatory_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'mandatoryProducts'->'thinnerGroups') > 0
),



-- PAYOUT PRODUCTS (FROM ADDITIONAL SCHEME)
payout_product_materials AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') AS material 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
),
payout_product_categories AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories') AS category 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'categories') > 0
),
payout_product_grps AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps') AS grp 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'grps') > 0
),
payout_product_wanda_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups') AS wanda_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'wandaGroups') > 0
),
payout_product_thinner_groups AS (
  SELECT jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups') AS thinner_group 
  FROM scheme, additional_scheme_index asi
  WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups' IS NOT NULL
    AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'thinnerGroups') > 0
),

-- SLABS (FROM ADDITIONAL SCHEME)
slabs AS (
  SELECT 
    COALESCE(NULLIF(slab->>'slabStart', ''), '0')::NUMERIC AS slab_start,
    COALESCE(NULLIF(slab->>'slabEnd', ''), '0')::NUMERIC AS slab_end,
    COALESCE(NULLIF(slab->>'growthPercent', ''), '0')::NUMERIC / 100.0 AS growth_rate,
    COALESCE(NULLIF(slab->>'dealerMayQualifyPercent', ''), '0')::NUMERIC / 100.0 AS qualification_rate,
    COALESCE(NULLIF(slab->>'rebatePerLitre', ''), '0')::NUMERIC AS rebate_per_litre,
    COALESCE(NULLIF(slab->>'additionalRebateOnGrowth', ''), '0')::NUMERIC AS additional_rebate_on_growth_per_litre,
    COALESCE(NULLIF(slab->>'fixedRebate', ''), '0')::NUMERIC AS fixed_rebate,
    COALESCE(NULLIF(slab->>'mandatoryProductTarget', ''), '0')::NUMERIC AS mandatory_product_target,
    COALESCE(NULLIF(slab->>'mandatoryProductGrowthPercent', ''), '0')::NUMERIC / 100.0 AS mandatory_product_growth_percent,
    COALESCE(NULLIF(slab->>'mandatoryProductTargetToActual', ''), '0')::NUMERIC / 100.0 AS mandatory_product_target_to_actual,
    COALESCE(NULLIF(slab->>'mandatoryProductRebate', ''), '0')::NUMERIC AS mandatory_product_rebate,
    COALESCE(NULLIF(slab->>'mandatoryMinShadesPPI', ''), '0')::NUMERIC AS mandatory_min_shades_ppi,
    row_number() OVER (ORDER BY COALESCE(NULLIF(slab->>'slabStart', ''), '0')::NUMERIC) AS slab_order
  FROM scheme, additional_scheme_index asi,
  LATERAL jsonb_array_elements(scheme_json->'additionalSchemes'->asi.idx->'slabData'->'mainScheme'->'slabs') AS slab
),

first_slab AS (
  SELECT 
    slab_start, growth_rate, qualification_rate, rebate_per_litre, 
    additional_rebate_on_growth_per_litre, fixed_rebate,
    mandatory_product_target, mandatory_product_growth_percent,
    mandatory_product_target_to_actual, mandatory_product_rebate,
    mandatory_min_shades_ppi
  FROM slabs
  WHERE slab_order = 1
),

-- PHASING PERIODS (FROM ADDITIONAL SCHEME)
phasing_periods AS (
  SELECT 
    COALESCE(NULLIF(phasing->>'id', ''), '0')::INTEGER AS phasing_id,
    COALESCE(NULLIF(phasing->>'rebateValue', ''), '0')::NUMERIC AS rebate_value,
    ((phasing->>'payoutToDate')::timestamp + INTERVAL '1 day')::date AS payout_to_date,
    ((phasing->>'phasingToDate')::timestamp + INTERVAL '1 day')::date AS phasing_to_date,
    ((phasing->>'payoutFromDate')::timestamp + INTERVAL '1 day')::date AS payout_from_date,
    ((phasing->>'phasingFromDate')::timestamp + INTERVAL '1 day')::date AS phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'phasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS phasing_bonus_target_percent,
    -- BONUS FIELDS (will be false/0 for additional schemes)
    COALESCE((phasing->>'isBonus')::boolean, false) AS is_bonus,
    COALESCE(NULLIF(phasing->>'bonusRebateValue', ''), '0')::NUMERIC AS bonus_rebate_value,
    ((phasing->>'bonusPayoutToDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_to_date,
    ((phasing->>'bonusPhasingToDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_to_date,
    ((phasing->>'bonusPayoutFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_payout_from_date,
    ((phasing->>'bonusPhasingFromDate')::timestamp + INTERVAL '1 day')::date AS bonus_phasing_from_date,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC / 100.0 AS bonus_phasing_target_percent,
    COALESCE(NULLIF(REPLACE(phasing->>'bonusPhasingTargetPercent', '%', ''), ''), '0')::NUMERIC AS bonus_phasing_target_percent_raw
  FROM scheme, additional_scheme_index asi,
  LATERAL jsonb_array_elements(scheme_json->'additionalSchemes'->asi.idx->'phasingPeriods') AS phasing
),

-- Extract enableStrataGrowth flag and payoutProducts configuration from JSON (FROM ADDITIONAL SCHEME)
scheme_config AS (
  SELECT 
    COALESCE((scheme_json->'additionalSchemes'->asi.idx->'slabData'->'mainScheme'->>'enableStrataGrowth')::boolean, false) AS enable_strata_growth,
    CASE 
      WHEN scheme_json->'additionalSchemes'->asi.idx->'configuration'->'enabledSections'->>'payoutProducts' = 'true' 
           AND scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts' IS NOT NULL
           AND jsonb_typeof(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts') = 'object'
           AND jsonb_array_length(COALESCE(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials', '[]'::jsonb)) > 0
      THEN true
      ELSE false
    END AS payout_products_enabled,
    scheme_json
  FROM scheme, additional_scheme_index asi
),

-- Base Period 1 Sales Data (using additional scheme products)
base_period_1_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_1_volume,
    CASE 
      WHEN bp1.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp1.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_1_value,
    bp1.sum_avg_method AS base_1_sum_avg_method,
    bp1.months_count AS base_1_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_1_config bp1
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp1.from_date AND bp1.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp1.sum_avg_method, bp1.months_count
),

-- Base Period 1 Final Calculations
base_period_1_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_volume / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_volume
    END AS base_1_volume_final,
    CASE 
      WHEN REPLACE(REPLACE(base_1_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_1_value / NULLIF(base_1_months_count, 0), 2)
      ELSE 
        base_1_value
    END AS base_1_value_final
  FROM base_period_1_sales
),

-- Base Period 2 Sales Data (using additional scheme products)
base_period_2_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.volume), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.volume), 0)
    END AS base_2_volume,
    CASE 
      WHEN bp2.sum_avg_method = '"average"' THEN 
        ROUND(COALESCE(SUM(sd.value), 0) / NULLIF(bp2.months_count, 0), 2)
      ELSE 
        COALESCE(SUM(sd.value), 0)
    END AS base_2_value,
    bp2.sum_avg_method AS base_2_sum_avg_method,
    bp2.months_count AS base_2_months_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_period_2_config bp2
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bp2.from_date AND bp2.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, bp2.sum_avg_method, bp2.months_count
),

-- Base Period 2 Final Calculations
base_period_2_finals AS (
  SELECT 
    *,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_volume / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_volume
    END AS base_2_volume_final,
    CASE 
      WHEN REPLACE(REPLACE(base_2_sum_avg_method, '"', ''), '"', '') = 'average' THEN 
        ROUND(base_2_value / NULLIF(base_2_months_count, 0), 2)
      ELSE 
        base_2_value
    END AS base_2_value_final
  FROM base_period_2_sales
),

base_sales AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    COALESCE(SUM(sd.volume), 0) AS total_volume,
    COALESCE(SUM(sd.value), 0) AS total_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

actuals AS (
  SELECT
    sd.credit_account,
    MIN(sd.customer_name) AS actual_customer_name,
    MIN(sd.so_name) AS actual_so_name,
    MIN(sd.state_name) AS actual_state_name,
    COALESCE(SUM(sd.volume), 0) AS actual_volume,
    COALESCE(SUM(sd.value), 0) AS actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials))
      AND (NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories))
      AND (NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps))
      AND (NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_actual_ppi AS (
  SELECT 
    sd.credit_account,
    COUNT(DISTINCT sd.material) AS mandatory_product_actual_ppi_count
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_base_volume AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS mandatory_base_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.from_date AND bd.to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

mandatory_product_actuals AS (
  SELECT 
    sd.credit_account,
    COALESCE(SUM(sd.volume), 0) AS mandatory_actual_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- COMPREHENSIVE MANDATORY PRODUCT FILTERING - ALL CONDITIONS MUST MATCH
    AND (
      (NOT EXISTS (SELECT 1 FROM mandatory_product_materials) OR sd.material::text IN (SELECT material FROM mandatory_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_categories) OR mm.category::text IN (SELECT category FROM mandatory_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_grps) OR mm.grp::text IN (SELECT grp FROM mandatory_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM mandatory_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM mandatory_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM mandatory_product_thinner_groups))
    )
  GROUP BY sd.credit_account
),

payout_product_actuals AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS customer_name,
    MIN(sd.so_name) AS so_name,
    MIN(sd.state_name) AS state_name,
    -- Return 0 if no payout products are defined or if they overlap with main scheme
    CASE 
      WHEN NOT EXISTS (
        SELECT 1 FROM scheme, additional_scheme_index asi
        WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
      ) THEN 0
      ELSE COALESCE(SUM(sd.volume), 0)
    END AS payout_product_actual_volume,
    CASE 
      WHEN NOT EXISTS (
        SELECT 1 FROM scheme, additional_scheme_index asi
        WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
          AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
      ) THEN 0
      ELSE COALESCE(SUM(sd.value), 0)
    END AS payout_product_actual_value
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN base_dates bd
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN bd.scheme_from_date AND bd.scheme_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    -- Only include products that are specifically in additional scheme payout products
    AND sd.material::text IN (
      SELECT mat_item FROM scheme, additional_scheme_index asi,
      LATERAL jsonb_array_elements_text(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') AS mat_item
      WHERE scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials' IS NOT NULL
        AND jsonb_array_length(scheme_json->'additionalSchemes'->asi.idx->'productData'->'mainScheme'->'payoutProducts'->'materials') > 0
    )
  GROUP BY sd.credit_account
),

all_accounts AS (
  SELECT credit_account FROM base_sales
  UNION
  SELECT credit_account FROM actuals
  UNION
  SELECT credit_account FROM mandatory_product_base_volume
  UNION
  SELECT credit_account FROM mandatory_product_actuals
  UNION
  SELECT credit_account FROM mandatory_product_actual_ppi
  UNION
  SELECT credit_account FROM payout_product_actuals
  UNION
  SELECT credit_account FROM base_period_1_finals
  UNION
  SELECT credit_account FROM base_period_2_finals
),

-- Get customer information for all credit accounts (fallback for missing data)
customer_info_fallback AS (
  SELECT 
    sd.credit_account,
    MIN(sd.customer_name) AS fallback_customer_name,
    MIN(sd.so_name) AS fallback_so_name,
    MIN(sd.state_name) AS fallback_state_name
  FROM sales_data sd
  WHERE sd.credit_account IN (SELECT credit_account FROM all_accounts)
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
  GROUP BY sd.credit_account
),





slab_applied AS (
  SELECT 
    aa.credit_account,
    COALESCE(bs.customer_name, a.actual_customer_name, bp1.customer_name, bp2.customer_name, ppa.customer_name, cif.fallback_customer_name, 'Unknown') AS customer_name,
    COALESCE(bs.so_name, a.actual_so_name, bp1.so_name, bp2.so_name, ppa.so_name, cif.fallback_so_name, 'Unknown') AS so_name,
    COALESCE(bs.state_name, a.actual_state_name, bp1.state_name, bp2.state_name, ppa.state_name, cif.fallback_state_name, 'Unknown') AS state_name,
    COALESCE(bs.total_volume, 0) AS total_volume,
    COALESCE(bs.total_value, 0) AS total_value,
    COALESCE(a.actual_volume, 0) AS actual_volume,
    COALESCE(a.actual_value, 0) AS actual_value,
    COALESCE(mpbv.mandatory_base_volume, 0) AS mandatory_product_base_volume,
    COALESCE(mpa.mandatory_actual_volume, 0) AS mandatory_product_actual_volume,
    COALESCE(mpapi.mandatory_product_actual_ppi_count, 0) AS mandatory_product_actual_ppi,
    COALESCE(ppa.payout_product_actual_volume, 0) AS payout_product_actual_volume,
    COALESCE(ppa.payout_product_actual_value, 0) AS payout_product_actual_value,
    COALESCE(bp1.base_1_volume, 0) AS base_1_volume,
    COALESCE(bp1.base_1_value, 0) AS base_1_value,
    COALESCE(REPLACE(REPLACE(bp1.base_1_sum_avg_method, '"', ''), '"', ''), 'sum') AS base_1_sum_avg_method,
    COALESCE(bp1.base_1_months_count, 0) AS base_1_months,
    COALESCE(bp1.base_1_volume_final, 0) AS base_1_volume_final,
    COALESCE(bp1.base_1_value_final, 0) AS base_1_value_final,
    COALESCE(bp2.base_2_volume, 0) AS base_2_volume,
    COALESCE(bp2.base_2_value, 0) AS base_2_value,
    COALESCE(REPLACE(REPLACE(bp2.base_2_sum_avg_method, '"', ''), '"', ''), 'sum') AS base_2_sum_avg_method,
    COALESCE(bp2.base_2_months_count, 0) AS base_2_months,
    COALESCE(bp2.base_2_volume_final, 0) AS base_2_volume_final,
    COALESCE(bp2.base_2_value_final, 0) AS base_2_value_final,
    COALESCE(sl.growth_rate, fs.growth_rate) AS growth_rate,
    COALESCE(sl.qualification_rate, fs.qualification_rate) AS qualification_rate,
    COALESCE(sl.rebate_per_litre, fs.rebate_per_litre) AS rebate_per_litre_applied,
    COALESCE(sl.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre) AS additional_rebate_on_growth_per_litre_applied,
    COALESCE(sl.fixed_rebate, fs.fixed_rebate) AS fixed_rebate,
    COALESCE(sl.mandatory_product_target, fs.mandatory_product_target) AS mandatory_product_target,
    COALESCE(sl.mandatory_product_growth_percent, fs.mandatory_product_growth_percent) AS mandatory_product_growth_percent,
    COALESCE(sl.mandatory_product_target_to_actual, fs.mandatory_product_target_to_actual) AS mandatory_product_target_to_actual,
    COALESCE(sl.mandatory_product_rebate, fs.mandatory_product_rebate) AS mandatory_product_rebate,
    COALESCE(sl.mandatory_min_shades_ppi, fs.mandatory_min_shades_ppi) AS mandatory_min_shades_ppi,
    GREATEST((1 + COALESCE(sl.growth_rate, fs.growth_rate)) * COALESCE(bs.total_volume, 0), COALESCE(sl.slab_start, fs.slab_start, 0)) AS target_volume
  FROM all_accounts aa
  LEFT JOIN base_sales bs ON aa.credit_account = bs.credit_account
  LEFT JOIN actuals a ON aa.credit_account = a.credit_account
  LEFT JOIN mandatory_product_base_volume mpbv ON aa.credit_account = mpbv.credit_account
  LEFT JOIN mandatory_product_actuals mpa ON aa.credit_account = mpa.credit_account
  LEFT JOIN mandatory_product_actual_ppi mpapi ON aa.credit_account = mpapi.credit_account
  LEFT JOIN payout_product_actuals ppa ON aa.credit_account = ppa.credit_account
  LEFT JOIN base_period_1_finals bp1 ON aa.credit_account = bp1.credit_account
  LEFT JOIN base_period_2_finals bp2 ON aa.credit_account = bp2.credit_account
  LEFT JOIN customer_info_fallback cif ON aa.credit_account = cif.credit_account
  LEFT JOIN slabs sl ON COALESCE(bs.total_volume, 0) BETWEEN sl.slab_start AND sl.slab_end
  CROSS JOIN first_slab fs
),

-- Calculate payouts and other derived fields
calculated_fields AS (
  SELECT 
    *,
    CASE 
      WHEN target_volume = 0 THEN 0
      ELSE actual_volume / NULLIF(target_volume, 0)
    END AS percent_achieved,
    
    -- BASIC PAYOUT: Dynamic check for payoutProducts configuration
    CASE 
      WHEN rebate_per_litre_applied > 0 THEN 
        CASE 
          WHEN sc.payout_products_enabled = false THEN rebate_per_litre_applied * actual_volume
          ELSE rebate_per_litre_applied * payout_product_actual_volume
        END
      ELSE 0
    END AS basic_payout,
    
    -- ADDITIONAL PAYOUT: Dynamic check for payoutProducts configuration
    CASE 
      WHEN additional_rebate_on_growth_per_litre_applied > 0 
           AND actual_volume > total_volume 
           AND (CASE WHEN target_volume = 0 THEN 0 ELSE actual_volume / NULLIF(target_volume, 0) END) >= 1.0 THEN 
        CASE 
          WHEN sc.payout_products_enabled = false THEN additional_rebate_on_growth_per_litre_applied * actual_volume
          ELSE additional_rebate_on_growth_per_litre_applied * payout_product_actual_volume
        END
      ELSE 0
    END AS additional_payout,
    
    -- Mandatory product calculations
    (1 + mandatory_product_growth_percent) * mandatory_product_base_volume AS mandatory_product_target_volume,
    
    -- MP FINAL PAYOUT: Dynamic check for payoutProducts configuration
    CASE 
      WHEN mandatory_product_rebate > 0 THEN 
        CASE 
          WHEN sc.payout_products_enabled = false THEN mandatory_product_rebate * actual_volume
          ELSE mandatory_product_rebate * payout_product_actual_volume
        END
      ELSE 0
    END AS mandatory_product_payout
    
  FROM slab_applied
  CROSS JOIN scheme_config sc
),

-- Second level calculations that depend on the first level
calculated_fields_2 AS (
  SELECT 
    *,
    -- Total payout (Basic Payout + Additional Payout + Fixed Rebate + MP Final Payout)
    -- Dynamic check for payoutProducts configuration
    basic_payout + additional_payout + fixed_rebate + 
    CASE 
      WHEN (CASE 
              WHEN GREATEST(
                COALESCE(mandatory_product_target_volume, 0), 
                COALESCE(mandatory_product_target, 0), 
                COALESCE(mandatory_product_target_to_actual * target_volume, 0)
              ) = 0 THEN 0
              ELSE ROUND(
                COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
                  GREATEST(
                    COALESCE(mandatory_product_target_volume, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE(mandatory_product_target_to_actual * target_volume, 0)
                  ), 0
                ), 6
              )
            END) >= 1.0 
      THEN CASE 
             WHEN sc.payout_products_enabled = false THEN COALESCE(actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
             ELSE COALESCE(payout_product_actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
           END
      ELSE 0
    END AS total_payout,
    
    -- PAYOUT PRODUCT PAYOUT: Dynamic check for payoutProducts configuration
    -- Based on specification: Additional Rebate on Growth per Litre * (actual_volume OR payout_product_actual_volume)
    CASE 
      WHEN sc.payout_products_enabled = false THEN 
        additional_rebate_on_growth_per_litre_applied * actual_volume
      ELSE 
        additional_rebate_on_growth_per_litre_applied * payout_product_actual_volume
    END AS payout_product_payout
    
  FROM calculated_fields
  CROSS JOIN scheme_config sc
),



-- Phasing calculations (simplified without bonus schemes)
-- PHASING PERIOD ACTUALS (for each phasing period)
phasing_actuals AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.phasing_from_date AND pp.phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (EXISTS (SELECT 1 FROM product_materials) AND sd.material::text IN (SELECT material FROM product_materials))
      OR (EXISTS (SELECT 1 FROM product_categories) AND mm.category::text IN (SELECT category FROM product_categories))
      OR (EXISTS (SELECT 1 FROM product_grps) AND mm.grp::text IN (SELECT grp FROM product_grps))
      OR (EXISTS (SELECT 1 FROM product_wanda_groups) AND mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (EXISTS (SELECT 1 FROM product_thinner_groups) AND mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- PHASING PAYOUT PERIOD ACTUALS (for payout calculations)
phasing_payout_actuals AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_payout_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (EXISTS (SELECT 1 FROM product_materials) AND sd.material::text IN (SELECT material FROM product_materials))
      OR (EXISTS (SELECT 1 FROM product_categories) AND mm.category::text IN (SELECT category FROM product_categories))
      OR (EXISTS (SELECT 1 FROM product_grps) AND mm.grp::text IN (SELECT grp FROM product_grps))
      OR (EXISTS (SELECT 1 FROM product_wanda_groups) AND mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (EXISTS (SELECT 1 FROM product_thinner_groups) AND mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- PHASING PAYOUT PERIOD PAYOUT PRODUCT VOLUMES
phasing_payout_product_volumes AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS phasing_payout_period_payout_product_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.payout_from_date AND pp.payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING ACTUALS
bonus_phasing_actuals AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.bonus_phasing_from_date AND pp.bonus_phasing_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (EXISTS (SELECT 1 FROM product_materials) AND sd.material::text IN (SELECT material FROM product_materials))
      OR (EXISTS (SELECT 1 FROM product_categories) AND mm.category::text IN (SELECT category FROM product_categories))
      OR (EXISTS (SELECT 1 FROM product_grps) AND mm.grp::text IN (SELECT grp FROM product_grps))
      OR (EXISTS (SELECT 1 FROM product_wanda_groups) AND mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (EXISTS (SELECT 1 FROM product_thinner_groups) AND mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUTS
bonus_phasing_payouts AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_payout_period_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (EXISTS (SELECT 1 FROM product_materials) AND sd.material::text IN (SELECT material FROM product_materials))
      OR (EXISTS (SELECT 1 FROM product_categories) AND mm.category::text IN (SELECT category FROM product_categories))
      OR (EXISTS (SELECT 1 FROM product_grps) AND mm.grp::text IN (SELECT grp FROM product_grps))
      OR (EXISTS (SELECT 1 FROM product_wanda_groups) AND mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups))
      OR (EXISTS (SELECT 1 FROM product_thinner_groups) AND mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

-- BONUS PHASING PAYOUT PERIOD PAYOUT PRODUCT VOLUMES
bonus_phasing_payout_product_volumes AS (
  SELECT 
    sd.credit_account,
    pp.phasing_id,
    COALESCE(SUM(sd.volume), 0) AS bonus_phasing_payout_period_payout_product_volume
  FROM sales_data sd
  JOIN material_master mm ON sd.material = mm.material
  CROSS JOIN phasing_periods pp
  WHERE 
    pp.is_bonus = true
    AND TO_DATE(sd.year || '-' || sd.month || '-' || sd.day, 'YYYY-Mon-DD')
      BETWEEN pp.bonus_payout_from_date AND pp.bonus_payout_to_date
    AND (NOT EXISTS (SELECT 1 FROM states) OR sd.state_name IN (SELECT state FROM states))
    AND (NOT EXISTS (SELECT 1 FROM regions) OR sd.region_name IN (SELECT region FROM regions))
    AND (NOT EXISTS (SELECT 1 FROM area_heads) OR sd.area_head_name IN (SELECT area_head FROM area_heads))
    AND (NOT EXISTS (SELECT 1 FROM divisions) OR sd.division::text IN (SELECT division FROM divisions))
    AND (NOT EXISTS (SELECT 1 FROM dealer_types) OR sd.dealer_type IN (SELECT dealer_type FROM dealer_types))
    AND (NOT EXISTS (SELECT 1 FROM distributors) OR sd.distributor IN (SELECT distributor FROM distributors))
    AND (
      (NOT EXISTS (SELECT 1 FROM payout_product_materials) OR sd.material::text IN (SELECT material FROM payout_product_materials))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_categories) OR mm.category::text IN (SELECT category FROM payout_product_categories))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_grps) OR mm.grp::text IN (SELECT grp FROM payout_product_grps))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM payout_product_wanda_groups))
      AND (NOT EXISTS (SELECT 1 FROM payout_product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM payout_product_thinner_groups))
    )
  GROUP BY sd.credit_account, pp.phasing_id
),

phasing_calculations AS (
  SELECT 
    fc.credit_account,
    fc.customer_name,
    fc.so_name,
    fc.state_name,
    fc.total_volume,
    fc.total_value,
    fc.actual_volume,
    fc.actual_value,
    fc.mandatory_product_base_volume,
    fc.mandatory_product_actual_volume,
    fc.mandatory_product_actual_ppi,
    fc.payout_product_actual_volume,
    fc.payout_product_actual_value,
    fc.base_1_volume,
    fc.base_1_value,
    fc.base_1_sum_avg_method,
    fc.base_1_months,
    fc.base_1_volume_final,
    fc.base_1_value_final,
    fc.base_2_volume,
    fc.base_2_value,
    fc.base_2_sum_avg_method,
    fc.base_2_months,
    fc.base_2_volume_final,
    fc.base_2_value_final,
    fc.growth_rate,
    fc.qualification_rate,
    fc.rebate_per_litre_applied,
    fc.additional_rebate_on_growth_per_litre_applied,
    fc.fixed_rebate,
    fc.mandatory_product_target,
    fc.mandatory_product_growth_percent,
    fc.mandatory_product_target_to_actual,
    fc.mandatory_product_rebate,
    fc.mandatory_min_shades_ppi,
    fc.target_volume,
    fc.percent_achieved,
    fc.basic_payout,
    fc.additional_payout,
    fc.mandatory_product_target_volume,
    fc.mandatory_product_payout,
    fc.total_payout,
    fc.payout_product_payout,
    
    -- PHASING PERIOD 1 (8 regular fields + 8 bonus fields)
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN pp.phasing_id END), 0) AS "Phasing Period No 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN pp.phasing_target_percent END), 0) AS "Phasing Target % 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.phasing_target_percent * fc.target_volume, 2) END), 0) AS "Phasing Target Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN pa.phasing_period_volume END), 0) AS "Phasing Period Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN ppay.phasing_payout_period_volume END), 0) AS "Phasing Payout Period Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN pppv.phasing_payout_period_payout_product_volume END), 0) AS "Phasing Period Payout Product Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END), 0) AS "% Phasing Achieved 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN pp.rebate_value END), 0) AS "Phasing Period Rebate 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END), 0) AS "Phasing Payout 1",
    
    -- BONUS FIELDS FOR PERIOD 1
    (MAX(CASE WHEN pp.phasing_id = 1 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN ROUND(pp.bonus_phasing_target_percent * fc.target_volume, 2) END) AS "Bonus Target Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN bpa.bonus_phasing_period_volume END), 0) AS "Bonus Phasing Period Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN bppay.bonus_phasing_payout_period_volume END), 0) AS "Bonus Payout Period Volume 1",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 1 THEN bpppv.bonus_phasing_payout_period_payout_product_volume END), 0) AS "Bonus Payout Period Payout Product Volume 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 1",
    MAX(CASE WHEN pp.phasing_id = 1 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 1",
    
    -- PHASING PERIOD 2 (8 regular fields + 8 bonus fields)
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN pp.phasing_id END), 0) AS "Phasing Period No 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN pp.phasing_target_percent END), 0) AS "Phasing Target % 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.phasing_target_percent * fc.target_volume, 2) END), 0) AS "Phasing Target Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN pa.phasing_period_volume END), 0) AS "Phasing Period Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN ppay.phasing_payout_period_volume END), 0) AS "Phasing Payout Period Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN pppv.phasing_payout_period_payout_product_volume END), 0) AS "Phasing Period Payout Product Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END), 0) AS "% Phasing Achieved 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN pp.rebate_value END), 0) AS "Phasing Period Rebate 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END), 0) AS "Phasing Payout 2",
    
    -- BONUS FIELDS FOR PERIOD 2
    (MAX(CASE WHEN pp.phasing_id = 2 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN ROUND(pp.bonus_phasing_target_percent * fc.target_volume, 2) END) AS "Bonus Target Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN bpa.bonus_phasing_period_volume END), 0) AS "Bonus Phasing Period Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN bppay.bonus_phasing_payout_period_volume END), 0) AS "Bonus Payout Period Volume 2",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 2 THEN bpppv.bonus_phasing_payout_period_payout_product_volume END), 0) AS "Bonus Payout Period Payout Product Volume 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 2",
    MAX(CASE WHEN pp.phasing_id = 2 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 2",
    
    -- PHASING PERIOD 3 (8 regular fields + 8 bonus fields)
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN pp.phasing_id END), 0) AS "Phasing Period No 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN pp.phasing_target_percent END), 0) AS "Phasing Target % 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.phasing_target_percent * fc.target_volume, 2) END), 0) AS "Phasing Target Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN pa.phasing_period_volume END), 0) AS "Phasing Period Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN ppay.phasing_payout_period_volume END), 0) AS "Phasing Payout Period Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN pppv.phasing_payout_period_payout_product_volume END), 0) AS "Phasing Period Payout Product Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END), 0) AS "% Phasing Achieved 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN pp.rebate_value END), 0) AS "Phasing Period Rebate 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(pa.phasing_period_volume, 0) / NULLIF(pp.phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(ppay.phasing_payout_period_volume, 0) * pp.rebate_value, 2)
            ELSE ROUND(COALESCE(pppv.phasing_payout_period_payout_product_volume, 0) * pp.rebate_value, 2)
          END
        ELSE 0
      END
    END), 0) AS "Phasing Payout 3",
    
    -- BONUS FIELDS FOR PERIOD 3
    (MAX(CASE WHEN pp.phasing_id = 3 THEN pp.is_bonus::integer END) = 1) AS "Is Bonus 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_rebate_value END) AS "Bonus Rebate Value 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN pp.bonus_phasing_target_percent END) AS "Bonus Phasing Target % 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN ROUND(pp.bonus_phasing_target_percent * fc.target_volume, 2) END) AS "Bonus Target Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN bpa.bonus_phasing_period_volume END), 0) AS "Bonus Phasing Period Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN bppay.bonus_phasing_payout_period_volume END), 0) AS "Bonus Payout Period Volume 3",
    COALESCE(MAX(CASE WHEN pp.phasing_id = 3 THEN bpppv.bonus_phasing_payout_period_payout_product_volume END), 0) AS "Bonus Payout Period Payout Product Volume 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN pp.bonus_phasing_target_percent * fc.target_volume = 0 THEN 0
        ELSE ROUND((COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)), 6)
      END 
    END) AS "% Bonus Achieved 3",
    MAX(CASE WHEN pp.phasing_id = 3 THEN 
      CASE 
        WHEN NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0) IS NULL THEN 0
        WHEN (COALESCE(bpa.bonus_phasing_period_volume, 0) / NULLIF(pp.bonus_phasing_target_percent * fc.target_volume, 0)) >= 1 THEN 
          CASE 
            WHEN sc.payout_products_enabled = false THEN ROUND(COALESCE(bpa.bonus_phasing_period_volume, 0) * pp.bonus_rebate_value, 2)
            ELSE ROUND(COALESCE(bpppv.bonus_phasing_payout_period_payout_product_volume, 0) * pp.bonus_rebate_value, 2)
          END
        ELSE 0
      END
    END) AS "Bonus Payout 3",
    
    -- Add the missing calculated fields from final_calculations
    CASE 
      WHEN fc.mandatory_product_target_volume = 0 THEN 0
      ELSE fc.mandatory_product_actual_volume / NULLIF(fc.mandatory_product_target_volume, 0)
    END AS "Mandatory Product Growth Target Achieved",
    
    CASE 
      WHEN fc.mandatory_min_shades_ppi = 0 THEN 0
      ELSE fc.mandatory_product_actual_ppi / NULLIF(fc.mandatory_min_shades_ppi, 0)
    END AS "Mandatory Product % PPI",
    
    CASE 
      WHEN fc.mandatory_product_target = 0 THEN 0
      ELSE fc.mandatory_product_actual_volume / NULLIF(fc.mandatory_product_target, 0)
    END AS "Mandatory Product Actual Volume % to Fixed Target Sales",
    
    CASE 
      WHEN fc.actual_volume = 0 THEN 0
      ELSE fc.mandatory_product_actual_volume / NULLIF(fc.actual_volume, 0)
    END AS "Mandatory Product Actual Volume % to Total Sales",
    
    fc.mandatory_product_target_to_actual * fc.target_volume AS "Mandatory Product % to Actual - Target Volume",
    
    -- Add scheme config fields for use in final_output
    sc.payout_products_enabled
    
  FROM calculated_fields_2 fc
  CROSS JOIN scheme_config sc
  LEFT JOIN phasing_periods pp ON 1=1  -- Cross join to get all phasing periods
  LEFT JOIN phasing_actuals pa ON fc.credit_account = pa.credit_account AND pp.phasing_id = pa.phasing_id
  LEFT JOIN phasing_payout_actuals ppay ON fc.credit_account = ppay.credit_account AND pp.phasing_id = ppay.phasing_id
  LEFT JOIN phasing_payout_product_volumes pppv ON fc.credit_account = pppv.credit_account AND pp.phasing_id = pppv.phasing_id
  LEFT JOIN bonus_phasing_actuals bpa ON fc.credit_account = bpa.credit_account AND pp.phasing_id = bpa.phasing_id
  LEFT JOIN bonus_phasing_payouts bppay ON fc.credit_account = bppay.credit_account AND pp.phasing_id = bppay.phasing_id
  LEFT JOIN bonus_phasing_payout_product_volumes bpppv ON fc.credit_account = bpppv.credit_account AND pp.phasing_id = bpppv.phasing_id
  GROUP BY fc.credit_account, fc.customer_name, fc.so_name, fc.state_name, fc.total_volume, fc.total_value, 
           fc.actual_volume, fc.actual_value, fc.mandatory_product_base_volume, fc.mandatory_product_actual_volume,
           fc.mandatory_product_actual_ppi, fc.payout_product_actual_volume, fc.payout_product_actual_value,
           fc.base_1_volume, fc.base_1_value, fc.base_1_sum_avg_method, fc.base_1_months, fc.base_1_volume_final, fc.base_1_value_final,
           fc.base_2_volume, fc.base_2_value, fc.base_2_sum_avg_method, fc.base_2_months, fc.base_2_volume_final, fc.base_2_value_final,
           fc.growth_rate, fc.qualification_rate, fc.rebate_per_litre_applied, fc.additional_rebate_on_growth_per_litre_applied,
           fc.fixed_rebate, fc.mandatory_product_target, fc.mandatory_product_growth_percent, fc.mandatory_product_target_to_actual,
           fc.mandatory_product_rebate, fc.mandatory_min_shades_ppi, fc.target_volume, fc.percent_achieved, fc.basic_payout,
           fc.additional_payout, fc.total_payout, fc.payout_product_payout, fc.mandatory_product_target_volume, fc.mandatory_product_payout,
           sc.payout_products_enabled
),

-- Final output without bonus schemes
final_output AS (
  SELECT 
    credit_account,
    customer_name,
    so_name,
    state_name,
    -- Base Period Columns
    base_1_volume AS "Base 1 Volume",
    base_1_value AS "Base 1 Value", 
    base_1_sum_avg_method AS "Base 1 SumAvg",
    base_1_months AS "Base 1 Months",
    base_1_volume_final AS "Base 1 Volume Final",
    base_1_value_final AS "Base 1 Value Final",
    base_2_volume AS "Base 2 Volume",
    base_2_value AS "Base 2 Value",
    base_2_sum_avg_method AS "Base 2 SumAvg",
    base_2_months AS "Base 2 Months",
    base_2_volume_final AS "Base 2 Volume Final",
    base_2_value_final AS "Base 2 Value Final",
    total_volume,
    total_value,
    actual_volume,
    actual_value,
    mandatory_product_base_volume AS "Mandatory Product Base Volume",
    mandatory_product_actual_volume AS "Mandatory Product Actual Volume",
    mandatory_product_target_volume AS "Mandatory Product Growth Target Volume",
    mandatory_product_payout AS "Mandatory Product Payout",
    mandatory_product_actual_ppi AS "Mandatory product actual PPI",
    growth_rate,
    target_volume,
    basic_payout,
    additional_payout,
    percent_achieved,
    fixed_rebate,
    rebate_per_litre_applied AS "Rebate per Litre",
    additional_rebate_on_growth_per_litre_applied AS "Additional Rebate on Growth per Litre",
    mandatory_product_target AS "Mandatory Product Fixed Target",
    mandatory_product_growth_percent AS "Mandatory Product Growth",
    mandatory_product_target_to_actual AS "Mandatory Product % Target to Actual Sales",
    mandatory_product_rebate AS "Mandatory Product Rebate",
    mandatory_min_shades_ppi AS "Mandatory Min. Shades - PPI",
    total_payout,
    payout_product_payout AS "Payout Product Payout",
    "Mandatory Product % to Actual - Target Volume",
    "Mandatory Product Actual Volume % to Total Sales",
    "Mandatory Product Actual Volume % to Fixed Target Sales",
    "Mandatory Product Growth Target Achieved",
    "Mandatory Product % PPI",
    payout_product_actual_volume AS "Payout Products Volume",
    payout_product_actual_value AS "Payout Products Value",
    
    -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD
    "Phasing Period No 1", "Phasing Target % 1", "Phasing Target Volume 1", "Phasing Period Volume 1", 
    "Phasing Payout Period Volume 1", "Phasing Period Payout Product Volume 1", "% Phasing Achieved 1", "Phasing Period Rebate 1", "Phasing Payout 1",
    "Is Bonus 1", "Bonus Rebate Value 1", "Bonus Phasing Target % 1", "Bonus Target Volume 1", "Bonus Phasing Period Volume 1",
    "Bonus Payout Period Volume 1", "Bonus Payout Period Payout Product Volume 1", "% Bonus Achieved 1", "Bonus Payout 1",
    
    "Phasing Period No 2", "Phasing Target % 2", "Phasing Target Volume 2", "Phasing Period Volume 2",
    "Phasing Payout Period Volume 2", "Phasing Period Payout Product Volume 2", "% Phasing Achieved 2", "Phasing Period Rebate 2", "Phasing Payout 2",
    "Is Bonus 2", "Bonus Rebate Value 2", "Bonus Phasing Target % 2", "Bonus Target Volume 2", "Bonus Phasing Period Volume 2",
    "Bonus Payout Period Volume 2", "Bonus Payout Period Payout Product Volume 2", "% Bonus Achieved 2", "Bonus Payout 2",
    
    "Phasing Period No 3", "Phasing Target % 3", "Phasing Target Volume 3", "Phasing Period Volume 3",
    "Phasing Payout Period Volume 3", "Phasing Period Payout Product Volume 3", "% Phasing Achieved 3", "Phasing Period Rebate 3", "Phasing Payout 3",
    "Is Bonus 3", "Bonus Rebate Value 3", "Bonus Phasing Target % 3", "Bonus Target Volume 3", "Bonus Phasing Period Volume 3",
    "Bonus Payout Period Volume 3", "Bonus Payout Period Payout Product Volume 3", "% Bonus Achieved 3", "Bonus Payout 3",

    -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)**
    GREATEST(
      COALESCE(mandatory_product_target_volume, 0), 
      COALESCE(mandatory_product_target, 0), 
      COALESCE("Mandatory Product % to Actual - Target Volume", 0)
    ) AS "MP FINAL TARGET",
    
    CASE 
      WHEN GREATEST(
        COALESCE(mandatory_product_target_volume, 0), 
        COALESCE(mandatory_product_target, 0), 
        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
      ) = 0 THEN 0
      ELSE ROUND(
        COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
          GREATEST(
            COALESCE(mandatory_product_target_volume, 0), 
            COALESCE(mandatory_product_target, 0), 
            COALESCE("Mandatory Product % to Actual - Target Volume", 0)
          ), 0
        ), 6
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN: MP FINAL PAYOUT** - Updated with payout products logic
    CASE 
      WHEN payout_products_enabled = false THEN
        CASE 
          WHEN (CASE 
                  WHEN GREATEST(
                    COALESCE(mandatory_product_target_volume, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                  ) = 0 THEN 0
                  ELSE ROUND(
                    COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
                      GREATEST(
                        COALESCE(mandatory_product_target_volume, 0), 
                        COALESCE(mandatory_product_target, 0), 
                        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                      ), 0
                    ), 6
                  )
                END) >= 1.0 
          THEN COALESCE(actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
          ELSE 0
        END
      ELSE
        CASE 
          WHEN (CASE 
                  WHEN GREATEST(
                    COALESCE(mandatory_product_target_volume, 0), 
                    COALESCE(mandatory_product_target, 0), 
                    COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                  ) = 0 THEN 0
                  ELSE ROUND(
                    COALESCE(mandatory_product_actual_volume, 0) / NULLIF(
                      GREATEST(
                        COALESCE(mandatory_product_target_volume, 0), 
                        COALESCE(mandatory_product_target, 0), 
                        COALESCE("Mandatory Product % to Actual - Target Volume", 0)
                      ), 0
                    ), 6
                  )
                END) >= 1.0 
          THEN COALESCE(payout_product_actual_volume, 0) * COALESCE(mandatory_product_rebate, 0)
          ELSE 0
        END
    END AS "MP Final Payout"
    
  FROM phasing_calculations
),

unioned AS (
  SELECT *, 0 AS is_grand_total FROM final_output
  UNION ALL
  SELECT
    'GRAND TOTAL',
    NULL, NULL, NULL,
    -- Base Period Grand Totals
    COALESCE(SUM("Base 1 Volume"), 0),
    COALESCE(SUM("Base 1 Value"), 0),
    NULL, -- Base 1 SumAvg
    NULL, -- Base 1 Months (no meaningful total)
    COALESCE(SUM("Base 1 Volume Final"), 0),
    COALESCE(SUM("Base 1 Value Final"), 0),
    COALESCE(SUM("Base 2 Volume"), 0),  
    COALESCE(SUM("Base 2 Value"), 0),
    NULL, -- Base 2 SumAvg
    NULL, -- Base 2 Months (no meaningful total)
    COALESCE(SUM("Base 2 Volume Final"), 0),
    COALESCE(SUM("Base 2 Value Final"), 0),
    COALESCE(SUM(total_volume), 0),
    COALESCE(SUM(total_value), 0),
    COALESCE(SUM(actual_volume), 0),
    COALESCE(SUM(actual_value), 0),
    COALESCE(SUM("Mandatory Product Base Volume"), 0),
    COALESCE(SUM("Mandatory Product Actual Volume"), 0),
    COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
    COALESCE(SUM("Mandatory Product Payout"), 0),
    COALESCE(SUM("Mandatory product actual PPI"), 0),
    NULL,
    COALESCE(SUM(target_volume), 0),
    COALESCE(SUM(basic_payout), 0),
    COALESCE(SUM(additional_payout), 0),
    CASE WHEN COALESCE(SUM(target_volume), 0) = 0 THEN 0 ELSE COALESCE(SUM(actual_volume), 0) / NULLIF(SUM(target_volume), 0) END AS percent_achieved,
    COALESCE(SUM(fixed_rebate), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Fixed Target"), 0),
    NULL,
    NULL,
    COALESCE(SUM("Mandatory Product Rebate"), 0),
    COALESCE(SUM("Mandatory Min. Shades - PPI"), 0),
    COALESCE(SUM(total_payout), 0),
    COALESCE(SUM("Payout Product Payout"), 0),
    COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0),
    CASE WHEN COALESCE(SUM(actual_volume), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM(actual_volume), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Fixed Target"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM("Mandatory Product Fixed Target"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Product Growth Target Volume"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(SUM("Mandatory Product Growth Target Volume"), 0) END,
    CASE WHEN COALESCE(SUM("Mandatory Min. Shades - PPI"), 0) = 0 THEN 0 ELSE COALESCE(SUM("Mandatory product actual PPI"), 0) / NULLIF(SUM("Mandatory Min. Shades - PPI"), 0) END,
    COALESCE(SUM("Payout Products Volume"), 0),
    COALESCE(SUM("Payout Products Value"), 0),
    
    -- Phasing Grand Totals (all 54 phasing fields - 18 per period × 3 periods)
    -- Period 1 (18 fields)
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 1"), 0), COALESCE(SUM("Phasing Period Volume 1"), 0), 
    COALESCE(SUM("Phasing Payout Period Volume 1"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 1"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 1"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,
    -- Period 2 (18 fields)  
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 2"), 0), COALESCE(SUM("Phasing Period Volume 2"), 0),
    COALESCE(SUM("Phasing Payout Period Volume 2"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 2"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 2"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,
    -- Period 3 (18 fields)
    NULL, NULL, COALESCE(SUM("Phasing Target Volume 3"), 0), COALESCE(SUM("Phasing Period Volume 3"), 0),
    COALESCE(SUM("Phasing Payout Period Volume 3"), 0), COALESCE(SUM("Phasing Period Payout Product Volume 3"), 0), NULL, NULL, COALESCE(SUM("Phasing Payout 3"), 0),
    false, 0, 0, 0, 0, 0, 0, 0, 0,

    -- **NEW COLUMNS GRAND TOTALS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT %**
    GREATEST(
      COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
      COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
      COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
    ) AS "MP FINAL TARGET",
    CASE 
      WHEN GREATEST(
        COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
        COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
        COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
      ) = 0 THEN 0
      ELSE COALESCE(SUM("Mandatory Product Actual Volume"), 0) / NULLIF(
        GREATEST(
          COALESCE(SUM("Mandatory Product Growth Target Volume"), 0),
          COALESCE(SUM("Mandatory Product Fixed Target"), 0), 
          COALESCE(SUM("Mandatory Product % to Actual - Target Volume"), 0)
        ), 0
      )
    END AS "MP FINAL ACHEIVMENT %",

    -- **NEW COLUMN GRAND TOTAL: MP FINAL PAYOUT**
    COALESCE(SUM("MP Final Payout"), 0) AS "MP Final Payout",
    
    1 AS is_grand_total
  FROM final_output
)

-- FINAL SELECT WITH CLEAN STRUCTURE (NO BONUS SCHEMES)
SELECT 
  u.state_name,
  u.credit_account,
  u.customer_name,
  u.so_name,
  u."Base 1 Volume", 
  u."Base 1 Value", 
  u."Base 1 SumAvg",
  u."Base 1 Months",
  u."Base 1 Volume Final",
  u."Base 1 Value Final",
  u."Base 2 Volume",
  u."Base 2 Value",
  u."Base 2 SumAvg",
  u."Base 2 Months",
  u."Base 2 Volume Final",
  u."Base 2 Value Final",
  u.total_volume,
  u.total_value,
  u.growth_rate,
  u.target_volume,
  u.actual_volume,
  u.actual_value,
  u.percent_achieved AS "% Achieved",
  u."Rebate per Litre",
  u.basic_payout,
  u."Additional Rebate on Growth per Litre",
  u.additional_payout,
  u.fixed_rebate AS "Fixed Rebate",
  u.total_payout AS "Total Payout",
  u."Payout Product Payout",
  u."Mandatory Product Base Volume",
  u."Mandatory Product Growth",
  u."Mandatory Product Growth Target Volume",
  u."Mandatory Product Actual Volume",
  u."Mandatory Product Growth Target Achieved",
  u."Mandatory Min. Shades - PPI",
  u."Mandatory product actual PPI",
  u."Mandatory Product % PPI",
  u."Mandatory Product Fixed Target",
  u."Mandatory Product Actual Volume % to Fixed Target Sales",
  u."Mandatory Product % Target to Actual Sales",
  u."Mandatory Product % to Actual - Target Volume",
  u."Mandatory Product Actual Volume % to Total Sales",
  u."Mandatory Product Rebate",
  u."Mandatory Product Payout",
  u."Payout Products Volume",
  u."Payout Products Value",
  
  -- REGULAR PHASING FIELDS WITH BONUS FIELDS FOR EACH PERIOD
  u."Phasing Period No 1", u."Phasing Target % 1", u."Phasing Target Volume 1", u."Phasing Period Volume 1", 
  u."Phasing Payout Period Volume 1", u."Phasing Period Payout Product Volume 1", u."% Phasing Achieved 1", u."Phasing Period Rebate 1", u."Phasing Payout 1",
  u."Is Bonus 1", u."Bonus Rebate Value 1", u."Bonus Phasing Target % 1", u."Bonus Target Volume 1", u."Bonus Phasing Period Volume 1",
  u."Bonus Payout Period Volume 1", u."Bonus Payout Period Payout Product Volume 1", u."% Bonus Achieved 1", u."Bonus Payout 1",
  
  u."Phasing Period No 2", u."Phasing Target % 2", u."Phasing Target Volume 2", u."Phasing Period Volume 2",
  u."Phasing Payout Period Volume 2", u."Phasing Period Payout Product Volume 2", u."% Phasing Achieved 2", u."Phasing Period Rebate 2", u."Phasing Payout 2",
  u."Is Bonus 2", u."Bonus Rebate Value 2", u."Bonus Phasing Target % 2", u."Bonus Target Volume 2", u."Bonus Phasing Period Volume 2",
  u."Bonus Payout Period Volume 2", u."Bonus Payout Period Payout Product Volume 2", u."% Bonus Achieved 2", u."Bonus Payout 2",
  
  u."Phasing Period No 3", u."Phasing Target % 3", u."Phasing Target Volume 3", u."Phasing Period Volume 3",
  u."Phasing Payout Period Volume 3", u."Phasing Period Payout Product Volume 3", u."% Phasing Achieved 3", u."Phasing Period Rebate 3", u."Phasing Payout 3",
  u."Is Bonus 3", u."Bonus Rebate Value 3", u."Bonus Phasing Target % 3", u."Bonus Target Volume 3", u."Bonus Phasing Period Volume 3",
  u."Bonus Payout Period Volume 3", u."Bonus Payout Period Payout Product Volume 3", u."% Bonus Achieved 3", u."Bonus Payout 3",

  -- **NEW COLUMNS: MP FINAL TARGET AND MP FINAL ACHIEVEMENT % (Added at the end)**
  u."MP FINAL TARGET",
  u."MP FINAL ACHEIVMENT %",
  u."MP Final Payout"
  
FROM unioned u
ORDER BY u.is_grand_total, u.credit_account;
"""