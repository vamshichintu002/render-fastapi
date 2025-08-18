CREATE OR REPLACE FUNCTION costing_sheet_volume_additionalscheme(p_scheme_id TEXT, p_scheme_index INTEGER)
RETURNS TABLE (
  additional_scheme_index INTEGER,
  scheme_number TEXT,
  credit_account TEXT,
  customer_name TEXT,
  so_name TEXT,
  state_name TEXT,
  "Base 1 Volume" NUMERIC,
  "Base 1 Value" NUMERIC,
  "Base 1 SumAvg" TEXT,
  "Base 1 Months" NUMERIC,
  "Base 1 Volume Final" NUMERIC,
  "Base 1 Value Final" NUMERIC,
  "Base 2 Volume" NUMERIC,
  "Base 2 Value" NUMERIC,
  "Base 2 SumAvg" TEXT,
  "Base 2 Months" NUMERIC,
  "Base 2 Volume Final" NUMERIC,
  "Base 2 Value Final" NUMERIC,
  "Final Base Volume" NUMERIC,
  "Final Base Value" NUMERIC,
  growth_rate NUMERIC,
  qualification_rate NUMERIC,
  target_volume NUMERIC,
  "Estimated Volume" NUMERIC,
  "Estimated Qualifiers" NUMERIC,
  basic_payout NUMERIC,
  "Estimated Base Payout" NUMERIC,
  spent_per_liter NUMERIC,
  "Estimated Value" NUMERIC,
  percent_growth_planned NUMERIC,
  percent_spent NUMERIC,
  "Rebate per Litre" NUMERIC,
  "Additional Rebate on Growth per Litre" NUMERIC,
  "Rebate Percent" NUMERIC
)
LANGUAGE SQL
AS $$
WITH global_config AS (
  SELECT p_scheme_index as scheme_index  -- Global scheme index variable
),
scheme AS (
  SELECT scheme_json
  FROM schemes_data
  WHERE scheme_id::text = p_scheme_id
),
additional_scheme_data AS (
  SELECT 
    gc.scheme_index,
    scheme_json->'additionalSchemes'->gc.scheme_index as additional_scheme,
    scheme_json->'additionalSchemes'->gc.scheme_index->>'schemeNumber' as scheme_number
  FROM scheme
  CROSS JOIN global_config gc
  WHERE jsonb_array_length(scheme_json->'additionalSchemes') > gc.scheme_index  -- Ensure additional scheme exists at index
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
slabs AS (
  SELECT 
    (slab->>'slabStart')::NUMERIC AS slab_start,
    (slab->>'slabEnd')::NUMERIC AS slab_end,
    CASE WHEN slab->>'growthPercent' = '' OR slab->>'growthPercent' IS NULL THEN 0 ELSE (slab->>'growthPercent')::NUMERIC / 100.0 END AS growth_rate,
    CASE WHEN slab->>'dealerMayQualifyPercent' = '' OR slab->>'dealerMayQualifyPercent' IS NULL THEN 0 ELSE (slab->>'dealerMayQualifyPercent')::NUMERIC / 100.0 END AS qualification_rate,
    CASE WHEN slab->>'rebatePerLitre' = '' OR slab->>'rebatePerLitre' IS NULL THEN 0 ELSE (slab->>'rebatePerLitre')::NUMERIC END AS rebate_per_litre,
    CASE WHEN slab->>'additionalRebateOnGrowth' = '' OR slab->>'additionalRebateOnGrowth' IS NULL THEN 0 ELSE (slab->>'additionalRebateOnGrowth')::NUMERIC END AS additional_rebate_on_growth_per_litre,
    -- Handle rebatePercent field from new structure
    CASE WHEN slab->>'rebatePercent' = '' OR slab->>'rebatePercent' IS NULL THEN 0 ELSE (slab->>'rebatePercent')::NUMERIC / 100.0 END AS rebate_percent,
    row_number() OVER (ORDER BY (slab->>'slabStart')::NUMERIC) AS slab_order
  FROM additional_scheme_data,
  LATERAL jsonb_array_elements(additional_scheme->'slabData'->'mainScheme'->'slabs') AS slab
  WHERE slab->>'slabStart' != '' AND slab->>'slabStart' IS NOT NULL 
    AND slab->>'slabEnd' != '' AND slab->>'slabEnd' IS NOT NULL
),
first_slab AS (
  SELECT slab_start, growth_rate, qualification_rate, rebate_per_litre, additional_rebate_on_growth_per_litre, rebate_percent
  FROM slabs
  WHERE slab_order = 1
),
-- Get product filters from additional scheme
product_materials AS (
  SELECT jsonb_array_elements_text(additional_scheme->'productData'->'mainScheme'->'materials') AS material 
  FROM additional_scheme_data
),
product_categories AS (
  SELECT jsonb_array_elements_text(additional_scheme->'productData'->'mainScheme'->'categories') AS category 
  FROM additional_scheme_data
),
product_grps AS (
  SELECT jsonb_array_elements_text(additional_scheme->'productData'->'mainScheme'->'grps') AS grp 
  FROM additional_scheme_data
),
product_wanda_groups AS (
  SELECT jsonb_array_elements_text(additional_scheme->'productData'->'mainScheme'->'wandaGroups') AS wanda_group 
  FROM additional_scheme_data
),
product_thinner_groups AS (
  SELECT jsonb_array_elements_text(additional_scheme->'productData'->'mainScheme'->'thinnerGroups') AS thinner_group 
  FROM additional_scheme_data
),
-- Use main scheme's applicability filters (states, regions, etc.)
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
    SUM(sd.volume) AS total_volume,
    SUM(sd.value) AS total_value
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
    -- Use additional scheme's product filters
    AND (
      NOT EXISTS (SELECT 1 FROM product_materials) OR sd.material::text IN (SELECT material FROM product_materials)
    )
    AND (
      NOT EXISTS (SELECT 1 FROM product_categories) OR mm.category::text IN (SELECT category FROM product_categories)
    )
    AND (
      NOT EXISTS (SELECT 1 FROM product_grps) OR mm.grp::text IN (SELECT grp FROM product_grps)
    )
    AND (
      NOT EXISTS (SELECT 1 FROM product_wanda_groups) OR mm.wanda_group::text IN (SELECT wanda_group FROM product_wanda_groups)
    )
    AND (
      NOT EXISTS (SELECT 1 FROM product_thinner_groups) OR mm.thinner_group::text IN (SELECT thinner_group FROM product_thinner_groups)
    )
  GROUP BY sd.credit_account
),
-- Create all_accounts CTE to ensure we capture all credit accounts
all_accounts AS (
  SELECT credit_account FROM base_sales
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
    COALESCE(bs.customer_name, bp1.customer_name, bp2.customer_name, cif.fallback_customer_name, 'Unknown') AS customer_name,
    COALESCE(bs.so_name, bp1.so_name, bp2.so_name, cif.fallback_so_name, 'Unknown') AS so_name,
    COALESCE(bs.state_name, bp1.state_name, bp2.state_name, cif.fallback_state_name, 'Unknown') AS state_name,
    asd.scheme_number,
    asd.scheme_index as additional_scheme_index,
    COALESCE(bs.total_volume, 0) AS total_volume,
    COALESCE(bs.total_value, 0) AS total_value,
    -- Base Period 1 data
    COALESCE(bp1.base_1_volume, 0) AS base_1_volume,
    COALESCE(bp1.base_1_value, 0) AS base_1_value,
    COALESCE(REPLACE(REPLACE(bp1.base_1_sum_avg_method, '"', ''), '"', ''), 'sum') AS base_1_sum_avg_method,
    COALESCE(bp1.base_1_months_count, 0) AS base_1_months,
    COALESCE(bp1.base_1_volume_final, 0) AS base_1_volume_final,
    COALESCE(bp1.base_1_value_final, 0) AS base_1_value_final,
    -- Base Period 2 data
    COALESCE(bp2.base_2_volume, 0) AS base_2_volume,
    COALESCE(bp2.base_2_value, 0) AS base_2_value,
    COALESCE(REPLACE(REPLACE(bp2.base_2_sum_avg_method, '"', ''), '"', ''), 'sum') AS base_2_sum_avg_method,
    COALESCE(bp2.base_2_months_count, 0) AS base_2_months,
    COALESCE(bp2.base_2_volume_final, 0) AS base_2_volume_final,
    COALESCE(bp2.base_2_value_final, 0) AS base_2_value_final,
    -- Slab calculations
    COALESCE(sl_base.growth_rate, fs.growth_rate) AS growth_rate,
    COALESCE(sl_base.qualification_rate, fs.qualification_rate) AS qualification_rate,
    COALESCE(sl_base.rebate_per_litre, fs.rebate_per_litre, 0) AS rebate_per_litre_applied,
    COALESCE(sl_base.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) AS additional_rebate_on_growth_per_litre_applied,
    COALESCE(sl_base.rebate_percent, fs.rebate_percent, 0) AS rebate_percent_applied,
    -- Target volume based on base period data
    CASE 
      WHEN COALESCE(bs.total_volume, 0) = 0 THEN fs.slab_start
      ELSE GREATEST((1 + COALESCE(sl_base.growth_rate, fs.growth_rate)) * COALESCE(bs.total_volume, 0), COALESCE(sl_base.slab_start, fs.slab_start))
    END AS target_volume,
    -- Estimated qualifiers
    (
      CASE 
        WHEN COALESCE(bs.total_volume, 0) = 0 THEN fs.qualification_rate * 1
        ELSE COALESCE(sl_base.qualification_rate, fs.qualification_rate) * 1
      END
    ) AS estimated_qualifiers,
    -- Estimated volume
    CASE 
      WHEN COALESCE(bs.total_volume, 0) = 0 THEN fs.qualification_rate * fs.slab_start
      ELSE COALESCE(sl_base.qualification_rate, fs.qualification_rate) *
           GREATEST((1 + COALESCE(sl_base.growth_rate, fs.growth_rate)) * COALESCE(bs.total_volume, 0), COALESCE(sl_base.slab_start, fs.slab_start))
    END AS estimated_volume,
    -- Basic payout: Handle both per litre and percentage based rebates
    CASE 
      WHEN COALESCE(sl_base.rebate_per_litre, fs.rebate_per_litre, 0) > 0 THEN
        COALESCE(sl_base.rebate_per_litre, fs.rebate_per_litre, 0) * COALESCE(bs.total_volume, 0)
      ELSE
        COALESCE(sl_base.rebate_percent, fs.rebate_percent, 0) * COALESCE(bs.total_value, 0)
    END AS basic_payout,
    -- Estimated basic payout: Handle both per litre and percentage based rebates
    CASE 
      WHEN COALESCE(sl_base.rebate_per_litre, fs.rebate_per_litre, 0) > 0 THEN
        (COALESCE(sl_base.additional_rebate_on_growth_per_litre, fs.additional_rebate_on_growth_per_litre, 0) + 
         COALESCE(sl_base.rebate_per_litre, fs.rebate_per_litre, 0)) *
          (
            CASE 
              WHEN COALESCE(bs.total_volume, 0) = 0 THEN fs.qualification_rate * fs.slab_start
              ELSE COALESCE(sl_base.qualification_rate, fs.qualification_rate) *
                   GREATEST((1 + COALESCE(sl_base.growth_rate, fs.growth_rate)) * COALESCE(bs.total_volume, 0), COALESCE(sl_base.slab_start, fs.slab_start))
            END
          )
      ELSE
        COALESCE(sl_base.qualification_rate, fs.qualification_rate) *
        (
          CASE 
            WHEN COALESCE(bs.total_volume, 0) = 0 THEN fs.slab_start
            ELSE GREATEST((1 + COALESCE(sl_base.growth_rate, fs.growth_rate)) * COALESCE(bs.total_volume, 0), COALESCE(sl_base.slab_start, fs.slab_start))
          END
        ) *
        (COALESCE(bs.total_value, 0) / NULLIF(COALESCE(bs.total_volume, 0), 0))
    END AS estimated_basic_payout
  FROM all_accounts aa
  LEFT JOIN base_sales bs ON aa.credit_account = bs.credit_account
  LEFT JOIN base_period_1_finals bp1 ON aa.credit_account = bp1.credit_account
  LEFT JOIN base_period_2_finals bp2 ON aa.credit_account = bp2.credit_account
  LEFT JOIN customer_info_fallback cif ON aa.credit_account = cif.credit_account
  LEFT JOIN slabs sl_base ON COALESCE(bs.total_volume, 0) BETWEEN sl_base.slab_start AND sl_base.slab_end
  CROSS JOIN first_slab fs
  CROSS JOIN additional_scheme_data asd
),
final_output AS (
  SELECT 
    additional_scheme_index,
    scheme_number,
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
    total_volume AS "Final Base Volume",
    total_value AS "Final Base Value",
    growth_rate,
    qualification_rate,
    target_volume,
    estimated_volume AS "Estimated Volume",
    estimated_qualifiers AS "Estimated Qualifiers",
    basic_payout,
    estimated_basic_payout AS "Estimated Base Payout",
    CASE WHEN estimated_volume = 0 THEN 0 ELSE estimated_basic_payout / estimated_volume END AS spent_per_liter,
    CASE WHEN total_volume = 0 THEN 0 ELSE estimated_volume * (total_value / total_volume) END AS "Estimated Value",
    CASE WHEN total_volume = 0 THEN 0 ELSE (estimated_volume / total_volume) - 1 END AS percent_growth_planned,
    CASE WHEN (estimated_volume = 0 OR total_volume = 0) THEN 0 ELSE (estimated_basic_payout / (estimated_volume * (total_value / total_volume))) END AS percent_spent,
    rebate_per_litre_applied AS "Rebate per Litre",
    additional_rebate_on_growth_per_litre_applied AS "Additional Rebate on Growth per Litre",
    rebate_percent_applied AS "Rebate Percent"
  FROM slab_applied
),
unioned AS (
  SELECT *, 0 AS is_grand_total FROM final_output
  UNION ALL
  SELECT
    MIN(additional_scheme_index),
    MIN(scheme_number),
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
    COALESCE(SUM("Final Base Volume"), 0),
    COALESCE(SUM("Final Base Value"), 0),
    NULL, -- growth_rate
    NULL, -- qualification_rate
    COALESCE(SUM(target_volume), 0),
    COALESCE(SUM("Estimated Volume"), 0),
    COALESCE(SUM("Estimated Qualifiers"), 0),
    COALESCE(SUM(basic_payout), 0),
    COALESCE(SUM("Estimated Base Payout"), 0),
    CASE WHEN SUM("Estimated Volume") = 0 THEN 0 ELSE SUM("Estimated Base Payout") / SUM("Estimated Volume") END AS spent_per_liter,
    CASE WHEN SUM("Final Base Volume") = 0 THEN 0 ELSE SUM("Estimated Volume") * (SUM("Final Base Value") / SUM("Final Base Volume")) END AS "Estimated Value",
    CASE WHEN SUM("Final Base Volume") = 0 THEN 0 ELSE (SUM("Estimated Volume") / SUM("Final Base Volume")) - 1 END AS percent_growth_planned,
    CASE WHEN (SUM("Estimated Volume") = 0 OR SUM("Final Base Volume") = 0) THEN 0 ELSE SUM("Estimated Base Payout") / (SUM("Estimated Volume") * (SUM("Final Base Value") / SUM("Final Base Volume"))) END AS percent_spent,
    NULL,
    NULL,
    NULL,
    1 AS is_grand_total
  FROM final_output
)
SELECT 
  additional_scheme_index,
  scheme_number,
  credit_account,
  customer_name,
  so_name,
  state_name,
  -- Base Period Columns
  "Base 1 Volume",
  "Base 1 Value", 
  "Base 1 SumAvg",
  "Base 1 Months",
  "Base 1 Volume Final",
  "Base 1 Value Final",
  "Base 2 Volume",
  "Base 2 Value",
  "Base 2 SumAvg",
  "Base 2 Months",
  "Base 2 Volume Final",
  "Base 2 Value Final",
  "Final Base Volume",
  "Final Base Value",
  growth_rate,
  qualification_rate,
  target_volume,
  "Estimated Volume",
  "Estimated Qualifiers",
  basic_payout,
  "Estimated Base Payout",
  spent_per_liter,
  "Estimated Value",
  percent_growth_planned,
  percent_spent,
  "Rebate per Litre",
  "Additional Rebate on Growth per Litre",
  "Rebate Percent"
FROM unioned
ORDER BY is_grand_total, credit_account;
$$;