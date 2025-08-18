# Complete JSON Schema Structure Analysis

## Overview
This JSON represents a comprehensive scheme management system with one main scheme and multiple additional schemes (0 to N). Each scheme contains detailed product data, slabs, phasing periods, bonus structures, and various configuration options.

## Root Level Structure

```json
{
  "basicInfo": { ... },           // Basic scheme metadata
  "createdAt": "...",             // Creation timestamp
  "updatedAt": "...",             // Update timestamp
  "mainScheme": { ... },          // Primary scheme definition
  "configuration": { ... },       // Global configuration settings
  "additionalSchemes": [ ... ]    // Array of additional schemes (0 to N)
}
```

## 1. Basic Info Section
```json
"basicInfo": {
  "createdBy": "uuid",
  "schemeType": "ho-scheme",
  "schemeTitle": "Scheme Name",
  "schemeNumber": "Scheme Number/Code",
  "schemeDescription": "Detailed description"
}
```

## 2. Main Scheme Structure

### 2.1 Main Scheme Core Components
```json
"mainScheme": {
  "slabData": {
    "slabs": [
      {
        "id": 1,
        "slabEnd": "100000",
        "slabStart": "50000",
        "fixedRebate": "",
        "growthPercent": "5",
        "rebatePercent": "1",
        "rebatePerLitre": "",
        "mandatoryMinShadesPPI": "",
        "mandatoryProductRebate": "",
        "mandatoryProductTarget": "",
        "dealerMayQualifyPercent": "70",
        "additionalRebateOnGrowth": "",
        "mandatoryProductGrowthPercent": "10",
        "mandatoryProductRebatePercent": "1",
        "mandatoryProductTargetToActual": ""
      }
    ],
    "enableStrataGrowth": false
  },
  "schemeBase": "target-based",
  "productData": {
    "grps": ["DT", "MA", "MB", ...],           // Product groups
    "skus": [...],                             // SKU codes
    "materials": [...],                        // Material codes
    "categories": [...],                       // Product categories
    "otherGroups": [...],                     // Other groupings
    "thinnerGroups": [...],                   // Thinner groups
    "payoutProducts": {                       // Products eligible for payout
      "grps": [...],
      "skus": [...],
      "materials": [...],
      "categories": [...],
      "otherGroups": [...]
    },
    "mandatoryProducts": {                    // Mandatory products
      "grps": [...],
      "skus": [...],
      "materials": [...],
      "categories": [...],
      "otherGroups": [...]
    }
  }
}
```

### 2.2 Phasing Periods Structure
```json
"phasingPeriods": [
  {
    "id": 1,
    "isBonus": true,
    "rebateValue": "",
    "payoutToDate": "2025-07-10T00:00:00.000Z",
    "phasingToDate": "2025-07-10T00:00:00.000Z",
    "payoutFromDate": "2025-07-01T00:00:00.000Z",
    "phasingFromDate": "2025-07-01T00:00:00.000Z",
    "bonusRebateValue": "",
    "rebatePercentage": "1",
    "bonusRebatePercentage": "2"
  }
]
```

### 2.3 Bonus Scheme Data Structure
```json
"bonusSchemeData": {
  "bonusSchemes": [
    {
      "id": 1,
      "name": "Bonus Scheme 1",
      "type": "scheme1",
      "bonusPayoutTo": "2025-07-10T00:00:00.000Z",
      "bonusPeriodTo": "2025-07-10T00:00:00.000Z",
      "minimumTarget": "12000",
      "bonusPayoutFrom": "2025-07-01T00:00:00.000Z",
      "bonusPeriodFrom": "2025-07-01T00:00:00.000Z",
      "rewardOnTotalPercent": "5",
      "mainSchemeTargetPercent": "25",
      "mandatoryProductTargetPercent": "15",
      "minimumMandatoryProductTarget": "8000",
      "rewardOnMandatoryProductPercent": "5"
    }
  ]
}
```

### 2.4 Reward Slab Data
```json
"rewardSlabData": [
  {
    "slabId": 1,
    "slabTo": "100000",
    "slabFrom": "50000",
    "schemeReward": "Rs. 1000 Credit Note"
  }
]
```

## 3. Configuration Structure
```json
"configuration": {
  "schemeType": "ho-scheme",
  "enabledSections": {
    "rewardSlabs": true,
    "bonusSchemes": true,
    "payoutProducts": true,
    "schemeApplicable": true,
    "mandatoryProducts": true
  },
  "enableStrataGrowth": false,
  "showMandatoryProduct": true,
  "additionalSchemeFeatures": {
    "schemeId": {
      "schemeType": "ho-scheme",
      "hasRewardSlabs": false,
      "hasBonusSchemes": false
    }
  }
}
```

## 4. Additional Schemes Structure

Additional schemes follow a similar structure to the main scheme but are contained in an array:

```json
"additionalSchemes": [
  {
    "id": 1755407811380,
    "slabData": {
      "mainScheme": {
        "slabs": [...],           // Similar to main scheme slabs
        "enableStrataGrowth": false
      },
      "additionalSchemes": {}     // Can be nested
    },
    "schemeBase": "target-based",
    "productData": {
      "mainScheme": {
        "grps": [...],
        "skus": [...],
        "materials": [...],
        "categories": [...],
        "otherGroups": [...]
      },
      "additionalSchemes": {}
    },
    "configuration": {
      "enabledSections": {
        "rewardSlabs": false,
        "bonusSchemes": false,
        "payoutProducts": false,
        "schemeApplicable": false,
        "mandatoryProducts": false
      },
      "enableStrataGrowth": false,
      "showMandatoryProduct": false
    },
    "phasingPeriods": [],
    "rewardSlabData": [],
    "baseVolSections": [],
    "bonusSchemeData": {},
    "mandatoryQualify": "no",
    "schemeApplicable": {},
    "volumeValueBased": "volume",
    "schemeNumber": "Additional Scheme Name",
    "schemeTitle": "Additional Scheme Title"
  }
]
```

## 5. Key Features and Patterns

### 5.1 Phasing Implementation
- **Main Scheme**: Contains phasing periods with bonus flags and date ranges
- **Additional Schemes**: Each can have its own phasing periods (usually empty array in examples)
- **Bonus Integration**: Phasing periods can be marked as bonus periods with `isBonus: true`

### 5.2 Product Data Hierarchy
1. **Main Scheme Level**: Direct product arrays (grps, skus, materials, etc.)
2. **Additional Scheme Level**: Nested under `productData.mainScheme`
3. **Payout Products**: Subset of products eligible for payouts
4. **Mandatory Products**: Required products for scheme qualification

### 5.3 Bonus Structure Implementation
- **Bonus Schemes**: Array of bonus configurations with targets and percentages
- **Reward Slabs**: Tiered rewards based on achievement levels
- **Phasing Integration**: Bonus periods integrated with phasing periods

### 5.4 Configuration Flexibility
- **Enabled Sections**: Granular control over which features are active
- **Scheme Types**: Different scheme types (ho-scheme, etc.)
- **Growth Features**: Optional strata growth and mandatory product controls

## 6. Data Relationships

### 6.1 Slab to Bonus Relationship
- Slabs define target ranges and rebate percentages
- Bonus schemes reference these targets with additional reward percentages
- Phasing periods can apply bonus multipliers to base rebates

### 6.2 Product to Payout Relationship
- Main product data defines all eligible products
- Payout products subset defines which products generate payouts
- Mandatory products define minimum requirements for qualification

### 6.3 Scheme Hierarchy
- Main scheme is always present and fully configured
- Additional schemes can be 0 to N, each with independent configuration
- Additional schemes can have nested additional schemes (recursive structure)

## 7. Memory Storage Implications

For efficient memory storage and retrieval:

1. **Cache Key Structure**: `scheme_id` + `scheme_type` + `timestamp`
2. **Indexed Fields**: Product groups, SKUs, materials for fast lookups
3. **Computed Fields**: Total eligible products, active bonus periods, current phase
4. **Relationship Maps**: Slab-to-bonus, product-to-payout, phase-to-bonus mappings

This structure allows for complex scheme management with flexible product assignments, multi-tiered bonus systems, and time-based phasing with comprehensive configuration control.
