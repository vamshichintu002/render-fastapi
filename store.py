"""
Data Storage and Extraction Module for Scheme JSON Processing
This module extracts and structures data from complex scheme JSON into pandas DataFrames
for efficient calculations without repeatedly parsing JSON.
"""

import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any, Tuple
import os


class SchemeDataExtractor:
    """
    Extracts and structures scheme data from JSON into organized pandas DataFrames
    """
    
    def __init__(self):
        self.products_df = None
        self.slabs_df = None
        self.phasing_df = None
        self.bonus_df = None
        self.rewards_df = None
        self.scheme_info_df = None
        
    def load_json_data(self, json_file_path: str) -> Dict[str, Any]:
        """Load JSON data from file"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return None
    
    def extract_all_data(self, json_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Main function to extract all data and return structured DataFrames
        """
        if not json_data:
            return {}
            
        # Extract all different types of data
        self.products_df = self._extract_products_data(json_data)
        self.slabs_df = self._extract_slabs_data(json_data)
        self.phasing_df = self._extract_phasing_data(json_data)
        self.bonus_df = self._extract_bonus_data(json_data)
        self.rewards_df = self._extract_rewards_data(json_data)
        self.scheme_info_df = self._extract_scheme_info(json_data)
        
        return {
            'products': self.products_df,
            'slabs': self.slabs_df,
            'phasing': self.phasing_df,
            'bonus': self.bonus_df,
            'rewards': self.rewards_df,
            'scheme_info': self.scheme_info_df
        }
    
    def _extract_products_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Extract all products data from main scheme and additional schemes
        Creates a comprehensive products table with boolean flags for different product types
        """
        products_list = []
        
        # Extract from main scheme
        main_scheme = json_data.get('mainScheme', {})
        basic_info = json_data.get('basicInfo', {})
        
        self._extract_scheme_products(
            products_list, 
            main_scheme, 
            'main_scheme', 
            basic_info.get('schemeTitle', 'Main Scheme'),
            basic_info.get('schemeType', 'unknown')
        )
        
        # Extract from additional schemes
        additional_schemes = json_data.get('additionalSchemes', [])
        
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_title = scheme.get('schemeTitle', f'Additional Scheme {idx + 1}')
            scheme_type = 'additional_scheme'
            
            # 🔧 COMPREHENSIVE FIX: Handle ALL possible JSON structures for additional schemes
            # Extract products from ALL available structures to ensure nothing is missing
            
            product_data = scheme.get('productData', {})
            products_extracted = False
            
            # Structure 1: Old format with mainScheme wrapper
            main_section = product_data.get('mainScheme', {})
            if main_section and any(key in main_section for key in ['grps', 'skus', 'materials', 'categories', 'otherGroups', 'payoutProducts', 'mandatoryProducts']):
                print(f"   📦 Additional scheme {idx+1} - Extracting from Structure 1 (mainScheme wrapper)")
                self._extract_scheme_products(
                    products_list,
                    {'productData': main_section},
                    scheme_type,
                    scheme_title,
                    'ho-scheme',
                    scheme_id
                )
                products_extracted = True
            
            # Structure 2: Direct productData with payout/mandatory products (like scheme 781457)
            if product_data and self._has_meaningful_payout_mandatory_data(product_data):
                print(f"   📦 Additional scheme {idx+1} - Extracting from Structure 2 (payout/mandatory products)")
                self._extract_additional_scheme_products_direct(
                    products_list,
                    product_data,
                    scheme_type,
                    scheme_title,
                    scheme_id
                )
                products_extracted = True
            
            # Structure 3: Direct productData with regular product lists (like scheme 311252)
            if product_data and any(key in product_data for key in ['grps', 'skus', 'materials', 'categories', 'otherGroups']):
                print(f"   📦 Additional scheme {idx+1} - Extracting from Structure 3 (direct product lists)")
                self._extract_scheme_products(
                    products_list,
                    {'productData': product_data},
                    scheme_type,
                    scheme_title,
                    'target-based',
                    scheme_id
                )
                products_extracted = True
            
            if not products_extracted:
                print(f"   ⚠️  Additional scheme {idx+1} - No products found in any structure")
                print(f"        Product data keys: {list(product_data.keys()) if product_data else 'None'}")
                if main_section:
                    print(f"        Main section keys: {list(main_section.keys())}")
        
        # Convert to DataFrame
        if products_list:
            df = pd.DataFrame(products_list)
            # Ensure boolean columns
            boolean_columns = ['is_mandatory_product', 'is_payout_product', 'is_product_data']
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)
            
            # Debug: Show extraction summary
            print(f"📊 Product extraction summary:")
            print(f"   Total products: {len(df)}")
            
            if not df.empty:
                scheme_counts = df['scheme_type'].value_counts()
                for scheme_type, count in scheme_counts.items():
                    print(f"   {scheme_type}: {count} products")
                
                # Additional scheme breakdown
                additional_products = df[df['scheme_type'] == 'additional_scheme']
                if not additional_products.empty:
                    print(f"   Additional scheme breakdown:")
                    for scheme_name, group in additional_products.groupby('scheme_name'):
                        mandatory_count = group['is_mandatory_product'].sum()
                        payout_count = group['is_payout_product'].sum()
                        print(f"      {scheme_name}: {len(group)} total ({mandatory_count} mandatory, {payout_count} payout)")
            
            return df
        else:
            print(f"⚠️  No products extracted from JSON data")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'scheme_type', 'scheme_name', 'scheme_id', 'material_code', 
                'product_type', 'is_mandatory_product', 'is_payout_product', 
                'is_product_data', 'category'
            ])
    
    def _extract_scheme_products(self, products_list: List[Dict], scheme_data: Dict, 
                                scheme_type: str, scheme_name: str, scheme_subtype: str, 
                                scheme_id: str = None):
        """Extract products from a single scheme - handles both regular and payout/mandatory products"""
        
        product_data = scheme_data.get('productData', {})
        
        # Get all regular product categories
        main_products = {
            'grps': product_data.get('grps', []),
            'skus': product_data.get('skus', []),
            'materials': product_data.get('materials', []),
            'categories': product_data.get('categories', []),
            'otherGroups': product_data.get('otherGroups', []),
            'thinnerGroups': product_data.get('thinnerGroups', []),
            'wandaGroups': product_data.get('wandaGroups', []),
            'productNames': product_data.get('productNames', [])
        }
        
        # Get mandatory products
        mandatory_products = product_data.get('mandatoryProducts', {})
        mandatory_sets = {
            'grps': set(mandatory_products.get('grps', [])),
            'skus': set(mandatory_products.get('skus', [])),
            'materials': set(mandatory_products.get('materials', [])),
            'categories': set(mandatory_products.get('categories', [])),
            'otherGroups': set(mandatory_products.get('otherGroups', [])),
            'thinnerGroups': set(mandatory_products.get('thinnerGroups', [])),
            'wandaGroups': set(mandatory_products.get('wandaGroups', [])),
            'productNames': set(mandatory_products.get('productNames', []))
        }
        
        # Get payout products
        payout_products = product_data.get('payoutProducts', {})
        payout_sets = {
            'grps': set(payout_products.get('grps', [])),
            'skus': set(payout_products.get('skus', [])),
            'materials': set(payout_products.get('materials', [])),
            'categories': set(payout_products.get('categories', [])),
            'otherGroups': set(payout_products.get('otherGroups', [])),
            'thinnerGroups': set(payout_products.get('thinnerGroups', [])),
            'wandaGroups': set(payout_products.get('wandaGroups', [])),
            'productNames': set(payout_products.get('productNames', []))
        }
        
        # Track all processed products to avoid duplicates
        processed_products = set()
        
        # Process each product category from regular products
        for product_type, products in main_products.items():
            for product_code in products:
                if product_code and str(product_code) not in processed_products:  # Skip empty codes and duplicates
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id or 'main',
                        'scheme_subtype': scheme_subtype,
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': product_code in mandatory_sets.get(product_type, set()),
                        'is_payout_product': product_code in payout_sets.get(product_type, set()),
                        'is_product_data': True,
                        'category': product_type
                    })
        
        # 🔧 ADDITIONAL FIX: Also process products that are ONLY in payout or mandatory lists
        # This ensures we capture products that might exist in payout/mandatory but not in main products
        
        # Process payout-only products
        for product_type, payout_codes in payout_sets.items():
            for product_code in payout_codes:
                if product_code and str(product_code) not in processed_products:
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id or 'main',
                        'scheme_subtype': scheme_subtype,
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': product_code in mandatory_sets.get(product_type, set()),
                        'is_payout_product': True,
                        'is_product_data': True,
                        'category': product_type
                    })
        
        # Process mandatory-only products
        for product_type, mandatory_codes in mandatory_sets.items():
            for product_code in mandatory_codes:
                if product_code and str(product_code) not in processed_products:
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id or 'main',
                        'scheme_subtype': scheme_subtype,
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': True,
                        'is_payout_product': product_code in payout_sets.get(product_type, set()),
                        'is_product_data': True,
                        'category': product_type
                    })
    
    def _extract_additional_scheme_products_direct(self, products_list: List[Dict], 
                                                  product_data: Dict, scheme_type: str, 
                                                  scheme_name: str, scheme_id: str):
        """Extract products from additional scheme with direct productData structure (like scheme 781457)"""
        
        # Get payout products (main products for the scheme)
        payout_products = product_data.get('payoutProducts', {})
        payout_product_categories = {
            'grps': payout_products.get('grps', []),
            'skus': payout_products.get('skus', []),
            'materials': payout_products.get('materials', []),
            'categories': payout_products.get('categories', []),
            'otherGroups': payout_products.get('otherGroups', []),
            'thinnerGroups': payout_products.get('thinnerGroups', []),
            'wandaGroups': payout_products.get('wandaGroups', []),
            'productNames': payout_products.get('productNames', [])
        }
        
        # Get mandatory products
        mandatory_products = product_data.get('mandatoryProducts', {})
        mandatory_sets = {
            'grps': set(mandatory_products.get('grps', [])),
            'skus': set(mandatory_products.get('skus', [])),
            'materials': set(mandatory_products.get('materials', [])),
            'categories': set(mandatory_products.get('categories', [])),
            'otherGroups': set(mandatory_products.get('otherGroups', [])),
            'thinnerGroups': set(mandatory_products.get('thinnerGroups', [])),
            'wandaGroups': set(mandatory_products.get('wandaGroups', [])),
            'productNames': set(mandatory_products.get('productNames', []))
        }
        
        # Create payout sets for consistency
        payout_sets = {
            'grps': set(payout_products.get('grps', [])),
            'skus': set(payout_products.get('skus', [])),
            'materials': set(payout_products.get('materials', [])),
            'categories': set(payout_products.get('categories', [])),
            'otherGroups': set(payout_products.get('otherGroups', [])),
            'thinnerGroups': set(payout_products.get('thinnerGroups', [])),
            'wandaGroups': set(payout_products.get('wandaGroups', [])),
            'productNames': set(payout_products.get('productNames', []))
        }
        
        # Track processed products to avoid duplicates
        processed_products = set()
        
        # Process payout products (main products)
        for product_type, products in payout_product_categories.items():
            for product_code in products:
                if product_code and str(product_code) not in processed_products:  # Skip empty codes and duplicates
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id,
                        'scheme_subtype': 'target-based',
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': product_code in mandatory_sets.get(product_type, set()),
                        'is_payout_product': True,  # These are payout products
                        'is_product_data': True,
                        'category': product_type
                    })
        
        # Process mandatory products that are not already in payout products
        for product_type, mandatory_codes in mandatory_sets.items():
            for product_code in mandatory_codes:
                if product_code and str(product_code) not in processed_products:
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id,
                        'scheme_subtype': 'target-based',
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': True,
                        'is_payout_product': False,  # Mandatory only, not payout
                        'is_product_data': True,
                        'category': product_type
                    })
        
        # 🔧 ADDITIONAL FIX: Also check if there are regular product lists in the productData
        # Some schemes might have both payout/mandatory AND regular product lists
        regular_product_categories = {
            'grps': product_data.get('grps', []),
            'skus': product_data.get('skus', []),
            'materials': product_data.get('materials', []),
            'categories': product_data.get('categories', []),
            'otherGroups': product_data.get('otherGroups', []),
            'thinnerGroups': product_data.get('thinnerGroups', []),
            'wandaGroups': product_data.get('wandaGroups', []),
            'productNames': product_data.get('productNames', [])
        }
        
        # Process regular products that aren't already captured
        for product_type, products in regular_product_categories.items():
            for product_code in products:
                if product_code and str(product_code) not in processed_products:
                    processed_products.add(str(product_code))
                    products_list.append({
                        'scheme_type': scheme_type,
                        'scheme_name': scheme_name,
                        'scheme_id': scheme_id,
                        'scheme_subtype': 'target-based',
                        'material_code': str(product_code),
                        'product_type': product_type,
                        'is_mandatory_product': product_code in mandatory_sets.get(product_type, set()),
                        'is_payout_product': product_code in payout_sets.get(product_type, set()),
                        'is_product_data': True,
                        'category': product_type
                    })
    
    def _has_meaningful_payout_mandatory_data(self, product_data: Dict) -> bool:
        """Check if payout/mandatory products contain actual data (not just empty placeholders)"""
        
        # All possible product category keys
        product_keys = ['grps', 'skus', 'materials', 'categories', 'otherGroups', 
                       'thinnerGroups', 'wandaGroups', 'productNames']
        
        # Check payout products
        payout_products = product_data.get('payoutProducts', {})
        has_payout_data = False
        if payout_products:
            has_payout_data = any(
                isinstance(payout_products.get(key), list) and payout_products.get(key)
                for key in product_keys
            )
        
        # Check mandatory products
        mandatory_products = product_data.get('mandatoryProducts', {})
        has_mandatory_data = False
        if mandatory_products:
            has_mandatory_data = any(
                isinstance(mandatory_products.get(key), list) and mandatory_products.get(key)
                for key in product_keys
            )
        
        return has_payout_data or has_mandatory_data
    
    def _extract_slabs_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """Extract slab data from all schemes"""
        slabs_list = []
        
        # Extract from main scheme
        main_scheme = json_data.get('mainScheme', {})
        basic_info = json_data.get('basicInfo', {})
        
        self._extract_scheme_slabs(
            slabs_list,
            main_scheme.get('slabData', {}),
            'main_scheme',
            basic_info.get('schemeTitle', 'Main Scheme'),
            'main'
        )
        
        # Extract from additional schemes
        additional_schemes = json_data.get('additionalSchemes', [])
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_title = scheme.get('schemeTitle', f'Additional Scheme {idx + 1}')
            
            # 🔧 FIX: Handle both JSON structures for slab data
            slab_data = scheme.get('slabData', {})
            
            # Structure 1: Old format with mainScheme wrapper
            main_slab_section = slab_data.get('mainScheme', {})
            if main_slab_section:
                self._extract_scheme_slabs(
                    slabs_list,
                    main_slab_section,
                    'additional_scheme',
                    scheme_title,
                    scheme_id
                )
            # Structure 2: New format with direct slabData (like scheme 781457)
            elif slab_data and 'slabs' in slab_data:
                self._extract_scheme_slabs(
                    slabs_list,
                    slab_data,  # Direct slab data
                    'additional_scheme',
                    scheme_title,
                    scheme_id
                )
        
        return pd.DataFrame(slabs_list) if slabs_list else pd.DataFrame(columns=[
            'scheme_type', 'scheme_name', 'scheme_id', 'slab_id', 'slab_start', 
            'slab_end', 'rebate_percent', 'growth_percent', 'rebate_per_litre',
            'dealer_qualify_percent', 'additional_rebate_on_growth'
        ])
    
    def _extract_scheme_slabs(self, slabs_list: List[Dict], slab_data: Dict, 
                             scheme_type: str, scheme_name: str, scheme_id: str):
        """Extract slabs from a single scheme"""
        slabs = slab_data.get('slabs', [])
        
        for slab in slabs:
            slabs_list.append({
                'scheme_type': scheme_type,
                'scheme_name': scheme_name,
                'scheme_id': scheme_id,
                'slab_id': slab.get('id'),
                'slab_start': self._safe_float(slab.get('slabStart')),
                'slab_end': self._safe_float(slab.get('slabEnd')),
                'rebate_percent': self._safe_float(slab.get('rebatePercent')),
                'growth_percent': self._safe_float(slab.get('growthPercent')),
                'rebate_per_litre': self._safe_float(slab.get('rebatePerLitre')),
                'fixed_rebate': self._safe_float(slab.get('fixedRebate')),
                'dealer_qualify_percent': self._safe_float(slab.get('dealerMayQualifyPercent')),
                'additional_rebate_on_growth': self._safe_float(slab.get('additionalRebateOnGrowth')),
                'mandatory_product_growth_percent': self._safe_float(slab.get('mandatoryProductGrowthPercent')),
                'mandatory_product_rebate': self._safe_float(slab.get('mandatoryProductRebate')),
                'mandatory_product_rebate_percent': self._safe_float(slab.get('mandatoryProductRebatePercent'))
            })
    
    def _extract_phasing_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """Extract phasing periods data"""
        phasing_list = []
        
        # Extract from main scheme - phasing periods are inside mainScheme
        main_scheme = json_data.get('mainScheme', {})
        main_phasing = main_scheme.get('phasingPeriods', [])
        basic_info = json_data.get('basicInfo', {})
        
        for phase in main_phasing:
            phasing_list.append({
                'scheme_type': 'main_scheme',
                'scheme_name': basic_info.get('schemeTitle', 'Main Scheme'),
                'scheme_id': 'main',
                'phase_id': phase.get('id'),
                'is_bonus': phase.get('isBonus', False),
                'phasing_from_date': phase.get('phasingFromDate'),
                'phasing_to_date': phase.get('phasingToDate'),
                'payout_from_date': phase.get('payoutFromDate'),
                'payout_to_date': phase.get('payoutToDate'),
                'rebate_percentage': self._safe_float(phase.get('rebatePercentage')),
                'bonus_rebate_percentage': self._safe_float(phase.get('bonusRebatePercentage')),
                'rebate_value': self._safe_float(phase.get('rebateValue')),
                'bonus_rebate_value': self._safe_float(phase.get('bonusRebateValue')),
                'phasing_target_percent': self._safe_float(phase.get('phasingTargetPercent')),
                'bonus_phasing_target_percent': self._safe_float(phase.get('bonusPhasingTargetPercent')),
                'bonus_phasing_from_date': phase.get('bonusPhasingFromDate'),
                'bonus_phasing_to_date': phase.get('bonusPhasingToDate'),
                'bonus_payout_from_date': phase.get('bonusPayoutFromDate'),
                'bonus_payout_to_date': phase.get('bonusPayoutToDate')
            })
        
        # Extract from additional schemes
        additional_schemes = json_data.get('additionalSchemes', [])
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_title = scheme.get('schemeTitle', f'Additional Scheme {idx + 1}')
            
            # Additional schemes phasing periods are directly under the scheme
            scheme_phasing = scheme.get('phasingPeriods', [])
            for phase in scheme_phasing:
                phasing_list.append({
                    'scheme_type': 'additional_scheme',
                    'scheme_name': scheme_title,
                    'scheme_id': scheme_id,
                    'phase_id': phase.get('id'),
                    'is_bonus': phase.get('isBonus', False),
                    'phasing_from_date': phase.get('phasingFromDate'),
                    'phasing_to_date': phase.get('phasingToDate'),
                    'payout_from_date': phase.get('payoutFromDate'),
                    'payout_to_date': phase.get('payoutToDate'),
                    'rebate_percentage': self._safe_float(phase.get('rebatePercentage')),
                    'bonus_rebate_percentage': self._safe_float(phase.get('bonusRebatePercentage')),
                    'rebate_value': self._safe_float(phase.get('rebateValue')),
                    'bonus_rebate_value': self._safe_float(phase.get('bonusRebateValue')),
                    'phasing_target_percent': self._safe_float(phase.get('phasingTargetPercent')),
                    'bonus_phasing_target_percent': self._safe_float(phase.get('bonusPhasingTargetPercent')),
                    'bonus_phasing_from_date': phase.get('bonusPhasingFromDate'),
                    'bonus_phasing_to_date': phase.get('bonusPhasingToDate'),
                    'bonus_payout_from_date': phase.get('bonusPayoutFromDate'),
                    'bonus_payout_to_date': phase.get('bonusPayoutToDate')
                })
        
        return pd.DataFrame(phasing_list) if phasing_list else pd.DataFrame(columns=[
            'scheme_type', 'scheme_name', 'scheme_id', 'phase_id', 'is_bonus',
            'phasing_from_date', 'phasing_to_date', 'payout_from_date', 'payout_to_date',
            'rebate_percentage', 'bonus_rebate_percentage', 'rebate_value', 'bonus_rebate_value',
            'phasing_target_percent', 'bonus_phasing_target_percent', 'bonus_phasing_from_date',
            'bonus_phasing_to_date', 'bonus_payout_from_date', 'bonus_payout_to_date'
        ])
    
    def _extract_bonus_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """Extract bonus schemes data"""
        bonus_list = []
        
        # Extract from main scheme - bonusSchemeData is inside mainScheme
        main_scheme = json_data.get('mainScheme', {})
        bonus_scheme_data = main_scheme.get('bonusSchemeData', {})
        bonus_schemes = bonus_scheme_data.get('bonusSchemes', []) if isinstance(bonus_scheme_data, dict) else []
        basic_info = json_data.get('basicInfo', {})
        
        for bonus in bonus_schemes:
            bonus_list.append({
                'scheme_type': 'main_scheme',
                'scheme_name': basic_info.get('schemeTitle', 'Main Scheme'),
                'scheme_id': 'main',
                'bonus_id': bonus.get('id'),
                'bonus_name': bonus.get('name'),
                'bonus_type': bonus.get('type'),
                'bonus_period_from': bonus.get('bonusPeriodFrom'),
                'bonus_period_to': bonus.get('bonusPeriodTo'),
                'bonus_payout_from': bonus.get('bonusPayoutFrom'),
                'bonus_payout_to': bonus.get('bonusPayoutTo'),
                'minimum_target': self._safe_float(bonus.get('minimumTarget')),
                'reward_on_total_percent': self._safe_float(bonus.get('rewardOnTotalPercent')),
                'main_scheme_target_percent': self._safe_float(bonus.get('mainSchemeTargetPercent')),
                'mandatory_product_target_percent': self._safe_float(bonus.get('mandatoryProductTargetPercent')),
                'minimum_mandatory_product_target': self._safe_float(bonus.get('minimumMandatoryProductTarget')),
                'reward_on_mandatory_product_percent': self._safe_float(bonus.get('rewardOnMandatoryProductPercent'))
            })
        
        # Extract from additional schemes
        additional_schemes = json_data.get('additionalSchemes', [])
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_title = scheme.get('schemeTitle', f'Additional Scheme {idx + 1}')
            
            scheme_bonus_data = scheme.get('bonusSchemeData', {})
            scheme_bonus_schemes = scheme_bonus_data.get('bonusSchemes', []) if isinstance(scheme_bonus_data, dict) else []
            
            for bonus in scheme_bonus_schemes:
                bonus_list.append({
                    'scheme_type': 'additional_scheme',
                    'scheme_name': scheme_title,
                    'scheme_id': scheme_id,
                    'bonus_id': bonus.get('id'),
                    'bonus_name': bonus.get('name'),
                    'bonus_type': bonus.get('type'),
                    'bonus_period_from': bonus.get('bonusPeriodFrom'),
                    'bonus_period_to': bonus.get('bonusPeriodTo'),
                    'bonus_payout_from': bonus.get('bonusPayoutFrom'),
                    'bonus_payout_to': bonus.get('bonusPayoutTo'),
                    'minimum_target': self._safe_float(bonus.get('minimumTarget')),
                    'reward_on_total_percent': self._safe_float(bonus.get('rewardOnTotalPercent')),
                    'main_scheme_target_percent': self._safe_float(bonus.get('mainSchemeTargetPercent')),
                    'mandatory_product_target_percent': self._safe_float(bonus.get('mandatoryProductTargetPercent')),
                    'minimum_mandatory_product_target': self._safe_float(bonus.get('minimumMandatoryProductTarget')),
                    'reward_on_mandatory_product_percent': self._safe_float(bonus.get('rewardOnMandatoryProductPercent'))
                })
        
        return pd.DataFrame(bonus_list) if bonus_list else pd.DataFrame(columns=[
            'scheme_type', 'scheme_name', 'scheme_id', 'bonus_id', 'bonus_name', 'bonus_type',
            'bonus_period_from', 'bonus_period_to', 'bonus_payout_from', 'bonus_payout_to',
            'minimum_target', 'reward_on_total_percent', 'main_scheme_target_percent',
            'mandatory_product_target_percent', 'minimum_mandatory_product_target',
            'reward_on_mandatory_product_percent'
        ])
    
    def _extract_rewards_data(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """Extract reward slab data"""
        rewards_list = []
        
        # Extract from main scheme - rewardSlabData is inside mainScheme
        main_scheme = json_data.get('mainScheme', {})
        reward_slab_data = main_scheme.get('rewardSlabData', [])
        basic_info = json_data.get('basicInfo', {})
        
        for reward in reward_slab_data:
            rewards_list.append({
                'scheme_type': 'main_scheme',
                'scheme_name': basic_info.get('schemeTitle', 'Main Scheme'),
                'scheme_id': 'main',
                'slab_id': reward.get('slabId'),
                'slab_from': self._safe_float(reward.get('slabFrom')),
                'slab_to': self._safe_float(reward.get('slabTo')),
                'scheme_reward': reward.get('schemeReward')
            })
        
        # Extract from additional schemes
        additional_schemes = json_data.get('additionalSchemes', [])
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_title = scheme.get('schemeTitle', f'Additional Scheme {idx + 1}')
            
            scheme_reward_data = scheme.get('rewardSlabData', [])
            for reward in scheme_reward_data:
                rewards_list.append({
                    'scheme_type': 'additional_scheme',
                    'scheme_name': scheme_title,
                    'scheme_id': scheme_id,
                    'slab_id': reward.get('slabId'),
                    'slab_from': self._safe_float(reward.get('slabFrom')),
                    'slab_to': self._safe_float(reward.get('slabTo')),
                    'scheme_reward': reward.get('schemeReward')
                })
        
        return pd.DataFrame(rewards_list) if rewards_list else pd.DataFrame(columns=[
            'scheme_type', 'scheme_name', 'scheme_id', 'slab_id', 'slab_from', 'slab_to', 'scheme_reward'
        ])
    
    def _extract_scheme_info(self, json_data: Dict[str, Any]) -> pd.DataFrame:
        """Extract basic scheme information"""
        scheme_info_list = []
        
        basic_info = json_data.get('basicInfo', {})
        configuration = json_data.get('configuration', {})
        
        # Main scheme info
        scheme_info_list.append({
            'scheme_type': 'main_scheme',
            'scheme_name': basic_info.get('schemeTitle', 'Main Scheme'),
            'scheme_id': 'main',
            'scheme_number': basic_info.get('schemeNumber', ''),
            'scheme_description': basic_info.get('schemeDescription', ''),
            'created_by': basic_info.get('createdBy', ''),
            'created_at': json_data.get('createdAt', ''),
            'updated_at': json_data.get('updatedAt', ''),
            'scheme_base': json_data.get('mainScheme', {}).get('schemeBase', ''),
            'enable_strata_growth': configuration.get('enableStrataGrowth', False),
            'show_mandatory_product': configuration.get('showMandatoryProduct', False),
            'reward_slabs_enabled': configuration.get('enabledSections', {}).get('rewardSlabs', False),
            'bonus_schemes_enabled': configuration.get('enabledSections', {}).get('bonusSchemes', False),
            'payout_products_enabled': configuration.get('enabledSections', {}).get('payoutProducts', False),
            'mandatory_products_enabled': configuration.get('enabledSections', {}).get('mandatoryProducts', False),
            'volume_value_based': json_data.get('mainScheme', {}).get('volumeValueBased', ''),
            'mandatory_qualify': json_data.get('mainScheme', {}).get('mandatoryQualify', ''),
            'scheme_applicable_enabled': json_data.get('mainScheme', {}).get('schemeApplicable', {}).get('enabled', False) if isinstance(json_data.get('mainScheme', {}).get('schemeApplicable'), dict) else False
        })
        
        # Additional schemes info
        additional_schemes = json_data.get('additionalSchemes', [])
        for idx, scheme in enumerate(additional_schemes):
            scheme_id = scheme.get('id', f'additional_{idx}')
            scheme_config = scheme.get('configuration', {})
            
            # 🔧 FIX: enableStrataGrowth is inside slabData.mainScheme for additional schemes
            slab_data = scheme.get('slabData', {}).get('mainScheme', {})
            enable_strata_growth = slab_data.get('enableStrataGrowth', False)
            
            scheme_info_list.append({
                'scheme_type': 'additional_scheme',
                'scheme_name': scheme.get('schemeTitle', f'Additional Scheme {idx + 1}'),
                'scheme_id': scheme_id,
                'scheme_number': scheme.get('schemeNumber', ''),
                'scheme_description': scheme.get('schemeDescription', ''),
                'created_by': '',
                'created_at': '',
                'updated_at': '',
                'scheme_base': scheme.get('schemeBase', ''),
                'enable_strata_growth': enable_strata_growth,  # Fixed: from slabData.mainScheme
                'show_mandatory_product': scheme_config.get('showMandatoryProduct', False),
                'reward_slabs_enabled': scheme_config.get('enabledSections', {}).get('rewardSlabs', False),
                'bonus_schemes_enabled': scheme_config.get('enabledSections', {}).get('bonusSchemes', False),
                'payout_products_enabled': scheme_config.get('enabledSections', {}).get('payoutProducts', False),
                'mandatory_products_enabled': scheme_config.get('enabledSections', {}).get('mandatoryProducts', False),
                'volume_value_based': scheme.get('volumeValueBased', ''),
                'mandatory_qualify': scheme.get('mandatoryQualify', ''),
                'scheme_applicable_enabled': scheme.get('schemeApplicable', {}).get('enabled', False) if isinstance(scheme.get('schemeApplicable'), dict) else False
            })
        
        return pd.DataFrame(scheme_info_list)
    
    def _safe_float(self, value) -> float:
        """Safely convert value to float"""
        if value is None or value == '':
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def save_to_excel(self, output_path: str = 'scheme_data_extracted.xlsx'):
        """Save all DataFrames to Excel file with separate sheets"""
        if not any([
            self.products_df is not None and not self.products_df.empty,
            self.slabs_df is not None and not self.slabs_df.empty,
            self.phasing_df is not None and not self.phasing_df.empty,
            self.bonus_df is not None and not self.bonus_df.empty,
            self.rewards_df is not None and not self.rewards_df.empty,
            self.scheme_info_df is not None and not self.scheme_info_df.empty
        ]):
            print("No data to save. Please extract data first.")
            return
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if self.products_df is not None and not self.products_df.empty:
                self.products_df.to_excel(writer, sheet_name='Products', index=False)
            
            if self.slabs_df is not None and not self.slabs_df.empty:
                self.slabs_df.to_excel(writer, sheet_name='Slabs', index=False)
            
            if self.phasing_df is not None and not self.phasing_df.empty:
                self.phasing_df.to_excel(writer, sheet_name='Phasing', index=False)
            
            if self.bonus_df is not None and not self.bonus_df.empty:
                self.bonus_df.to_excel(writer, sheet_name='Bonus_Schemes', index=False)
            
            if self.rewards_df is not None and not self.rewards_df.empty:
                self.rewards_df.to_excel(writer, sheet_name='Rewards', index=False)
            
            if self.scheme_info_df is not None and not self.scheme_info_df.empty:
                self.scheme_info_df.to_excel(writer, sheet_name='Scheme_Info', index=False)
        
        print(f"Data successfully saved to {output_path}")
    
    def get_products_by_scheme(self, scheme_id: str = None, scheme_type: str = None) -> pd.DataFrame:
        """Get products filtered by scheme"""
        if self.products_df is None or self.products_df.empty:
            return pd.DataFrame()
        
        df = self.products_df.copy()
        
        if scheme_id:
            df = df[df['scheme_id'] == scheme_id]
        
        if scheme_type:
            df = df[df['scheme_type'] == scheme_type]
        
        return df
    
    def get_mandatory_products(self, scheme_id: str = None) -> pd.DataFrame:
        """Get only mandatory products"""
        if self.products_df is None or self.products_df.empty:
            return pd.DataFrame()
        
        df = self.products_df[self.products_df['is_mandatory_product'] == True]
        
        if scheme_id:
            df = df[df['scheme_id'] == scheme_id]
        
        return df
    
    def get_payout_products(self, scheme_id: str = None) -> pd.DataFrame:
        """Get only payout products"""
        if self.products_df is None or self.products_df.empty:
            return pd.DataFrame()
        
        df = self.products_df[self.products_df['is_payout_product'] == True]
        
        if scheme_id:
            df = df[df['scheme_id'] == scheme_id]
        
        return df


def process_scheme_json(json_file_path: str, output_excel_path: str = None, auto_save: bool = False) -> SchemeDataExtractor:
    """
    Main function to process scheme JSON and return structured data
    
    Args:
        json_file_path: Path to the JSON file
        output_excel_path: Optional path to save Excel file
    
    Returns:
        SchemeDataExtractor instance with all structured data
    """
    extractor = SchemeDataExtractor()
    
    # Load JSON data
    json_data = extractor.load_json_data(json_file_path)
    if not json_data:
        print("Failed to load JSON data")
        return extractor
    
    # Extract all data
    dataframes = extractor.extract_all_data(json_data)
    
    # Print summary
    print("\n=== Data Extraction Summary ===")
    for name, df in dataframes.items():
        if df is not None and not df.empty:
            print(f"{name.upper()}: {len(df)} records")
        else:
            print(f"{name.upper()}: No data")
    
    # Save to Excel only if explicitly requested
    if output_excel_path and auto_save:
        extractor.save_to_excel(output_excel_path)
        print(f"📁 Excel file saved to: {output_excel_path}")
    elif output_excel_path:
        print(f"📁 Excel export available - call extractor.save_to_excel('{output_excel_path}') if needed")
    
    return extractor


if __name__ == "__main__":
    # Example usage
    json_file = "examplestructure.json"
    excel_file = "scheme_data_extracted.xlsx"
    
    if os.path.exists(json_file):
        extractor = process_scheme_json(json_file, excel_file)
        
        # Example of accessing specific data
        print("\n=== Sample Data Access ===")
        
        # Get all products from main scheme
        main_products = extractor.get_products_by_scheme(scheme_type='main_scheme')
        print(f"Main scheme products: {len(main_products)}")
        
        # Get mandatory products
        mandatory = extractor.get_mandatory_products()
        print(f"Total mandatory products: {len(mandatory)}")
        
        # Get payout products
        payout = extractor.get_payout_products()
        print(f"Total payout products: {len(payout)}")
        
    else:
        print(f"JSON file '{json_file}' not found")
