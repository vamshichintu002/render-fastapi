class SchemeApplicableFetcher:
    def __init__(self, scheme_json):
        self.scheme_json = scheme_json
        self.applicable_info = None

    def fetch_scheme_applicable(self):
        """Extract ALL scheme applicable filters from stored JSON"""
        main_scheme = self.scheme_json.get('mainScheme', {})
        scheme_applicable = main_scheme.get('schemeApplicable', {})

        # Extract ALL scheme applicable filters
        self.applicable_info = {
            'enabled': scheme_applicable.get('enabled', False),
            'selected_slocs': scheme_applicable.get('selectedSlocs', []),
            'selected_states': scheme_applicable.get('selectedStates', []),
            'selected_regions': scheme_applicable.get('selectedRegions', []),
            'selected_area_heads': scheme_applicable.get('selectedAreaHeads', []),
            'selected_divisions': scheme_applicable.get('selectedDivisions', []),
            'filtered_record_count': scheme_applicable.get('filteredRecordCount', 0),
            'selected_dealer_types': scheme_applicable.get('selectedDealerTypes', []),
            'selected_rack_dealers': scheme_applicable.get('selectedRackDealers', []),
            'selected_distributors': scheme_applicable.get('selectedDistributors', []),
            'selected_fixed_dealers': scheme_applicable.get('selectedFixedDealers', []),
            'selected_customer_names': scheme_applicable.get('selectedCustomerNames', []),
            'selected_credit_accounts': [str(ca) for ca in scheme_applicable.get('selectedCreditAccounts', [])]
        }
        
        print(f"✅ Scheme applicable filters extracted")
        print(f"   • Enabled: {self.applicable_info['enabled']}")
        print(f"   • States: {len(self.applicable_info['selected_states'])}")
        print(f"   • Regions: {len(self.applicable_info['selected_regions'])}")
        print(f"   • Credit Accounts: {len(self.applicable_info['selected_credit_accounts'])}")
        print(f"   • Customer Names: {len(self.applicable_info['selected_customer_names'])}")
        
        return self.applicable_info

    def print_scheme_applicable(self):
        """Print ALL scheme applicable filter details"""
        if self.applicable_info is None:
            self.fetch_scheme_applicable()
        
        print("\n📋 Complete Scheme Applicable Details:")
        print(f"   • Enabled: {self.applicable_info['enabled']}")
        print(f"   • Selected SLOCs: {self.applicable_info['selected_slocs']}")
        print(f"   • Selected States: {self.applicable_info['selected_states']}")
        print(f"   • Selected Regions: {self.applicable_info['selected_regions']}")
        print(f"   • Selected Area Heads: {self.applicable_info['selected_area_heads']}")
        print(f"   • Selected Divisions: {self.applicable_info['selected_divisions']}")
        print(f"   • Filtered Record Count: {self.applicable_info['filtered_record_count']}")
        print(f"   • Selected Dealer Types: {self.applicable_info['selected_dealer_types']}")
        print(f"   • Selected Rack Dealers: {self.applicable_info['selected_rack_dealers']}")
        print(f"   • Selected Distributors: {self.applicable_info['selected_distributors']}")
        print(f"   • Selected Fixed Dealers: {self.applicable_info['selected_fixed_dealers']}")
        print(f"   • Selected Customer Names: {self.applicable_info['selected_customer_names']}")
        print(f"   • Selected Credit Accounts: {self.applicable_info['selected_credit_accounts']}")

    def get_stored_applicable(self):
        """Get stored applicable information"""
        return self.applicable_info

    def get_all_filters_for_sql(self):
        """Get filters ready for SQL WHERE clauses"""
        if not self.applicable_info:
            return {}
        
        return {
            'states': self.applicable_info['selected_states'],
            'regions': self.applicable_info['selected_regions'], 
            'divisions': self.applicable_info['selected_divisions'],
            'credit_accounts': self.applicable_info['selected_credit_accounts'],
            'customer_names': self.applicable_info['selected_customer_names'],
            'dealer_types': self.applicable_info['selected_dealer_types'],
            'rack_dealers': self.applicable_info['selected_rack_dealers'],
            'distributors': self.applicable_info['selected_distributors'],
            'fixed_dealers': self.applicable_info['selected_fixed_dealers'],
            'area_heads': self.applicable_info['selected_area_heads']
        }
