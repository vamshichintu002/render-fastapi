"""
Configuration Manager Module
Handles scheme configuration flags for conditional calculations
"""

import json
from typing import Dict, Any, Optional


class SchemeConfigurationManager:
    """Manages configuration flags for schemes to enable conditional calculations"""
    
    def __init__(self, json_data: Dict[str, Any]):
        """
        Initialize configuration manager with JSON data
        
        Args:
            json_data: Raw JSON data containing scheme configurations
        """
        self.json_data = json_data
        self.main_config = self._parse_main_scheme_config()
        self.additional_configs = self._parse_additional_scheme_configs()
    
    def _parse_main_scheme_config(self) -> Dict[str, bool]:
        """Parse main scheme configuration flags"""
        config = {
            'payoutProducts': False,
            'mandatoryProducts': False,
            'bonusSchemes': False,
            'rewardSlabs': False,
            'schemeApplicable': False,
            'enableStrataGrowth': False,
            'showMandatoryProduct': False
        }
        
        try:
            # Get main scheme configuration
            main_scheme_config = self.json_data.get('configuration', {})
            enabled_sections = main_scheme_config.get('enabledSections', {})
            
            # Update configuration flags
            config.update({
                'payoutProducts': enabled_sections.get('payoutProducts', False),
                'mandatoryProducts': enabled_sections.get('mandatoryProducts', False),
                'bonusSchemes': enabled_sections.get('bonusSchemes', False),
                'rewardSlabs': enabled_sections.get('rewardSlabs', False),
                'schemeApplicable': enabled_sections.get('schemeApplicable', False),
                'enableStrataGrowth': main_scheme_config.get('enableStrataGrowth', False),
                'showMandatoryProduct': main_scheme_config.get('showMandatoryProduct', False)
            })
            
            print(f"📊 Main scheme configuration parsed: {sum(config.values())}/7 features enabled")
            
        except Exception as e:
            print(f"⚠️ Error parsing main scheme config: {e}")
        
        return config
    
    def _parse_additional_scheme_configs(self) -> Dict[str, Dict[str, bool]]:
        """Parse additional schemes configuration flags"""
        additional_configs = {}
        
        try:
            additional_schemes = self.json_data.get('additionalSchemes', [])
            
            for index, scheme in enumerate(additional_schemes):
                scheme_id = str(scheme.get('id', ''))
                scheme_name = scheme.get('schemeNumber', f'Additional_{scheme_id}')
                scheme_index = index + 1  # 1-based indexing
                
                # Default configuration
                config = {
                    'payoutProducts': False,
                    'mandatoryProducts': False,
                    'bonusSchemes': False,
                    'rewardSlabs': False,
                    'schemeApplicable': False,
                    'enableStrataGrowth': False,
                    'showMandatoryProduct': False
                }
                
                # Get scheme-specific configuration
                scheme_config = scheme.get('configuration', {})
                enabled_sections = scheme_config.get('enabledSections', {})
                
                # Update configuration flags
                config.update({
                    'payoutProducts': enabled_sections.get('payoutProducts', False),
                    'mandatoryProducts': enabled_sections.get('mandatoryProducts', False),
                    'bonusSchemes': enabled_sections.get('bonusSchemes', False),
                    'rewardSlabs': enabled_sections.get('rewardSlabs', False),
                    'schemeApplicable': enabled_sections.get('schemeApplicable', False),
                    'enableStrataGrowth': scheme_config.get('enableStrataGrowth', False),
                    'showMandatoryProduct': scheme_config.get('showMandatoryProduct', False)
                })
                
                # Store by both scheme ID and index for easy lookup
                additional_configs[scheme_id] = config
                additional_configs[str(scheme_index)] = config  # Store by 1-based index
                print(f"📊 Additional scheme '{scheme_name}' ({scheme_id}) [Index {scheme_index}]: {sum(config.values())}/7 features enabled")
        
        except Exception as e:
            print(f"⚠️ Error parsing additional scheme configs: {e}")
        
        return additional_configs
    
    def is_main_feature_enabled(self, feature: str) -> bool:
        """
        Check if a feature is enabled for the main scheme
        
        Args:
            feature: Feature name (payoutProducts, mandatoryProducts, etc.)
        
        Returns:
            True if feature is enabled, False otherwise
        """
        return self.main_config.get(feature, False)
    
    def is_additional_feature_enabled(self, scheme_id: str, feature: str) -> bool:
        """
        Check if a feature is enabled for an additional scheme
        
        Args:
            scheme_id: Additional scheme ID
            feature: Feature name (payoutProducts, mandatoryProducts, etc.)
        
        Returns:
            True if feature is enabled, False otherwise
        """
        scheme_config = self.additional_configs.get(str(scheme_id), {})
        return scheme_config.get(feature, False)
    
    def should_calculate_main_payouts(self) -> bool:
        """Check if main scheme payout calculations should be performed"""
        return self.is_main_feature_enabled('payoutProducts')
    
    def should_calculate_main_mandatory_products(self) -> bool:
        """Check if main scheme mandatory product calculations should be performed"""
        return self.is_main_feature_enabled('mandatoryProducts')
    
    def should_calculate_additional_payouts(self, scheme_id: str) -> bool:
        """Check if additional scheme payout calculations should be performed"""
        return self.is_additional_feature_enabled(scheme_id, 'payoutProducts')
    
    def should_calculate_additional_mandatory_products(self, scheme_id: str) -> bool:
        """Check if additional scheme mandatory product calculations should be performed"""
        return self.is_additional_feature_enabled(scheme_id, 'mandatoryProducts')
    
    def should_calculate_main_phasing(self) -> bool:
        """Check if main scheme phasing calculations should be performed"""
        return self.is_main_feature_enabled('bonusSchemes')
    
    def should_calculate_additional_phasing(self, scheme_id: str) -> bool:
        """Check if additional scheme phasing calculations should be performed"""
        return self.is_additional_feature_enabled(scheme_id, 'bonusSchemes')
    
    def should_use_strata_growth(self, scheme_id: Optional[str] = None) -> bool:
        """
        Check if strata growth should be used instead of slab growth
        
        Args:
            scheme_id: Optional additional scheme ID. If None, checks main scheme
        
        Returns:
            True if strata growth is enabled, False otherwise
        """
        if scheme_id is None:
            return self.is_main_feature_enabled('enableStrataGrowth')
        else:
            return self.is_additional_feature_enabled(scheme_id, 'enableStrataGrowth')
    
    def get_enabled_features_summary(self) -> Dict[str, Any]:
        """Get a summary of all enabled features for logging/debugging"""
        summary = {
            'main_scheme': {
                'enabled_features': [k for k, v in self.main_config.items() if v],
                'disabled_features': [k for k, v in self.main_config.items() if not v],
                'total_enabled': sum(self.main_config.values())
            },
            'additional_schemes': {}
        }
        
        for scheme_id, config in self.additional_configs.items():
            summary['additional_schemes'][scheme_id] = {
                'enabled_features': [k for k, v in config.items() if v],
                'disabled_features': [k for k, v in config.items() if not v],
                'total_enabled': sum(config.values())
            }
        
        return summary
    
    def print_configuration_summary(self):
        """Print a detailed summary of all configuration flags"""
        print("\n🔧 SCHEME CONFIGURATION SUMMARY:")
        print("=" * 50)
        
        print("📊 MAIN SCHEME:")
        for feature, enabled in self.main_config.items():
            status = "✅ ENABLED" if enabled else "❌ DISABLED"
            print(f"   {feature}: {status}")
        
        print(f"\n📊 ADDITIONAL SCHEMES ({len(self.additional_configs)}):")
        for scheme_id, config in self.additional_configs.items():
            scheme_name = f"Additional Scheme {scheme_id}"
            enabled_count = sum(config.values())
            print(f"   🔹 {scheme_name}: {enabled_count}/7 features enabled")
            
            for feature, enabled in config.items():
                if enabled:  # Only show enabled features to reduce clutter
                    print(f"      ✅ {feature}")
        
        print("\n💡 CALCULATION OPTIMIZATION:")
        print("   🚀 Only enabled features will be calculated")
        print("   ⚡ Disabled features will be skipped for better performance")
