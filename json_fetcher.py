from datetime import datetime
import psycopg2
from supabaseconfig import SUPABASE_CONFIG

class JSONFetcher:
    def __init__(self):
        self.scheme_json = None
        self.scheme_config = None
    
    def fetch_and_store_json(self, scheme_id):
        """Fetch JSON once and store in memory"""
        query = "SELECT scheme_json FROM schemes_data WHERE scheme_id::TEXT = %s"
        
        with psycopg2.connect(**SUPABASE_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (scheme_id,))
                result = cur.fetchone()
                
                if not result:
                    raise ValueError(f"Scheme {scheme_id} not found")
                
                self.scheme_json = result[0]
                self.scheme_config = self._parse_config(scheme_id)
                
                print(f"SUCCESS: JSON fetched and stored for scheme {scheme_id}")
                return self.scheme_config
    
    def _parse_config(self, scheme_id):
        """Parse JSON WITHOUT any +1 day adjustment"""
        
        main_scheme = self.scheme_json.get('mainScheme', {})
        base_vol_sections = main_scheme.get('baseVolSections', [])
        
        # Parse base periods with EXACT dates (no +1 day)
        base_periods = []
        for i, period in enumerate(base_vol_sections):
            from_date = period.get('fromDate')
            to_date = period.get('toDate')
            
            if from_date and to_date:
                # Use EXACT dates from JSON - NO +1 day adjustment
                from_dt = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                to_dt = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
                
                base_periods.append({
                    'period_number': i + 1,
                    'from_date': from_dt.strftime('%Y-%m-%d'),    # EXACT DATE
                    'to_date': to_dt.strftime('%Y-%m-%d'),        # EXACT DATE
                    'sum_avg_method': period.get('sumAvg', 'sum')
                })
        
        # Scheme period dates (also exact)
        scheme_period = main_scheme.get('schemePeriod', {})
        scheme_from = scheme_period.get('fromDate')
        scheme_to = scheme_period.get('toDate')
        
        if scheme_from:
            scheme_from_dt = datetime.fromisoformat(scheme_from.replace('Z', '+00:00'))
            scheme_from = scheme_from_dt.strftime('%Y-%m-%d')  # No +1 day
        
        if scheme_to:
            scheme_to_dt = datetime.fromisoformat(scheme_to.replace('Z', '+00:00'))
            scheme_to = scheme_to_dt.strftime('%Y-%m-%d')  # No +1 day
        
        return {
            'scheme_id': scheme_id,
            'scheme_number': self.scheme_json.get('basicInfo', {}).get('schemeNumber'),
            'calculation_mode': main_scheme.get('volumeValueBased', 'volume'),
            'scheme_from': scheme_from,
            'scheme_to': scheme_to,
            'base_periods': base_periods,
            'base_periods_count': len(base_periods),
            'has_single_base_period': len(base_periods) == 1,
            'has_double_base_period': len(base_periods) == 2
        }
    
    def get_stored_config(self):
        return self.scheme_config
    
    def get_stored_json(self):
        return self.scheme_json
