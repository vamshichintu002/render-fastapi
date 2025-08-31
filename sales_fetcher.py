import psycopg2
import csv
import pandas as pd
from supabaseconfig import SUPABASE_CONFIG

class SalesFetcher:
    def __init__(self):
        self.sales_data = None
        self.base_period1_data = None
        self.base_period2_data = None
        self.scheme_period_data = None

    def fetch_sales_for_period(self, start_date, end_date, filters, calc_mode, period_name):
        """
        Fetch sales data for a specific period (base period 1, base period 2, or scheme period)
        """
        print(f"🔍 Fetching {period_name} sales data with filters")
        print(f"🔧 Debug: start_date={start_date}, end_date={end_date}, type={type(start_date)}")
        
        # Build WHERE clauses for filters
        where_clauses = []
        params = []
        
        def add_in_filter(field, values):
            if values:
                placeholders = ','.join(['%s'] * len(values))
                clause = f"{field} IN ({placeholders})"
                return clause, values
            return None
        
        filter_fields = {
            'state_name': filters.get('states', []),
            'region_name': filters.get('regions', []),
            'credit_account': [str(ca) for ca in filters.get('credit_accounts', [])],
            'customer_name': filters.get('customer_names', []),
            'dealer_type': filters.get('dealer_types', []),
            'rack_dealers': filters.get('rack_dealers', []),
            'distributor': filters.get('distributors', []),
            'fixed_dealers': filters.get('fixed_dealers', []),
            'area_head_name': filters.get('area_heads', []),
            'division': filters.get('divisions', [])
        }
        
        # Apply filters, but handle credit_account filtering specially
        credit_accounts_filter = filter_fields.pop('credit_account', [])
        
        # Apply all other filters first
        for field, values in filter_fields.items():
            clause_result = add_in_filter(field, values)
            if clause_result:
                clause, vals = clause_result
                where_clauses.append(clause)
                params.extend(vals)
        
        # MODIFIED LOGIC: Don't apply credit account filter to allow all accounts with sales data
        # The goal is to include ALL accounts that have sales data and match other filters (state, region, etc.)
        # Credit account filtering can exclude valid accounts that should be in the tracker
        if credit_accounts_filter:
            print(f"   🔍 Credit account filter: {len(credit_accounts_filter)} accounts specified")
            print(f"   ⚠️ SKIPPING credit account filter to include all accounts with sales data")
            print(f"   📝 Rationale: All accounts with sales data should appear in tracker, even if targets=0")
        else:
            print(f"   ℹ️ No credit account filter specified - including all accounts")
        
        calc_column = 'value' if calc_mode == 'value' else 'volume'
        
        print(f"   📊 {period_name}: {start_date} to {end_date}")
        
        # Build SQL query (include ALL data, even negative values)
        date_clause = "TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD') BETWEEN %s AND %s"
        # Remove ALL filtering of negative values - fetch everything
        all_where_clauses = [date_clause] + where_clauses
        where_sql = " AND ".join(all_where_clauses)
        
        # Aggregated query to summarize by credit_account and date
        query = f"""
        SELECT 
            MIN(id) as id,
            division, distributor, location, year, month, day,
            credit_account, 
            MIN(customer_name) as customer_name,
            MIN(region_name) as region_name,
            MIN(state_name) as state_name,
            MIN(area_head_name) as area_head_name,
            MIN(so_name) as so_name,
            MIN(dealer_type) as dealer_type,
            MIN(fixed_dealers) as fixed_dealers,
            MIN(rack_dealers) as rack_dealers,
            material,
            SUM(volume) as volume,
            SUM(value) as value,
            MIN(created_at) as created_at,
            MIN(area_head_code) as area_head_code,
            TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD') as sale_date,
            COUNT(*) as record_count
        FROM sales_data
        WHERE {where_sql}
        GROUP BY division, distributor, location, year, month, day, credit_account, material
        ORDER BY credit_account, TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD')
        """
        
        params_for_query = [start_date, end_date] + params
        
        print(f"🔧 Debug Filter count: {len(where_clauses)} additional filters")
        
        with psycopg2.connect(**SUPABASE_CONFIG) as conn:
            with conn.cursor() as cur:
                # First test without filters to see if date range works
                test_query = f"""
                SELECT COUNT(*) 
                FROM sales_data 
                WHERE TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD') BETWEEN %s AND %s
                """
                cur.execute(test_query, [start_date, end_date])
                test_count = cur.fetchone()[0]
                print(f"🔧 Debug: Records with date range only: {test_count}")
                
                # Now execute the full query
                cur.execute(query, params_for_query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                sales_data = [dict(zip(columns, row)) for row in rows]
                
                print(f"      📊 {period_name} records fetched: {len(sales_data)}")
                
                # Show aggregation summary if we have data
                if sales_data and 'record_count' in sales_data[0]:
                    total_original_records = sum(row.get('record_count', 1) for row in sales_data)
                    aggregated_records = len(sales_data)
                    print(f"      📈 Aggregation: {total_original_records} original → {aggregated_records} summarized")
                    
                    # Check for negative values after aggregation (informational)
                    negative_volume = sum(1 for row in sales_data if row.get('volume') is not None and row.get('volume') < 0)
                    negative_value = sum(1 for row in sales_data if row.get('value') is not None and row.get('value') < 0)
                    zero_volume = sum(1 for row in sales_data if row.get('volume') is not None and row.get('volume') == 0)
                    zero_value = sum(1 for row in sales_data if row.get('value') is not None and row.get('value') == 0)
                    positive_records = len(sales_data) - negative_volume - negative_value
                    print(f"      📊 Data quality: {positive_records} positive, {negative_volume} neg-vol, {negative_value} neg-val, {zero_volume} zero-vol, {zero_value} zero-val")
                
                # If no data found with filters, try with minimal filters (division only)
                if len(sales_data) == 0 and test_count > 0:
                    print(f"⚠️ No data found with full filters. Trying with minimal filters...")
                    
                    # Use only division filter if available
                    minimal_filters = []
                    minimal_params = []
                    
                    if filters.get('divisions'):
                        minimal_filters.append(f"division IN ({','.join(['%s'] * len(filters['divisions']))})")
                        minimal_params.extend(filters['divisions'])
                    
                    # Apply NO calc_column filtering to minimal query - fetch all data
                    if minimal_filters:
                        minimal_where = " AND ".join([date_clause] + minimal_filters)
                    else:
                        minimal_where = date_clause
                    
                    # Aggregated minimal query
                    minimal_query = f"""
                    SELECT 
                        MIN(id) as id,
                        division, distributor, location, year, month, day,
                        credit_account,
                        MIN(customer_name) as customer_name,
                        MIN(region_name) as region_name,
                        MIN(state_name) as state_name,
                        MIN(area_head_name) as area_head_name,
                        MIN(so_name) as so_name,
                        MIN(dealer_type) as dealer_type,
                        MIN(fixed_dealers) as fixed_dealers,
                        MIN(rack_dealers) as rack_dealers,
                        material,
                        SUM(volume) as volume,
                        SUM(value) as value,
                        MIN(created_at) as created_at,
                        MIN(area_head_code) as area_head_code,
                        TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD') as sale_date,
                        COUNT(*) as record_count
                    FROM sales_data
                    WHERE {minimal_where}
                    GROUP BY division, distributor, location, year, month, day, credit_account, material
                    ORDER BY credit_account, TO_DATE(year || '-' || month || '-' || day, 'YYYY-Mon-DD')
                    """
                    
                    minimal_query_params = [start_date, end_date] + minimal_params
                    
                    cur.execute(minimal_query, minimal_query_params)
                    rows = cur.fetchall()
                    sales_data = [dict(zip(columns, row)) for row in rows]
                    
                    print(f"      📊 {period_name} records with minimal filters: {len(sales_data)}")
                    
                    # Show minimal query aggregation summary
                    if sales_data and 'record_count' in sales_data[0]:
                        total_original_records = sum(row.get('record_count', 1) for row in sales_data)
                        aggregated_records = len(sales_data)
                        print(f"      📈 Minimal aggregation: {total_original_records} original → {aggregated_records} summarized")
        
        return sales_data

    def save_period_data_to_file(self, filename, data):
        """Save period data to a CSV file"""
        self._save_to_temp_file(filename, data)
        print(f"💾 {filename}: {len(data)} records saved")

    def fetch_all_sales_with_filters(self, scheme_config, filters):
        """
        Fetch sales data for base period(s) and scheme period SEPARATELY,
        then combine them later.
        """
        base_periods = scheme_config['base_periods']
        calc_mode = scheme_config['calculation_mode']
        scheme_from = scheme_config['scheme_from']
        scheme_to = scheme_config['scheme_to']
        scheme_id = scheme_config['scheme_id']
        
        print(f"🔍 Fetching sales data for each period separately")

        # Fetch Base Period 1 data
        base_period1 = base_periods[0]
        self.base_period1_data = self.fetch_sales_for_period(
            base_period1['from_date'],
            base_period1['to_date'],
            filters,
            calc_mode,
            "Base Period 1"
        )
        print(f"✓ Base period 1 fetched and stored in memory")
        
        # Fetch Base Period 2 data if it exists
        if isinstance(base_periods, list) and len(base_periods) > 1:
            base_period2 = base_periods[1]
            self.base_period2_data = self.fetch_sales_for_period(
                base_period2['from_date'],
                base_period2['to_date'],
                filters,
                calc_mode,
                "Base Period 2"
            )
            print(f"✓ Base period 2 fetched and stored in memory")
        
        # Fetch Scheme Period data
        self.scheme_period_data = self.fetch_sales_for_period(
            scheme_from,
            scheme_to,
            filters,
            calc_mode,
            "Scheme Period"
        )
        print(f"✓ Scheme period fetched and stored in memory")
        
        # Combine all fetched data
        combined_sales = []
        combined_sales.extend(self.base_period1_data)
        if self.base_period2_data:
            combined_sales.extend(self.base_period2_data)
        combined_sales.extend(self.scheme_period_data)
        
        # Store combined data in memory
        self.sales_data = combined_sales
        
        print(f"✅ Combined sales data: {len(combined_sales)} records stored in memory")

        return combined_sales
    
    def save_combined_sales_only(self, scheme_id):
        """Save only the combined sales data to file when specifically requested"""
        if self.sales_data:
            combined_filename = f"{scheme_id}_combined_sales_data.csv"
            self.save_period_data_to_file(combined_filename, self.sales_data)
            print(f"💾 Combined sales data saved to: {combined_filename}")
            return combined_filename
        else:
            print("⚠️ No combined sales data to save")
            return None

    def _save_to_temp_file(self, filename, data):
        if not data:
            print("⚠️ No data to save")
            return
        
        try:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            print(f"📄 CSV saved with {len(df.columns)} columns")
        except Exception as e:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                if data:
                    fieldnames = data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
            print(f"⚠️ Used CSV fallback: {str(e)}")

    def get_stored_sales_data(self):
        return self.sales_data
        
    def get_base_period1_data(self):
        return self.base_period1_data
    
    def get_base_period2_data(self):
        return self.base_period2_data
        
    def get_scheme_period_data(self):
        return self.scheme_period_data

    def get_sales_summary(self):
        if not self.sales_data:
            return "No sales data loaded"
        
        total_records = len(self.sales_data)
        unique_accounts = len(set(row['credit_account'] for row in self.sales_data))
        total_volume = sum(row['volume'] for row in self.sales_data if row['volume'])
        total_value = sum(row['value'] for row in self.sales_data if row['value'])
        dates = [row['sale_date'] for row in self.sales_data if row.get('sale_date')]
        date_range = f"{min(dates)} to {max(dates)}" if dates else "No dates"
        
        # Aggregation statistics
        total_original_records = sum(row.get('record_count', 1) for row in self.sales_data)
        aggregation_ratio = total_original_records / total_records if total_records > 0 else 0
        
        # Data quality checks
        negative_volume_count = sum(1 for row in self.sales_data if row.get('volume') is not None and row.get('volume') < 0)
        negative_value_count = sum(1 for row in self.sales_data if row.get('value') is not None and row.get('value') < 0)
        zero_volume_count = sum(1 for row in self.sales_data if row.get('volume') is not None and row.get('volume') == 0)
        zero_value_count = sum(1 for row in self.sales_data if row.get('value') is not None and row.get('value') == 0)
        
        return {
            'total_records': total_records,
            'total_original_records': total_original_records,
            'aggregation_ratio': round(aggregation_ratio, 2),
            'unique_credit_accounts': unique_accounts,
            'total_volume': total_volume,
            'total_value': total_value,
            'date_range': date_range,
            'data_quality': {
                'negative_volumes': negative_volume_count,
                'negative_values': negative_value_count,
                'zero_volumes': zero_volume_count,
                'zero_values': zero_value_count,
                'clean_records': total_records - negative_volume_count - negative_value_count
            }
        }
