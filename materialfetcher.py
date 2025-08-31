import psycopg2
import pandas as pd
import os
from supabaseconfig import SUPABASE_CONFIG

class MaterialFetcher:
    def __init__(self):
        self.materials_data = None
        # Create output directory for CSV files
        self.output_dir = os.path.join(os.getcwd())
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def fetch_all_material_master(self):
        """Fetch complete material_master table and store in memory"""
        
        print("🔍 Fetching material master data...")
        
        query = "SELECT * FROM material_master"
        
        with psycopg2.connect(**SUPABASE_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                # Get column names
                columns = [desc[0] for desc in cur.description]
                
                # Convert to list of dictionaries
                self.materials_data = [dict(zip(columns, row)) for row in rows]
                
                print(f"✅ Fetched {len(self.materials_data)} records from material_master")
                print(f"📄 Columns: {columns}")
        
        return self.materials_data

    def save_materials_to_csv(self, scheme_id):
        """Save material master data to CSV file in output directory"""
        if not self.materials_data:
            print("⚠️ No material data to save")
            return
        
        # Create full file path
        filename = f"{scheme_id}_material_master.csv"
        full_path = os.path.join(self.output_dir, filename)
        
        try:
            df = pd.DataFrame(self.materials_data)
            df.to_csv(full_path, index=False)
            print(f"💾 Material master saved to: {full_path}")
            return full_path
        except Exception as e:
            import csv
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                if self.materials_data:
                    fieldnames = self.materials_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.materials_data)
            print(f"💾 Material master saved to: {full_path} (CSV fallback)")
            return full_path

    def get_stored_materials(self):
        return self.materials_data

    def get_materials_summary(self):
        if not self.materials_data:
            return "No material data loaded"
        
        total_materials = len(self.materials_data)
        unique_categories = len(set(row.get('category', 'Unknown') for row in self.materials_data))
        
        return {
            'total_materials': total_materials,
            'unique_categories': unique_categories,
            'columns': list(self.materials_data[0].keys()) if self.materials_data else []
        }
