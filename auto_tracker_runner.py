#!/usr/bin/env python3
"""
Automated Tracker Runner for Finance-Approved Schemes
This script automatically generates tracker data for all finance-approved schemes
that don't have tracker data yet.
"""

import psycopg2
import pandas as pd
import sys
import time
from datetime import datetime
from tracker_runner import (
    fetch_scheme_config, 
    build_queries_from_templates, 
    run_multiple_queries_and_combine,
    db_params
)

def get_finance_approved_schemes_without_tracker_data():
    """Get all finance-approved schemes that don't have tracker data yet."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        query = """
        SELECT sd.scheme_id, 
               sd.scheme_json->'basicInfo'->>'schemeTitle' as scheme_title
        FROM schemes_data sd
        WHERE sd.status = 'finance_approved'
          AND NOT EXISTS (
            SELECT 1 FROM scheme_tracker_runs str 
            WHERE str.scheme_id = sd.scheme_id 
            AND str.run_status = 'completed'
            AND str.tracker_data IS NOT NULL
          )
        ORDER BY sd.fad_reviewed_at DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        print(f"❌ Error fetching finance-approved schemes: {e}")
        return []

def process_scheme_tracker(scheme_id, scheme_title):
    """Process tracker data for a single scheme."""
    print(f"\n🔄 Processing tracker for Scheme {scheme_id}: {scheme_title}")
    
    try:
        conn = psycopg2.connect(**db_params)
        scheme_config_df = fetch_scheme_config(conn, scheme_id)
        conn.close()
        
        if scheme_config_df.empty:
            print(f"⚠️  No scheme configuration found for scheme {scheme_id}")
            return False
        
        queries, scheme_names = build_queries_from_templates(scheme_id, scheme_config_df)
        success = run_multiple_queries_and_combine(queries, scheme_names, scheme_config_df)
        
        if success:
            print(f"✅ Successfully generated tracker data for scheme {scheme_id}")
            return True
        else:
            print(f"❌ Failed to generate tracker data for scheme {scheme_id}")
            return False
            
    except Exception as e:
        print(f"❌ Error processing scheme {scheme_id}: {e}")
        return False

def main():
    """Main function to process all finance-approved schemes."""
    print("🚀 Starting Automated Tracker Runner for Finance-Approved Schemes")
    print("=" * 60)
    
    # Get all finance-approved schemes without tracker data
    schemes = get_finance_approved_schemes_without_tracker_data()
    
    if not schemes:
        print("✅ No finance-approved schemes found that need tracker data generation.")
        return
    
    print(f"📋 Found {len(schemes)} finance-approved schemes that need tracker data:")
    for scheme_id, scheme_title in schemes:
        print(f"   - Scheme {scheme_id}: {scheme_title}")
    
    print("\n🔄 Starting processing...")
    
    success_count = 0
    failed_count = 0
    
    for scheme_id, scheme_title in schemes:
        try:
            if process_scheme_tracker(scheme_id, scheme_title):
                success_count += 1
            else:
                failed_count += 1
                
            # Add a small delay between processing to avoid overwhelming the database
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n⚠️  Process interrupted by user")
            break
        except Exception as e:
            print(f"❌ Unexpected error processing scheme {scheme_id}: {e}")
            failed_count += 1
    
    print("\n" + "=" * 60)
    print("📊 Processing Summary:")
    print(f"   ✅ Successfully processed: {success_count} schemes")
    print(f"   ❌ Failed to process: {failed_count} schemes")
    print(f"   📋 Total schemes: {len(schemes)}")
    
    if success_count > 0:
        print(f"\n🎉 {success_count} schemes now have tracker data and will appear in the dashboard!")

if __name__ == "__main__":
    main()