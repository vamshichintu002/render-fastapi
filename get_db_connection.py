#!/usr/bin/env python3
"""
Script to help get the correct database connection string for Supabase
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_supabase_connection_info():
    """Extract connection info from Supabase URL"""
    supabase_url = os.getenv("SUPABASE_URL", "")
    
    if not supabase_url:
        print("❌ SUPABASE_URL not found in .env file")
        return
    
    # Extract project reference from URL
    # Format: https://PROJECT_REF.supabase.co
    if "supabase.co" in supabase_url:
        project_ref = supabase_url.replace("https://", "").replace(".supabase.co", "")
        
        print("🔍 Supabase Connection Information")
        print("=" * 50)
        print(f"Project Reference: {project_ref}")
        print(f"Supabase URL: {supabase_url}")
        print()
        
        print("📝 Database Connection Options:")
        print()
        
        print("1. Direct Connection (Recommended for development):")
        print(f"   postgresql://postgres:[YOUR_PASSWORD]@db.{project_ref}.supabase.co:5432/postgres")
        print()
        
        print("2. Connection Pooling (Recommended for production):")
        print(f"   postgresql://postgres.{project_ref}:[YOUR_PASSWORD]@aws-0-ap-south-1.pooler.supabase.com:6543/postgres")
        print()
        
        print("3. Session Mode (Alternative):")
        print(f"   postgresql://postgres.{project_ref}:[YOUR_PASSWORD]@aws-0-ap-south-1.pooler.supabase.com:5432/postgres")
        print()
        
        print("🔑 To get your database password:")
        print("1. Go to your Supabase dashboard")
        print("2. Navigate to Settings > Database")
        print("3. Look for 'Connection string' or 'Database password'")
        print("4. Copy the password and replace [YOUR_PASSWORD] in the connection string above")
        print()
        
        print("📋 Update your .env file with:")
        print(f"DATABASE_URL=postgresql://postgres:[YOUR_PASSWORD]@db.{project_ref}.supabase.co:5432/postgres")
        print()
        
        # Generate a sample .env update
        sample_connection = f"postgresql://postgres:YOUR_PASSWORD_HERE@db.{project_ref}.supabase.co:5432/postgres"
        
        print("💡 Quick fix for your .env file:")
        print(f"Replace the DATABASE_URL line with:")
        print(f"DATABASE_URL={sample_connection}")
        
    else:
        print("❌ Invalid Supabase URL format")

if __name__ == "__main__":
    get_supabase_connection_info()