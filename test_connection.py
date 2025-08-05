#!/usr/bin/env python3
"""
Test database connection script
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_connection():
    """Test database connection with different methods"""
    
    database_url = os.getenv("DATABASE_URL", "")
    
    if not database_url:
        print("❌ DATABASE_URL not found in .env file")
        return
    
    print(f"🔍 Testing database connection...")
    print(f"Connection string: {database_url[:50]}...")
    
    # Test different connection methods
    connection_attempts = [
        ("Direct connection", database_url.replace("pooler.supabase.com:6543", "supabase.co:5432").replace("postgres.ozgkgkenzpngnptdqbqf:", "postgres:")),
        ("Pooler connection", database_url),
        ("No SSL", database_url + "?sslmode=disable"),
        ("Require SSL", database_url + "?sslmode=require")
    ]
    
    for name, connection_url in connection_attempts:
        try:
            print(f"\n🔄 Trying {name}...")
            
            # Test basic connection
            conn = await asyncpg.connect(connection_url)
            
            # Test a simple query
            result = await conn.fetchval("SELECT 1")
            
            if result == 1:
                print(f"✅ {name} - SUCCESS!")
                
                # Test if we can access our functions
                try:
                    functions_result = await conn.fetch("""
                        SELECT routine_name 
                        FROM information_schema.routines 
                        WHERE routine_name LIKE '%costing_sheet%' 
                        LIMIT 3
                    """)
                    
                    if functions_result:
                        print(f"   📋 Found {len(functions_result)} costing functions")
                        for func in functions_result:
                            print(f"      - {func['routine_name']}")
                    else:
                        print("   ⚠️  No costing functions found")
                        
                except Exception as func_error:
                    print(f"   ⚠️  Could not check functions: {func_error}")
                
                await conn.close()
                print(f"   🎯 Use this connection string in your .env file:")
                print(f"   DATABASE_URL={connection_url}")
                return connection_url
            
        except Exception as e:
            print(f"❌ {name} - FAILED: {e}")
            continue
    
    print("\n❌ All connection attempts failed!")
    print("\n💡 Troubleshooting steps:")
    print("1. Check your Supabase dashboard for the correct connection string")
    print("2. Verify your database password is correct")
    print("3. Check if your IP is whitelisted in Supabase settings")
    print("4. Try connecting from Supabase SQL editor to verify database is accessible")
    
    return None

if __name__ == "__main__":
    asyncio.run(test_connection())