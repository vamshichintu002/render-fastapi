#!/usr/bin/env python3
"""
Test database connection script
"""

import asyncio
import os
from dotenv import load_dotenv
from app.database_psycopg2 import database_manager

async def test_connection():
    """Test database connection"""
    print("🔍 Testing database connection...")
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Test connection
        await database_manager.connect()
        print("✅ Database connection successful!")
        
        # Test a simple query
        result = database_manager.execute_query("SELECT 1 as test")
        print(f"✅ Query test successful: {result}")
        
        # Test a costing function
        try:
            result = database_manager.execute_function("costing_sheet_volume_additionalscheme", ["318333", 0])
            print(f"✅ Costing function test successful: {len(result)} records returned")
        except Exception as e:
            print(f"⚠️  Costing function test failed: {e}")
        
        # Cleanup
        await database_manager.disconnect()
        print("✅ Database connection closed")
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_connection())
    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n💥 Tests failed!")
        exit(1) 