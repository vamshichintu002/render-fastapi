#!/usr/bin/env python3
"""
Startup script for the NEW Costing API using SchemeProcessor
"""

import uvicorn
import os

if __name__ == "__main__":
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Environment variables loaded")
    except ImportError:
        print("ℹ️  python-dotenv not available, continuing without .env")
    
    print("🚀 Starting NEW Costing API v2.0 with SchemeProcessor")
    print("📋 Features:")
    print("  ✅ Complete calculation engine from astra-main")
    print("  ✅ No Excel file saving - JSON responses only")
    print("  ✅ No Streamlit dependencies")
    print("  ✅ Memory-based processing")
    print("  ✅ Vectorized calculations")
    print("  ✅ FastAPI interface")
    print()
    print("🌐 Server: http://localhost:8000")
    print("📖 API Docs: http://localhost:8000/docs")
    print("🔧 Test endpoint: http://localhost:8000/test")
    print()
    
    uvicorn.run(
        "fastapi_app:app",  # Use our new FastAPI app
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True
    )