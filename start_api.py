#!/usr/bin/env python3
"""
Startup script for the new Costing API using SchemeProcessor from astra-main
"""

import uvicorn
import os
import sys

def start_api():
    """Start the FastAPI server"""
    print("🚀 Starting Costing API v2.0 with SchemeProcessor")
    print("📋 Features:")
    print("  ✅ Complete calculation engine from astra-main")
    print("  ✅ No Excel file saving - JSON responses only")
    print("  ✅ No Streamlit dependencies")
    print("  ✅ Memory-based processing")
    print("  ✅ Vectorized calculations")
    print("  ✅ FastAPI interface")
    print()
    
    # Load environment variables if .env exists
    try:
        from dotenv import load_dotenv
        if os.path.exists('.env'):
            load_dotenv()
            print("✅ Environment variables loaded")
    except ImportError:
        print("ℹ️  python-dotenv not installed, skipping .env loading")
    
    print("🌐 Starting server on http://localhost:8000")
    print("📖 API Documentation: http://localhost:8000/docs")
    print()
    
    # Start the server
    uvicorn.run(
        "fastapi_app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    start_api()