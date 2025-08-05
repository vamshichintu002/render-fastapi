#!/usr/bin/env python3
"""
Startup script for the Costing API
"""

import uvicorn
import os
from app.config import settings

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    print(f"🚀 Starting Costing API on {settings.API_HOST}:{settings.API_PORT}")
    print(f"📊 Debug mode: {settings.DEBUG}")
    print(f"🔗 Database URL: {settings.DATABASE_URL[:50]}...")
    print(f"🌐 Allowed origins: {settings.ALLOWED_ORIGINS}")
    
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info",
        access_log=True
    )