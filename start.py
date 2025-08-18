#!/usr/bin/env python3
"""
Startup script for the Costing API
"""

import uvicorn
import os
import logging
from app.config import settings

class HealthCheckFilter(logging.Filter):
    """Filter out health check requests from access logs"""
    def filter(self, record):
        # Filter out health check endpoints completely
        if hasattr(record, 'msg'):
            msg = str(record.msg)
            # Block all health check related logs
            if any(pattern in msg for pattern in ['/health', 'GET /health/', 'health']):
                return False
        
        # Also check record message and args
        if hasattr(record, 'getMessage'):
            try:
                full_msg = record.getMessage()
                if any(pattern in full_msg for pattern in ['/health', 'GET /health/', 'health']):
                    return False
            except:
                pass
                
        return True

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Configure logging to exclude health checks from all loggers
    health_filter = HealthCheckFilter()
    
    # Apply to uvicorn access logs
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.addFilter(health_filter)
    
    # Apply to uvicorn general logs
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.addFilter(health_filter)
    
    # Apply to root logger to catch any other health logs
    root_logger = logging.getLogger()
    root_logger.addFilter(health_filter)
    
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
        access_log=True,
        workers=2  # 2 concurrent requests per machine
    )