"""
Health check endpoints
"""

from fastapi import APIRouter, HTTPException
from app.database_psycopg2 import database_manager
import asyncio

router = APIRouter()

@router.get("/")
async def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "costing-api"}

@router.get("/database")
async def database_health():
    """Database health check"""
    try:
        is_healthy = await database_manager.health_check()
        if is_healthy:
            return {"status": "healthy", "database": "connected"}
        else:
            raise HTTPException(status_code=503, detail="Database connection failed")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database health check failed: {str(e)}")

@router.get("/detailed")
async def detailed_health():
    """Detailed health check with system info"""
    try:
        db_healthy = await database_manager.health_check()
        pool_status = "initialized" if database_manager.pool is not None else "not_initialized"
        
        return {
            "status": "healthy" if db_healthy else "unhealthy",
            "database": "connected" if db_healthy else "disconnected",
            "database_pool": pool_status,
            "version": "1.0.0",
            "python_version": "3.x",
            "components": {
                "database": "healthy" if db_healthy else "unhealthy",
                "database_pool": pool_status,
                "api": "healthy"
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "database_pool": "error"
        }