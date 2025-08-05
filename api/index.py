"""
Vercel serverless function entry point for FastAPI application
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sys
import os
import logging
import asyncio
from contextlib import asynccontextmanager

# Add the parent directory to the Python path so we can import our app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global database manager instance
database_manager = None

try:
    from app.routers import costing, health
    from app.config import settings
    from app.database_psycopg2 import database_manager as db_manager
    database_manager = db_manager
except ImportError as e:
    logger.error(f"Import error: {e}")
    # Fallback configuration for serverless
    class FallbackSettings:
        ALLOWED_ORIGINS = ["*"]
    settings = FallbackSettings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for serverless - initialize database connection"""
    # Startup
    logger.info("Starting Costing API on Vercel...")
    
    if database_manager:
        try:
            await database_manager.connect()
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            logger.warning("API starting without database connection")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Costing API...")
    if database_manager:
        try:
            await database_manager.disconnect()
            logger.info("Database disconnected")
        except Exception as e:
            logger.error(f"Error during database disconnect: {e}")

# Create FastAPI app for serverless deployment with lifespan
app = FastAPI(
    title="Costing API",
    description="High-performance backend for complex costing calculations",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, 'ALLOWED_ORIGINS', ["*"]),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Middleware to ensure database connection on each request
@app.middleware("http")
async def ensure_database_connection(request: Request, call_next):
    """Ensure database is connected for each request"""
    if database_manager and not database_manager.pool:
        try:
            logger.info("Initializing database connection for request...")
            await database_manager.connect()
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
    
    response = await call_next(request)
    return response

# Try to include routers, but handle gracefully if they fail
try:
    app.include_router(health.router, prefix="/api/health", tags=["health"])
    app.include_router(costing.router, prefix="/api/costing", tags=["costing"])
except Exception as e:
    logger.warning(f"Could not include routers: {e}")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Costing API is running on Vercel",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/api")
async def api_root():
    """API root endpoint"""
    return {
        "message": "Costing API is running on Vercel",
        "version": "1.0.0",
        "endpoints": {
            "health": "/api/health",
            "costing": "/api/costing",
            "docs": "/docs"
        }
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for serverless"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"}
    )