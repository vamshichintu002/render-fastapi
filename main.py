"""
FastAPI backend for costing calculations
Handles complex costing functions that timeout in the frontend
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv

from app.routers import costing, health, tracker
from app.database_psycopg2 import database_manager
from app.config import settings

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Costing API...")
    
    try:
        await database_manager.connect()
        logger.info("Database connected successfully")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.warning("API starting without database connection - some endpoints will not work")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Costing API...")
    try:
        await database_manager.disconnect()
        logger.info("Database disconnected")
    except Exception as e:
        logger.error(f"Error during database disconnect: {e}")

# Create FastAPI app
app = FastAPI(
    title="Costing API",
    description="High-performance backend for complex costing calculations",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(costing.router, prefix="/api/costing", tags=["costing"])
app.include_router(tracker.router, prefix="/api/tracker", tags=["tracker"])

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Costing API is running",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )