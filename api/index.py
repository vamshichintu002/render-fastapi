"""
Vercel serverless function entry point for FastAPI application
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sys
import os
import logging

# Add the parent directory to the Python path so we can import our app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.routers import costing, health
    from app.config import settings
except ImportError as e:
    logging.error(f"Import error: {e}")
    # Fallback configuration for serverless
    class FallbackSettings:
        ALLOWED_ORIGINS = ["*"]
    settings = FallbackSettings()

# Create FastAPI app for serverless deployment (no lifespan for serverless)
app = FastAPI(
    title="Costing API",
    description="High-performance backend for complex costing calculations",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, 'ALLOWED_ORIGINS', ["*"]),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Try to include routers, but handle gracefully if they fail
try:
    app.include_router(health.router, prefix="/api/health", tags=["health"])
    app.include_router(costing.router, prefix="/api/costing", tags=["costing"])
except Exception as e:
    logging.warning(f"Could not include routers: {e}")

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
    logging.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )