"""
FastAPI Application for Costing Sheet API
Uses the SchemeProcessor from astra-main for complete calculation functionality
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging
import time
from datetime import datetime
import sys
import os

# Import the SchemeProcessor from main.py
from main import SchemeProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Costing Sheet API",
    description="High-performance costing sheet calculations using SchemeProcessor",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Pydantic models
class CostingRequest(BaseModel):
    scheme_id: str

class CostingResponse(BaseModel):
    success: bool
    message: str
    scheme_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    execution_time_seconds: Optional[float] = None
    error_message: Optional[str] = None
    summary: Optional[Dict[str, Any]] = None

class ValidationResponse(BaseModel):
    success: bool
    message: str
    scheme_id: str
    validation_details: Dict[str, Any]

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Costing Sheet API v2.0 - Using SchemeProcessor",
        "version": "2.0.0",
        "endpoints": {
            "calculate": "POST /calculate",
            "validate": "GET /validate/{scheme_id}",
            "summary": "GET /summary/{scheme_id}",
            "health": "GET /health"
        }
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "processor": "SchemeProcessor"
    }

# Main calculation endpoint
@app.post("/calculate", response_model=CostingResponse)
async def calculate_costing_sheet(request: CostingRequest):
    """
    Calculate costing sheet using the full SchemeProcessor from astra-main
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting costing calculation for scheme_id: {request.scheme_id}")
        
        # Initialize SchemeProcessor
        processor = SchemeProcessor()
        
        # Process the scheme (this runs all calculations)
        success = processor.process_scheme(request.scheme_id)
        
        if not success:
            return CostingResponse(
                success=False,
                message="Scheme processing failed",
                scheme_id=request.scheme_id,
                error_message="SchemeProcessor returned failure status"
            )
        
        # Get calculation results as JSON
        result_data = processor.get_calculation_results_json(request.scheme_id)
        
        if not result_data:
            return CostingResponse(
                success=False,
                message="No calculation results available",
                scheme_id=request.scheme_id,
                error_message="get_calculation_results_json returned None"
            )
        
        execution_time = time.time() - start_time
        
        # Get additional summary data
        stored_data = processor.get_stored_data()
        calc_summary = processor.get_calculation_summary()
        
        summary = {
            "execution_time_seconds": execution_time,
            "total_records": result_data['summary']['total_records'],
            "total_columns": result_data['summary']['total_columns'],
            "sales_records": len(stored_data.get('combined_sales_data', [])),
            "material_records": len(stored_data.get('material_master_data', [])),
            "calculation_summary": calc_summary
        }
        
        logger.info(f"Costing calculation completed successfully in {execution_time:.2f}s")
        
        return CostingResponse(
            success=True,
            message="Costing calculation completed successfully",
            scheme_id=request.scheme_id,
            data=result_data,
            execution_time_seconds=execution_time,
            summary=summary
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Error in costing calculation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return CostingResponse(
            success=False,
            message="Costing calculation failed",
            scheme_id=request.scheme_id,
            execution_time_seconds=execution_time,
            error_message=error_msg
        )

# Validation endpoint
@app.get("/validate/{scheme_id}")
async def validate_scheme(scheme_id: str):
    """
    Validate scheme without running full calculation
    """
    try:
        logger.info(f"Validating scheme: {scheme_id}")
        
        # Basic validation (in real implementation, check database/JSON availability)
        if not scheme_id or not scheme_id.strip():
            return ValidationResponse(
                success=False,
                message="Invalid scheme ID",
                scheme_id=scheme_id,
                validation_details={"errors": ["Scheme ID is required"]}
            )
        
        # For now, return basic validation
        validation_details = {
            "scheme_id": scheme_id,
            "is_numeric": scheme_id.isdigit(),
            "length": len(scheme_id),
            "validation_timestamp": datetime.now().isoformat()
        }
        
        return ValidationResponse(
            success=True,
            message="Scheme validation completed",
            scheme_id=scheme_id,
            validation_details=validation_details
        )
        
    except Exception as e:
        error_msg = f"Error validating scheme: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return ValidationResponse(
            success=False,
            message="Scheme validation failed",
            scheme_id=scheme_id,
            validation_details={"error": error_msg}
        )

# Summary endpoint (light processing)
@app.get("/summary/{scheme_id}")
async def get_scheme_summary(scheme_id: str):
    """
    Get summary information about a scheme (without full calculation)
    """
    try:
        logger.info(f"Getting summary for scheme: {scheme_id}")
        
        summary_data = {
            "scheme_id": scheme_id,
            "summary_timestamp": datetime.now().isoformat(),
            "capabilities": [
                "Base period data processing",
                "Memory-based calculations", 
                "Vectorized calculations",
                "Multi-scheme support",
                "Advanced calculation modules",
                "JSON output format"
            ],
            "calculation_modules": [
                "Base calculations",
                "Growth calculations", 
                "Target and actual calculations",
                "Enhanced costing tracker fields",
                "Comprehensive tracker fixes",
                "Scheme final payout",
                "Conditional payout calculations",
                "Estimated calculations",
                "Rewards calculations",
                "Percentage conversions"
            ]
        }
        
        return CostingResponse(
            success=True,
            message="Summary retrieved successfully",
            scheme_id=scheme_id,
            data=summary_data
        )
        
    except Exception as e:
        error_msg = f"Error getting summary: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return CostingResponse(
            success=False,
            message="Failed to get summary",
            scheme_id=scheme_id,
            error_message=error_msg
        )

# Test endpoint
@app.get("/test")
async def test_api():
    """
    Test endpoint to verify API functionality
    """
    return {
        "message": "Costing Sheet API is running",
        "processor": "SchemeProcessor from astra-main",
        "features": [
            "✅ Complete calculation engine from astra-main",
            "✅ No Excel file saving - JSON responses only",
            "✅ No Streamlit dependencies", 
            "✅ Memory-based processing",
            "✅ Vectorized calculations",
            "✅ FastAPI interface"
        ],
        "timestamp": datetime.now().isoformat()
    }

# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return CostingResponse(
        success=False,
        message="Internal server error",
        error_message=str(exc)
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)