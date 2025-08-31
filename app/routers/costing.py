"""
Costing Sheet Calculator API endpoints
Using the new costing_sheet_main.py implementation
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import asyncio
import logging
from datetime import datetime
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from costing_sheet import CostingSheetCalculator

logger = logging.getLogger(__name__)
router = APIRouter()


# Pydantic models for API requests and responses
class CostingRequest(BaseModel):
    scheme_id: str


class CostingResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    execution_time_seconds: Optional[float] = None
    output_file: Optional[str] = None
    processing_time_seconds: Optional[float] = None
    error_message: Optional[str] = None
    calculation_summary: Optional[Dict[str, Any]] = None
    data_summary: Optional[Dict[str, Any]] = None
    output_info: Optional[Dict[str, Any]] = None


@router.post("/calculate", response_model=CostingResponse)
async def calculate_costing_sheet(request: CostingRequest, background_tasks: BackgroundTasks):
    """
    Calculate costing sheet for a given scheme ID using the new implementation
    """
    try:
        logger.info(f"Starting costing sheet calculation for scheme_id: {request.scheme_id}")

        # Initialize the calculator
        calculator = CostingSheetCalculator()

        # Validate scheme first
        logger.info("Validating scheme requirements...")
        validation = calculator.validate_scheme_requirements(request.scheme_id)

        if not validation['is_valid']:
            error_msg = f"Scheme validation failed: {', '.join(validation['errors'])}"
            logger.error(error_msg)
            return CostingResponse(
                success=False,
                message="Scheme validation failed",
                error_message=error_msg,
                data=validation
            )

        # Perform costing calculation
        logger.info("Starting costing sheet calculation...")

        # Run calculation in background for better performance
        def run_calculation():
            return calculator.calculate_costing_sheet(request.scheme_id)

        # For now, run synchronously (can be made async later)
        result = run_calculation()

        if result['status'] == 'success':
            logger.info(f"Costing sheet calculation completed successfully for scheme_id: {request.scheme_id}")

            return CostingResponse(
                success=True,
                message="Costing calculation completed successfully",
                data=result,
                execution_time_seconds=result.get('processing_time_seconds'),
                output_file=result.get('output_file'),  # Will be None in API mode
                processing_time_seconds=result.get('processing_time_seconds'),
                calculation_summary=result.get('calculation_summary'),
                data_summary=result.get('data_summary'),
                output_info=result.get('output_info')
            )
        else:
            error_msg = result.get('error_message', 'Unknown error occurred')
            logger.error(f"Costing sheet calculation failed: {error_msg}")
            return CostingResponse(
                success=False,
                message="Costing sheet calculation failed",
                error_message=error_msg,
                data=result
            )

    except Exception as e:
        error_msg = f"Unexpected error in costing sheet calculation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return CostingResponse(
            success=False,
            message="Internal server error",
            error_message=error_msg
        )


@router.get("/scheme/{scheme_id}/validate", response_model=CostingResponse)
async def validate_scheme(scheme_id: str):
    """
    Validate scheme requirements without performing full calculation
    """
    try:
        logger.info(f"Validating scheme requirements for scheme_id: {scheme_id}")

        # Initialize calculator and validate
        calculator = CostingSheetCalculator()
        validation = calculator.validate_scheme_requirements(scheme_id)

        return CostingResponse(
            success=validation['is_valid'],
            message="Scheme validation completed",
            data=validation
        )

    except Exception as e:
        error_msg = f"Error validating scheme: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return CostingResponse(
            success=False,
            message="Scheme validation failed",
            error_message=error_msg
        )


@router.get("/scheme/{scheme_id}/summary")
async def get_costing_summary(scheme_id: str):
    """
    Get summary information about a scheme's costing data
    """
    try:
        logger.info(f"Getting costing summary for scheme_id: {scheme_id}")

        # Initialize calculator
        calculator = CostingSheetCalculator()

        # Validate scheme first
        validation = calculator.validate_scheme_requirements(scheme_id)
        if not validation['is_valid']:
            return CostingResponse(
                success=False,
                message="Scheme validation failed",
                error_message=f"Invalid scheme: {', '.join(validation['errors'])}",
                data=validation
            )

        # Get data summary
        data_summary = calculator.get_data_summary()

        return CostingResponse(
            success=True,
            message="Costing summary retrieved successfully",
            data=data_summary
        )

    except Exception as e:
        error_msg = f"Error getting costing summary: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return CostingResponse(
            success=False,
            message="Failed to get costing summary",
            error_message=error_msg
        )


@router.get("/demo")
async def demo_mode():
    """
    Run demo mode to show system capabilities
    """
    try:
        logger.info("Running costing sheet calculator demo")

        demo_info = {
            "system_capabilities": [
                "✅ Base period data processing",
                "✅ Memory-based calculations",
                "✅ Simplified cost estimation",
                "✅ Main + Additional schemes",
                "✅ Target volume/value calculations",
                "✅ Professional Excel output",
                "✅ Summary and metadata sheets"
            ],
            "calculation_scope": {
                "columns": "Start through Target Volume/Target Value",
                "data_source": "Base period only (no scheme period)",
                "purpose": "Cost estimation and validation",
                "output": "Single Excel file with multiple sheets"
            },
            "technical_features": [
                "Memory-only processing (no intermediate files)",
                "Vectorized calculations for performance",
                "Automatic scheme validation",
                "Professional Excel formatting",
                "Error handling and recovery"
            ],
            "cost_estimation_focus": [
                "Simplified calculation model",
                "Base data validation",
                "Target projection analysis",
                "Multi-scheme comparison"
            ],
            "use_cases": [
                "Scheme cost estimation",
                "Budget planning and validation",
                "Quick scheme analysis",
                "Cost comparison between schemes"
            ],
            "output_structure": {
                "Sheet 1": "Main costing calculations",
                "Sheet 2": "Summary analysis",
                "Sheet 3": "Metadata and configuration"
            }
        }

        return CostingResponse(
            success=True,
            message="Costing Sheet Calculator Demo",
            data=demo_info
        )

    except Exception as e:
        error_msg = f"Error in demo mode: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return CostingResponse(
            success=False,
            message="Demo mode failed",
            error_message=error_msg
        )