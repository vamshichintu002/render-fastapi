"""
Costing calculation endpoints
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import asyncio
import logging
from datetime import datetime

from app.database import database_manager
from app.services.costing_service_sync import CostingService
from app.models.costing_models import (
    CostingRequest,
    CostingResponse,
    MainSchemeCostingRequest,
    AdditionalSchemeCostingRequest,
    SummaryCostingRequest
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize costing service
costing_service = CostingService()

@router.post("/main-scheme/value", response_model=CostingResponse)
async def calculate_main_scheme_value_costing(request: MainSchemeCostingRequest):
    """Calculate value-based main scheme costing"""
    try:
        logger.info(f"Processing main scheme value costing for scheme_id: {request.scheme_id}")
        
        result = await costing_service.calculate_main_scheme_value_costing(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme value costing calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in main scheme value costing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/main-scheme/volume", response_model=CostingResponse)
async def calculate_main_scheme_volume_costing(request: MainSchemeCostingRequest):
    """Calculate volume-based main scheme costing"""
    try:
        logger.info(f"Processing main scheme volume costing for scheme_id: {request.scheme_id}")
        
        result = await costing_service.calculate_main_scheme_volume_costing(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme volume costing calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in main scheme volume costing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/additional-scheme/value", response_model=CostingResponse)
async def calculate_additional_scheme_value_costing(request: AdditionalSchemeCostingRequest):
    """Calculate value-based additional scheme costing"""
    try:
        logger.info(f"Processing additional scheme value costing for scheme_id: {request.scheme_id}, scheme_index: {request.scheme_index}")
        
        result = await costing_service.calculate_additional_scheme_value_costing(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme value costing calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in additional scheme value costing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/additional-scheme/volume", response_model=CostingResponse)
async def calculate_additional_scheme_volume_costing(request: AdditionalSchemeCostingRequest):
    """Calculate volume-based additional scheme costing"""
    try:
        logger.info(f"Processing additional scheme volume costing for scheme_id: {request.scheme_id}, scheme_index: {request.scheme_index}")
        
        result = await costing_service.calculate_additional_scheme_volume_costing(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme volume costing calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in additional scheme volume costing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summary/main-scheme/value", response_model=CostingResponse)
async def calculate_main_scheme_value_summary(request: MainSchemeCostingRequest):
    """Calculate value-based main scheme summary"""
    try:
        logger.info(f"Processing main scheme value summary for scheme_id: {request.scheme_id}")
        
        result = await costing_service.calculate_main_scheme_value_summary(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme value summary calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in main scheme value summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summary/main-scheme/volume", response_model=CostingResponse)
async def calculate_main_scheme_volume_summary(request: MainSchemeCostingRequest):
    """Calculate volume-based main scheme summary"""
    try:
        logger.info(f"Processing main scheme volume summary for scheme_id: {request.scheme_id}")
        
        result = await costing_service.calculate_main_scheme_volume_summary(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme volume summary calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in main scheme volume summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summary/additional-scheme/value", response_model=CostingResponse)
async def calculate_additional_scheme_value_summary(request: AdditionalSchemeCostingRequest):
    """Calculate value-based additional scheme summary"""
    try:
        logger.info(f"Processing additional scheme value summary for scheme_id: {request.scheme_id}, scheme_index: {request.scheme_index}")
        
        result = await costing_service.calculate_additional_scheme_value_summary(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme value summary calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in additional scheme value summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/summary/additional-scheme/volume", response_model=CostingResponse)
async def calculate_additional_scheme_volume_summary(request: AdditionalSchemeCostingRequest):
    """Calculate volume-based additional scheme summary"""
    try:
        logger.info(f"Processing additional scheme volume summary for scheme_id: {request.scheme_id}, scheme_index: {request.scheme_index}")
        
        result = await costing_service.calculate_additional_scheme_volume_summary(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme volume summary calculated successfully",
            execution_time=result.get('execution_time', 0),
            record_count=len(result.get('data', []))
        )
        
    except Exception as e:
        logger.error(f"Error in additional scheme volume summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scheme/{scheme_id}/complexity")
async def analyze_scheme_complexity(scheme_id: str):
    """Analyze scheme complexity to predict processing time"""
    try:
        complexity = await costing_service.analyze_scheme_complexity(scheme_id)
        return complexity
    except Exception as e:
        logger.error(f"Error analyzing scheme complexity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch")
async def batch_costing_calculation(requests: List[CostingRequest]):
    """Process multiple costing calculations in batch"""
    try:
        logger.info(f"Processing batch costing calculation with {len(requests)} requests")
        
        results = await costing_service.process_batch_calculations(requests)
        
        return {
            "success": True,
            "results": results,
            "total_requests": len(requests),
            "successful": len([r for r in results if r.get('success', False)]),
            "failed": len([r for r in results if not r.get('success', False)])
        }
        
    except Exception as e:
        logger.error(f"Error in batch costing calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))