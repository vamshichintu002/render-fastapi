"""
Costing calculation endpoints
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import asyncio
import logging
import time
from datetime import datetime

from app.database import database_manager
from app.services.costing_service_sync import CostingService
from app.services.optimized_costing_service import OptimizedCostingService
from app.services.vectorized_costing_service import VectorizedCostingService
from app.models.costing_models import (
    CostingRequest,
    CostingResponse,
    MainSchemeCostingRequest,
    AdditionalSchemeCostingRequest,
    SummaryCostingRequest
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize costing services
costing_service = CostingService()
optimized_costing_service = OptimizedCostingService()
vectorized_costing_service = VectorizedCostingService()

# Performance optimization flags
USE_OPTIMIZED_SERVICE = False  # DISABLED - Testing vectorized service only
USE_VECTORIZED_SERVICE = True   # ENABLED - Using only vectorized service for performance testing

def get_best_service():
    """Get the best available service based on performance flags"""
    if USE_VECTORIZED_SERVICE:
        return vectorized_costing_service, "vectorized"
    elif USE_OPTIMIZED_SERVICE:
        return optimized_costing_service, "optimized"
    else:
        return costing_service, "original"

@router.post("/main-scheme/value", response_model=CostingResponse)
async def calculate_main_scheme_value_costing(request: MainSchemeCostingRequest):
    """Calculate value-based main scheme costing"""
    try:
        logger.info(f"Processing main scheme value costing for scheme_id: {request.scheme_id}")
        
        # Use best available service
        service, service_type = get_best_service()
        
        # Use vectorized method if available
        if service_type == "vectorized" and hasattr(service, 'calculate_main_scheme_value_costing_vectorized'):
            result = await service.calculate_main_scheme_value_costing_vectorized(request.scheme_id)
        else:
            result = await service.calculate_main_scheme_value_costing(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme value costing calculated successfully ({service_type})",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_main_scheme_volume_costing(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme volume costing calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_additional_scheme_value_costing(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme value costing calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_additional_scheme_volume_costing(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme volume costing calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_main_scheme_value_summary(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme value summary calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_main_scheme_volume_summary(request.scheme_id)
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Main scheme volume summary calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_additional_scheme_value_summary(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme value summary calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        result = await service.calculate_additional_scheme_volume_summary(
            request.scheme_id, 
            request.scheme_index
        )
        
        return CostingResponse(
            success=True,
            data=result,
            message=f"Additional scheme volume summary calculated successfully {'(optimized)' if USE_OPTIMIZED_SERVICE else ''}",
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
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        complexity = await service.analyze_scheme_complexity(scheme_id)
        return complexity
    except Exception as e:
        logger.error(f"Error analyzing scheme complexity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scheme/{scheme_id}/additional-schemes")
async def get_additional_schemes(scheme_id: str):
    """Get additional schemes information for a given scheme ID"""
    try:
        logger.info(f"Fetching additional schemes for scheme_id: {scheme_id}")
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        schemes = await service.get_additional_schemes(scheme_id)
        
        return {
            "status": "success",
            "data": schemes
        }
    except Exception as e:
        logger.error(f"Error getting additional schemes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/set-no-timeout")
async def set_no_timeout():
    """Set no timeout for database operations"""
    try:
        logger.info("Setting no timeout for database operations")
        
        # This is a placeholder - in a real implementation, you might set a session variable
        # For now, we'll just return success
        return {
            "status": "success",
            "message": "No timeout set for this session"
        }
    except Exception as e:
        logger.error(f"Error setting no timeout: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scheme/{scheme_id}/exists")
async def check_scheme_exists(scheme_id: str):
    """Check if a scheme exists"""
    try:
        logger.info(f"Checking if scheme exists: {scheme_id}")
        
        # Check if scheme exists in the database
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        exists = await service.check_scheme_exists(scheme_id)
        
        return {
            "status": "success",
            "exists": exists,
            "scheme_id": scheme_id
        }
    except Exception as e:
        logger.error(f"Error checking scheme existence: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch")
async def batch_costing_calculation(requests: List[CostingRequest]):
    """Process multiple costing calculations in batch"""
    try:
        logger.info(f"Processing batch costing calculation with {len(requests)} requests")
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        results = await service.process_batch_calculations(requests)
        
        return {
            "success": True,
            "results": results,
            "total_requests": len(requests),
            "successful": len([r for r in results if r.get('success', False)]),
            "failed": len([r for r in results if not r.get('success', False)]),
            "using_optimized": USE_OPTIMIZED_SERVICE
        }
        
    except Exception as e:
        logger.error(f"Error in batch costing calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/toggle-optimization")
async def toggle_optimization():
    """Toggle between optimized and original costing service"""
    global USE_OPTIMIZED_SERVICE
    USE_OPTIMIZED_SERVICE = not USE_OPTIMIZED_SERVICE
    
    return {
        "status": "success",
        "message": f"Switched to {'optimized' if USE_OPTIMIZED_SERVICE else 'original'} costing service",
        "using_optimized": USE_OPTIMIZED_SERVICE
    }

@router.get("/optimization-status")
async def get_optimization_status():
    """Get current optimization status"""
    service, service_type = get_best_service()
    return {
        "using_optimized": USE_OPTIMIZED_SERVICE,
        "using_vectorized": USE_VECTORIZED_SERVICE,
        "active_service": service_type,
        "performance_level": "ultra-high" if USE_VECTORIZED_SERVICE else ("high" if USE_OPTIMIZED_SERVICE else "standard")
    }

@router.post("/toggle-vectorization")
async def toggle_vectorization():
    """Toggle vectorized processing (Numba JIT compilation)"""
    global USE_VECTORIZED_SERVICE
    USE_VECTORIZED_SERVICE = not USE_VECTORIZED_SERVICE
    
    service, service_type = get_best_service()
    
    return {
        "status": "success",
        "message": f"Switched to {service_type} costing service",
        "using_vectorized": USE_VECTORIZED_SERVICE,
        "using_optimized": USE_OPTIMIZED_SERVICE,
        "active_service": service_type,
        "performance_boost": "numba_jit_vectorization" if USE_VECTORIZED_SERVICE else "python_optimization"
    }

@router.post("/batch-parallel")
async def batch_costing_calculation_parallel(requests: List[CostingRequest]):
    """Process multiple costing calculations with true parallelism"""
    try:
        logger.info(f"Processing parallel batch costing calculation with {len(requests)} requests")
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        
        # Use parallel processing method if available
        if hasattr(service, 'process_parallel_calculations'):
            results = await service.process_parallel_calculations(requests)
        else:
            results = await service.process_batch_calculations(requests)
        
        return {
            "success": True,
            "results": results,
            "total_requests": len(requests),
            "successful": len([r for r in results if r.get('success', False)]),
            "failed": len([r for r in results if not r.get('success', False)]),
            "using_optimized": USE_OPTIMIZED_SERVICE,
            "processing_type": "parallel"
        }
        
    except Exception as e:
        logger.error(f"Error in parallel batch costing calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cache-status")
async def get_cache_status():
    """Get cache status information"""
    try:
        if USE_OPTIMIZED_SERVICE and hasattr(optimized_costing_service, '_data_cache'):
            cache_info = {
                "cached_datasets": len(optimized_costing_service._data_cache),
                "cache_ttl_seconds": optimized_costing_service._cache_ttl,
                "max_cache_size": optimized_costing_service._max_cache_size,
                "cache_keys": list(optimized_costing_service._data_cache.keys())
            }
        else:
            cache_info = {
                "cached_datasets": 0,
                "cache_ttl_seconds": 0,
                "max_cache_size": 0,
                "cache_keys": []
            }
        
        return {
            "status": "success",
            "cache_info": cache_info,
            "using_optimized": USE_OPTIMIZED_SERVICE
        }
        
    except Exception as e:
        logger.error(f"Error getting cache status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/clear-cache")
async def clear_cache():
    """Clear the data cache"""
    try:
        if USE_OPTIMIZED_SERVICE and hasattr(optimized_costing_service, '_data_cache'):
            with optimized_costing_service._cache_lock:
                cache_size = len(optimized_costing_service._data_cache)
                optimized_costing_service._data_cache.clear()
            
            return {
                "status": "success",
                "message": f"Cleared {cache_size} cache entries",
                "using_optimized": USE_OPTIMIZED_SERVICE
            }
        else:
            return {
                "status": "success",
                "message": "No cache to clear",
                "using_optimized": USE_OPTIMIZED_SERVICE
            }
        
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class CalculateAllRequest(BaseModel):
    calculation_types: Optional[List[str]] = None

@router.post("/scheme/{scheme_id}/calculate-all")
async def calculate_all_schemes_parallel(scheme_id: str, request: CalculateAllRequest = None):
    """
    Discover all schemes (main + additional) and execute all calculations in parallel
    
    Args:
        scheme_id: The main scheme ID
        calculation_types: Optional list of calculation types to run. 
                          Defaults to ['main_value', 'main_volume', 'additional_value', 'additional_volume']
    """
    try:
        logger.info(f"Starting parallel calculation for all schemes under scheme_id: {scheme_id}")
        
        calculation_types = request.calculation_types if request and request.calculation_types else ['main_value', 'main_volume', 'additional_value', 'additional_volume']
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        
        # Step 1: Get all additional schemes
        logger.info(f"Discovering additional schemes for {scheme_id}")
        additional_schemes_result = await service.get_additional_schemes(scheme_id)
        
        # Handle different response formats
        if isinstance(additional_schemes_result, dict):
            additional_schemes = additional_schemes_result.get('data', [])
        elif isinstance(additional_schemes_result, list):
            additional_schemes = additional_schemes_result
        else:
            additional_schemes = []
        
        logger.info(f"Found {len(additional_schemes)} additional schemes")
        
        # Step 2: Build all calculation requests
        all_requests = []
        
        # Add main scheme calculations
        for calc_type in calculation_types:
            if calc_type.startswith('main_'):
                all_requests.append({
                    "scheme_id": scheme_id,
                    "calculation_type": calc_type,
                    "scheme_index": None,
                    "scheme_name": "Main Scheme"
                })
        
        # Add additional scheme calculations
        for idx, additional_scheme in enumerate(additional_schemes):
            for calc_type in calculation_types:
                if calc_type.startswith('additional_'):
                    all_requests.append({
                        "scheme_id": scheme_id,
                        "calculation_type": calc_type,
                        "scheme_index": idx,
                        "scheme_name": additional_scheme.get('scheme_name', f'Additional Scheme {idx}')
                    })
        
        logger.info(f"Generated {len(all_requests)} total calculation requests")
        
        # Step 3: Execute all calculations in parallel
        start_time = time.time()
        
        if hasattr(service, 'process_parallel_calculations'):
            # Use the optimized parallel processing
            results = await service.process_parallel_calculations([
                CostingRequest(**req) for req in all_requests
            ])
        else:
            # Fallback to batch processing
            results = await service.process_batch_calculations([
                CostingRequest(**req) for req in all_requests
            ])
        
        total_time = time.time() - start_time
        
        # Step 4: Organize results by scheme and calculation type
        organized_results = {
            "main_scheme": {},
            "additional_schemes": {}
        }
        
        successful_count = 0
        failed_count = 0
        
        for result in results:
            if result.get('success', False):
                successful_count += 1
                request_info = result['request']
                calc_type = request_info['calculation_type']
                scheme_idx = request_info.get('scheme_index')
                
                if scheme_idx is None:  # Main scheme
                    organized_results["main_scheme"][calc_type] = result['result']
                else:  # Additional scheme
                    if scheme_idx not in organized_results["additional_schemes"]:
                        organized_results["additional_schemes"][scheme_idx] = {}
                    organized_results["additional_schemes"][scheme_idx][calc_type] = result['result']
            else:
                failed_count += 1
        
        return {
            "success": True,
            "scheme_id": scheme_id,
            "total_schemes": 1 + len(additional_schemes),
            "main_scheme_count": 1,
            "additional_schemes_count": len(additional_schemes),
            "calculation_types": calculation_types,
            "total_calculations": len(all_requests),
            "successful_calculations": successful_count,
            "failed_calculations": failed_count,
            "total_execution_time": total_time,
            "using_optimized": USE_OPTIMIZED_SERVICE,
            "processing_type": "parallel_discovery",
            "results": organized_results,
            "raw_results": results if failed_count > 0 else None  # Include raw results if there were failures
        }
        
    except Exception as e:
        logger.error(f"Error in parallel all-scheme calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/scheme/{scheme_id}/calculate-all-summary")
async def calculate_all_schemes_summary_parallel(scheme_id: str):
    """
    Calculate all summary reports (main + additional schemes) in parallel
    """
    try:
        logger.info(f"Starting parallel summary calculation for all schemes under scheme_id: {scheme_id}")
        
        service = optimized_costing_service if USE_OPTIMIZED_SERVICE else costing_service
        
        # Step 1: Get all additional schemes
        additional_schemes_result = await service.get_additional_schemes(scheme_id)
        
        # Handle different response formats
        if isinstance(additional_schemes_result, dict):
            additional_schemes = additional_schemes_result.get('data', [])
        elif isinstance(additional_schemes_result, list):
            additional_schemes = additional_schemes_result
        else:
            additional_schemes = []
        
        # Step 2: Build summary calculation requests
        all_requests = []
        
        # Main scheme summaries
        all_requests.extend([
            {"scheme_id": scheme_id, "calculation_type": "summary_main_value", "scheme_index": None},
            {"scheme_id": scheme_id, "calculation_type": "summary_main_volume", "scheme_index": None}
        ])
        
        # Additional scheme summaries
        for idx in range(len(additional_schemes)):
            all_requests.extend([
                {"scheme_id": scheme_id, "calculation_type": "summary_additional_value", "scheme_index": idx},
                {"scheme_id": scheme_id, "calculation_type": "summary_additional_volume", "scheme_index": idx}
            ])
        
        # Step 3: Execute in parallel
        start_time = time.time()
        
        if hasattr(service, 'process_parallel_calculations'):
            results = await service.process_parallel_calculations([
                CostingRequest(**req) for req in all_requests
            ])
        else:
            results = await service.process_batch_calculations([
                CostingRequest(**req) for req in all_requests
            ])
        
        total_time = time.time() - start_time
        
        # Step 4: Organize summary results
        summary_results = {
            "main_scheme": {},
            "additional_schemes": {}
        }
        
        successful_count = sum(1 for r in results if r.get('success', False))
        
        for result in results:
            if result.get('success', False):
                request_info = result['request']
                calc_type = request_info['calculation_type']
                scheme_idx = request_info.get('scheme_index')
                
                if scheme_idx is None:  # Main scheme
                    summary_results["main_scheme"][calc_type] = result['result']
                else:  # Additional scheme
                    if scheme_idx not in summary_results["additional_schemes"]:
                        summary_results["additional_schemes"][scheme_idx] = {}
                    summary_results["additional_schemes"][scheme_idx][calc_type] = result['result']
        
        return {
            "success": True,
            "scheme_id": scheme_id,
            "total_schemes": 1 + len(additional_schemes),
            "total_summaries": len(all_requests),
            "successful_summaries": successful_count,
            "total_execution_time": total_time,
            "using_optimized": USE_OPTIMIZED_SERVICE,
            "processing_type": "parallel_summary",
            "summaries": summary_results
        }
        
    except Exception as e:
        logger.error(f"Error in parallel summary calculation: {e}")
        raise HTTPException(status_code=500, detail=str(e))