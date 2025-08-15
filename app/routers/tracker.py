from fastapi import APIRouter, HTTPException, BackgroundTasks
from ..models.tracker_models import TrackerRunRequest, TrackerRunResponse, TrackerStatusResponse
from ..services.tracker_service import TrackerService
import asyncio

router = APIRouter(tags=["tracker"])

tracker_service = TrackerService()


@router.post("/run", response_model=TrackerRunResponse)
async def run_tracker(request: TrackerRunRequest):
    """
    Run tracker for a given scheme ID.
    
    This endpoint is optimized for Vercel's function timeout constraints.
    If the process takes too long, it will return immediately with a 'processing' status.
    """
    try:
        if not request.scheme_id or not request.scheme_id.strip():
            raise HTTPException(status_code=400, detail="scheme_id is required and cannot be empty")
        
        result = await tracker_service.run_tracker(request.scheme_id.strip())
        
        return TrackerRunResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/status/{scheme_id}", response_model=TrackerStatusResponse)
async def get_tracker_status(scheme_id: str):
    """
    Get the status of a tracker run for a given scheme ID.
    """
    try:
        if not scheme_id or not scheme_id.strip():
            raise HTTPException(status_code=400, detail="scheme_id is required and cannot be empty")
        
        result = await tracker_service.get_tracker_status(scheme_id.strip())
        
        return TrackerStatusResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/debug/scheme-config/{scheme_id}")
async def debug_scheme_config(scheme_id: str):
    """
    Debug endpoint to check scheme configuration lookup.
    """
    try:
        if not scheme_id or not scheme_id.strip():
            raise HTTPException(status_code=400, detail="scheme_id is required and cannot be empty")
        
        result = await tracker_service.debug_scheme_config(scheme_id.strip())
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/debug/test-db")
async def debug_test_db():
    """
    Debug endpoint to test database connection.
    """
    try:
        result = await tracker_service.debug_test_db()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/run-background")
async def run_tracker_background(request: TrackerRunRequest, background_tasks: BackgroundTasks):
    """
    Run tracker in the background (alternative endpoint for long-running processes).
    
    This endpoint immediately returns and processes the tracker in the background.
    Use the /status endpoint to check the progress.
    """
    try:
        if not request.scheme_id or not request.scheme_id.strip():
            raise HTTPException(status_code=400, detail="scheme_id is required and cannot be empty")
        
        # Mark as processing immediately
        await tracker_service._mark_tracker_as_processing(request.scheme_id.strip())
        
        # Add to background tasks
        background_tasks.add_task(run_tracker_background_task, request.scheme_id.strip())
        
        return {
            "success": True,
            "message": f"Tracker for scheme {request.scheme_id} has been queued for background processing.",
            "status": "running",
            "scheme_id": request.scheme_id.strip()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def run_tracker_background_task(scheme_id: str):
    """Background task to run tracker without timeout constraints"""
    try:
        # Run the tracker without timeout constraints
        result = await tracker_service._execute_tracker(scheme_id)
        print(f"Background tracker completed for scheme {scheme_id}: {result}")
    except Exception as e:
        print(f"Background tracker failed for scheme {scheme_id}: {str(e)}")
        # Mark as error in database
        try:
            await tracker_service._mark_tracker_as_error(scheme_id, str(e))
        except:
            pass