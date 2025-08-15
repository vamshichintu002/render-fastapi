from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TrackerRunRequest(BaseModel):
    scheme_id: str


class TrackerRunResponse(BaseModel):
    success: bool
    message: str
    status: str  # 'completed', 'running', 'failed', 'cancelled'
    scheme_id: str
    total_rows: Optional[int] = None
    total_columns: Optional[int] = None


class TrackerStatusResponse(BaseModel):
    success: bool
    scheme_id: str
    status: Optional[str] = None  # 'running', 'completed', 'failed', 'cancelled'
    run_date: Optional[str] = None
    run_timestamp: Optional[str] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    updated_at: Optional[str] = None
    message: Optional[str] = None