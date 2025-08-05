"""
Pydantic models for costing API requests and responses
"""

from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime

class MainSchemeCostingRequest(BaseModel):
    scheme_id: str = Field(..., description="The scheme ID to calculate costing for")

class AdditionalSchemeCostingRequest(BaseModel):
    scheme_id: str = Field(..., description="The scheme ID to calculate costing for")
    scheme_index: int = Field(..., description="The additional scheme index")

class SummaryCostingRequest(BaseModel):
    scheme_id: str = Field(..., description="The scheme ID to calculate summary for")
    scheme_index: Optional[int] = Field(None, description="The scheme index (for additional schemes)")

class CostingRequest(BaseModel):
    scheme_id: str = Field(..., description="The scheme ID")
    scheme_index: Optional[int] = Field(None, description="The scheme index (for additional schemes)")
    calculation_type: str = Field(..., description="Type of calculation (main_value, main_volume, additional_value, additional_volume, summary_main_value, etc.)")

class CostingResponse(BaseModel):
    success: bool = Field(..., description="Whether the calculation was successful")
    data: Dict[str, Any] = Field(..., description="The calculation results")
    message: str = Field(..., description="Success or error message")
    execution_time: float = Field(..., description="Execution time in seconds")
    record_count: int = Field(..., description="Number of records returned")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")

class SchemeComplexityAnalysis(BaseModel):
    scheme_id: str
    volume_value_based: str
    scheme_base: str
    material_count: int
    state_count: int
    complexity_level: str
    estimated_processing_time: str
    recommendations: List[str]

class BatchCostingRequest(BaseModel):
    requests: List[CostingRequest] = Field(..., description="List of costing requests to process")
    parallel: bool = Field(default=True, description="Whether to process requests in parallel")

class BatchCostingResponse(BaseModel):
    success: bool
    results: List[CostingResponse]
    total_requests: int
    successful: int
    failed: int
    total_execution_time: float