"""
Core costing calculation service
Handles all costing function calls with optimized performance
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import time

from app.database_psycopg2 import database_manager
from app.models.costing_models import CostingRequest, SchemeComplexityAnalysis

logger = logging.getLogger(__name__)

class CostingService:
    """Service for handling costing calculations"""
    
    def __init__(self):
        self.function_mapping = {
            'main_value': 'costing_sheet_value_mainscheme',
            'main_volume': 'costing_sheet_volume_mainscheme',
            'additional_value': 'costing_sheet_value_additionalscheme',
            'additional_volume': 'costing_sheet_volume_additionalscheme',
            'summary_main_value': 'summary_costing_sheet_value_mainscheme',
            'summary_main_volume': 'summary_costing_sheet_volume_mainscheme',
            'summary_additional_value': 'summary_costing_sheet_value_additionalscheme',
            'summary_additional_volume': 'summary_costing_sheet_volume_additionalscheme'
        }
    
    async def calculate_main_scheme_value_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme costing"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['main_value']
            params = {'p_scheme_id': scheme_id}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in main scheme value costing: {e}")
            raise Exception(f"Main scheme value costing failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_main_scheme_volume_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme costing"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['main_volume']
            params = {'p_scheme_id': scheme_id}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in main scheme volume costing: {e}")
            raise Exception(f"Main scheme volume costing failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_additional_scheme_value_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme costing"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['additional_value']
            params = {'p_scheme_id': scheme_id, 'p_scheme_index': scheme_index}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}, index {scheme_index}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id,
                'scheme_index': scheme_index
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in additional scheme value costing: {e}")
            raise Exception(f"Additional scheme value costing failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_additional_scheme_volume_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme costing"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['additional_volume']
            params = {'p_scheme_id': scheme_id, 'p_scheme_index': scheme_index}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}, index {scheme_index}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id,
                'scheme_index': scheme_index
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in additional scheme volume costing: {e}")
            raise Exception(f"Additional scheme volume costing failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_main_scheme_value_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme summary"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['summary_main_value']
            params = {'p_scheme_id': scheme_id}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in main scheme value summary: {e}")
            raise Exception(f"Main scheme value summary failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_main_scheme_volume_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme summary"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['summary_main_volume']
            params = {'p_scheme_id': scheme_id}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in main scheme volume summary: {e}")
            raise Exception(f"Main scheme volume summary failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_additional_scheme_value_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme summary"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['summary_additional_value']
            params = {'p_scheme_id': scheme_id, 'p_scheme_index': scheme_index}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}, index {scheme_index}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id,
                'scheme_index': scheme_index
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in additional scheme value summary: {e}")
            raise Exception(f"Additional scheme value summary failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_additional_scheme_volume_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme summary"""
        start_time = time.time()
        
        try:
            function_name = self.function_mapping['summary_additional_volume']
            params = {'p_scheme_id': scheme_id, 'p_scheme_index': scheme_index}
            
            logger.info(f"Executing {function_name} for scheme {scheme_id}, index {scheme_index}")
            
            data = await database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'scheme_id': scheme_id,
                'scheme_index': scheme_index
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in additional scheme volume summary: {e}")
            raise Exception(f"Additional scheme volume summary failed after {execution_time:.2f}s: {str(e)}")
    
    async def analyze_scheme_complexity(self, scheme_id: str) -> SchemeComplexityAnalysis:
        """Analyze scheme complexity to predict processing time"""
        try:
            query = """
            SELECT 
                scheme_json->'mainScheme'->>'volumeValueBased' as volume_value_based,
                scheme_json->'mainScheme'->>'schemeBase' as scheme_base,
                jsonb_array_length(COALESCE(scheme_json->'mainScheme'->'schemeApplicable'->'selectedStates', '[]'::jsonb)) as state_count,
                jsonb_array_length(COALESCE(scheme_json->'mainScheme'->'productData'->'materials', '[]'::jsonb)) as material_count
            FROM schemes_data 
            WHERE scheme_id = $1
            """
            
            result = await database_manager.execute_query(query, [int(scheme_id)])
            
            if not result:
                raise Exception(f"Scheme {scheme_id} not found")
            
            data = result[0]
            
            volume_value_based = data.get('volume_value_based', 'volume')
            scheme_base = data.get('scheme_base', 'target-based')
            material_count = data.get('material_count', 0)
            state_count = data.get('state_count', 0)
            
            # Determine complexity level
            complexity_level = 'low'
            estimated_time = '< 5 seconds'
            recommendations = []
            
            if volume_value_based == 'value':
                if material_count > 1000:
                    complexity_level = 'extreme'
                    estimated_time = '5+ minutes'
                    recommendations.extend([
                        'Consider reducing product selection to < 500 materials',
                        'Switch to volume-based calculations for better performance',
                        'Run calculations during off-peak hours'
                    ])
                elif material_count > 500:
                    complexity_level = 'high'
                    estimated_time = '1-3 minutes'
                    recommendations.extend([
                        'Consider reducing product selection to < 200 materials',
                        'Monitor calculation progress'
                    ])
                elif material_count > 100:
                    complexity_level = 'medium'
                    estimated_time = '15-60 seconds'
                    recommendations.append('Calculation may take longer than usual')
            else:
                if material_count > 2000:
                    complexity_level = 'high'
                    estimated_time = '30-60 seconds'
                elif material_count > 1000:
                    complexity_level = 'medium'
                    estimated_time = '10-30 seconds'
            
            if state_count > 10:
                recommendations.append('Multiple states selected may increase processing time')
            
            return SchemeComplexityAnalysis(
                scheme_id=scheme_id,
                volume_value_based=volume_value_based,
                scheme_base=scheme_base,
                material_count=material_count,
                state_count=state_count,
                complexity_level=complexity_level,
                estimated_processing_time=estimated_time,
                recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Error analyzing scheme complexity: {e}")
            raise Exception(f"Failed to analyze scheme complexity: {str(e)}")
    
    async def process_batch_calculations(self, requests: List[CostingRequest]) -> List[Dict[str, Any]]:
        """Process multiple costing calculations in batch"""
        logger.info(f"Processing batch of {len(requests)} costing calculations")
        
        async def process_single_request(request: CostingRequest) -> Dict[str, Any]:
            try:
                if request.calculation_type == 'main_value':
                    result = await self.calculate_main_scheme_value_costing(request.scheme_id)
                elif request.calculation_type == 'main_volume':
                    result = await self.calculate_main_scheme_volume_costing(request.scheme_id)
                elif request.calculation_type == 'additional_value':
                    result = await self.calculate_additional_scheme_value_costing(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'additional_volume':
                    result = await self.calculate_additional_scheme_volume_costing(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'summary_main_value':
                    result = await self.calculate_main_scheme_value_summary(request.scheme_id)
                elif request.calculation_type == 'summary_main_volume':
                    result = await self.calculate_main_scheme_volume_summary(request.scheme_id)
                elif request.calculation_type == 'summary_additional_value':
                    result = await self.calculate_additional_scheme_value_summary(request.scheme_id, request.scheme_index)
                elif request.calculation_type == 'summary_additional_volume':
                    result = await self.calculate_additional_scheme_volume_summary(request.scheme_id, request.scheme_index)
                else:
                    raise Exception(f"Unknown calculation type: {request.calculation_type}")
                
                return {
                    'success': True,
                    'request': request.dict(),
                    'result': result
                }
                
            except Exception as e:
                logger.error(f"Error processing request {request.dict()}: {e}")
                return {
                    'success': False,
                    'request': request.dict(),
                    'error': str(e)
                }
        
        # Process all requests concurrently
        tasks = [process_single_request(request) for request in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({
                    'success': False,
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results