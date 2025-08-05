"""
Synchronous costing calculation service using psycopg2
Based on the working Python script provided
"""

import logging
from typing import Dict, Any, List, Optional
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

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
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def _execute_function_sync(self, function_name: str, params: List[Any]) -> Dict[str, Any]:
        """Execute function synchronously"""
        start_time = time.time()
        
        try:
            logger.info(f"Executing {function_name} with params: {params}")
            
            data = database_manager.execute_function(function_name, params)
            
            execution_time = time.time() - start_time
            
            return {
                'data': data,
                'execution_time': execution_time,
                'function_name': function_name,
                'params': params
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in {function_name}: {e}")
            raise Exception(f"{function_name} failed after {execution_time:.2f}s: {str(e)}")
    
    async def calculate_main_scheme_value_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme costing"""
        function_name = self.function_mapping['main_value']
        params = [scheme_id]
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        return result
    
    async def calculate_main_scheme_volume_costing(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme costing"""
        function_name = self.function_mapping['main_volume']
        params = [scheme_id]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        return result
    
    async def calculate_additional_scheme_value_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme costing"""
        function_name = self.function_mapping['additional_value']
        params = [scheme_id, scheme_index]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        result['scheme_index'] = scheme_index
        return result
    
    async def calculate_additional_scheme_volume_costing(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme costing"""
        function_name = self.function_mapping['additional_volume']
        params = [scheme_id, scheme_index]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        result['scheme_index'] = scheme_index
        return result
    
    async def calculate_main_scheme_value_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate value-based main scheme summary"""
        function_name = self.function_mapping['summary_main_value']
        params = [scheme_id]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        return result
    
    async def calculate_main_scheme_volume_summary(self, scheme_id: str) -> Dict[str, Any]:
        """Calculate volume-based main scheme summary"""
        function_name = self.function_mapping['summary_main_volume']
        params = [scheme_id]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        return result
    
    async def calculate_additional_scheme_value_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate value-based additional scheme summary"""
        function_name = self.function_mapping['summary_additional_value']
        params = [scheme_id, scheme_index]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        result['scheme_index'] = scheme_index
        return result
    
    async def calculate_additional_scheme_volume_summary(self, scheme_id: str, scheme_index: int) -> Dict[str, Any]:
        """Calculate volume-based additional scheme summary"""
        function_name = self.function_mapping['summary_additional_volume']
        params = [scheme_id, scheme_index]
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor, 
            self._execute_function_sync, 
            function_name, 
            params
        )
        
        result['scheme_id'] = scheme_id
        result['scheme_index'] = scheme_index
        return result
    
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
            WHERE scheme_id = %s
            """
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                database_manager.execute_query,
                query,
                [int(scheme_id)]
            )
            
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