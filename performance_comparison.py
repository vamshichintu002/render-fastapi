"""
Performance comparison script between original and optimized costing services
"""

import asyncio
import time
import logging
from typing import Dict, Any
import json

from app.services.costing_service_sync import CostingService
from app.services.optimized_costing_service import OptimizedCostingService
from app.database_psycopg2 import database_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceComparison:
    def __init__(self):
        self.original_service = CostingService()
        self.optimized_service = OptimizedCostingService()
    
    async def setup(self):
        """Initialize database connections"""
        await database_manager.connect()
    
    async def compare_calculation(self, scheme_id: str, calculation_type: str = "main_value") -> Dict[str, Any]:
        """Compare performance between original and optimized services"""
        
        results = {
            "scheme_id": scheme_id,
            "calculation_type": calculation_type,
            "original": {},
            "optimized": {},
            "improvement": {}
        }
        
        try:
            # Test original service
            logger.info(f"Testing original service for {calculation_type}")
            start_time = time.time()
            
            if calculation_type == "main_value":
                original_result = await self.original_service.calculate_main_scheme_value_costing(scheme_id)
            elif calculation_type == "main_volume":
                original_result = await self.original_service.calculate_main_scheme_volume_costing(scheme_id)
            else:
                raise ValueError(f"Unsupported calculation type: {calculation_type}")
            
            original_time = time.time() - start_time
            
            results["original"] = {
                "execution_time": original_time,
                "record_count": len(original_result.get('data', [])),
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Original service failed: {e}")
            results["original"] = {
                "execution_time": 0,
                "record_count": 0,
                "success": False,
                "error": str(e)
            }
        
        try:
            # Test optimized service
            logger.info(f"Testing optimized service for {calculation_type}")
            start_time = time.time()
            
            if calculation_type == "main_value":
                optimized_result = await self.optimized_service.calculate_main_scheme_value_costing(scheme_id)
            elif calculation_type == "main_volume":
                optimized_result = await self.optimized_service.calculate_main_scheme_volume_costing(scheme_id)
            else:
                raise ValueError(f"Unsupported calculation type: {calculation_type}")
            
            optimized_time = time.time() - start_time
            
            results["optimized"] = {
                "execution_time": optimized_time,
                "record_count": len(optimized_result.get('data', [])),
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Optimized service failed: {e}")
            results["optimized"] = {
                "execution_time": 0,
                "record_count": 0,
                "success": False,
                "error": str(e)
            }
        
        # Calculate improvements
        if results["original"]["success"] and results["optimized"]["success"]:
            original_time = results["original"]["execution_time"]
            optimized_time = results["optimized"]["execution_time"]
            
            if original_time > 0:
                speed_improvement = (original_time - optimized_time) / original_time * 100
                speed_multiplier = original_time / optimized_time if optimized_time > 0 else float('inf')
            else:
                speed_improvement = 0
                speed_multiplier = 1
            
            results["improvement"] = {
                "speed_improvement_percent": round(speed_improvement, 2),
                "speed_multiplier": round(speed_multiplier, 2),
                "time_saved_seconds": round(original_time - optimized_time, 2)
            }
        
        return results
    
    async def run_comprehensive_test(self, scheme_ids: list) -> Dict[str, Any]:
        """Run comprehensive performance tests"""
        
        all_results = []
        summary = {
            "total_tests": 0,
            "successful_tests": 0,
            "total_time_saved": 0,
            "average_improvement": 0,
            "best_improvement": 0,
            "worst_improvement": 0
        }
        
        for scheme_id in scheme_ids:
            for calc_type in ["main_value", "main_volume"]:
                try:
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Testing Scheme: {scheme_id}, Type: {calc_type}")
                    logger.info(f"{'='*50}")
                    
                    result = await self.compare_calculation(scheme_id, calc_type)
                    all_results.append(result)
                    
                    summary["total_tests"] += 1
                    
                    if result["original"]["success"] and result["optimized"]["success"]:
                        summary["successful_tests"] += 1
                        improvement = result["improvement"]
                        
                        summary["total_time_saved"] += improvement["time_saved_seconds"]
                        
                        if summary["successful_tests"] == 1:
                            summary["best_improvement"] = improvement["speed_improvement_percent"]
                            summary["worst_improvement"] = improvement["speed_improvement_percent"]
                        else:
                            summary["best_improvement"] = max(summary["best_improvement"], improvement["speed_improvement_percent"])
                            summary["worst_improvement"] = min(summary["worst_improvement"], improvement["speed_improvement_percent"])
                        
                        # Print individual results
                        logger.info(f"Original Time: {result['original']['execution_time']:.2f}s")
                        logger.info(f"Optimized Time: {result['optimized']['execution_time']:.2f}s")
                        logger.info(f"Speed Improvement: {improvement['speed_improvement_percent']:.2f}%")
                        logger.info(f"Speed Multiplier: {improvement['speed_multiplier']:.2f}x")
                        logger.info(f"Time Saved: {improvement['time_saved_seconds']:.2f}s")
                    
                except Exception as e:
                    logger.error(f"Test failed for {scheme_id} {calc_type}: {e}")
        
        # Calculate average improvement
        if summary["successful_tests"] > 0:
            improvements = [r["improvement"]["speed_improvement_percent"] 
                          for r in all_results 
                          if r["original"]["success"] and r["optimized"]["success"]]
            summary["average_improvement"] = sum(improvements) / len(improvements) if improvements else 0
        
        return {
            "summary": summary,
            "detailed_results": all_results
        }
    
    def print_summary_report(self, test_results: Dict[str, Any]):
        """Print a formatted summary report"""
        summary = test_results["summary"]
        
        print(f"\n{'='*60}")
        print(f"PERFORMANCE COMPARISON SUMMARY")
        print(f"{'='*60}")
        print(f"Total Tests Run: {summary['total_tests']}")
        print(f"Successful Tests: {summary['successful_tests']}")
        print(f"Success Rate: {(summary['successful_tests']/summary['total_tests']*100):.1f}%" if summary['total_tests'] > 0 else "N/A")
        print(f"\nPERFORMANCE IMPROVEMENTS:")
        print(f"Average Speed Improvement: {summary['average_improvement']:.2f}%")
        print(f"Best Speed Improvement: {summary['best_improvement']:.2f}%")
        print(f"Worst Speed Improvement: {summary['worst_improvement']:.2f}%")
        print(f"Total Time Saved: {summary['total_time_saved']:.2f} seconds")
        print(f"{'='*60}")
        
        # Print individual test results
        print(f"\nDETAILED RESULTS:")
        for result in test_results["detailed_results"]:
            if result["original"]["success"] and result["optimized"]["success"]:
                print(f"\nScheme {result['scheme_id']} ({result['calculation_type']}):")
                print(f"  Original: {result['original']['execution_time']:.2f}s")
                print(f"  Optimized: {result['optimized']['execution_time']:.2f}s")
                print(f"  Improvement: {result['improvement']['speed_improvement_percent']:.2f}% ({result['improvement']['speed_multiplier']:.2f}x faster)")
            else:
                print(f"\nScheme {result['scheme_id']} ({result['calculation_type']}): FAILED")
                if not result["original"]["success"]:
                    print(f"  Original Error: {result['original'].get('error', 'Unknown')}")
                if not result["optimized"]["success"]:
                    print(f"  Optimized Error: {result['optimized'].get('error', 'Unknown')}")

async def main():
    """Main function to run performance comparison"""
    
    # Test scheme IDs (you can modify these based on your actual data)
    test_scheme_ids = ["1", "2", "3"]  # Add actual scheme IDs from your database
    
    comparator = PerformanceComparison()
    
    try:
        await comparator.setup()
        
        logger.info("Starting comprehensive performance comparison...")
        results = await comparator.run_comprehensive_test(test_scheme_ids)
        
        # Print summary report
        comparator.print_summary_report(results)
        
        # Save results to file
        with open("performance_results.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info("Performance comparison completed. Results saved to performance_results.json")
        
    except Exception as e:
        logger.error(f"Performance comparison failed: {e}")
    finally:
        await database_manager.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
