"""
Database connection and management
"""

import asyncpg
import logging
from typing import Optional, Dict, Any, List
from app.config import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create database connection pool"""
        try:
            logger.info("Connecting to database...")
            logger.info(f"Using connection URL: {settings.DATABASE_URL[:50]}...")
            
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=300,  # 5 minutes
                server_settings={
                    'statement_timeout': '0',  # No timeout
                    'idle_in_transaction_session_timeout': '0'
                }
            )
            logger.info("Database pool created successfully")
            
            # Test the connection
            async with self.pool.acquire() as connection:
                result = await connection.fetchval("SELECT 1")
                if result == 1:
                    logger.info("Database connection test successful")
                else:
                    raise Exception("Database connection test failed")
                    
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def execute_function(
        self, 
        function_name: str, 
        params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute a PostgreSQL function with parameters"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        try:
            async with self.pool.acquire() as connection:
                # Set no timeout for this connection
                await connection.execute("SET statement_timeout = 0")
                
                # Build the function call
                param_placeholders = ", ".join([f"${i+1}" for i in range(len(params))])
                query = f"SELECT * FROM {function_name}({param_placeholders})"
                
                # Execute the function
                logger.info(f"Executing function: {function_name} with params: {params}")
                start_time = asyncio.get_event_loop().time()
                
                rows = await connection.fetch(query, *params.values())
                
                end_time = asyncio.get_event_loop().time()
                execution_time = end_time - start_time
                
                logger.info(f"Function {function_name} completed in {execution_time:.2f}s, returned {len(rows)} rows")
                
                # Convert rows to dictionaries
                result = [dict(row) for row in rows]
                return result
                
        except Exception as e:
            logger.error(f"Error executing function {function_name}: {e}")
            raise
    
    async def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        try:
            async with self.pool.acquire() as connection:
                await connection.execute("SET statement_timeout = 0")
                
                if params:
                    rows = await connection.fetch(query, *params)
                else:
                    rows = await connection.fetch(query)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Check database connection health"""
        try:
            if not self.pool:
                return False
            
            async with self.pool.acquire() as connection:
                await connection.fetchval("SELECT 1")
                return True
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global database manager instance
database_manager = DatabaseManager()