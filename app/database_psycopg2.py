"""
Database connection using psycopg2 (synchronous) instead of asyncpg
Based on the working Python script provided
"""

import psycopg2
import psycopg2.pool
import logging
import os
from typing import Optional, Dict, Any, List
from app.config import settings
import threading
import time

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._lock = threading.Lock()
    
    async def connect(self):
        """Create database connection pool using psycopg2"""
        try:
            # Use environment variables for database connection
            db_params = {
                "dbname": os.getenv("DATABASE_NAME", "postgres"),
                "user": os.getenv("DATABASE_USER", "postgres.ozgkgkenzpngnptdqbqf"),
                "password": os.getenv("DATABASE_PASSWORD", "PwbcCdD?Yq4Jn.v"),
                "host": os.getenv("DATABASE_HOST", "aws-0-ap-south-1.pooler.supabase.com"),
                "port": int(os.getenv("DATABASE_PORT", "5432"))
            }
            
            logger.info(f"Connecting to database at {db_params['host']}:{db_params['port']}")
            
            # Create connection pool
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                **db_params
            )
            
            # Test the connection
            conn = self.pool.getconn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result[0] == 1:
                    logger.info("Database connection test successful")
                else:
                    raise Exception("Database connection test failed")
                cur.close()
            finally:
                self.pool.putconn(conn)
            
            logger.info("Database pool created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database pool closed")
    
    def execute_function(
        self, 
        function_name: str, 
        params: List[Any]
    ) -> List[Dict[str, Any]]:
        """Execute a PostgreSQL function with parameters (synchronous)"""
        if not self.pool:
            logger.error("Database pool is None - attempting to reconnect...")
            # Try to reconnect if pool is None
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in an async context, can't call async connect from sync method
                    raise RuntimeError("Database pool not initialized - server may not be properly started")
                else:
                    # We can try to reconnect
                    asyncio.run(self.connect())
            except Exception as e:
                logger.error(f"Failed to reconnect to database: {e}")
                raise RuntimeError("Database pool not initialized")
        
        conn = None
        try:
            conn = self.pool.getconn()
            cur = conn.cursor()
            
            # Set no timeout for this connection
            cur.execute("SET statement_timeout = 0")
            conn.commit()
            
            # Build the function call
            param_placeholders = ", ".join(["%s" for _ in params])
            query = f"SELECT * FROM {function_name}({param_placeholders})"
            
            # Execute the function
            logger.info(f"Executing function: {function_name} with params: {params}")
            start_time = time.time()
            
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.info(f"Function {function_name} completed in {execution_time:.2f}s, returned {len(rows)} rows")
            
            # Convert rows to dictionaries
            result = [dict(zip(columns, row)) for row in rows]
            
            cur.close()
            return result
            
        except Exception as e:
            logger.error(f"Error executing function {function_name}: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a raw SQL query (synchronous)"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        conn = None
        try:
            conn = self.pool.getconn()
            cur = conn.cursor()
            
            cur.execute("SET statement_timeout = 0")
            conn.commit()
            
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            
            result = [dict(zip(columns, row)) for row in rows]
            
            cur.close()
            return result
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    async def health_check(self) -> bool:
        """Check database connection health"""
        try:
            if not self.pool:
                return False
            
            conn = self.pool.getconn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                result = cur.fetchone()
                cur.close()
                return result[0] == 1
            finally:
                self.pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global database manager instance
database_manager = DatabaseManager()