from http.server import BaseHTTPRequestHandler
import json
import os
from dotenv import load_dotenv
import psycopg2
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create a database connection for serverless function"""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DATABASE_NAME", "postgres"),
            user=os.getenv("DATABASE_USER", "postgres.ozgkgkenzpngnptdqbqf"),
            password=os.getenv("DATABASE_PASSWORD", "PwbcCdD?Yq4Jn.v"),
            host=os.getenv("DATABASE_HOST", "aws-0-ap-south-1.pooler.supabase.com"),
            port=int(os.getenv("DATABASE_PORT", "5432"))
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def execute_costing_function(scheme_id, scheme_index):
    """Execute the costing function"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Database connection failed")
        
        cur = conn.cursor()
        
        # Set no timeout for this connection
        cur.execute("SET statement_timeout = 0")
        conn.commit()
        
        # Execute the costing function
        logger.info(f"Executing costing_sheet_volume_additionalscheme with params: ['{scheme_id}', {scheme_index}]")
        start_time = time.time()
        
        cur.execute("SELECT * FROM costing_sheet_volume_additionalscheme(%s, %s)", [scheme_id, scheme_index])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info(f"Function completed in {execution_time:.2f}s, returned {len(rows)} rows")
        
        # Convert rows to dictionaries
        result = [dict(zip(columns, row)) for row in rows]
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "data": {
                "data": result,
                "execution_time": execution_time,
                "function_name": "costing_sheet_volume_additionalscheme",
                "params": [scheme_id, scheme_index],
                "scheme_id": scheme_id,
                "scheme_index": scheme_index
            },
            "message": "Additional scheme volume costing calculated successfully",
            "execution_time": execution_time,
            "record_count": len(result)
        }
        
    except Exception as e:
        logger.error(f"Error executing costing function: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": f"costing_sheet_volume_additionalscheme failed after 0.00s: {str(e)}"
        }

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        """Handle POST requests for additional scheme volume costing"""
        try:
            # Read request body
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            request_data = json.loads(post_data.decode('utf-8'))
            
            # Extract parameters
            scheme_id = request_data.get('scheme_id')
            scheme_index = request_data.get('scheme_index', 0)
            
            if not scheme_id:
                self.send_error(400, "scheme_id is required")
                return
            
            # Execute costing function
            result = execute_costing_function(scheme_id, scheme_index)
            
            # Send response
            if result["success"]:
                self.send_response(200)
            else:
                self.send_response(500)
            
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            self.end_headers()
            
            self.wfile.write(json.dumps(result).encode())
            
        except Exception as e:
            logger.error(f"Error in handler: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            error_response = {
                "success": False,
                "error": str(e),
                "message": "Internal server error"
            }
            self.wfile.write(json.dumps(error_response).encode())

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        return 