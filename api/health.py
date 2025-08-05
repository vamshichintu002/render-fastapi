from http.server import BaseHTTPRequestHandler
import json
import os
from dotenv import load_dotenv
import psycopg2
import logging

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

def test_database():
    """Test database connection"""
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            result = cur.fetchone()
            cur.close()
            conn.close()
            return result[0] == 1
        return False
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        return False

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET requests for health checks"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
        # Test database connection
        db_healthy = test_database()
        
        response = {
            "status": "healthy" if db_healthy else "unhealthy",
            "database": "connected" if db_healthy else "disconnected",
            "service": "costing-api",
            "version": "1.0.0"
        }
        
        self.wfile.write(json.dumps(response).encode())
        return

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        return 