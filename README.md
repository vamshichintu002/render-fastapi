# Costing API

A high-performance FastAPI backend for handling complex costing calculations that timeout in the frontend.

## Features

- **Async Processing**: Handles long-running costing calculations without timeouts
- **Database Optimization**: Direct PostgreSQL connections with optimized connection pooling
- **Batch Processing**: Process multiple calculations concurrently
- **Complexity Analysis**: Analyze scheme complexity to predict processing time
- **Health Monitoring**: Comprehensive health checks for API and database
- **Error Handling**: Robust error handling with detailed logging

## API Endpoints

### Main Scheme Costing
- `POST /api/costing/main-scheme/value` - Value-based main scheme costing
- `POST /api/costing/main-scheme/volume` - Volume-based main scheme costing

### Additional Scheme Costing
- `POST /api/costing/additional-scheme/value` - Value-based additional scheme costing
- `POST /api/costing/additional-scheme/volume` - Volume-based additional scheme costing

### Summary Calculations
- `POST /api/costing/summary/main-scheme/value` - Value-based main scheme summary
- `POST /api/costing/summary/main-scheme/volume` - Volume-based main scheme summary
- `POST /api/costing/summary/additional-scheme/value` - Value-based additional scheme summary
- `POST /api/costing/summary/additional-scheme/volume` - Volume-based additional scheme summary

### Utility Endpoints
- `GET /api/costing/scheme/{scheme_id}/complexity` - Analyze scheme complexity
- `POST /api/costing/batch` - Batch processing of multiple calculations

### Health Checks
- `GET /health/` - Basic health check
- `GET /health/database` - Database health check
- `GET /health/detailed` - Detailed system health

## Setup

### 1. Install Dependencies

```bash
cd costing-api
pip install -r requirements.txt
```

### 2. Environment Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```env
DATABASE_URL=postgresql://postgres:password@localhost:5432/your_database
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

API_HOST=0.0.0.0
API_PORT=8000
DEBUG=True

ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
```

### 3. Start the API

```bash
python start.py
```

Or using uvicorn directly:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. Access Documentation

- API Documentation: http://localhost:8000/docs
- Alternative Docs: http://localhost:8000/redoc
- Health Check: http://localhost:8000/health/

## Usage Examples

### Main Scheme Value Costing

```bash
curl -X POST "http://localhost:8000/api/costing/main-scheme/value" \
  -H "Content-Type: application/json" \
  -d '{"scheme_id": "168725"}'
```

### Additional Scheme Volume Costing

```bash
curl -X POST "http://localhost:8000/api/costing/additional-scheme/volume" \
  -H "Content-Type: application/json" \
  -d '{"scheme_id": "168725", "scheme_index": 0}'
```

### Complexity Analysis

```bash
curl -X GET "http://localhost:8000/api/costing/scheme/168725/complexity"
```

### Batch Processing

```bash
curl -X POST "http://localhost:8000/api/costing/batch" \
  -H "Content-Type: application/json" \
  -d '[
    {"scheme_id": "168725", "calculation_type": "main_value"},
    {"scheme_id": "168725", "scheme_index": 0, "calculation_type": "additional_volume"}
  ]'
```

## Performance Optimizations

1. **Connection Pooling**: Optimized PostgreSQL connection pool (5-20 connections)
2. **No Timeout**: Database queries run without statement timeout
3. **Async Processing**: All operations are asynchronous
4. **Batch Processing**: Multiple calculations processed concurrently
5. **Memory Efficient**: Streaming results for large datasets

## Monitoring

The API provides comprehensive logging and health monitoring:

- Request/response logging
- Database connection monitoring
- Performance metrics (execution time, record counts)
- Error tracking with stack traces

## Integration with Frontend

Update your frontend to call the FastAPI backend instead of Supabase RPC:

```typescript
// Instead of:
const result = await supabase.rpc('costing_sheet_value_mainscheme', { p_scheme_id: schemeId });

// Use:
const response = await fetch('http://localhost:8000/api/costing/main-scheme/value', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ scheme_id: schemeId })
});
const result = await response.json();
```

## Deployment

For production deployment:

1. Set `DEBUG=False` in `.env`
2. Use a production WSGI server like Gunicorn
3. Set up reverse proxy with Nginx
4. Configure proper logging and monitoring
5. Use environment variables for sensitive configuration

```bash
# Production startup
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```