# Tracker API Documentation

## Overview

The Tracker API provides endpoints to run scheme tracking calculations and store results in the database. It's optimized for Vercel's function timeout constraints.

## Endpoints

### 1. Run Tracker

**POST** `/api/tracker/run`

Runs tracker calculations for a given scheme ID.

**Request Body:**
```json
{
  "scheme_id": "your_scheme_id"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Tracker completed successfully for scheme_id: your_scheme_id. Data saved to database.",
  "status": "completed",
  "scheme_id": "your_scheme_id",
  "total_rows": 150,
  "total_columns": 45
}
```

**Status Values:**
- `completed`: Tracker finished successfully
- `running`: Tracker is running in background (due to timeout)
- `failed`: Tracker failed
- `cancelled`: Tracker was cancelled

### 2. Get Tracker Status

**GET** `/api/tracker/status/{scheme_id}`

Gets the current status of a tracker run.

**Response:**
```json
{
  "success": true,
  "scheme_id": "your_scheme_id",
  "status": "completed",
  "run_date": "2024-01-15",
  "run_timestamp": "2024-01-15T10:30:00",
  "from_date": "2024-01-01",
  "to_date": "2024-01-31",
  "updated_at": "2024-01-15T10:30:00"
}
```

### 3. Run Tracker in Background

**POST** `/api/tracker/run-background`

Immediately queues tracker for background processing.

**Request Body:**
```json
{
  "scheme_id": "your_scheme_id"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Tracker for scheme your_scheme_id has been queued for background processing.",
  "status": "running",
  "scheme_id": "your_scheme_id"
}
```

## Database Schema

The tracker results are stored in the existing `scheme_tracker_runs` table:

```sql
-- Existing table structure in Supabase
CREATE TABLE scheme_tracker_runs (
    scheme_id INTEGER NOT NULL CHECK (scheme_id >= 100000 AND scheme_id <= 999999),
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_status TEXT DEFAULT 'running' CHECK (run_status IN ('running', 'completed', 'failed', 'cancelled')),
    costing_sheet_data JSONB,
    costing_summary_data JSONB,
    tracker_data JSONB,
    product_wise_discount_data JSONB,
    from_date DATE,
    to_date DATE,
    auditor_approve BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (scheme_id, run_date)
);
```

## Vercel Optimization

The API is optimized for Vercel's 60-second function timeout:

1. **Timeout Protection**: If processing takes longer than 45 seconds, the function returns immediately with `processing` status
2. **Background Processing**: Use `/run-background` endpoint for guaranteed background processing
3. **Status Checking**: Use `/status/{scheme_id}` to check progress
4. **Database Storage**: All results are stored in the database, not as files

## Usage Examples

### JavaScript/TypeScript

```javascript
// Run tracker
const runTracker = async (schemeId) => {
  const response = await fetch('/api/tracker/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scheme_id: schemeId })
  });
  return response.json();
};

// Check status
const checkStatus = async (schemeId) => {
  const response = await fetch(`/api/tracker/status/${schemeId}`);
  return response.json();
};

// Run in background
const runBackground = async (schemeId) => {
  const response = await fetch('/api/tracker/run-background', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scheme_id: schemeId })
  });
  return response.json();
};
```

### Python

```python
import requests

# Run tracker
def run_tracker(scheme_id):
    response = requests.post('/api/tracker/run', 
                           json={'scheme_id': scheme_id})
    return response.json()

# Check status
def check_status(scheme_id):
    response = requests.get(f'/api/tracker/status/{scheme_id}')
    return response.json()
```

## Error Handling

All endpoints return consistent error responses:

```json
{
  "success": false,
  "message": "Error description",
  "scheme_id": "your_scheme_id"
}
```

Common error scenarios:
- Invalid or empty scheme_id
- Database connection issues
- Query execution failures
- Timeout issues

## Testing

Use the provided test script:

```bash
python test_tracker_endpoint.py
```

Make sure to update the `BASE_URL` and `SCHEME_ID` in the test script.

## Configuration

Before using the tracker API:

1. The `scheme_tracker_runs` table already exists in Supabase
2. Update `tracker_queries.py` with your actual SQL queries from the original tracker_runner.py
3. Configure database connection in `.env` file
4. Deploy to Vercel or run locally

## Performance Notes

- Queries are limited to 30-second execution time
- Large datasets are processed in chunks
- Results are stored as compressed JSON in the database
- Indexes are created for optimal query performance