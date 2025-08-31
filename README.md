# Costing Sheet Calculator API

A high-performance FastAPI backend for generating comprehensive costing sheets using the new costing_sheet implementation. This system provides cost estimation and validation for paint scheme calculations with professional Excel output.

## 🚀 Features

### Core Functionality
- **Costing Sheet Generation**: Complete cost estimation system using base period data
- **Multi-Scheme Support**: Handles main scheme + additional schemes simultaneously
- **Professional Output**: Generates formatted Excel files with multiple sheets
- **Memory-Based Processing**: No intermediate file storage, single output file
- **Base Period Focus**: Uses only base period data for cost estimation

### Technical Features
- **FastAPI Backend**: High-performance async API with automatic documentation
- **Comprehensive Validation**: Pre-calculation scheme validation
- **Error Handling**: Robust error handling and recovery
- **Background Processing**: Support for long-running calculations
- **Docker Support**: Container-ready deployment
- **Self-Contained**: All dependencies included for easy deployment

### Calculation Scope
- **Data Source**: Base period data only (excludes scheme period data)
- **Columns**: Start through Target Volume/Target Value
- **Schemes**: Main scheme + multiple additional schemes
- **Output**: Single Excel file with 3 sheets (Main data, Summary, Metadata)

## 📋 System Architecture

```
costing-api/
├── main.py                          # FastAPI application entry point
├── app/
│   ├── __init__.py
│   ├── config.py                    # Application configuration
│   ├── routers/
│   │   ├── costing.py               # NEW: Costing sheet endpoints
│   │   ├── health.py                # Health check endpoints
│   │   └── tracker.py               # Existing tracker endpoints
│   ├── services/                    # Business logic services
│   └── models/                      # Pydantic models
├── costing_sheet/                   # NEW: Costing sheet modules
│   ├── __init__.py
│   ├── calculator.py                # Main calculator orchestrator
│   ├── data_processor.py            # Data fetching and processing
│   ├── base_estimator.py            # Base cost calculations
│   ├── mandatory_product_calculator.py
│   ├── phasing_calculator.py
│   ├── bonus_calculator.py
│   ├── output_generator.py          # Excel file generation
│   └── README.md
├── costing_sheet_main.py            # NEW: CLI entry point
├── requirements.txt                 # Updated with all dependencies
└── Docker support files
```

## 🛠️ Quick Start

### Local Development

1. **Install Dependencies**:
```bash
cd costing-api
pip install -r requirements.txt
```

2. **Run the Server**:
```bash
python main.py
```

The API will be available at `http://localhost:8000`

### Docker Deployment

1. **Build and Run**:
```bash
docker-compose up --build
```

2. **Access the API**:
- API: `http://localhost:8000`
- Documentation: `http://localhost:8000/docs`
- Health Check: `http://localhost:8000/health`

## 📊 API Endpoints

### Costing Sheet Endpoints

#### `POST /api/costing/calculate`
Generate a complete costing sheet for a scheme ID.

**Request Body**:
```json
{
  "scheme_id": "563458"
}
```

**Response**:
```json
{
  "success": true,
  "message": "Costing sheet generated successfully",
  "output_file": "563458_COSTING_SHEET_20250822_103701.xlsx",
  "processing_time_seconds": 128.24,
  "calculation_summary": {
    "total_accounts": 5072,
    "main_scheme": {
      "base1_volume_total": 10734707.7,
      "base1_value_total": 3722941696.0,
      "target_volume_total": 0.0,
      "target_value_total": 4116241957.06
    },
    "additional_schemes": {
      "scheme_1": {
        "target_volume_total": 1190136.09,
        "target_value_total": 0.0
      }
    }
  },
  "data_summary": {
    "scheme_info": {...},
    "base_period_summary": {...},
    "memory_usage": {...}
  },
  "output_info": {
    "file_size_mb": 3.63,
    "created_time": "2025-08-22 10:37:02"
  }
}
```

#### `GET /api/costing/scheme/{scheme_id}/validate`
Validate scheme requirements without full calculation.

#### `GET /api/costing/scheme/{scheme_id}/summary`
Get data summary for a scheme.

#### `GET /api/costing/demo`
Show system capabilities and demo information.

### Legacy Endpoints (Still Available)
- `POST /api/costing/main-scheme/value`
- `POST /api/costing/main-scheme/volume`
- `POST /api/costing/additional-scheme/value`
- `POST /api/costing/additional-scheme/volume`

## 📈 Calculation Process

The costing sheet calculation follows these steps:

1. **Scheme Validation**: Verify scheme requirements and data availability
2. **Data Processing**: Fetch and process base period data only
3. **Base Calculations**: Calculate base period metrics and aggregations
4. **Scheme Metadata**: Add scheme configuration and settings
5. **Growth Calculations**: Apply simplified growth rate calculations
6. **Target Calculations**: Calculate volume/value targets using costing formulas
7. **Additional Schemes**: Process additional schemes with scheme-specific data
8. **Mandatory Products**: Calculate mandatory product requirements and rebates
9. **Phasing**: Apply phasing calculations for multi-period schemes
10. **Bonus Schemes**: Calculate bonus scheme payouts
11. **Final Payout**: Calculate comprehensive estimated final payout
12. **Output Generation**: Create professional Excel file with multiple sheets

## 📊 Output Structure

The generated Excel file contains three sheets:

### Sheet 1: Main Costing Calculations
- Account-level costing data with all calculated columns
- From base period data through target volume/value
- Includes mandatory products, phasing, and bonus calculations

### Sheet 2: Summary Analysis
- Overall statistics and metrics
- Scheme performance summary
- Key calculation totals

### Sheet 3: Metadata and Configuration
- Processing information
- Scheme configuration details
- System metadata

## 🔧 Configuration

### Environment Variables
Create a `.env` file in the costing-api directory:

```env
# Database Configuration (from supabaseconfig.py)
DATABASE_URL=postgresql://user:password@host:port/database

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true

# CORS Configuration
ALLOWED_ORIGINS=http://localhost:3000,https://your-frontend.com
```

### Application Settings
Modify `app/config.py` for application-specific settings.

## 🚀 Deployment

### Production Deployment

1. **Using Docker Compose**:
```bash
docker-compose -f docker-compose.yml up -d
```

2. **Using Docker directly**:
```bash
docker build -t costing-api .
docker run -p 8000:8000 costing-api
```

3. **Using Uvicorn**:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Nginx Configuration
The provided `nginx.conf` can be used for production deployment with Nginx reverse proxy.

## 🧪 Testing

### API Testing
Use the interactive documentation at `/docs` or test with curl:

```bash
# Calculate costing sheet
curl -X POST "http://localhost:8000/api/costing/calculate" \
  -H "Content-Type: application/json" \
  -d '{"scheme_id": "563458"}'

# Validate scheme
curl "http://localhost:8000/api/costing/scheme/563458/validate"

# Demo mode
curl "http://localhost:8000/api/costing/demo"
```

### CLI Testing
You can also run the costing sheet generator directly:

```bash
cd costing-api
python costing_sheet_main.py
```

## 📝 Development

### Project Structure
- `costing_sheet/` - Core costing calculation modules
- `app/routers/` - FastAPI route definitions
- `app/models/` - Pydantic data models
- `app/services/` - Business logic services

### Adding New Features
1. Add calculation logic to `costing_sheet/` modules
2. Create API endpoints in `app/routers/costing.py`
3. Add Pydantic models in `app/models/`
4. Update documentation and tests

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Check the interactive API documentation at `/docs`
- Review the demo mode at `/api/costing/demo`
- Check application logs for detailed error information