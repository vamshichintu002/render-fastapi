# Vercel Deployment Guide for Costing API

This guide will help you deploy your FastAPI Costing API to Vercel.

## Prerequisites

1. **Vercel Account**: Sign up at [vercel.com](https://vercel.com)
2. **Vercel CLI** (optional): `npm i -g vercel`
3. **Git Repository**: Your code should be in a Git repository (GitHub, GitLab, etc.)

## Environment Variables Setup

Before deploying, you'll need to set up the following environment variables in your Vercel project:

### Required Environment Variables

```bash
# Database Configuration
DATABASE_URL=your_database_connection_string
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=False

# CORS Configuration
ALLOWED_ORIGINS=https://your-frontend-domain.com,https://another-domain.com

# Performance Settings
MAX_CONCURRENT_REQUESTS=10
REQUEST_TIMEOUT=300
```

### How to Set Environment Variables

1. **Via Vercel Dashboard**:
   - Go to your project dashboard
   - Navigate to Settings → Environment Variables
   - Add each variable with appropriate values

2. **Via Vercel CLI**:
   ```bash
   vercel env add DATABASE_URL
   vercel env add SUPABASE_URL
   # ... repeat for all variables
   ```

## Deployment Steps

### Option 1: Deploy via Vercel Dashboard

1. **Connect Repository**:
   - Go to [vercel.com/dashboard](https://vercel.com/dashboard)
   - Click "New Project"
   - Import your Git repository

2. **Configure Project**:
   - Framework Preset: Select "Other"
   - Root Directory: Leave as default (or specify if needed)
   - Build Command: Leave empty (Vercel will auto-detect)
   - Output Directory: Leave empty

3. **Set Environment Variables**:
   - Add all required environment variables as listed above

4. **Deploy**:
   - Click "Deploy"
   - Vercel will automatically build and deploy your API

### Option 2: Deploy via Vercel CLI

1. **Install Vercel CLI**:
   ```bash
   npm i -g vercel
   ```

2. **Login to Vercel**:
   ```bash
   vercel login
   ```

3. **Deploy**:
   ```bash
   vercel
   ```

4. **Set Environment Variables**:
   ```bash
   vercel env add DATABASE_URL
   vercel env add SUPABASE_URL
   # ... add all other variables
   ```

## API Endpoints

After deployment, your API will be available at:

- **Base URL**: `https://your-project-name.vercel.app`
- **Health Check**: `https://your-project-name.vercel.app/health`
- **API Documentation**: `https://your-project-name.vercel.app/docs`
- **Costing Endpoints**: `https://your-project-name.vercel.app/api/costing/`

### Available Endpoints

#### Health Endpoints
- `GET /health/` - Basic health check
- `GET /health/database` - Database health check
- `GET /health/detailed` - Detailed health check

#### Costing Endpoints
- `POST /api/costing/main-scheme/value` - Main scheme value costing
- `POST /api/costing/main-scheme/volume` - Main scheme volume costing
- `POST /api/costing/additional-scheme/value` - Additional scheme value costing
- `POST /api/costing/additional-scheme/volume` - Additional scheme volume costing
- `POST /api/costing/summary/main-scheme/value` - Main scheme value summary
- `POST /api/costing/summary/main-scheme/volume` - Main scheme volume summary
- `POST /api/costing/summary/additional-scheme/value` - Additional scheme value summary
- `POST /api/costing/summary/additional-scheme/volume` - Additional scheme volume summary
- `GET /api/costing/scheme/{scheme_id}/complexity` - Scheme complexity analysis
- `POST /api/costing/batch` - Batch costing calculations

## Configuration Files

The following files are configured for Vercel deployment:

- `vercel.json` - Vercel configuration
- `.vercelignore` - Files to exclude from deployment
- `requirements.txt` - Python dependencies
- `main.py` - FastAPI application entry point

## Troubleshooting

### Common Issues

1. **Build Failures**:
   - Check that all dependencies are in `requirements.txt`
   - Ensure `main.py` is the correct entry point
   - Verify Python version compatibility

2. **Environment Variables**:
   - Ensure all required environment variables are set
   - Check that database connection strings are correct
   - Verify CORS origins are properly configured

3. **Function Timeouts**:
   - The API is configured with a 30-second timeout
   - For longer operations, consider implementing background tasks

4. **Database Connection Issues**:
   - Ensure your database is accessible from Vercel's servers
   - Check that connection strings include SSL configuration if required

### Debugging

1. **Check Logs**:
   - Go to your Vercel dashboard
   - Navigate to Functions → View Function Logs

2. **Test Locally**:
   ```bash
   vercel dev
   ```

3. **Health Check**:
   - Visit `/health` endpoint to verify API is running
   - Check `/health/database` for database connectivity

## Performance Considerations

1. **Cold Starts**: Vercel functions may experience cold starts
2. **Timeout Limits**: Functions have a 30-second timeout
3. **Memory Limits**: Functions have memory constraints
4. **Database Connections**: Use connection pooling for better performance

## Security

1. **Environment Variables**: Never commit sensitive data to Git
2. **CORS**: Configure allowed origins properly
3. **Database**: Use SSL connections and proper authentication
4. **API Keys**: Store all keys as environment variables

## Monitoring

1. **Vercel Analytics**: Monitor function performance
2. **Logs**: Check function logs for errors
3. **Health Checks**: Implement monitoring for your endpoints
4. **Database**: Monitor database connection health

## Support

- [Vercel Documentation](https://vercel.com/docs)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Vercel Python Runtime](https://vercel.com/docs/functions/runtimes/python) 