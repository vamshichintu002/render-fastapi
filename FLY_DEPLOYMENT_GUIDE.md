# Fly.io Deployment Guide for Costing API

This guide will help you deploy your Costing API project to Fly.io with proper environment variable handling.

## Prerequisites

1. **Fly.io Account**: Sign up at [fly.io](https://fly.io)
2. **Fly.io CLI**: Install the command-line tool
3. **GitHub Repository**: Your project should be connected to GitHub

## Step 1: Install Fly.io CLI

### Windows (PowerShell)
```powershell
iwr https://fly.io/install.ps1 -useb | iex
```

### macOS
```bash
curl -L https://fly.io/install.sh | sh
```

### Linux
```bash
curl -L https://fly.io/install.sh | sh
```

## Step 2: Authenticate with Fly.io

```bash
flyctl auth login
```

Follow the browser prompts to complete authentication.

## Step 3: Set Environment Variables

Your application requires several environment variables. You have two options:

### Option A: Set as Fly.io Secrets (Recommended for sensitive data)

```bash
# Database configuration
flyctl secrets set DATABASE_URL="postgresql://username:password@host:port/database_name"
flyctl secrets set SUPABASE_URL="https://your-project.supabase.co"
flyctl secrets set SUPABASE_KEY="your_supabase_anon_key"
flyctl secrets set SUPABASE_SERVICE_ROLE_KEY="your_supabase_service_role_key"

# API configuration
flyctl secrets set API_HOST="0.0.0.0"
flyctl secrets set API_PORT="8000"
flyctl secrets set DEBUG="False"

# CORS configuration
flyctl secrets set ALLOWED_ORIGINS="http://localhost:3000,http://localhost:5173,https://yourdomain.com"

# Performance settings
flyctl secrets set MAX_CONCURRENT_REQUESTS="10"
flyctl secrets set REQUEST_TIMEOUT="300"
```

### Option B: Set in fly.toml (Non-sensitive data only)

Edit the `fly.toml` file and add under the `[env]` section:

```toml
[env]
  PYTHON_VERSION = "3.11"
  PORT = "8000"
  API_HOST = "0.0.0.0"
  API_PORT = "8000"
  DEBUG = "False"
  MAX_CONCURRENT_REQUESTS = "10"
  REQUEST_TIMEOUT = "300"
```

**Important**: Never put sensitive information like database URLs or API keys in `fly.toml` as it will be committed to your repository.

## Step 4: Deploy Your Application

### First-time deployment
```bash
flyctl launch --no-deploy
```

This will create the app configuration. Then deploy:

```bash
flyctl deploy
```

### Subsequent deployments
```bash
flyctl deploy
```

### Using the deployment script
```powershell
.\deploy_to_fly.ps1
```

## Step 5: Verify Deployment

Check your app status:
```bash
flyctl status
```

View logs:
```bash
flyctl logs
```

Open your app:
```bash
flyctl open
```

## Step 6: Update Environment Variables

To update environment variables after deployment:

```bash
# Update secrets
flyctl secrets set VARIABLE_NAME="new_value"

# Redeploy to apply changes
flyctl deploy
```

## Environment Variables Reference

| Variable | Description | Required | Example |
|----------|-------------|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Yes | `postgresql://user:pass@host:5432/db` |
| `SUPABASE_URL` | Supabase project URL | Yes | `https://project.supabase.co` |
| `SUPABASE_KEY` | Supabase anonymous key | Yes | `eyJhbGciOiJIUzI1NiIs...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key | Yes | `eyJhbGciOiJIUzI1NiIs...` |
| `API_HOST` | API host binding | No | `0.0.0.0` |
| `API_PORT` | API port | No | `8000` |
| `DEBUG` | Debug mode | No | `False` |
| `ALLOWED_ORIGINS` | CORS allowed origins | No | `http://localhost:3000` |
| `MAX_CONCURRENT_REQUESTS` | Max concurrent requests | No | `10` |
| `REQUEST_TIMEOUT` | Request timeout in seconds | No | `300` |

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify `DATABASE_URL` is correct
   - Check if database is accessible from Fly.io
   - Ensure database allows external connections

2. **Environment Variables Not Loading**
   - Use `flyctl secrets list` to verify secrets are set
   - Check `flyctl logs` for errors
   - Redeploy after setting secrets

3. **Build Failures**
   - Check `fly.toml` syntax
   - Verify all required files are present
   - Check `.dockerignore` isn't excluding necessary files

### Useful Commands

```bash
# View app configuration
flyctl config show

# View app logs
flyctl logs

# SSH into running instance
flyctl ssh console

# Scale your app
flyctl scale count 2

# View app metrics
flyctl dashboard
```

## Security Best Practices

1. **Never commit sensitive data** to your repository
2. **Use Fly.io secrets** for all sensitive environment variables
3. **Regularly rotate** API keys and database passwords
4. **Monitor logs** for suspicious activity
5. **Use HTTPS** (enabled by default in Fly.io)

## Cost Optimization

- **Auto-scaling**: Your app is configured to scale to 0 when not in use
- **Resource limits**: Currently set to 1 CPU and 512MB RAM
- **Regions**: Deployed to `iad` (Washington DC) by default

## Support

- **Fly.io Documentation**: [fly.io/docs](https://fly.io/docs)
- **Community Forum**: [community.fly.io](https://community.fly.io)
- **GitHub Issues**: For application-specific issues

---

**Note**: This deployment will create a new Fly.io application. Make sure you have sufficient credits in your Fly.io account.

