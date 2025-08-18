# Fly.io Deployment Script for Costing API
# Run this script after setting up your environment variables

Write-Host "🚀 Starting Fly.io deployment for Costing API..." -ForegroundColor Green

# Check if flyctl is installed
try {
    $flyVersion = flyctl version
    Write-Host "✅ Fly.io CLI found: $flyVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Fly.io CLI not found. Please install it first:" -ForegroundColor Red
    Write-Host "   Visit: https://fly.io/docs/hands-on/install-flyctl/" -ForegroundColor Yellow
    exit 1
}

# Check if user is logged in
try {
    $whoami = flyctl auth whoami
    Write-Host "✅ Logged in as: $whoami" -ForegroundColor Green
} catch {
    Write-Host "❌ Not logged in to Fly.io. Please run: flyctl auth login" -ForegroundColor Red
    exit 1
}

# Check if app exists
try {
    $appInfo = flyctl apps list | Select-String "costing-api"
    if ($appInfo) {
        Write-Host "✅ App 'costing-api' already exists" -ForegroundColor Green
    } else {
        Write-Host "ℹ️  App 'costing-api' not found, will be created during deployment" -ForegroundColor Yellow
    }
} catch {
    Write-Host "ℹ️  Could not check existing apps, proceeding..." -ForegroundColor Yellow
}

# Deploy the application
Write-Host "🚀 Deploying to Fly.io..." -ForegroundColor Green
try {
    flyctl deploy --remote-only
    Write-Host "✅ Deployment completed successfully!" -ForegroundColor Green
    Write-Host "🌐 Your app should be available at: https://costing-api.fly.dev" -ForegroundColor Cyan
} catch {
    Write-Host "❌ Deployment failed!" -ForegroundColor Red
    Write-Host "   Check the error messages above and try again." -ForegroundColor Yellow
    exit 1
}

# Show app status
Write-Host "📊 Checking app status..." -ForegroundColor Green
try {
    flyctl status
} catch {
    Write-Host "⚠️  Could not retrieve app status" -ForegroundColor Yellow
}

Write-Host "🎉 Deployment process completed!" -ForegroundColor Green
Write-Host "📖 For more information, visit: https://fly.io/docs/" -ForegroundColor Cyan
