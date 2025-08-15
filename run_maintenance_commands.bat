@echo off
REM =============================================================================
REM DATABASE MAINTENANCE SCRIPT FOR WINDOWS
REM Runs VACUUM and ANALYZE commands outside transaction blocks
REM =============================================================================

REM Configuration - Update these variables for your environment
set DB_HOST=localhost
set DB_PORT=5432
set DB_NAME=your_database_name
set DB_USER=your_username

echo Starting database maintenance...
echo.

REM Step 1: Update statistics
echo === STEP 1: UPDATING STATISTICS ===
echo Running: Analyzing sales_data table
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "ANALYZE sales_data;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to analyze sales_data
    pause
    exit /b 1
)

echo Running: Analyzing material_master table
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "ANALYZE material_master;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to analyze material_master
    pause
    exit /b 1
)

echo Running: Analyzing schemes_data table
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "ANALYZE schemes_data;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to analyze schemes_data
    pause
    exit /b 1
)

echo.

REM Step 2: Vacuum and Analyze
echo === STEP 2: VACUUM AND ANALYZE ===
echo Running: Vacuum and analyze sales_data
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "VACUUM ANALYZE sales_data;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to vacuum and analyze sales_data
    pause
    exit /b 1
)

echo Running: Vacuum and analyze material_master
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "VACUUM ANALYZE material_master;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to vacuum and analyze material_master
    pause
    exit /b 1
)

echo Running: Vacuum and analyze schemes_data
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "VACUUM ANALYZE schemes_data;"
if %errorlevel% neq 0 (
    echo ERROR: Failed to vacuum and analyze schemes_data
    pause
    exit /b 1
)

echo.

REM Step 3: Check index usage
echo === STEP 3: INDEX USAGE REPORT ===
echo Checking index usage statistics...
psql -h %DB_HOST% -p %DB_PORT% -d %DB_NAME% -U %DB_USER% -c "SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE tablename IN ('sales_data', 'material_master', 'schemes_data') ORDER BY tablename, idx_scan DESC;"

echo.
echo Database maintenance completed successfully!
echo.
pause