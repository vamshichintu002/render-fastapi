#!/bin/bash

# =============================================================================
# DATABASE MAINTENANCE SCRIPT
# Runs VACUUM and ANALYZE commands outside transaction blocks
# =============================================================================

# Configuration - Update these variables for your environment
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="your_database_name"
DB_USER="your_username"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting database maintenance...${NC}"

# Function to run psql command
run_sql() {
    local sql_command="$1"
    local description="$2"
    
    echo -e "${YELLOW}Running: $description${NC}"
    
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c "$sql_command"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Completed: $description${NC}"
    else
        echo -e "${RED}✗ Failed: $description${NC}"
        exit 1
    fi
    
    echo ""
}

# Step 1: Update statistics
echo -e "${GREEN}=== STEP 1: UPDATING STATISTICS ===${NC}"
run_sql "ANALYZE sales_data;" "Analyzing sales_data table"
run_sql "ANALYZE material_master;" "Analyzing material_master table"
run_sql "ANALYZE schemes_data;" "Analyzing schemes_data table"

# Step 2: Vacuum and Analyze
echo -e "${GREEN}=== STEP 2: VACUUM AND ANALYZE ===${NC}"
run_sql "VACUUM ANALYZE sales_data;" "Vacuum and analyze sales_data"
run_sql "VACUUM ANALYZE material_master;" "Vacuum and analyze material_master"
run_sql "VACUUM ANALYZE schemes_data;" "Vacuum and analyze schemes_data"

# Step 3: Check index usage
echo -e "${GREEN}=== STEP 3: INDEX USAGE REPORT ===${NC}"
run_sql "SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE tablename IN ('sales_data', 'material_master', 'schemes_data') ORDER BY tablename, idx_scan DESC;" "Checking index usage statistics"

echo -e "${GREEN}Database maintenance completed successfully!${NC}"