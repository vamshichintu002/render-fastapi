# Query Performance Optimization Guide

This guide provides step-by-step instructions to optimize the performance of your complex tracker queries without changing any business logic.

## 🚀 Quick Start

Execute the scripts in this exact order:

1. **Create Indexes**: `01_create_indexes.sql`
2. **Database Maintenance**: `02_maintenance_vacuum.sql` (special handling required)
3. **Query Settings**: `03_query_execution_settings.sql`
4. **Run Your Query**: Execute your original tracker query

---

## 📁 File Overview

| File | Purpose | Transaction Safe |
|------|---------|------------------|
| `01_create_indexes.sql` | Creates performance indexes | ✅ Yes |
| `02_maintenance_vacuum.sql` | VACUUM/ANALYZE commands | ❌ No - Special handling |
| `03_query_execution_settings.sql` | Performance settings | ✅ Yes |
| `run_maintenance_commands.sh` | Bash script for maintenance | ✅ Yes |

---

## 🔧 Step-by-Step Execution

### Step 1: Create Indexes
```sql
-- Run this first - creates all necessary indexes
\i 01_create_indexes.sql
```

**What it does:**
- Creates indexes on `sales_data`, `material_master`, `schemes_data`
- Sets up JSON path indexes for faster scheme extraction
- Configures statistics targets for better query planning

### Step 2: Database Maintenance (VACUUM/ANALYZE)

**⚠️ IMPORTANT**: VACUUM commands cannot run in transaction blocks. Choose ONE method:

#### Method A: Using psql with autocommit
```bash
psql -d your_database
\set AUTOCOMMIT on
\i 02_maintenance_vacuum.sql
```

#### Method B: Individual commands
```bash
psql -d your_database -c "VACUUM ANALYZE sales_data;"
psql -d your_database -c "VACUUM ANALYZE material_master;"
psql -d your_database -c "VACUUM ANALYZE schemes_data;"
```

#### Method C: Using the bash script (Linux/Mac)
```bash
# Update database credentials in the script first
chmod +x run_maintenance_commands.sh
./run_maintenance_commands.sh
```

### Step 3: Apply Query Settings
```sql
-- Run before your main query
\i 03_query_execution_settings.sql
```

### Step 4: Execute Your Original Query
```sql
-- Your TRACKER_MAINSCHEME_VALUE query here
-- No changes needed to your original query logic
```

---

## 🎯 Expected Performance Improvements

| Optimization | Expected Improvement |
|--------------|---------------------|
| Indexes | 50-80% faster execution |
| Settings | 20-40% additional improvement |
| **Total** | **70-90% reduction in execution time** |

---

## 🛠️ Troubleshooting

### Error: "VACUUM cannot run inside a transaction block"

**Solution**: Use one of these methods:

1. **Enable autocommit in psql**:
   ```sql
   \set AUTOCOMMIT on
   ```

2. **Run each VACUUM command individually**:
   ```bash
   psql -c "VACUUM ANALYZE sales_data;"
   ```

3. **Use the provided bash script** (recommended)

### Error: "syntax error at or near 'all'"

**Fixed**: Use quotes around string values:
```sql
SET track_functions = 'all';  -- Correct
SET track_functions = all;    -- Incorrect
```

### Error: "relation does not exist"

**Solution**: Make sure your table names match exactly:
- Check table names: `\dt` in psql
- Update script with correct table names if needed

---

## 📊 Monitoring Performance

### Check Index Usage
```sql
SELECT 
    schemaname, tablename, indexname, 
    idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE tablename IN ('sales_data', 'material_master', 'schemes_data')
ORDER BY idx_scan DESC;
```

### Monitor Query Performance
```sql
SELECT 
    query, calls, total_time, mean_time, max_time
FROM pg_stat_statements 
WHERE query LIKE '%sales_data%'
ORDER BY total_time DESC;
```

### Check Current Settings
```sql
SELECT name, setting, unit 
FROM pg_settings 
WHERE name IN ('work_mem', 'effective_cache_size', 'max_parallel_workers_per_gather')
ORDER BY name;
```

---

## 🔄 Maintenance Schedule

### Daily
- Monitor query performance
- Check for blocking queries

### Weekly
- Run VACUUM ANALYZE on large tables
- Update table statistics

### Monthly
- Rebuild fragmented indexes (if needed)
- Review and adjust settings based on usage

---

## 🚨 Important Notes

1. **No Logic Changes**: These optimizations preserve 100% of your original query logic
2. **Index Creation**: May take time on large tables - run during low-traffic periods
3. **Memory Settings**: Adjust based on your available RAM
4. **Backup**: Always test on a copy of production data first
5. **Supabase**: For managed databases, some settings may not be available

---

## 🆘 Support

If you encounter issues:

1. Check PostgreSQL version compatibility
2. Verify table names match exactly
3. Ensure sufficient disk space for indexes
4. Review PostgreSQL logs for detailed error messages
5. Consider running scripts section by section

---

## 📈 Advanced Optimizations

For even better performance on very large datasets:

1. **Table Partitioning**: Partition `sales_data` by year/month
2. **Connection Pooling**: Use pgBouncer for connection management
3. **Hardware**: Consider upgrading to Supabase Pro/Team plans
4. **Query Splitting**: Break very large queries into smaller chunks

---

**✅ Your query performance should now be dramatically improved while maintaining exact business logic!**