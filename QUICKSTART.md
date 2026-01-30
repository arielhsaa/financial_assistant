# Quick Start Guide - Financial Close Agentic Solution

This guide will help you get the financial close solution up and running in your Azure Databricks environment in under 1 hour.

## Prerequisites

Before you begin, ensure you have:

- [ ] Azure Databricks workspace (Premium or Enterprise tier)
- [ ] Unity Catalog enabled on your workspace
- [ ] Genie feature enabled (check with Databricks support if needed)
- [ ] Permissions to create catalogs, schemas, and tables
- [ ] Access to create notebooks and workflows
- [ ] Basic familiarity with Databricks and SQL

## 30-Minute Quick Start

### Step 1: Clone Repository (2 minutes)

1. Navigate to your Databricks workspace
2. In the Workspace panel, create a new folder: `/Financial_Close`
3. Import all notebooks from the `notebooks/` directory

### Step 2: Configure Environment (3 minutes)

1. Open notebook `01_setup_schema_and_tables.py`
2. Update the configuration section:
   ```python
   CATALOG = "financial_close_catalog"  # Change if needed
   CATALOG_LOCATION = "abfss://your-container@your-storage.dfs.core.windows.net/financial_close"
   ```
3. If you don't have ADLS Gen2, Databricks managed storage will work fine for testing

### Step 3: Create Lakehouse Structure (5 minutes)

1. Attach notebook `01_setup_schema_and_tables.py` to a cluster (any size will work)
2. Run the entire notebook (⌘/Ctrl + Shift + Enter)
3. Verify completion: You should see "✓ Setup Complete!" at the end

Expected output:
- 1 catalog created
- 3 schemas created
- 15 tables created

### Step 4: Generate Test Data (3 minutes)

1. Open notebook `02_synthetic_data_generation.py`
2. Verify the period configuration:
   ```python
   CURRENT_PERIOD = 202601  # January 2026
   ```
3. Run the entire notebook
4. Verify: Bronze tables should now contain synthetic data

Expected output:
- 2,000+ FX rate records
- 2,000+ trial balance records
- 500+ segmented close records
- 1,000+ forecast records

### Step 5: Process Financial Close Data (7 minutes)

1. Run notebook `03_ingest_and_standardize_phase1_2.py` (Phase 1 & 2)
   - Wait for completion (~3 minutes)
   - Verify Silver and Gold tables are populated

2. Run notebook `04_ingest_and_standardize_phase3.py` (Phase 3)
   - Wait for completion (~2 minutes)
   - Verify close results and forecast results are available

### Step 6: Run Agents (5 minutes)

Run the agent notebooks in sequence:

1. `05_agent_logic_close_supervisor.py` - Orchestrator agent
2. `06_agent_logic_fx_and_pre_close.py` - FX and PreClose agents
3. `07_agent_logic_segmented_and_forecast.py` - Segmented, Forecast, and Reporting agents

Each should complete in 1-2 minutes and log actions to `close_agent_logs`.

### Step 7: Create Dashboard Views (2 minutes)

1. Open notebook `08_dashboards_and_genie_instructions.sql`
2. Run all cells in the "Helper Views for Dashboards" section
3. Verify: 6 views should be created in the Gold schema

### Step 8: Set Up Genie Space (3 minutes)

1. In Databricks workspace, navigate to **Genie** in the left sidebar
2. Click **Create Space** → Name it "Financial Close Assistant"
3. Add tables:
   - Navigate to "Add Data"
   - Select `financial_close_catalog.gold_layer.*`
   - Add all Gold tables and the views you created
4. Add instructions:
   - Click "Space Settings" → "Instructions"
   - Copy/paste the Genie instructions from notebook 08
5. Save the space

### Step 9: Test Genie (2 minutes)

In your Genie space, try these queries:

1. "Show me the current close status by BU"
2. "What is the consolidated revenue for period 202601?"
3. "Which tasks are overdue?"

Verify that Genie returns accurate results with natural language.

### Step 10: Verify Everything Works (3 minutes)

Run these verification queries in a SQL notebook:

```sql
USE CATALOG financial_close_catalog;
USE SCHEMA gold_layer;

-- Check close status
SELECT * FROM close_status_gold WHERE period = 202601;

-- Check close results
SELECT bu, revenue_reporting, operating_profit_reporting, operating_margin_pct
FROM close_results_gold 
WHERE period = 202601 AND segment = 'ALL' AND product = 'ALL';

-- Check agent logs
SELECT agent_name, action, status, COUNT(*) as actions
FROM close_agent_logs
WHERE period = 202601
GROUP BY agent_name, action, status
ORDER BY agent_name, action;

-- Check KPIs
SELECT kpi_name, kpi_value, kpi_unit, status
FROM close_kpi_gold
WHERE period = 202601 AND bu = 'CONSOLIDATED';
```

All queries should return data without errors.

## What You've Built

Congratulations! In 30 minutes, you've created:

✅ A complete Lakehouse with Bronze/Silver/Gold layers  
✅ Synthetic financial data for testing  
✅ 5 intelligent agents automating the close process  
✅ A Genie space for natural language analytics  
✅ Helper views ready for dashboard creation  

## Next Steps (Optional)

### Create Databricks SQL Dashboards (30 minutes)

1. Navigate to **SQL** → **Dashboards** → **Create Dashboard**
2. Name it "Close Cockpit Dashboard"
3. Add visualizations using queries from notebook 08:
   - Overall Close Progress (counter)
   - Tasks by Status (pie chart)
   - Overdue Tasks (table)
   - Close Timeline (bar chart)
4. Repeat for other dashboards (Close Results, Forecast, KPI Scorecard)

### Set Up Workflows for Automation (15 minutes)

1. Navigate to **Workflows** → **Create Job**
2. Create "Daily FX Update Job":
   - Task: Run notebook `06_agent_logic_fx_and_pre_close.py`
   - Schedule: Daily at 8 AM
   - Cluster: New job cluster (single node, small)
3. Create "Hourly Close Monitor":
   - Task: Run notebook `05_agent_logic_close_supervisor.py`
   - Schedule: Every hour during close period
   - Cluster: Existing cluster or new job cluster

### Configure Permissions (10 minutes)

1. Navigate to **Catalog** → `financial_close_catalog`
2. Click **Permissions**
3. Grant access:
   ```
   FP&A Team → SELECT on all schemas
   Leadership → SELECT on gold_layer
   Agents Service Principal → ALL PRIVILEGES
   ```

## Troubleshooting

### Issue: "Catalog not found"
**Solution:** Verify Unity Catalog is enabled on your workspace. Check with your admin.

### Issue: "Permission denied"
**Solution:** Ensure you have `CREATE CATALOG` privilege. Ask your workspace admin to grant it.

### Issue: Genie not available
**Solution:** Genie requires Premium/Enterprise tier. Contact Databricks support to enable.

### Issue: Notebooks fail with import errors
**Solution:** Ensure you're using DBR 13.3 LTS or higher. Update your cluster runtime.

### Issue: Synthetic data looks wrong
**Solution:** This is expected - it's random synthetic data for demo purposes. Adjust the generation logic in notebook 02 for your needs.

## Common Commands

### Check Your Databricks Runtime
```python
print(spark.version)  # Should be 13.3 or higher
```

### List All Tables
```sql
SHOW TABLES IN financial_close_catalog.gold_layer;
```

### Check Table Row Counts
```sql
SELECT 
  'bronze_layer' as layer,
  'fx_rates_raw' as table_name,
  COUNT(*) as row_count
FROM financial_close_catalog.bronze_layer.fx_rates_raw
UNION ALL
SELECT 'silver_layer', 'fx_rates_std', COUNT(*) 
FROM financial_close_catalog.silver_layer.fx_rates_std
UNION ALL
SELECT 'gold_layer', 'close_status_gold', COUNT(*) 
FROM financial_close_catalog.gold_layer.close_status_gold;
```

### View Agent Logs
```sql
SELECT * 
FROM financial_close_catalog.gold_layer.close_agent_logs
ORDER BY log_timestamp DESC
LIMIT 100;
```

### Reset Everything (Start Over)
```sql
-- WARNING: This deletes all data!
DROP CATALOG IF EXISTS financial_close_catalog CASCADE;
```
Then re-run notebook 01 to start fresh.

## Demo Script for Stakeholders

Use this 5-minute demo script to showcase the solution:

1. **Show Genie (2 minutes)**
   - Ask: "What is the current close status?"
   - Ask: "Show me the top 3 variance drivers"
   - Ask: "Which BU has the highest operating margin?"

2. **Show Dashboard (2 minutes)**
   - Open Close Cockpit dashboard
   - Point out: task progress, SLA tracking, overdue items
   - Show consolidated P&L with variance

3. **Show Agent Logs (1 minute)**
   - Open agent logs table
   - Show automated actions taken by agents
   - Highlight anomaly detection, variance analysis

## Tips for Success

### For FP&A Users
- Bookmark the Genie space for quick access
- Save your most-used queries in Genie
- Subscribe to dashboard email reports
- Use Genie for variance investigation

### For Administrators
- Set up workspace-level permissions early
- Create service principal for agent jobs
- Configure email alerts for agent failures
- Schedule regular optimization of Delta tables

### For Developers
- Use the agent logs extensively for debugging
- Add custom KPIs in notebook 07
- Extend agent logic for your specific needs
- Create additional views for custom dashboards

## Production Checklist

Before going live with real data:

- [ ] Update synthetic data generators to real data connectors
- [ ] Configure proper Unity Catalog permissions
- [ ] Set up email/Slack notifications in agents
- [ ] Create Databricks workflows for automation
- [ ] Set up monitoring and alerting (Azure Monitor)
- [ ] Document BU submission process and timelines
- [ ] Train FP&A team on Genie and dashboards
- [ ] Test disaster recovery procedures
- [ ] Archive old periods (data retention policy)
- [ ] Set up nightly optimization jobs for large tables

## Support Resources

- **Documentation:** See `README.md` and `ARCHITECTURE.md`
- **Sample Queries:** See notebook 08 for dashboard queries
- **Genie Examples:** Pre-loaded in the Genie space
- **Databricks Docs:** [docs.databricks.com](https://docs.databricks.com)
- **Unity Catalog Guide:** [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)

## Performance Optimization

If you experience slow queries as data grows:

1. **Optimize tables monthly:**
   ```sql
   OPTIMIZE financial_close_catalog.gold_layer.close_results_gold
   ZORDER BY (period, bu, segment);
   ```

2. **Vacuum old versions quarterly:**
   ```sql
   VACUUM financial_close_catalog.gold_layer.close_results_gold RETAIN 168 HOURS;
   ```

3. **Enable Photon acceleration** in cluster configuration

4. **Use Liquid Clustering** for very large tables (100+ GB)

## Customization Ideas

- Add custom KPIs specific to your business
- Create BU-specific dashboards with row-level security
- Integrate with your ERP for automated data loading
- Add machine learning for anomaly detection (Advanced)
- Implement predictive analytics for cycle time (Advanced)
- Build Slack bot for close status updates
- Create mobile-friendly dashboards

## FAQ

**Q: Can I use this with multiple legal entities?**  
A: Yes! Add a `legal_entity` column to your tables and adjust partitioning.

**Q: How do I add more BUs?**  
A: Update the `BUSINESS_UNITS` list in notebook 02 and regenerate data.

**Q: Can I customize the close phases?**  
A: Yes! Modify the phase definitions in notebook 03 and adjust agent logic.

**Q: How much does this cost to run?**  
A: For 5 BUs with small clusters, expect ~$50-100/month in Databricks costs during active close periods.

**Q: Can this handle real-time close monitoring?**  
A: Yes! Set agents to run every 15-30 minutes during close periods.

---

**Need Help?**  
- Check the `ARCHITECTURE.md` for detailed technical documentation
- Review agent logs for troubleshooting
- Consult Databricks documentation for platform questions

**Happy Closing!** 🎉
