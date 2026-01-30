# Quick Start Guide

## Get Started in 30 Minutes

Follow this guide to deploy the financial close solution to your Azure Databricks workspace.

---

## Prerequisites

Before you begin, ensure you have:

- ✅ Azure Databricks workspace (Premium tier)
- ✅ Unity Catalog enabled with metastore configured
- ✅ Permissions to:
  - Create catalogs, schemas, and tables
  - Create and run notebooks
  - Create Databricks workflows
  - Create Genie spaces (optional)
  - Create SQL dashboards (optional)

---

## Step 1: Upload Notebooks (5 minutes)

### Option A: Via Databricks UI
1. Open your Databricks workspace
2. Navigate to **Workspace** → **Users** → your username
3. Create folder: `financial_close_solution`
4. Click **Import** → **Import from files**
5. Upload all notebooks from the `notebooks/` folder

### Option B: Via Databricks CLI
```bash
# Install Databricks CLI if not already installed
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload notebooks
databricks workspace import_dir ./notebooks /Users/your.email@company.com/financial_close_solution
```

---

## Step 2: Run Setup Notebooks (15 minutes)

Execute notebooks **in order**:

### 2.1 Create Schema and Tables
```python
# Run: notebooks/01_setup_schema_and_tables.py
```
**What it does:**
- Creates `financial_close_lakehouse` catalog
- Creates Bronze/Silver/Gold schemas
- Defines all Delta tables with column comments
- Inserts configuration data (BUs, phases, agents)

**Execution time:** ~2 minutes  
**Expected output:** "✅ LAKEHOUSE SETUP COMPLETE"

---

### 2.2 Generate Synthetic Data
```python
# Run: notebooks/02_synthetic_data_generation.py
```
**What it does:**
- Generates 2 years of FX rates
- Creates trial balance for 6 BUs × 12 months
- Generates segmented and forecast data

**Execution time:** ~5 minutes  
**Expected output:** "✅ SYNTHETIC DATA GENERATION COMPLETE" (~78K rows)

---

### 2.3 Process Phase 1 & 2 Data
```python
# Run: notebooks/03_ingest_and_standardize_phase1_2.py
```
**What it does:**
- Validates and standardizes FX rates
- Converts trial balance to reporting currency
- Populates close status for Phase 1 & 2

**Execution time:** ~3 minutes  
**Expected output:** "✅ PHASE 1 & 2 PROCESSING COMPLETE"

---

### 2.4 Process Phase 3 Data
```python
# Run: notebooks/04_ingest_and_standardize_phase3.py
```
**What it does:**
- Processes segmented close data
- Integrates forecast data
- Calculates variances

**Execution time:** ~3 minutes  
**Expected output:** "✅ PHASE 3 PROCESSING COMPLETE"

---

### 2.5 Run Agent Notebooks
```python
# Run in sequence:
# - notebooks/05_agent_logic_close_supervisor.py
# - notebooks/06_agent_logic_fx_and_pre_close.py
# - notebooks/07_agent_logic_segmented_and_forecast.py
```
**What it does:**
- Orchestrates close workflow
- Validates FX and trial balance
- Calculates final KPIs

**Execution time:** ~5 minutes total  
**Expected output:** "✅ FINANCIAL CLOSE FOR 2025-12 IS COMPLETE"

---

### 2.6 Set Up Dashboards and Genie
```sql
# Run: notebooks/08_dashboards_and_genie_instructions.sql
```
**What it does:**
- Creates 10 SQL views for dashboards
- Provides Genie configuration instructions

**Execution time:** ~1 minute  
**Expected output:** List of views created

---

## Step 3: Verify Installation (5 minutes)

### Check Data in Gold Layer
```sql
-- In Databricks SQL editor, run:

USE CATALOG financial_close_lakehouse;

-- Check close status
SELECT * FROM gold.close_status_gold 
WHERE period = '2025-12' 
LIMIT 10;

-- Check close results
SELECT * FROM gold.close_results_gold 
WHERE period = '2025-12' 
  AND bu_code = 'CONSOLIDATED' 
LIMIT 10;

-- Check KPIs
SELECT * FROM gold.close_kpi_gold 
WHERE period = '2025-12';
```

**Expected results:**
- ✅ `close_status_gold`: ~84 rows (all phases complete)
- ✅ `close_results_gold`: ~300+ rows (segmented + consolidated)
- ✅ `close_kpi_gold`: ~12 rows (6 BUs + group-level KPIs)

---

## Step 4: Optional - Set Up Genie Space (5 minutes)

1. Navigate to **Genie** in Databricks UI
2. Click **Create Space**
3. Name: "Financial Close Assistant"
4. Add tables:
   - `gold.close_status_gold`
   - `gold.close_results_gold`
   - `gold.forecast_results_gold`
   - `gold.close_kpi_gold`
   - All views (`gold.vw_*`)
5. Paste **General Instructions** from notebook 08 output
6. Test with query: "Show me close status for December 2025"

---

## Step 5: Optional - Create Dashboards (10 minutes)

1. Open **Databricks SQL** workspace
2. Create **New Dashboard** → "Close Cockpit"
3. Add visualizations using queries from notebook 08:
   - Gauge: Overall progress
   - Bar chart: Status by phase
   - Table: Overdue tasks
4. Configure auto-refresh (15 min)
5. Repeat for "Close Results" and "Forecast Analysis" dashboards

---

## Testing the Solution

### Test 1: Query Close Status
```sql
-- Via Genie or SQL editor
SELECT 
  phase_name, 
  COUNT(*) as total_tasks,
  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
FROM gold.close_status_gold
WHERE period = '2025-12'
GROUP BY phase_name;
```
**Expected:** All phases show 100% completion

---

### Test 2: Check Variance Analysis
```sql
-- Top variances vs forecast
SELECT 
  bu_code,
  account_category,
  variance_vs_forecast,
  variance_vs_forecast_pct
FROM gold.close_results_gold
WHERE period = '2025-12'
  AND bu_code != 'CONSOLIDATED'
  AND segment IS NULL
ORDER BY ABS(variance_vs_forecast) DESC
LIMIT 5;
```
**Expected:** List of BU-level variances

---

### Test 3: Review Agent Logs
```sql
-- Recent agent actions
SELECT 
  agent_name,
  action_type,
  decision_rationale,
  log_timestamp
FROM gold.close_agent_logs
WHERE period = '2025-12'
ORDER BY log_timestamp DESC
LIMIT 10;
```
**Expected:** Log entries from supervisor, FX, pre-close, and reporting agents

---

## Automating the Close (Optional)

### Create Databricks Workflow

1. Navigate to **Workflows** → **Create Job**
2. Name: "Monthly_Close_Automation"
3. Add tasks in sequence:
   
   **Task 1:** Ingest Data (Parallel)
   - Notebook: `03_ingest_and_standardize_phase1_2.py`
   - Notebook: `04_ingest_and_standardize_phase3.py`
   
   **Task 2:** Run Supervisor Agent
   - Notebook: `05_agent_logic_close_supervisor.py`
   - Depends on: Task 1
   
   **Task 3:** Run Domain Agents (Parallel)
   - Notebook: `06_agent_logic_fx_and_pre_close.py`
   - Notebook: `07_agent_logic_segmented_and_forecast.py`
   - Depends on: Task 2

4. **Schedule:** Daily at 8 AM during close period (Days 1-12 of each month)
5. **Alerts:** Email on failure, success notification to FP&A team
6. **Cluster:** Use existing all-purpose cluster or create job cluster (Standard_DS3_v2)

---

## Sample Genie Queries to Try

Once your Genie space is set up, try these queries:

```
1. "Show me close status for December 2025 by phase"
2. "What tasks are overdue?"
3. "Show me consolidated P&L for December 2025"
4. "Which BUs exceeded forecast this month?"
5. "What's the operating margin for each BU?"
6. "Explain the top 3 variance drivers"
7. "What did the Pre-Close Agent flag?"
8. "How long did the close take?"
9. "What's our forecast accuracy this month?"
10. "Show me revenue trend for the last 6 months"
```

---

## Troubleshooting

### Issue: "Catalog not found"
**Solution:** Ensure Unity Catalog is enabled. Run:
```sql
SHOW CATALOGS;
```
If `financial_close_lakehouse` is missing, re-run notebook 01.

---

### Issue: "Permission denied"
**Solution:** Grant yourself permissions:
```sql
GRANT ALL PRIVILEGES ON CATALOG financial_close_lakehouse TO `your.email@company.com`;
```

---

### Issue: "No data in Gold tables"
**Solution:** Verify Bronze and Silver tables have data:
```sql
SELECT COUNT(*) FROM bronze.fx_rates_raw;
SELECT COUNT(*) FROM silver.fx_rates_std;
```
If counts are 0, re-run notebooks 02-04.

---

### Issue: "Genie returns 'no results'"
**Solution:** 
1. Check table permissions in Genie space settings
2. Verify tables are added to the space
3. Try a simpler query: "Show me tables in gold schema"

---

## Next Steps After Setup

1. **Customize for your organization:**
   - Update BU list in `config.business_units`
   - Modify account structure in notebook 02
   - Adjust close phase tasks in `config.close_phase_definitions`

2. **Integrate real data sources:**
   - Replace notebook 02 with Auto Loader for real files
   - Connect to SAP/Oracle via JDBC for trial balance
   - Pull FX rates from Bloomberg/Reuters APIs

3. **Enhance agents:**
   - Add ML-based anomaly detection
   - Implement forecast automation
   - Create NLP summaries of review meetings

4. **Train your team:**
   - Schedule demo session
   - Create Genie cheat sheet
   - Document custom business rules

---

## Support and Resources

### Documentation
- **Architecture:** See `README.md`
- **Notebooks:** Inline comments in each notebook
- **Genie:** Instructions in notebook 08 output

### Getting Help
- **Databricks Issues:** Contact your Databricks account team
- **Solution Questions:** Review agent logs in `gold.close_agent_logs`
- **Data Quality:** Check Bronze/Silver validation steps

### Community
- Share your success story with the Databricks community
- Contribute enhancements back to this repo
- Connect with other FP&A teams using Databricks

---

## Success Criteria

You've successfully deployed the solution when:

✅ All notebooks run without errors  
✅ Gold tables contain data for period 2025-12  
✅ All close phases show 100% completion  
✅ Agent logs show decisions and actions  
✅ SQL queries return expected results  
✅ Genie space answers questions correctly  
✅ Dashboards display charts and metrics  

**Congratulations! You now have an intelligent financial close solution running on Azure Databricks.** 🎉

---

## Estimated Timeline

| Step | Time | Cumulative |
|------|------|------------|
| Upload notebooks | 5 min | 5 min |
| Run setup (notebooks 01-04) | 15 min | 20 min |
| Run agent notebooks (05-07) | 5 min | 25 min |
| Verify installation | 5 min | 30 min |
| **TOTAL** | **30 min** | **Ready to use** |

*Optional steps (Genie + Dashboards): +15 minutes*

---

For detailed architecture and design decisions, see the main `README.md`.
