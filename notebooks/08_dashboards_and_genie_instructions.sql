# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Dashboards and Genie Configuration
# MAGIC 
# MAGIC **Purpose:** Create SQL views for dashboards and provide Genie space configuration
# MAGIC 
# MAGIC **Creates:**
# MAGIC - SQL views optimized for dashboard queries
# MAGIC - Sample queries for Genie space
# MAGIC - Dashboard layout suggestions
# MAGIC - Genie instructions and metadata
# MAGIC 
# MAGIC **Note:** This notebook generates the SQL artifacts. Actual dashboard creation and Genie space
# MAGIC setup should be done via Databricks SQL/AI-BI UI and Genie UI respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

spark.sql("USE CATALOG financial_close_lakehouse")

print("✓ Creating views and configuration for dashboards and Genie")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create SQL Views for Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 1: Close Cockpit - Overall Progress

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_close_cockpit_progress AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   COUNT(*) as total_tasks,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
# MAGIC   SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_tasks,
# MAGIC   SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
# MAGIC   SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked_tasks,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) as completion_pct,
# MAGIC   MAX(updated_at) as last_update
# MAGIC FROM gold.close_status_gold
# MAGIC GROUP BY period
# MAGIC ORDER BY period DESC

# COMMAND ----------

print("✓ Created view: vw_close_cockpit_progress")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 2: Close Cockpit - Status by Phase

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_close_status_by_phase AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   phase_id,
# MAGIC   phase_name,
# MAGIC   COUNT(*) as total_tasks,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
# MAGIC   SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_tasks,
# MAGIC   SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
# MAGIC   SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked_tasks,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) as completion_pct,
# MAGIC   MIN(planned_due_date) as earliest_due_date,
# MAGIC   MAX(actual_completion_timestamp) as latest_completion
# MAGIC FROM gold.close_status_gold
# MAGIC GROUP BY period, phase_id, phase_name
# MAGIC ORDER BY period DESC, phase_id

# COMMAND ----------

print("✓ Created view: vw_close_status_by_phase")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 3: Close Cockpit - Overdue Tasks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_overdue_tasks AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   phase_name,
# MAGIC   task_id,
# MAGIC   task_name,
# MAGIC   bu_code,
# MAGIC   owner_role,
# MAGIC   planned_due_date,
# MAGIC   status,
# MAGIC   DATEDIFF(CURRENT_DATE(), planned_due_date) as days_overdue,
# MAGIC   comments
# MAGIC FROM gold.close_status_gold
# MAGIC WHERE status != 'completed'
# MAGIC   AND planned_due_date < CURRENT_DATE()
# MAGIC ORDER BY days_overdue DESC, phase_id, task_id

# COMMAND ----------

print("✓ Created view: vw_overdue_tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 4: Close Cockpit - Tasks by BU

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_close_status_by_bu AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   COALESCE(bu_code, 'Group') as bu_code,
# MAGIC   phase_name,
# MAGIC   COUNT(*) as total_tasks,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) as completion_pct
# MAGIC FROM gold.close_status_gold
# MAGIC GROUP BY period, bu_code, phase_name
# MAGIC ORDER BY period DESC, bu_code, phase_name

# COMMAND ----------

print("✓ Created view: vw_close_status_by_bu")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 5: Close Results - P&L Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_close_pl_summary AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   bu_code,
# MAGIC   segment,
# MAGIC   account_category,
# MAGIC   actual_amount,
# MAGIC   forecast_amount,
# MAGIC   prior_period_amount,
# MAGIC   variance_vs_forecast,
# MAGIC   variance_vs_forecast_pct,
# MAGIC   variance_vs_prior,
# MAGIC   variance_vs_prior_pct,
# MAGIC   fx_impact
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE account_category IN ('Revenue', 'COGS', 'OpEx', 'Operating Profit')
# MAGIC ORDER BY period DESC, bu_code, account_category

# COMMAND ----------

print("✓ Created view: vw_close_pl_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 6: Close Results - Consolidated P&L

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_consolidated_pl AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   account_category,
# MAGIC   actual_amount,
# MAGIC   forecast_amount,
# MAGIC   prior_period_amount,
# MAGIC   variance_vs_forecast,
# MAGIC   variance_vs_forecast_pct,
# MAGIC   variance_vs_prior,
# MAGIC   variance_vs_prior_pct
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE bu_code = 'CONSOLIDATED'
# MAGIC   AND segment IS NULL
# MAGIC   AND account_category IN ('Revenue', 'COGS', 'OpEx', 'Operating Profit')
# MAGIC ORDER BY period DESC,
# MAGIC   CASE account_category
# MAGIC     WHEN 'Revenue' THEN 1
# MAGIC     WHEN 'COGS' THEN 2
# MAGIC     WHEN 'OpEx' THEN 3
# MAGIC     WHEN 'Operating Profit' THEN 4
# MAGIC   END

# COMMAND ----------

print("✓ Created view: vw_consolidated_pl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 7: Close Results - Top Variances

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_top_variances AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   bu_code,
# MAGIC   segment,
# MAGIC   product,
# MAGIC   region,
# MAGIC   account_category,
# MAGIC   actual_amount,
# MAGIC   forecast_amount,
# MAGIC   variance_vs_forecast,
# MAGIC   variance_vs_forecast_pct,
# MAGIC   ABS(variance_vs_forecast) as abs_variance
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE bu_code != 'CONSOLIDATED'
# MAGIC   AND variance_vs_forecast IS NOT NULL
# MAGIC   AND ABS(variance_vs_forecast_pct) > 10  -- Only significant variances
# MAGIC ORDER BY period DESC, abs_variance DESC

# COMMAND ----------

print("✓ Created view: vw_top_variances")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 8: Forecast Dashboard - Scenario Comparison

# MAGIC %md
# MAGIC ### View 8: Forecast Dashboard - Scenario Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_forecast_scenarios AS
# MAGIC SELECT 
# MAGIC   forecast_period,
# MAGIC   bu_code,
# MAGIC   segment,
# MAGIC   scenario,
# MAGIC   account_category,
# MAGIC   forecast_amount
# MAGIC FROM gold.forecast_results_gold
# MAGIC WHERE account_category IN ('Revenue', 'COGS', 'OpEx', 'Operating Profit')
# MAGIC ORDER BY forecast_period DESC, bu_code, scenario, account_category

# COMMAND ----------

print("✓ Created view: vw_forecast_scenarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 9: KPI Dashboard - All Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_close_kpis AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   kpi_category,
# MAGIC   kpi_name,
# MAGIC   kpi_value,
# MAGIC   kpi_unit,
# MAGIC   target_value,
# MAGIC   COALESCE(bu_code, 'Group') as bu_code,
# MAGIC   CASE 
# MAGIC     WHEN target_value IS NULL THEN 'N/A'
# MAGIC     WHEN kpi_unit = '%' AND kpi_value >= target_value THEN 'On Target'
# MAGIC     WHEN kpi_unit = 'days' AND kpi_value <= target_value THEN 'On Target'
# MAGIC     WHEN kpi_unit = 'count' AND kpi_value <= target_value THEN 'On Target'
# MAGIC     ELSE 'Below Target'
# MAGIC   END as target_status,
# MAGIC   calculation_notes
# MAGIC FROM gold.close_kpi_gold
# MAGIC ORDER BY period DESC, kpi_category, bu_code, kpi_name

# COMMAND ----------

print("✓ Created view: vw_close_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View 10: Agent Activity Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_agent_activity AS
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   agent_name,
# MAGIC   action_type,
# MAGIC   log_timestamp,
# MAGIC   task_id,
# MAGIC   COALESCE(bu_code, 'Group') as bu_code,
# MAGIC   decision_rationale,
# MAGIC   status,
# MAGIC   execution_time_ms
# MAGIC FROM gold.close_agent_logs
# MAGIC ORDER BY log_timestamp DESC

# COMMAND ----------

print("✓ Created view: vw_agent_activity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Sample Dashboard Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 1: Close Cockpit

# COMMAND ----------

print("\n" + "="*80)
print("DASHBOARD 1: CLOSE COCKPIT")
print("="*80)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 1: Overall Progress Gauge
# MAGIC SELECT 
# MAGIC   completion_pct as value,
# MAGIC   'Overall Progress' as metric
# MAGIC FROM gold.vw_close_cockpit_progress
# MAGIC WHERE period = '2025-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 2: Days Since Period End
# MAGIC SELECT 
# MAGIC   DATEDIFF(CURRENT_DATE(), LAST_DAY(TO_DATE('2025-12-01', 'yyyy-MM-dd'))) as days_elapsed,
# MAGIC   12 as target_days

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 3: Status by Phase (Stacked Bar)
# MAGIC SELECT 
# MAGIC   phase_name,
# MAGIC   completed_tasks,
# MAGIC   in_progress_tasks,
# MAGIC   pending_tasks,
# MAGIC   blocked_tasks
# MAGIC FROM gold.vw_close_status_by_phase
# MAGIC WHERE period = '2025-12'
# MAGIC ORDER BY phase_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 4: Overdue Tasks Table
# MAGIC SELECT 
# MAGIC   task_name,
# MAGIC   bu_code,
# MAGIC   owner_role,
# MAGIC   days_overdue,
# MAGIC   status
# MAGIC FROM gold.vw_overdue_tasks
# MAGIC WHERE period = '2025-12'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 5: Recent Agent Actions
# MAGIC SELECT 
# MAGIC   agent_name,
# MAGIC   action_type,
# MAGIC   decision_rationale,
# MAGIC   log_timestamp
# MAGIC FROM gold.vw_agent_activity
# MAGIC WHERE period = '2025-12'
# MAGIC ORDER BY log_timestamp DESC
# MAGIC LIMIT 10

# COMMAND ----------

print("✓ Close Cockpit dashboard queries ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 2: Close Results

# COMMAND ----------

print("\n" + "="*80)
print("DASHBOARD 2: CLOSE RESULTS")
print("="*80)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 1: Consolidated P&L
# MAGIC SELECT 
# MAGIC   account_category,
# MAGIC   actual_amount,
# MAGIC   forecast_amount,
# MAGIC   prior_period_amount,
# MAGIC   variance_vs_forecast,
# MAGIC   variance_vs_forecast_pct
# MAGIC FROM gold.vw_consolidated_pl
# MAGIC WHERE period = '2025-12'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 2: BU Performance Matrix (Revenue Growth vs Margin)
# MAGIC WITH bu_metrics AS (
# MAGIC   SELECT 
# MAGIC     bu_code,
# MAGIC     SUM(CASE WHEN account_category = 'Revenue' THEN variance_vs_prior_pct ELSE 0 END) as revenue_growth_pct,
# MAGIC     100.0 * SUM(CASE WHEN account_category = 'Operating Profit' THEN actual_amount ELSE 0 END) / 
# MAGIC       NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN actual_amount ELSE 0 END), 0) as margin_pct
# MAGIC   FROM gold.close_results_gold
# MAGIC   WHERE period = '2025-12'
# MAGIC     AND bu_code != 'CONSOLIDATED'
# MAGIC     AND segment IS NULL
# MAGIC   GROUP BY bu_code
# MAGIC )
# MAGIC SELECT * FROM bu_metrics
# MAGIC ORDER BY margin_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 3: Top 10 Variances
# MAGIC SELECT 
# MAGIC   bu_code,
# MAGIC   segment,
# MAGIC   account_category,
# MAGIC   variance_vs_forecast,
# MAGIC   variance_vs_forecast_pct
# MAGIC FROM gold.vw_top_variances
# MAGIC WHERE period = '2025-12'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 4: Revenue Trend (12 months)
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   SUM(actual_amount) as total_revenue
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE account_category = 'Revenue'
# MAGIC   AND bu_code = 'CONSOLIDATED'
# MAGIC   AND segment IS NULL
# MAGIC   AND period >= '2025-01'
# MAGIC GROUP BY period
# MAGIC ORDER BY period

# COMMAND ----------

print("✓ Close Results dashboard queries ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 3: Forecast Analysis

# COMMAND ----------

print("\n" + "="*80)
print("DASHBOARD 3: FORECAST ANALYSIS")
print("="*80)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 1: Forecast vs Actuals (Combo Chart)
# MAGIC SELECT 
# MAGIC   period,
# MAGIC   SUM(CASE WHEN account_category = 'Revenue' THEN actual_amount ELSE 0 END) as actual_revenue,
# MAGIC   SUM(CASE WHEN account_category = 'Revenue' THEN forecast_amount ELSE 0 END) as forecast_revenue,
# MAGIC   SUM(CASE WHEN account_category = 'Operating Profit' THEN actual_amount ELSE 0 END) as actual_op,
# MAGIC   SUM(CASE WHEN account_category = 'Operating Profit' THEN forecast_amount ELSE 0 END) as forecast_op
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE bu_code = 'CONSOLIDATED'
# MAGIC   AND segment IS NULL
# MAGIC   AND period >= '2025-01'
# MAGIC GROUP BY period
# MAGIC ORDER BY period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 2: Scenario Comparison (Base vs Upside vs Downside)
# MAGIC SELECT 
# MAGIC   scenario,
# MAGIC   SUM(CASE WHEN account_category = 'Revenue' THEN forecast_amount ELSE 0 END) as revenue,
# MAGIC   SUM(CASE WHEN account_category = 'Operating Profit' THEN forecast_amount ELSE 0 END) as operating_profit
# MAGIC FROM gold.vw_forecast_scenarios
# MAGIC WHERE forecast_period = '2025-12'
# MAGIC   AND bu_code = 'CONSOLIDATED'
# MAGIC GROUP BY scenario
# MAGIC ORDER BY scenario

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 3: Forecast Accuracy by BU (MAPE)
# MAGIC SELECT 
# MAGIC   bu_code,
# MAGIC   AVG(ABS(variance_vs_forecast_pct)) as mape
# MAGIC FROM gold.close_results_gold
# MAGIC WHERE period = '2025-12'
# MAGIC   AND bu_code != 'CONSOLIDATED'
# MAGIC   AND segment IS NULL
# MAGIC   AND account_category IN ('Revenue', 'Operating Profit')
# MAGIC   AND forecast_amount != 0
# MAGIC GROUP BY bu_code
# MAGIC ORDER BY mape

# COMMAND ----------

print("✓ Forecast Analysis dashboard queries ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Genie Space Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie Space Setup Instructions

# COMMAND ----------

genie_config = """
================================================================================
GENIE SPACE CONFIGURATION FOR FINANCIAL CLOSE
================================================================================

SPACE NAME: Financial Close Assistant

DESCRIPTION:
AI-powered assistant for FP&A and Controlling teams to analyze monthly 
financial close results, track progress, and investigate variances.

================================================================================
TABLES TO INCLUDE:
================================================================================

Gold Layer (Primary):
  ✓ gold.close_status_gold - Close progress tracking
  ✓ gold.close_results_gold - Final P&L with variances
  ✓ gold.forecast_results_gold - Forecast and scenario analysis
  ✓ gold.close_kpi_gold - Key performance indicators
  ✓ gold.close_agent_logs - Agent decision audit trail

Views (Recommended):
  ✓ gold.vw_close_cockpit_progress
  ✓ gold.vw_close_status_by_phase
  ✓ gold.vw_overdue_tasks
  ✓ gold.vw_consolidated_pl
  ✓ gold.vw_top_variances
  ✓ gold.vw_close_kpis

Silver Layer (For Drill-Down):
  ○ silver.close_trial_balance_std - Detailed trial balance
  ○ silver.segmented_close_std - Segment-level detail

Config (Reference):
  ○ config.business_units - BU master data
  ○ config.close_phase_definitions - Task definitions

================================================================================
GENERAL INSTRUCTIONS (Paste into Genie Space Settings):
================================================================================

You are a financial close assistant for FP&A and Controlling teams.

**Fiscal Calendar:**
- Periods are labeled as 'YYYY-MM' (e.g., '2025-12' for December 2025)
- Close cycle SLA: 12 business days after month-end
- Current period: 2025-12 (use this as default if period not specified)

**KPI Definitions:**
- **Operating Profit** = Revenue + COGS + OpEx (COGS and OpEx are negative)
- **Operating Margin** = Operating Profit / Revenue × 100
- **FX Impact** = Difference between local currency and reporting currency (USD) results
- **Cycle Time** = Days from period end to final publication
- **Timeliness Score** = % of tasks completed by planned due date
- **MAPE (Forecast Accuracy)** = Mean Absolute Percentage Error

**Business Unit Codes:**
- NA = North America (USD)
- EU = Europe (EUR)
- UK = United Kingdom (GBP)
- ASIA = Asia Pacific (CNY)
- LATAM = Latin America (BRL)
- INDIA = India (INR)
- CONSOLIDATED = Group consolidated results

**Account Categories:**
- Revenue: Product sales, services, other income
- COGS: Cost of goods sold, direct labor
- OpEx: Operating expenses (S&M, R&D, G&A, D&A)
- Interest: Interest expense
- Tax: Income tax expense

**Response Style:**
1. Start with executive summary (2-3 sentences)
2. Provide key numbers with proper formatting (use $ for USD, % for percentages)
3. Offer drill-down suggestions if the user might want more detail
4. Always cite the table, period, and BU for transparency
5. Flag unusual items (variances >10%, overdue tasks, quality issues)

**Data Freshness:**
- Gold tables are updated after each close phase completes
- Agent logs are written in real-time as agents run
- Status updates occur hourly during close period

**Common Patterns:**
- "Show me X for [period]" - default to period='2025-12' if not specified
- "What caused Y?" - look at variances, agent logs, and segmented data
- "Which BUs..." - exclude 'CONSOLIDATED' unless explicitly requested
- "Top N..." - order by absolute value of variance or amount

**Example Queries:**
- "Show me close status for December 2025 by phase"
- "What are the top 3 variance drivers vs forecast?"
- "List overdue tasks and their owners"
- "Explain why Europe's operating profit is below forecast"
- "Compare Base vs Upside scenario for Q1 2026"

================================================================================
"""

print(genie_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Genie Queries (To Save in Space)

# COMMAND ----------

sample_queries = """
================================================================================
SAMPLE GENIE QUERIES TO SAVE IN SPACE
================================================================================

1. CLOSE STATUS & PROGRESS
   ========================
   
   Query: "Show me the status of the current month close by BU and phase"
   SQL Hint: SELECT bu_code, phase_name, completion_pct FROM gold.vw_close_status_by_bu WHERE period='2025-12'
   
   Query: "What tasks are overdue and who owns them?"
   SQL Hint: SELECT * FROM gold.vw_overdue_tasks WHERE period='2025-12'
   
   Query: "How many days has the close been running?"
   SQL Hint: SELECT DATEDIFF(CURRENT_DATE(), LAST_DAY(TO_DATE('2025-12-01'))) as days_elapsed
   
   Query: "What phase are we in and what's the completion percentage?"
   SQL Hint: SELECT phase_name, completion_pct FROM gold.vw_close_status_by_phase WHERE period='2025-12' AND completion_pct < 100 ORDER BY phase_id LIMIT 1

2. VARIANCE ANALYSIS
   ==================
   
   Query: "Show me the top 3 drivers of variance between this month's close and the previous forecast"
   SQL Hint: SELECT * FROM gold.vw_top_variances WHERE period='2025-12' LIMIT 3
   
   Query: "Explain why BU Europe's operating profit is below forecast this month"
   Multi-step:
     1. Get BU-level variance: SELECT * FROM gold.close_results_gold WHERE period='2025-12' AND bu_code='EU' AND account_category='Operating Profit'
     2. Get segment detail: SELECT segment, variance_vs_forecast FROM gold.close_results_gold WHERE period='2025-12' AND bu_code='EU' ORDER BY ABS(variance_vs_forecast) DESC
     3. Check agent findings: SELECT decision_rationale FROM gold.close_agent_logs WHERE period='2025-12' AND bu_code='EU' AND action_type='validation'
   
   Query: "Which BUs exceeded forecast this month?"
   SQL Hint: SELECT bu_code, variance_vs_forecast_pct FROM gold.close_results_gold WHERE period='2025-12' AND account_category='Operating Profit' AND variance_vs_forecast_pct > 0
   
   Query: "What's the FX impact by BU?"
   SQL Hint: SELECT bu_code, SUM(fx_impact) as total_fx_impact FROM gold.close_results_gold WHERE period='2025-12' GROUP BY bu_code

3. CONSOLIDATED RESULTS
   =====================
   
   Query: "Show me consolidated P&L for December 2025"
   SQL Hint: SELECT * FROM gold.vw_consolidated_pl WHERE period='2025-12'
   
   Query: "What's our operating margin this month vs last month?"
   SQL Hint: SELECT period, 100.0 * SUM(CASE WHEN account_category='Operating Profit' THEN actual_amount END) / SUM(CASE WHEN account_category='Revenue' THEN actual_amount END) as margin FROM gold.close_results_gold WHERE bu_code='CONSOLIDATED' AND period IN ('2025-11','2025-12') GROUP BY period
   
   Query: "How did revenue trend over the last 6 months?"
   SQL Hint: SELECT period, actual_amount FROM gold.close_results_gold WHERE account_category='Revenue' AND bu_code='CONSOLIDATED' AND period >= '2025-07' ORDER BY period

4. FORECAST ANALYSIS
   ==================
   
   Query: "Compare forecast scenarios (Base, Upside, Downside) for revenue"
   SQL Hint: SELECT scenario, forecast_amount FROM gold.forecast_results_gold WHERE forecast_period='2025-12' AND account_category='Revenue' AND bu_code='CONSOLIDATED'
   
   Query: "What's our forecast accuracy (MAPE) this month?"
   SQL Hint: SELECT kpi_value FROM gold.vw_close_kpis WHERE period='2025-12' AND kpi_name='Forecast Accuracy (MAPE)'
   
   Query: "Which BU has the best forecast accuracy?"
   SQL Hint: SELECT bu_code, AVG(ABS(variance_vs_forecast_pct)) as mape FROM gold.close_results_gold WHERE period='2025-12' AND bu_code !='CONSOLIDATED' AND account_category='Revenue' GROUP BY bu_code ORDER BY mape LIMIT 1

5. AGENT INSIGHTS
   ===============
   
   Query: "What did the Pre-Close Agent flag this month?"
   SQL Hint: SELECT decision_rationale, bu_code FROM gold.close_agent_logs WHERE period='2025-12' AND agent_name='pre_close_agent' AND status='warning'
   
   Query: "Show me all agent alerts and warnings"
   SQL Hint: SELECT agent_name, decision_rationale, log_timestamp FROM gold.vw_agent_activity WHERE period='2025-12' AND status IN ('warning','error') ORDER BY log_timestamp DESC
   
   Query: "Did any agents detect unusual variances?"
   SQL Hint: SELECT decision_rationale FROM gold.close_agent_logs WHERE period='2025-12' AND action_type='validation' AND decision_rationale LIKE '%unusual%'

6. KPIs & METRICS
   ===============
   
   Query: "Show me all KPIs for this close"
   SQL Hint: SELECT * FROM gold.vw_close_kpis WHERE period='2025-12' AND bu_code='Group'
   
   Query: "How long did the close take vs our SLA?"
   SQL Hint: SELECT kpi_value, target_value FROM gold.vw_close_kpis WHERE period='2025-12' AND kpi_name='Close Cycle Time'
   
   Query: "Which BU had the best timeliness score?"
   SQL Hint: SELECT bu_code, kpi_value FROM gold.vw_close_kpis WHERE period='2025-12' AND kpi_name='BU Timeliness Score' ORDER BY kpi_value DESC LIMIT 1

================================================================================
"""

print(sample_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Dashboard Layout Suggestions

# COMMAND ----------

dashboard_layouts = """
================================================================================
DASHBOARD LAYOUTS FOR DATABRICKS SQL / AI-BI
================================================================================

DASHBOARD 1: CLOSE COCKPIT
===========================
Purpose: Real-time monitoring of close progress
Refresh: Every 15 minutes (or on-demand)
Audience: FP&A team, Close manager

Layout (3 rows):

Row 1: Key Metrics (Counters)
  [Overall Progress]  [Days Elapsed]  [Overdue Tasks]  [Agent Alerts]
     Gauge: 85%        Counter: 12       Counter: 3      Counter: 2
    Target: 100%      Target: 12 days    Red if >0       Red if >0

Row 2: Phase Progress (Stacked Bar + Table)
  [Status by Phase - Horizontal Stacked Bar]          [Overdue Tasks - Table]
  Phase 1: ████████████████████ 100% (18/18)          Task | BU | Owner | Days
  Phase 2: ████████████████████ 100% (30/30)          301  | EU | FP&A  | 2
  Phase 3: ████████████████████ 100% (18/18)          302  | UK | FP&A  | 1
  Phase 4: ████████████████████ 100% (12/12)
  Phase 5: ████████████████████ 100% (6/6)

Row 3: BU Heatmap + Agent Activity
  [Tasks by BU - Heatmap]                              [Recent Agent Actions]
         Phase1 Phase2 Phase3 Phase4 Phase5           Agent     | Action | Time
  NA       ✅     ✅     ✅     ✅     ✅               FX Agent  | Validated | 10:15
  EU       ✅     ✅     ✅     ✅     ✅               Pre-Close | Flagged   | 11:30
  UK       ✅     ✅     ⚠️     ⏳     ⏳               Supervisor| Transitioned|12:00
  ASIA     ✅     ✅     ✅     ✅     ✅
  
Filters: Period (dropdown), Status (multi-select)


DASHBOARD 2: CLOSE RESULTS
===========================
Purpose: Financial analysis and variance investigation
Refresh: After Phase 5 completion
Audience: CFO, FP&A leadership, BU controllers

Layout (4 rows):

Row 1: Consolidated Summary (Cards)
  [Revenue]           [Operating Profit]     [Operating Margin]    [vs Forecast]
  $125.5M             $18.3M                 14.6%                 -2.1% ⚠️
  +3.2% vs LM        +1.8% vs LM            -30 bps vs LM         -$2.7M

Row 2: P&L Waterfall + BU Performance
  [P&L Waterfall Chart]                                [BU Performance Matrix]
  Prior │ Volume │ Price │ FX │ Mix │ Current          Scatter: x=Rev Growth%, y=Margin%
  $122M │ +$2.5M │ +$1.5M│-$0.5M│...│ $125.5M         NA (high/high), EU (low/high), etc.

Row 3: Segment Analysis
  [Segment Tree Map - by Operating Profit]            [Top 10 Variances - Bar Chart]
  Enterprise (largest)                                 1. EU-Enterprise-Revenue: -$1.2M
    └ NA: $5M, EU: $3M, ASIA: $2M                     2. ASIA-Consumer-COGS: +$0.8M
  SMB                                                  3. NA-SMB-OpEx: +$0.6M
  Consumer
  Government (smallest)

Row 4: Trend Analysis
  [Revenue & OP Trend - Line Chart (12 months)]       [Margin Trend - Area Chart]
  Revenue: upward trend, slight dip in Dec            Margin: stable 14-15% range
  OP: following revenue pattern

Filters: Period, BU, Segment, Account Category
Actions: Click on variance → Drill to segment detail


DASHBOARD 3: FORECAST ANALYSIS
===============================
Purpose: Forward-looking analysis and scenario planning
Refresh: After Phase 4 completion
Audience: CFO, FP&A leadership, Planning team

Layout (3 rows):

Row 1: Actual vs Forecast
  [Revenue: Actual vs Forecast - Combo Chart]         [Operating Profit: Actual vs Forecast]
  Bars=Actual, Line=Forecast, 12-month view           Same layout as Revenue
  Show variance bands (±5% tolerance)

Row 2: Scenario Comparison
  [Base vs Upside vs Downside - Grouped Bar Chart]    [Scenario Sensitivities - Table]
                Base    Upside  Downside              Scenario  | Rev | OP  | Margin
  Revenue      $125M    $144M   $106M                 Base      |$125M|$18M | 14.6%
  Op Profit    $18M     $22M    $15M                  Upside    |$144M|$22M | 15.3%
  Margin       14.6%    15.3%   14.2%                 Downside  |$106M|$15M | 14.2%

Row 3: Forecast Accuracy
  [MAPE by BU - Horizontal Bar]                       [Forecast vs Actual Trend - Line]
  NA:    3.2% ✅                                      12-month comparison showing accuracy
  EU:    5.8% ⚠️                                     improvement over time
  UK:    4.1% ✅
  ASIA:  6.2% ⚠️
  LATAM: 7.5% 🔴
  INDIA: 4.8% ✅
  
Filters: Period, Scenario, BU
Actions: Export to PowerPoint, Schedule email

================================================================================
DASHBOARD IMPLEMENTATION STEPS
================================================================================

1. In Databricks SQL workspace, click "Create" → "Dashboard"
2. Add visualizations using the queries from Part 2
3. Configure chart types:
   - Gauges: Use "Counter" visualization with target line
   - Stacked bars: Use "Bar" with stacking enabled
   - Heatmaps: Use "Table" with conditional formatting
   - Waterfalls: Use "Bar" with category ordering
   - Scatter: Use "Scatter" visualization
   - Tree map: Use "Tree Map" (if available) or nested bars
4. Add filters at dashboard level (period, BU, segment)
5. Configure auto-refresh (15 min for Close Cockpit, manual for others)
6. Set permissions:
   - Viewers: All FP&A users
   - Editors: FP&A architects
   - Owners: Finance IT team
7. Schedule email delivery (daily during close, weekly otherwise)
8. Add dashboard to Genie space for easy discovery

================================================================================
"""

print(dashboard_layouts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Summary and Next Steps

# COMMAND ----------

summary = """
================================================================================
DASHBOARDS & GENIE CONFIGURATION - SUMMARY
================================================================================

✅ COMPLETED:
-------------
1. Created 10 SQL views optimized for dashboards:
   - Close Cockpit: progress, phases, overdue tasks, BU status
   - Close Results: P&L, variances, trends
   - Forecast: scenarios, accuracy
   - KPIs: all metrics with targets
   - Agent activity logs

2. Provided sample queries for 3 dashboards:
   - Close Cockpit (real-time monitoring)
   - Close Results (variance analysis)
   - Forecast Analysis (scenario planning)

3. Generated Genie space configuration:
   - Table selection guidance
   - General instructions with KPI definitions
   - 25+ sample queries organized by category
   - Response style guidelines

4. Created dashboard layout specifications:
   - Detailed mockups for 3 dashboards
   - Chart type recommendations
   - Filter and interaction design
   - Implementation steps

📋 NEXT STEPS (Manual Setup Required):
---------------------------------------

A. Set Up Genie Space:
   1. Navigate to Genie in Databricks UI
   2. Create new space: "Financial Close Assistant"
   3. Add tables from list in "Genie Config" section above
   4. Paste General Instructions into space settings
   5. Create and save sample queries from list above
   6. Test with queries: "Show me close status for December 2025"
   7. Grant access to FP&A team

B. Create Dashboards:
   1. Open Databricks SQL workspace
   2. Create Dashboard 1: Close Cockpit
      - Add 5 visualizations using queries from Part 2
      - Configure auto-refresh (15 min)
   3. Create Dashboard 2: Close Results
      - Add 6 visualizations
      - Set up drill-down actions
   4. Create Dashboard 3: Forecast Analysis
      - Add 5 visualizations
      - Configure scenario filters
   5. Test all dashboards with current data
   6. Schedule email delivery to FP&A team

C. Integration:
   1. Add dashboard links to Genie space description
   2. Create Databricks workspace homepage with:
      - Quick link to Genie space
      - Embedded Close Cockpit dashboard
      - Links to other dashboards
   3. Set up alerts:
      - Email when tasks overdue
      - Slack when agents raise warnings
      - Daily summary during close period

D. Training & Documentation:
   1. Create 15-min video walkthrough of:
      - How to use Genie for close queries
      - Dashboard navigation and filters
      - Where to find agent logs and insights
   2. Prepare Genie "cheat sheet" with top 20 queries
   3. Schedule training session for FP&A team
   4. Document troubleshooting (e.g., "no data found" → permissions)

================================================================================
TESTING CHECKLIST:
================================================================================

□ Views created successfully (run SELECT * FROM gold.vw_* to verify)
□ Genie space created and tables added
□ Sample query in Genie returns results
□ Close Cockpit dashboard renders without errors
□ Filters on dashboards work correctly
□ Drill-down actions navigate to detail views
□ Email alerts configured and tested
□ Permissions granted to FP&A users
□ Training materials prepared

================================================================================
SUPPORT CONTACTS:
================================================================================

Genie Issues: Databricks Support (Premium tier)
Dashboard Design: FP&A Architecture Team
Data Questions: Data Engineering Team
Access/Permissions: IT Security Team

For questions about the financial close solution:
- Solution architecture: FP&A Architects
- Agent behavior: Review gold.close_agent_logs
- Data quality: Check silver layer validation rules

================================================================================
"""

print(summary)

# COMMAND ----------

# Show all views created
views_created = spark.sql("""
  SELECT table_name, comment
  FROM financial_close_lakehouse.information_schema.tables
  WHERE table_schema = 'gold'
    AND table_type = 'VIEW'
  ORDER BY table_name
""")

print("\n✓ Views Created:")
display(views_created)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Dashboards and Genie Configuration Complete
# MAGIC 
# MAGIC **Deliverables:**
# MAGIC - ✓ 10 SQL views for dashboards
# MAGIC - ✓ Sample queries for 3 dashboards
# MAGIC - ✓ Genie space configuration with instructions
# MAGIC - ✓ 25+ sample Genie queries
# MAGIC - ✓ Dashboard layout specifications
# MAGIC - ✓ Implementation guide
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Follow "Next Steps" section above to create Genie space
# MAGIC 2. Build dashboards in Databricks SQL using provided queries
# MAGIC 3. Test with FP&A team and gather feedback
# MAGIC 4. Schedule training session
# MAGIC 
# MAGIC **Documentation:**
# MAGIC - All configuration text is captured in notebook output
# MAGIC - Copy-paste instructions directly into Genie UI
# MAGIC - Use dashboard layouts as mockups for implementation
