# Databricks notebook source
# MAGIC %sql
# MAGIC -- Financial Close - Dashboards and Genie Instructions
# MAGIC -- 
# MAGIC -- This notebook contains:
# MAGIC -- 1. Helper views for dashboards
# MAGIC -- 2. Sample dashboard queries
# MAGIC -- 3. Genie space instructions and example prompts
# MAGIC -- 
# MAGIC -- Use these queries to create Databricks SQL dashboards and configure Genie spaces.

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

-- Set catalog and schema
USE CATALOG financial_close_catalog;
USE SCHEMA gold_layer;

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Views for Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 1: Current Close Status Summary

# COMMAND ----------

CREATE OR REPLACE VIEW v_current_close_status AS
SELECT 
  period,
  bu,
  phase_id,
  phase_name,
  overall_status,
  pct_tasks_completed,
  total_tasks,
  completed_tasks,
  blocked_tasks,
  days_since_period_end,
  days_to_sla,
  sla_status,
  key_issues,
  last_milestone,
  next_milestone,
  agent_summary,
  load_timestamp
FROM financial_close_catalog.gold_layer.close_status_gold
WHERE period = (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_status_gold)
ORDER BY 
  CASE WHEN bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
  bu;

SELECT 'View v_current_close_status created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 2: Task Details with Ownership

# COMMAND ----------

CREATE OR REPLACE VIEW v_task_details AS
SELECT 
  period,
  phase_id,
  phase_name,
  task_id,
  task_name,
  bu,
  owner_role,
  planned_due_date,
  actual_start_timestamp,
  actual_completion_timestamp,
  status,
  priority,
  blocking_reason,
  comments,
  agent_assigned,
  CASE 
    WHEN status = 'completed' THEN 0
    WHEN planned_due_date < CURRENT_TIMESTAMP() THEN 
      CAST((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(planned_due_date)) / 3600 AS INT)
    ELSE 0
  END AS hours_overdue,
  last_updated_timestamp
FROM financial_close_catalog.gold_layer.close_phase_tasks
ORDER BY period DESC, phase_id, task_id;

SELECT 'View v_task_details created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 3: Financial Results with Prior Period Comparison

# COMMAND ----------

CREATE OR REPLACE VIEW v_financial_results_comparison AS
WITH current_period AS (
  SELECT 
    period,
    bu,
    segment,
    revenue_reporting,
    operating_profit_reporting,
    operating_margin_pct,
    fx_impact_reporting
  FROM financial_close_catalog.gold_layer.close_results_gold
  WHERE period = (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_results_gold)
    AND segment = 'ALL'
    AND product = 'ALL'
    AND region = 'ALL'
),
prior_period AS (
  SELECT 
    period,
    bu,
    revenue_reporting AS prior_revenue,
    operating_profit_reporting AS prior_operating_profit,
    operating_margin_pct AS prior_margin
  FROM financial_close_catalog.gold_layer.close_results_gold
  WHERE period = (SELECT MAX(period) - 1 FROM financial_close_catalog.gold_layer.close_results_gold)
    AND segment = 'ALL'
    AND product = 'ALL'
    AND region = 'ALL'
)
SELECT 
  c.period,
  c.bu,
  c.segment,
  c.revenue_reporting AS current_revenue,
  p.prior_revenue,
  c.revenue_reporting - p.prior_revenue AS revenue_variance,
  ROUND(((c.revenue_reporting - p.prior_revenue) / p.prior_revenue) * 100, 2) AS revenue_variance_pct,
  c.operating_profit_reporting AS current_op_profit,
  p.prior_operating_profit AS prior_op_profit,
  c.operating_profit_reporting - p.prior_operating_profit AS op_profit_variance,
  ROUND(((c.operating_profit_reporting - p.prior_operating_profit) / p.prior_operating_profit) * 100, 2) AS op_profit_variance_pct,
  c.operating_margin_pct AS current_margin,
  p.prior_margin,
  c.operating_margin_pct - p.prior_margin AS margin_variance_ppt,
  c.fx_impact_reporting
FROM current_period c
LEFT JOIN prior_period p ON c.bu = p.bu
ORDER BY 
  CASE WHEN c.bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
  c.bu;

SELECT 'View v_financial_results_comparison created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 4: Forecast Accuracy Analysis

# COMMAND ----------

CREATE OR REPLACE VIEW v_forecast_accuracy AS
SELECT 
  forecast_period,
  bu,
  segment,
  scenario,
  revenue_forecast_reporting,
  revenue_actual_reporting,
  variance_revenue,
  variance_revenue_pct,
  operating_profit_forecast_reporting,
  operating_profit_actual_reporting,
  variance_operating_profit,
  variance_operating_profit_pct,
  forecast_accuracy_score,
  assumptions,
  variance_drivers
FROM financial_close_catalog.gold_layer.forecast_results_gold
WHERE scenario = 'Base'
  AND revenue_actual_reporting IS NOT NULL
ORDER BY 
  forecast_period DESC,
  CASE WHEN bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
  bu,
  segment;

SELECT 'View v_forecast_accuracy created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 5: KPI Scorecard

# COMMAND ----------

CREATE OR REPLACE VIEW v_kpi_scorecard AS
SELECT 
  period,
  kpi_name,
  kpi_category,
  bu,
  kpi_value,
  kpi_unit,
  target_value,
  variance_vs_target,
  status,
  trend,
  description,
  CASE kpi_unit
    WHEN 'currency' THEN CONCAT('$', FORMAT_NUMBER(kpi_value, 2))
    WHEN 'percentage' THEN CONCAT(FORMAT_NUMBER(kpi_value, 2), '%')
    WHEN 'days' THEN CONCAT(FORMAT_NUMBER(kpi_value, 0), ' days')
    ELSE FORMAT_NUMBER(kpi_value, 0)
  END AS formatted_value,
  CASE kpi_unit
    WHEN 'currency' THEN CONCAT('$', FORMAT_NUMBER(target_value, 2))
    WHEN 'percentage' THEN CONCAT(FORMAT_NUMBER(target_value, 2), '%')
    WHEN 'days' THEN CONCAT(FORMAT_NUMBER(target_value, 0), ' days')
    ELSE FORMAT_NUMBER(target_value, 0)
  END AS formatted_target
FROM financial_close_catalog.gold_layer.close_kpi_gold
WHERE period = (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_kpi_gold)
ORDER BY 
  CASE kpi_category
    WHEN 'Timeliness' THEN 1
    WHEN 'Financial' THEN 2
    WHEN 'Quality' THEN 3
    WHEN 'Efficiency' THEN 4
    ELSE 5
  END,
  kpi_name;

SELECT 'View v_kpi_scorecard created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View 6: Agent Activity Log

# COMMAND ----------

CREATE OR REPLACE VIEW v_agent_activity AS
SELECT 
  log_timestamp,
  period,
  agent_name,
  action,
  target_table,
  target_record_key,
  status,
  message,
  execution_time_ms,
  DATE(log_timestamp) AS log_date
FROM financial_close_catalog.gold_layer.close_agent_logs
ORDER BY log_timestamp DESC;

SELECT 'View v_agent_activity created' AS status;

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample Dashboard Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 1: Close Cockpit

# COMMAND ----------

-- Query 1.1: Overall Close Progress
SELECT 
  bu,
  phase_name,
  pct_tasks_completed,
  total_tasks,
  completed_tasks,
  blocked_tasks,
  days_since_period_end,
  days_to_sla,
  sla_status
FROM v_current_close_status
ORDER BY 
  CASE WHEN bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
  bu;

# COMMAND ----------

-- Query 1.2: Tasks by Status and Phase
SELECT 
  phase_name,
  status,
  COUNT(*) AS task_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY phase_name), 1) AS pct_of_phase
FROM v_task_details
WHERE period = (SELECT MAX(period) FROM v_task_details)
GROUP BY phase_name, status
ORDER BY 
  CASE phase_name
    WHEN 'Data Gathering' THEN 1
    WHEN 'Adjustments' THEN 2
    WHEN 'Review' THEN 3
    WHEN 'Reporting & Sign-off' THEN 4
    ELSE 5
  END,
  CASE status
    WHEN 'completed' THEN 1
    WHEN 'in_progress' THEN 2
    WHEN 'pending' THEN 3
    WHEN 'blocked' THEN 4
    ELSE 5
  END;

# COMMAND ----------

-- Query 1.3: Overdue Tasks
SELECT 
  task_id,
  task_name,
  bu,
  owner_role,
  planned_due_date,
  hours_overdue,
  priority,
  status
FROM v_task_details
WHERE status IN ('pending', 'in_progress')
  AND hours_overdue > 0
  AND period = (SELECT MAX(period) FROM v_task_details)
ORDER BY hours_overdue DESC, priority DESC;

# COMMAND ----------

-- Query 1.4: Close Timeline (Gantt-style data)
SELECT 
  phase_id,
  phase_name,
  MIN(planned_due_date) AS phase_start,
  MAX(planned_due_date) AS phase_end,
  COUNT(*) AS total_tasks,
  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_tasks,
  ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_complete
FROM v_task_details
WHERE period = (SELECT MAX(period) FROM v_task_details)
GROUP BY phase_id, phase_name
ORDER BY phase_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 2: Close Results

# COMMAND ----------

-- Query 2.1: P&L by BU
SELECT 
  bu,
  current_revenue,
  current_op_profit,
  current_margin,
  prior_revenue,
  prior_op_profit,
  revenue_variance,
  revenue_variance_pct,
  op_profit_variance,
  op_profit_variance_pct
FROM v_financial_results_comparison
WHERE bu != 'CONSOLIDATED'
ORDER BY current_revenue DESC;

# COMMAND ----------

-- Query 2.2: Consolidated P&L with Variance Waterfall
SELECT 
  bu,
  current_revenue,
  prior_revenue,
  revenue_variance,
  revenue_variance_pct,
  current_op_profit,
  prior_op_profit,
  op_profit_variance,
  op_profit_variance_pct,
  fx_impact_reporting
FROM v_financial_results_comparison
WHERE bu = 'CONSOLIDATED';

# COMMAND ----------

-- Query 2.3: Segmented Results (Top Segments by Revenue)
SELECT 
  period,
  bu,
  segment,
  product,
  revenue_reporting,
  operating_profit_reporting,
  operating_margin_pct,
  variance_vs_forecast,
  variance_vs_forecast_pct
FROM financial_close_catalog.gold_layer.close_results_gold
WHERE period = (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_results_gold)
  AND product != 'ALL'
  AND bu != 'CONSOLIDATED'
ORDER BY revenue_reporting DESC
LIMIT 20;

# COMMAND ----------

-- Query 2.4: FX Impact Analysis
SELECT 
  bu,
  local_currency,
  SUM(revenue_local) AS revenue_local,
  SUM(revenue_reporting) AS revenue_reporting,
  SUM(fx_impact_reporting) AS total_fx_impact
FROM financial_close_catalog.gold_layer.close_results_gold
WHERE period = (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_results_gold)
  AND segment = 'ALL'
  AND product = 'ALL'
  AND region = 'ALL'
  AND bu != 'CONSOLIDATED'
GROUP BY bu, local_currency
ORDER BY ABS(SUM(fx_impact_reporting)) DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 3: Forecast Dashboard

# COMMAND ----------

-- Query 3.1: Forecast vs. Actual by BU
SELECT 
  bu,
  segment,
  revenue_forecast_reporting,
  revenue_actual_reporting,
  variance_revenue,
  variance_revenue_pct,
  operating_profit_forecast_reporting,
  operating_profit_actual_reporting,
  variance_operating_profit,
  variance_operating_profit_pct,
  forecast_accuracy_score
FROM v_forecast_accuracy
WHERE segment = 'ALL'
ORDER BY 
  CASE WHEN bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
  bu;

# COMMAND ----------

-- Query 3.2: Top Forecast Variances
SELECT 
  bu,
  segment,
  revenue_forecast_reporting,
  revenue_actual_reporting,
  variance_revenue,
  variance_revenue_pct,
  operating_profit_forecast_reporting,
  operating_profit_actual_reporting,
  variance_operating_profit,
  variance_operating_profit_pct,
  variance_drivers
FROM v_forecast_accuracy
WHERE segment != 'ALL'
  AND ABS(variance_operating_profit_pct) > 10
ORDER BY ABS(variance_operating_profit) DESC;

# COMMAND ----------

-- Query 3.3: Scenario Comparison
SELECT 
  forecast_period,
  bu,
  scenario,
  SUM(revenue_forecast_reporting) AS total_revenue,
  SUM(operating_profit_forecast_reporting) AS total_op_profit,
  ROUND(SUM(operating_profit_forecast_reporting) / SUM(revenue_forecast_reporting) * 100, 2) AS op_margin
FROM financial_close_catalog.gold_layer.forecast_results_gold
WHERE forecast_period >= (SELECT MAX(period) FROM financial_close_catalog.gold_layer.close_results_gold)
  AND bu != 'CONSOLIDATED'
GROUP BY forecast_period, bu, scenario
ORDER BY forecast_period, bu, scenario;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 4: KPI Scorecard

# COMMAND ----------

-- Query 4.1: All KPIs for Current Period
SELECT 
  kpi_category,
  kpi_name,
  formatted_value AS current_value,
  formatted_target AS target,
  status,
  trend,
  description
FROM v_kpi_scorecard
ORDER BY 
  CASE kpi_category
    WHEN 'Timeliness' THEN 1
    WHEN 'Financial' THEN 2
    WHEN 'Quality' THEN 3
    ELSE 4
  END,
  kpi_name;

# COMMAND ----------

-- Query 4.2: KPI Trends (Last 6 Periods)
SELECT 
  period,
  kpi_name,
  kpi_value,
  target_value,
  status
FROM financial_close_catalog.gold_layer.close_kpi_gold
WHERE period >= (SELECT MAX(period) - 5 FROM financial_close_catalog.gold_layer.close_kpi_gold)
  AND bu = 'CONSOLIDATED'
  AND kpi_name IN ('Close Cycle Time', 'Operating Margin', 'Forecast Accuracy')
ORDER BY kpi_name, period;

# COMMAND ----------

-- Query 4.3: KPI Status Summary
SELECT 
  status,
  COUNT(*) AS kpi_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM v_kpi_scorecard
GROUP BY status
ORDER BY 
  CASE status
    WHEN 'on_target' THEN 1
    WHEN 'warning' THEN 2
    WHEN 'critical' THEN 3
    ELSE 4
  END;

# COMMAND ----------

# MAGIC %md
# MAGIC # Genie Space Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Instructions
# MAGIC 
# MAGIC When setting up the Genie space for "Financial Close Assistant", include these instructions:
# MAGIC 
# MAGIC ```
# MAGIC You are a financial close assistant for FP&A and controlling teams.
# MAGIC 
# MAGIC Context:
# MAGIC - Fiscal periods follow calendar months (January = Period 1 / 202601, December = Period 12 / 202612)
# MAGIC - All amounts are in reporting currency (USD) unless specified as local_currency
# MAGIC - Business units include: North America, Europe, Asia Pacific, Latin America, Middle East
# MAGIC - Segments include: Product A, Product B, Services, Other
# MAGIC - Close phases: 1=Data Gathering, 2=Adjustments, 3=Data Gathering, 4=Review, 5=Reporting & Sign-off
# MAGIC 
# MAGIC KPI Naming Conventions:
# MAGIC - "Revenue" = Total revenue from all sources
# MAGIC - "Operating Profit" = Revenue - COGS - Operating Expenses
# MAGIC - "Operating Margin" = Operating Profit / Revenue (as percentage)
# MAGIC - "FX Impact" = Difference between local currency and reporting currency amounts
# MAGIC - "Variance" = Actual - Forecast (positive variance = favorable for revenue/profit)
# MAGIC - "Cycle Time" = Days from period end to close completion
# MAGIC 
# MAGIC Response Style:
# MAGIC - Start with executive summary (2-3 sentences highlighting key findings)
# MAGIC - Then provide detailed breakdown with numbers formatted as currency ($X,XXX) or percentages (X.X%)
# MAGIC - Highlight anomalies or items requiring attention
# MAGIC - Use tables for multi-dimensional data
# MAGIC - When showing trends, include at least 3 data points for context
# MAGIC 
# MAGIC Available Data:
# MAGIC - close_status_gold: Overall close status by period and BU
# MAGIC - close_results_gold: Final P&L results (segmented and consolidated)
# MAGIC - forecast_results_gold: Forecast data with variance analysis vs. actuals
# MAGIC - close_kpi_gold: Key performance indicators for close process
# MAGIC - close_phase_tasks: Detailed task tracking across all phases
# MAGIC - close_agent_logs: Audit log of all agent actions
# MAGIC 
# MAGIC Common Questions:
# MAGIC - For status questions, always check close_status_gold first
# MAGIC - For financial questions, use close_results_gold (consolidated or by BU/segment)
# MAGIC - For variance analysis, join close_results_gold with forecast_results_gold
# MAGIC - For process questions, use close_phase_tasks and close_kpi_gold
# MAGIC - For troubleshooting, check close_agent_logs for recent agent activity
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Genie Queries
# MAGIC 
# MAGIC Save these queries in the Genie space for quick access:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Current Close Status
# MAGIC 
# MAGIC **User prompt:** "Show me the status of the current month close by BU and phase"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   bu,
# MAGIC   phase_name,
# MAGIC   pct_tasks_completed,
# MAGIC   days_since_period_end,
# MAGIC   days_to_sla,
# MAGIC   sla_status,
# MAGIC   agent_summary
# MAGIC FROM v_current_close_status
# MAGIC ORDER BY 
# MAGIC   CASE WHEN bu = 'CONSOLIDATED' THEN 1 ELSE 2 END,
# MAGIC   bu;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Top Variance Drivers
# MAGIC 
# MAGIC **User prompt:** "Explain the top 3 drivers of variance between this month's close and the previous forecast"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   bu,
# MAGIC   segment,
# MAGIC   operating_profit_actual_reporting,
# MAGIC   operating_profit_forecast_reporting,
# MAGIC   variance_operating_profit,
# MAGIC   variance_operating_profit_pct,
# MAGIC   variance_drivers
# MAGIC FROM v_forecast_accuracy
# MAGIC WHERE segment != 'ALL'
# MAGIC ORDER BY ABS(variance_operating_profit) DESC
# MAGIC LIMIT 3;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Overdue Tasks
# MAGIC 
# MAGIC **User prompt:** "List all tasks that are overdue or blocked and who owns them"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   task_name,
# MAGIC   bu,
# MAGIC   owner_role,
# MAGIC   status,
# MAGIC   planned_due_date,
# MAGIC   hours_overdue,
# MAGIC   blocking_reason,
# MAGIC   priority
# MAGIC FROM v_task_details
# MAGIC WHERE period = (SELECT MAX(period) FROM v_task_details)
# MAGIC   AND (status = 'blocked' OR (status IN ('pending', 'in_progress') AND hours_overdue > 0))
# MAGIC ORDER BY 
# MAGIC   CASE WHEN status = 'blocked' THEN 1 ELSE 2 END,
# MAGIC   hours_overdue DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: BU Deep Dive
# MAGIC 
# MAGIC **User prompt:** "Investigate why BU Europe's operating profit is below forecast this month, and summarize the root causes by segment"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   segment,
# MAGIC   revenue_actual_reporting,
# MAGIC   revenue_forecast_reporting,
# MAGIC   variance_revenue_pct,
# MAGIC   operating_profit_actual_reporting,
# MAGIC   operating_profit_forecast_reporting,
# MAGIC   variance_operating_profit,
# MAGIC   variance_operating_profit_pct,
# MAGIC   assumptions,
# MAGIC   variance_drivers
# MAGIC FROM v_forecast_accuracy
# MAGIC WHERE bu = 'Europe'
# MAGIC   AND segment != 'ALL'
# MAGIC ORDER BY ABS(variance_operating_profit) DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5: FX Impact Analysis
# MAGIC 
# MAGIC **User prompt:** "What is the total FX impact on operating profit for the current period across all BUs?"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   bu,
# MAGIC   fx_impact_reporting,
# MAGIC   operating_profit_reporting
# MAGIC FROM v_financial_results_comparison
# MAGIC WHERE bu != 'CONSOLIDATED'
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'TOTAL' AS bu,
# MAGIC   SUM(fx_impact_reporting) AS fx_impact_reporting,
# MAGIC   SUM(operating_profit_reporting) AS operating_profit_reporting
# MAGIC FROM v_financial_results_comparison
# MAGIC WHERE bu != 'CONSOLIDATED'
# MAGIC ORDER BY fx_impact_reporting DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 6: Close Performance Trend
# MAGIC 
# MAGIC **User prompt:** "Compare the current close cycle time to the average of the last 6 months"
# MAGIC 
# MAGIC **Expected SQL:**
# MAGIC ```sql
# MAGIC WITH cycle_time_history AS (
# MAGIC   SELECT 
# MAGIC     period,
# MAGIC     kpi_value AS cycle_time_days
# MAGIC   FROM financial_close_catalog.gold_layer.close_kpi_gold
# MAGIC   WHERE kpi_name = 'Close Cycle Time'
# MAGIC     AND bu = 'CONSOLIDATED'
# MAGIC     AND period >= (SELECT MAX(period) - 5 FROM financial_close_catalog.gold_layer.close_kpi_gold)
# MAGIC )
# MAGIC SELECT 
# MAGIC   MAX(CASE WHEN period = (SELECT MAX(period) FROM cycle_time_history) THEN cycle_time_days END) AS current_cycle_time,
# MAGIC   ROUND(AVG(cycle_time_days), 1) AS avg_last_6_months,
# MAGIC   MAX(cycle_time_days) AS max_last_6_months,
# MAGIC   MIN(cycle_time_days) AS min_last_6_months
# MAGIC FROM cycle_time_history;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Setup Checklist
# MAGIC 
# MAGIC 1. **Create Genie Space**
# MAGIC    - Name: "Financial Close Assistant"
# MAGIC    - Description: "AI assistant for financial close process, analysis, and reporting"
# MAGIC 
# MAGIC 2. **Add Tables to Genie Space**
# MAGIC    - close_status_gold
# MAGIC    - close_results_gold
# MAGIC    - forecast_results_gold
# MAGIC    - close_kpi_gold
# MAGIC    - close_phase_tasks
# MAGIC    - close_agent_logs
# MAGIC    - All helper views (v_*)
# MAGIC 
# MAGIC 3. **Configure Space Instructions**
# MAGIC    - Copy the instructions from above
# MAGIC    - Customize for your organization's specific needs
# MAGIC 
# MAGIC 4. **Add Example Queries**
# MAGIC    - Save all 6 example queries as "Saved Queries"
# MAGIC    - Name them clearly for easy discovery
# MAGIC 
# MAGIC 5. **Set Permissions**
# MAGIC    - FP&A teams: Full access
# MAGIC    - BU Controllers: Read access
# MAGIC    - Leadership: Read access
# MAGIC    - Restrict write access to service principals/agents
# MAGIC 
# MAGIC 6. **Test Genie Responses**
# MAGIC    - Test each example query
# MAGIC    - Verify responses are accurate and well-formatted
# MAGIC    - Adjust instructions if needed

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '
# MAGIC ========================================
# MAGIC Dashboard and Genie Setup Complete!
# MAGIC ========================================
# MAGIC 
# MAGIC Created:
# MAGIC - 6 helper views for dashboard queries
# MAGIC - 15+ sample dashboard queries across 4 dashboards
# MAGIC - Genie space instructions and configuration
# MAGIC - 6 example Genie queries for common use cases
# MAGIC 
# MAGIC Next Steps:
# MAGIC 1. Create Databricks SQL dashboards using the sample queries
# MAGIC 2. Set up Genie space with instructions and saved queries
# MAGIC 3. Grant appropriate permissions to user groups
# MAGIC 4. Train FP&A users on Genie and dashboard usage
# MAGIC 5. Schedule dashboard refreshes (if needed)
# MAGIC 
# MAGIC Dashboard Recommendations:
# MAGIC - Close Cockpit: Refresh every hour during close period
# MAGIC - Close Results: Refresh daily
# MAGIC - Forecast: Refresh weekly
# MAGIC - KPI Scorecard: Refresh daily
# MAGIC 
# MAGIC Genie Best Practices:
# MAGIC - Use natural language questions
# MAGIC - Be specific about periods and BUs
# MAGIC - Ask follow-up questions for deeper analysis
# MAGIC - Save frequently used queries for quick access
# MAGIC 
# MAGIC ========================================
# MAGIC ' AS summary;
