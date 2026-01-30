# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Schema and Table Setup
# MAGIC 
# MAGIC This notebook creates the Unity Catalog structure for the intelligent financial close solution:
# MAGIC - Catalog: `financial_close_catalog`
# MAGIC - Schemas: `bronze_layer`, `silver_layer`, `gold_layer`
# MAGIC - All Delta tables with proper schema, partitioning, and comments
# MAGIC 
# MAGIC **Run this notebook once** during initial setup or when schema changes are needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema configuration
CATALOG = "financial_close_catalog"
BRONZE_SCHEMA = "bronze_layer"
SILVER_SCHEMA = "silver_layer"
GOLD_SCHEMA = "gold_layer"

# Storage location (adjust based on your environment)
CATALOG_LOCATION = "abfss://your-container@your-storage-account.dfs.core.windows.net/financial_close"

print(f"Setting up catalog: {CATALOG}")
print(f"Storage location: {CATALOG_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schemas

# COMMAND ----------

# Create catalog
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG}
COMMENT 'Financial close intelligent solution - Lakehouse for FP&A automation'
""")

# Set current catalog
spark.sql(f"USE CATALOG {CATALOG}")

print(f"✓ Catalog {CATALOG} created/verified")

# COMMAND ----------

# Create Bronze schema
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}
COMMENT 'Raw landing zone for all financial close input files'
""")

# Create Silver schema
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}
COMMENT 'Standardized and cleaned financial close data'
""")

# Create Gold schema
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
COMMENT 'Consumption-ready financial close results and KPIs'
""")

print(f"✓ Schema {BRONZE_SCHEMA} created/verified")
print(f"✓ Schema {SILVER_SCHEMA} created/verified")
print(f"✓ Schema {GOLD_SCHEMA} created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fx_rates_raw - Raw FX rates from providers

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.fx_rates_raw (
  load_timestamp TIMESTAMP COMMENT 'Timestamp when file was loaded',
  file_name STRING COMMENT 'Source file name',
  provider STRING COMMENT 'FX rate provider (e.g., Bloomberg, Reuters)',
  rate_date DATE COMMENT 'Date for which the FX rate is valid',
  from_currency STRING COMMENT 'Source currency code (ISO 4217)',
  to_currency STRING COMMENT 'Target currency code (ISO 4217)',
  exchange_rate DECIMAL(18,6) COMMENT 'Exchange rate from source to target currency',
  rate_type STRING COMMENT 'Rate type: spot, average, month-end',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (rate_date)
COMMENT 'Raw foreign exchange rates from external providers'
""")

print("✓ Table fx_rates_raw created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### bu_pre_close_raw - Preliminary close files from BUs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.bu_pre_close_raw (
  load_timestamp TIMESTAMP COMMENT 'Timestamp when file was loaded',
  file_name STRING COMMENT 'Source file name',
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code',
  cut_type STRING COMMENT 'Preliminary, first_cut, or final_cut',
  account_code STRING COMMENT 'GL account code',
  account_name STRING COMMENT 'GL account description',
  cost_center STRING COMMENT 'Cost center code',
  local_currency STRING COMMENT 'Local currency code',
  local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
  reporting_currency STRING COMMENT 'Reporting currency code (typically USD)',
  reporting_amount DECIMAL(18,2) COMMENT 'Amount in reporting currency',
  segment STRING COMMENT 'Business segment',
  region STRING COMMENT 'Geographic region',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period, bu)
COMMENT 'Raw preliminary close trial balance files submitted by business units'
""")

print("✓ Table bu_pre_close_raw created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### bu_segmented_raw - Segmented close files from BUs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.bu_segmented_raw (
  load_timestamp TIMESTAMP COMMENT 'Timestamp when file was loaded',
  file_name STRING COMMENT 'Source file name',
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code',
  segment STRING COMMENT 'Business segment (Product A, Product B, Services, Other)',
  product STRING COMMENT 'Product line',
  region STRING COMMENT 'Geographic region',
  revenue_local DECIMAL(18,2) COMMENT 'Revenue in local currency',
  cogs_local DECIMAL(18,2) COMMENT 'Cost of goods sold in local currency',
  opex_local DECIMAL(18,2) COMMENT 'Operating expenses in local currency',
  local_currency STRING COMMENT 'Local currency code',
  revenue_reporting DECIMAL(18,2) COMMENT 'Revenue in reporting currency',
  cogs_reporting DECIMAL(18,2) COMMENT 'COGS in reporting currency',
  opex_reporting DECIMAL(18,2) COMMENT 'Operating expenses in reporting currency',
  reporting_currency STRING COMMENT 'Reporting currency code',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period, bu)
COMMENT 'Raw segmented close files with revenue and costs by segment, product, and region'
""")

print("✓ Table bu_segmented_raw created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### bu_forecast_raw - Forecast files from BUs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.bu_forecast_raw (
  load_timestamp TIMESTAMP COMMENT 'Timestamp when file was loaded',
  file_name STRING COMMENT 'Source file name',
  forecast_period INT COMMENT 'Period being forecasted in YYYYMM format',
  submission_date DATE COMMENT 'Date when forecast was submitted',
  bu STRING COMMENT 'Business unit code',
  segment STRING COMMENT 'Business segment',
  scenario STRING COMMENT 'Forecast scenario: Base, Upside, Downside',
  revenue_local DECIMAL(18,2) COMMENT 'Forecasted revenue in local currency',
  cogs_local DECIMAL(18,2) COMMENT 'Forecasted COGS in local currency',
  opex_local DECIMAL(18,2) COMMENT 'Forecasted operating expenses in local currency',
  local_currency STRING COMMENT 'Local currency code',
  revenue_reporting DECIMAL(18,2) COMMENT 'Forecasted revenue in reporting currency',
  cogs_reporting DECIMAL(18,2) COMMENT 'Forecasted COGS in reporting currency',
  opex_reporting DECIMAL(18,2) COMMENT 'Forecasted operating expenses in reporting currency',
  reporting_currency STRING COMMENT 'Reporting currency code',
  assumptions STRING COMMENT 'Key forecast assumptions in text format',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (forecast_period, bu)
COMMENT 'Raw forecast files submitted by business units with multiple scenarios'
""")

print("✓ Table bu_forecast_raw created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fx_forecast_raw - Forecast FX rates

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.fx_forecast_raw (
  load_timestamp TIMESTAMP COMMENT 'Timestamp when file was loaded',
  file_name STRING COMMENT 'Source file name',
  forecast_period INT COMMENT 'Period being forecasted in YYYYMM format',
  from_currency STRING COMMENT 'Source currency code',
  to_currency STRING COMMENT 'Target currency code',
  exchange_rate DECIMAL(18,6) COMMENT 'Forecasted exchange rate',
  scenario STRING COMMENT 'FX scenario: Base, Upside, Downside',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (forecast_period)
COMMENT 'Forecasted foreign exchange rates for future periods'
""")

print("✓ Table fx_forecast_raw created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### fx_rates_std - Standardized FX rates

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.fx_rates_std (
  rate_date DATE COMMENT 'Date for which the FX rate is valid',
  from_currency STRING COMMENT 'Source currency code',
  to_currency STRING COMMENT 'Target currency code',
  exchange_rate DECIMAL(18,6) COMMENT 'Standardized exchange rate',
  rate_type STRING COMMENT 'Rate type: spot, average, month-end',
  is_latest BOOLEAN COMMENT 'Flag indicating if this is the most recent rate for the date',
  provider STRING COMMENT 'Primary FX rate provider',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when standardized',
  data_quality_score DECIMAL(3,2) COMMENT 'Data quality score (0-1)',
  anomaly_flag BOOLEAN COMMENT 'Flag for unusual rate movements',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (rate_date)
COMMENT 'Standardized and validated foreign exchange rates with quality flags'
""")

print("✓ Table fx_rates_std created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_trial_balance_std - Standardized trial balance

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.close_trial_balance_std (
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code',
  cut_type STRING COMMENT 'preliminary, first_cut, or final_cut',
  cut_version INT COMMENT 'Version number within cut type',
  account_code STRING COMMENT 'Standardized GL account code',
  account_name STRING COMMENT 'Standardized GL account description',
  account_category STRING COMMENT 'P&L category: Revenue, COGS, Operating Expense, Other',
  cost_center STRING COMMENT 'Cost center code',
  segment STRING COMMENT 'Business segment',
  region STRING COMMENT 'Geographic region',
  local_currency STRING COMMENT 'Local currency code',
  local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
  reporting_currency STRING COMMENT 'Reporting currency code',
  reporting_amount DECIMAL(18,2) COMMENT 'Amount in reporting currency',
  fx_rate DECIMAL(18,6) COMMENT 'FX rate used for conversion',
  fx_impact DECIMAL(18,2) COMMENT 'FX impact vs. prior period rate',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when standardized',
  data_quality_flag STRING COMMENT 'Data quality indicator: PASS, WARN, FAIL',
  variance_vs_prior_pct DECIMAL(10,2) COMMENT 'Variance vs. prior period (%)',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period, bu)
COMMENT 'Standardized trial balance with all versions and cuts for financial close'
""")

print("✓ Table close_trial_balance_std created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### segmented_close_std - Standardized segmented close

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.segmented_close_std (
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code',
  segment STRING COMMENT 'Business segment',
  product STRING COMMENT 'Product line',
  region STRING COMMENT 'Geographic region',
  local_currency STRING COMMENT 'Local currency code',
  reporting_currency STRING COMMENT 'Reporting currency code',
  revenue_local DECIMAL(18,2) COMMENT 'Revenue in local currency',
  cogs_local DECIMAL(18,2) COMMENT 'COGS in local currency',
  opex_local DECIMAL(18,2) COMMENT 'Operating expenses in local currency',
  operating_profit_local DECIMAL(18,2) COMMENT 'Operating profit in local currency',
  revenue_reporting DECIMAL(18,2) COMMENT 'Revenue in reporting currency',
  cogs_reporting DECIMAL(18,2) COMMENT 'COGS in reporting currency',
  opex_reporting DECIMAL(18,2) COMMENT 'Operating expenses in reporting currency',
  operating_profit_reporting DECIMAL(18,2) COMMENT 'Operating profit in reporting currency',
  fx_impact_reporting DECIMAL(18,2) COMMENT 'FX impact on operating profit',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when standardized',
  data_quality_flag STRING COMMENT 'Data quality indicator',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period, bu)
COMMENT 'Standardized segmented close with P&L by segment, product, and region'
""")

print("✓ Table segmented_close_std created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### forecast_std - Standardized forecast

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.forecast_std (
  forecast_period INT COMMENT 'Period being forecasted in YYYYMM format',
  submission_date DATE COMMENT 'Date when forecast was submitted',
  bu STRING COMMENT 'Business unit code',
  segment STRING COMMENT 'Business segment',
  scenario STRING COMMENT 'Forecast scenario: Base, Upside, Downside',
  local_currency STRING COMMENT 'Local currency code',
  reporting_currency STRING COMMENT 'Reporting currency code',
  revenue_local DECIMAL(18,2) COMMENT 'Forecasted revenue in local currency',
  cogs_local DECIMAL(18,2) COMMENT 'Forecasted COGS in local currency',
  opex_local DECIMAL(18,2) COMMENT 'Forecasted operating expenses in local currency',
  operating_profit_local DECIMAL(18,2) COMMENT 'Forecasted operating profit in local currency',
  revenue_reporting DECIMAL(18,2) COMMENT 'Forecasted revenue in reporting currency',
  cogs_reporting DECIMAL(18,2) COMMENT 'Forecasted COGS in reporting currency',
  opex_reporting DECIMAL(18,2) COMMENT 'Forecasted operating expenses in reporting currency',
  operating_profit_reporting DECIMAL(18,2) COMMENT 'Forecasted operating profit in reporting currency',
  fx_rate DECIMAL(18,6) COMMENT 'FX rate used for forecast conversion',
  assumptions STRING COMMENT 'Key forecast assumptions',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when standardized',
  data_quality_flag STRING COMMENT 'Data quality indicator',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (forecast_period, bu)
COMMENT 'Standardized forecast data with multiple scenarios and assumptions'
""")

print("✓ Table forecast_std created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_phase_tasks - Task tracking for close phases

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.close_phase_tasks (
  period INT COMMENT 'Period in YYYYMM format',
  phase_id INT COMMENT 'Phase number (1-5)',
  phase_name STRING COMMENT 'Phase name (Data Gathering, Adjustments, etc.)',
  task_id STRING COMMENT 'Unique task identifier',
  task_name STRING COMMENT 'Task description',
  bu STRING COMMENT 'Business unit (if task is BU-specific)',
  owner_role STRING COMMENT 'Role responsible (FP&A, BU Controller, Tech)',
  planned_due_date TIMESTAMP COMMENT 'Planned completion date/time',
  actual_start_timestamp TIMESTAMP COMMENT 'Actual start timestamp',
  actual_completion_timestamp TIMESTAMP COMMENT 'Actual completion timestamp',
  status STRING COMMENT 'Task status: pending, in_progress, completed, blocked, cancelled',
  blocking_reason STRING COMMENT 'Reason if status is blocked',
  comments STRING COMMENT 'Free-text comments and notes',
  agent_assigned STRING COMMENT 'Agent handling this task (if automated)',
  priority STRING COMMENT 'Task priority: low, medium, high, critical',
  dependencies STRING COMMENT 'Comma-separated list of task_ids this task depends on',
  last_updated_timestamp TIMESTAMP COMMENT 'Last status update timestamp',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period)
COMMENT 'Task tracking for all financial close phases and activities'
""")

print("✓ Table close_phase_tasks created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_status_gold - Overall close status

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.close_status_gold (
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code (or CONSOLIDATED for overall)',
  phase_id INT COMMENT 'Current phase number',
  phase_name STRING COMMENT 'Current phase name',
  overall_status STRING COMMENT 'Overall status: not_started, in_progress, completed, issues',
  pct_tasks_completed DECIMAL(5,2) COMMENT 'Percentage of tasks completed',
  total_tasks INT COMMENT 'Total number of tasks',
  completed_tasks INT COMMENT 'Number of completed tasks',
  blocked_tasks INT COMMENT 'Number of blocked tasks',
  days_since_period_end INT COMMENT 'Days elapsed since period end',
  days_to_sla INT COMMENT 'Days remaining to SLA target',
  sla_status STRING COMMENT 'SLA status: on_track, at_risk, overdue',
  key_issues STRING COMMENT 'Summary of key issues or blockers',
  last_milestone STRING COMMENT 'Last completed milestone',
  next_milestone STRING COMMENT 'Next milestone due',
  agent_summary STRING COMMENT 'Summary generated by agents',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when updated',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period)
COMMENT 'High-level close status and progress tracking by BU and period'
""")

print("✓ Table close_status_gold created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_results_gold - Final close results

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.close_results_gold (
  period INT COMMENT 'Period in YYYYMM format',
  bu STRING COMMENT 'Business unit code (or CONSOLIDATED)',
  segment STRING COMMENT 'Business segment (or ALL for BU total)',
  product STRING COMMENT 'Product line (or ALL)',
  region STRING COMMENT 'Geographic region (or ALL)',
  local_currency STRING COMMENT 'Local currency code',
  reporting_currency STRING COMMENT 'Reporting currency code',
  revenue_local DECIMAL(18,2) COMMENT 'Revenue in local currency',
  cogs_local DECIMAL(18,2) COMMENT 'COGS in local currency',
  gross_profit_local DECIMAL(18,2) COMMENT 'Gross profit in local currency',
  opex_local DECIMAL(18,2) COMMENT 'Operating expenses in local currency',
  operating_profit_local DECIMAL(18,2) COMMENT 'Operating profit in local currency',
  revenue_reporting DECIMAL(18,2) COMMENT 'Revenue in reporting currency',
  cogs_reporting DECIMAL(18,2) COMMENT 'COGS in reporting currency',
  gross_profit_reporting DECIMAL(18,2) COMMENT 'Gross profit in reporting currency',
  opex_reporting DECIMAL(18,2) COMMENT 'Operating expenses in reporting currency',
  operating_profit_reporting DECIMAL(18,2) COMMENT 'Operating profit in reporting currency',
  fx_impact_reporting DECIMAL(18,2) COMMENT 'FX impact on operating profit vs. prior period',
  variance_vs_prior_period DECIMAL(18,2) COMMENT 'Variance vs. prior period (reporting currency)',
  variance_vs_prior_pct DECIMAL(10,2) COMMENT 'Variance vs. prior period (%)',
  variance_vs_forecast DECIMAL(18,2) COMMENT 'Variance vs. forecast (reporting currency)',
  variance_vs_forecast_pct DECIMAL(10,2) COMMENT 'Variance vs. forecast (%)',
  gross_margin_pct DECIMAL(10,2) COMMENT 'Gross margin percentage',
  operating_margin_pct DECIMAL(10,2) COMMENT 'Operating margin percentage',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when finalized',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period)
COMMENT 'Final financial close results with segmentation and variance analysis'
""")

print("✓ Table close_results_gold created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### forecast_results_gold - Forecast results and variance

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.forecast_results_gold (
  forecast_period INT COMMENT 'Period being forecasted in YYYYMM format',
  submission_date DATE COMMENT 'Forecast submission date',
  bu STRING COMMENT 'Business unit code (or CONSOLIDATED)',
  segment STRING COMMENT 'Business segment (or ALL)',
  scenario STRING COMMENT 'Forecast scenario: Base, Upside, Downside',
  local_currency STRING COMMENT 'Local currency code',
  reporting_currency STRING COMMENT 'Reporting currency code',
  revenue_forecast_local DECIMAL(18,2) COMMENT 'Forecasted revenue in local currency',
  cogs_forecast_local DECIMAL(18,2) COMMENT 'Forecasted COGS in local currency',
  opex_forecast_local DECIMAL(18,2) COMMENT 'Forecasted operating expenses in local currency',
  operating_profit_forecast_local DECIMAL(18,2) COMMENT 'Forecasted operating profit in local currency',
  revenue_forecast_reporting DECIMAL(18,2) COMMENT 'Forecasted revenue in reporting currency',
  cogs_forecast_reporting DECIMAL(18,2) COMMENT 'Forecasted COGS in reporting currency',
  opex_forecast_reporting DECIMAL(18,2) COMMENT 'Forecasted operating expenses in reporting currency',
  operating_profit_forecast_reporting DECIMAL(18,2) COMMENT 'Forecasted operating profit in reporting currency',
  revenue_actual_reporting DECIMAL(18,2) COMMENT 'Actual revenue (if period closed)',
  operating_profit_actual_reporting DECIMAL(18,2) COMMENT 'Actual operating profit (if period closed)',
  variance_revenue DECIMAL(18,2) COMMENT 'Variance: Actual - Forecast revenue',
  variance_operating_profit DECIMAL(18,2) COMMENT 'Variance: Actual - Forecast operating profit',
  variance_revenue_pct DECIMAL(10,2) COMMENT 'Revenue variance percentage',
  variance_operating_profit_pct DECIMAL(10,2) COMMENT 'Operating profit variance percentage',
  forecast_accuracy_score DECIMAL(5,2) COMMENT 'Forecast accuracy score (if actuals available)',
  assumptions STRING COMMENT 'Key forecast assumptions',
  variance_drivers STRING COMMENT 'Key drivers of variance (if actuals available)',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when finalized',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (forecast_period)
COMMENT 'Forecast results with variance analysis vs. actuals when available'
""")

print("✓ Table forecast_results_gold created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_kpi_gold - Close process KPIs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.close_kpi_gold (
  period INT COMMENT 'Period in YYYYMM format',
  kpi_name STRING COMMENT 'KPI name',
  kpi_category STRING COMMENT 'KPI category: Timeliness, Quality, Efficiency, Financial',
  bu STRING COMMENT 'Business unit (or CONSOLIDATED)',
  kpi_value DECIMAL(18,2) COMMENT 'KPI numeric value',
  kpi_unit STRING COMMENT 'KPI unit: days, count, percentage, currency',
  target_value DECIMAL(18,2) COMMENT 'Target value for this KPI',
  variance_vs_target DECIMAL(18,2) COMMENT 'Variance vs. target',
  status STRING COMMENT 'KPI status: on_target, warning, critical',
  trend STRING COMMENT 'Trend vs. prior periods: improving, stable, declining',
  description STRING COMMENT 'KPI description and calculation method',
  load_timestamp TIMESTAMP COMMENT 'Timestamp when calculated',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (period)
COMMENT 'Key performance indicators for the financial close process'
""")

print("✓ Table close_kpi_gold created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ### close_agent_logs - Agent activity logs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.close_agent_logs (
  log_timestamp TIMESTAMP COMMENT 'Timestamp of agent action',
  period INT COMMENT 'Period in YYYYMM format (if applicable)',
  agent_name STRING COMMENT 'Name of the agent: Orchestrator, FX, PreClose, Segmented, Reporting',
  action STRING COMMENT 'Action performed by agent',
  target_table STRING COMMENT 'Target table affected',
  target_record_key STRING COMMENT 'Key of record(s) affected',
  status STRING COMMENT 'Action status: success, warning, error',
  message STRING COMMENT 'Log message or error description',
  execution_time_ms LONG COMMENT 'Execution time in milliseconds',
  user_context STRING COMMENT 'User or system context triggering action',
  metadata STRING COMMENT 'Additional metadata in JSON format'
)
USING DELTA
PARTITIONED BY (DATE(log_timestamp))
COMMENT 'Audit log of all agent actions and automation activities'
""")

print("✓ Table close_agent_logs created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Tables

# COMMAND ----------

# Optimize Gold tables for query performance
print("Optimizing Gold tables for query performance...")

spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.close_phase_tasks ZORDER BY (period, phase_id, status)")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.close_status_gold ZORDER BY (period, bu, phase_id)")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.close_results_gold ZORDER BY (period, bu, segment)")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.forecast_results_gold ZORDER BY (forecast_period, bu, scenario)")
spark.sql(f"OPTIMIZE {GOLD_SCHEMA}.close_kpi_gold ZORDER BY (period, kpi_category)")

print("✓ Tables optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions (Optional)

# COMMAND ----------

# Example permission grants - adjust based on your organization's needs
# Uncomment and modify as needed

# # FP&A group: read access to all layers, write to Gold via agents only
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{BRONZE_SCHEMA} TO `fpa_users`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{SILVER_SCHEMA} TO `fpa_users`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{GOLD_SCHEMA} TO `fpa_users`")
# 
# # BU Controllers: read access to their own BU data
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{GOLD_SCHEMA} TO `bu_controllers`")
# 
# # Leadership: read access to Gold only
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG}.{GOLD_SCHEMA} TO `leadership`")
# 
# # Agents service principal: full access
# spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO `agents_service_principal`")

print("✓ Permissions configured (modify grants as needed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all tables
print("\n=== Bronze Layer Tables ===")
display(spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}"))

print("\n=== Silver Layer Tables ===")
display(spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}"))

print("\n=== Gold Layer Tables ===")
display(spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("""
✓ Setup Complete!

Created:
- Catalog: financial_close_catalog
- Schemas: bronze_layer, silver_layer, gold_layer
- 5 Bronze tables (raw landing)
- 4 Silver tables (standardized)
- 6 Gold tables (consumption-ready)

Next Steps:
1. Run notebook 02_synthetic_data_generation.py to generate test data
2. Configure Databricks workflows for automation
3. Set up Genie space with Gold tables

For production use:
- Adjust CATALOG_LOCATION to your ADLS Gen2 path
- Configure Unity Catalog permissions for user groups
- Set up monitoring and alerting
""")
