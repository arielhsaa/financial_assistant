# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close Lakehouse - Schema and Table Setup
# MAGIC 
# MAGIC **Purpose:** Initialize Unity Catalog structure for the financial close solution
# MAGIC 
# MAGIC **Creates:**
# MAGIC - Catalog: `financial_close_lakehouse`
# MAGIC - Schemas: `bronze`, `silver`, `gold`, `config`
# MAGIC - All Delta tables with column comments and constraints
# MAGIC 
# MAGIC **Execution time:** ~2 minutes
# MAGIC 
# MAGIC **Prerequisites:** Unity Catalog enabled, permissions to create catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog and Schemas

# COMMAND ----------

# Create catalog if not exists
spark.sql("""
  CREATE CATALOG IF NOT EXISTS financial_close_lakehouse
  COMMENT 'Lakehouse for intelligent financial close automation with Bronze/Silver/Gold layers'
""")

# Set as current catalog
spark.sql("USE CATALOG financial_close_lakehouse")

print("✓ Catalog 'financial_close_lakehouse' ready")

# COMMAND ----------

# Create Bronze schema (raw data landing)
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw data landing zone - unchanged from source systems'
""")

# Create Silver schema (standardized)
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Standardized and validated data with business rules applied'
""")

# Create Gold schema (consumption-ready)
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Curated, aggregated data optimized for analytics and dashboards'
""")

# Create Config schema (metadata and reference data)
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS config
  COMMENT 'Configuration tables for business units, close phases, and agent settings'
""")

print("✓ Schemas created: bronze, silver, gold, config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Config Tables (Metadata)

# COMMAND ----------

# Business Units reference table
spark.sql("""
  CREATE TABLE IF NOT EXISTS config.business_units (
    bu_code STRING COMMENT 'Business unit code (e.g., NA, EU, ASIA)',
    bu_name STRING COMMENT 'Business unit full name',
    region STRING COMMENT 'Geographic region',
    functional_currency STRING COMMENT 'Functional currency code (e.g., USD, EUR)',
    reporting_currency STRING COMMENT 'Group reporting currency (always USD)',
    is_active BOOLEAN COMMENT 'Whether BU is active in current close',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
  )
  USING DELTA
  COMMENT 'Master list of business units participating in financial close'
""")

print("✓ Created: config.business_units")

# COMMAND ----------

# Close phases and tasks metadata
spark.sql("""
  CREATE TABLE IF NOT EXISTS config.close_phase_definitions (
    phase_id INT COMMENT 'Phase number (1-5)',
    phase_name STRING COMMENT 'Phase name (e.g., Data Gathering, Adjustments)',
    phase_description STRING COMMENT 'Detailed description of phase activities',
    typical_duration_days INT COMMENT 'Expected duration in business days',
    task_id INT COMMENT 'Task number within phase',
    task_name STRING COMMENT 'Task name',
    task_description STRING COMMENT 'Detailed task description',
    owner_role STRING COMMENT 'Responsible role (FP&A, Accounting, Tech)',
    is_bu_specific BOOLEAN COMMENT 'Whether task applies to each BU separately',
    depends_on_tasks ARRAY<INT> COMMENT 'List of task_ids that must complete first',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
  )
  USING DELTA
  COMMENT 'Master definition of close phases and tasks - template for each period'
""")

print("✓ Created: config.close_phase_definitions")

# COMMAND ----------

# Agent configuration
spark.sql("""
  CREATE TABLE IF NOT EXISTS config.agent_configuration (
    agent_name STRING COMMENT 'Agent identifier (e.g., fx_agent, supervisor_agent)',
    agent_type STRING COMMENT 'Agent type (orchestrator, domain, reporting)',
    is_enabled BOOLEAN COMMENT 'Whether agent is active',
    run_frequency STRING COMMENT 'How often agent runs (hourly, daily, on_demand)',
    config_json STRING COMMENT 'JSON blob with agent-specific configuration',
    last_run_timestamp TIMESTAMP COMMENT 'Last execution timestamp',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp'
  )
  USING DELTA
  COMMENT 'Configuration and runtime state for all agents'
""")

print("✓ Created: config.agent_configuration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Bronze Tables (Raw Data Landing)

# COMMAND ----------

# FX rates raw
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.fx_rates_raw (
    load_timestamp TIMESTAMP COMMENT 'When file was loaded into Databricks',
    file_name STRING COMMENT 'Source file name',
    provider STRING COMMENT 'FX data provider (e.g., Bloomberg, Reuters, ECB)',
    rate_date DATE COMMENT 'Date for which rate is valid',
    base_currency STRING COMMENT 'Base currency code (e.g., USD)',
    quote_currency STRING COMMENT 'Quote currency code (e.g., EUR)',
    rate DECIMAL(18,6) COMMENT 'Exchange rate (1 base_currency = rate quote_currency)',
    rate_type STRING COMMENT 'Rate type (spot, average, month-end)',
    raw_record STRING COMMENT 'Original record as received (for audit)'
  )
  USING DELTA
  COMMENT 'Raw exchange rate files from external providers - unvalidated'
""")

print("✓ Created: bronze.fx_rates_raw")

# COMMAND ----------

# BU preliminary close raw
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.bu_pre_close_raw (
    load_timestamp TIMESTAMP COMMENT 'When file was loaded',
    file_name STRING COMMENT 'Source file name',
    bu_code STRING COMMENT 'Business unit code',
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    cut_version STRING COMMENT 'Version: preliminary, cut1, cut2/final',
    account_code STRING COMMENT 'GL account code',
    account_name STRING COMMENT 'GL account name',
    cost_center STRING COMMENT 'Cost center code',
    segment STRING COMMENT 'Segment code',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
    raw_record STRING COMMENT 'Original record as received'
  )
  USING DELTA
  COMMENT 'Preliminary trial balance files from business units - all versions'
""")

print("✓ Created: bronze.bu_pre_close_raw")

# COMMAND ----------

# BU segmented close raw
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.bu_segmented_raw (
    load_timestamp TIMESTAMP COMMENT 'When file was loaded',
    file_name STRING COMMENT 'Source file name',
    bu_code STRING COMMENT 'Business unit code',
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    segment STRING COMMENT 'Segment identifier',
    product STRING COMMENT 'Product line',
    region STRING COMMENT 'Geographic region',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx)',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
    raw_record STRING COMMENT 'Original record as received'
  )
  USING DELTA
  COMMENT 'Segmented P&L data from business units - detailed by segment/product/region'
""")

print("✓ Created: bronze.bu_segmented_raw")

# COMMAND ----------

# BU forecast raw
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.bu_forecast_raw (
    load_timestamp TIMESTAMP COMMENT 'When file was loaded',
    file_name STRING COMMENT 'Source file name',
    bu_code STRING COMMENT 'Business unit code',
    forecast_version STRING COMMENT 'Forecast version (e.g., Jan26_v1)',
    forecast_period STRING COMMENT 'Period being forecasted (YYYY-MM)',
    scenario STRING COMMENT 'Scenario: Base, Upside, Downside',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx)',
    segment STRING COMMENT 'Segment identifier',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Forecasted amount in local currency',
    raw_record STRING COMMENT 'Original record as received'
  )
  USING DELTA
  COMMENT 'Forecast submissions from business units - multiple scenarios and versions'
""")

print("✓ Created: bronze.bu_forecast_raw")

# COMMAND ----------

# FX forecast raw
spark.sql("""
  CREATE TABLE IF NOT EXISTS bronze.fx_forecast_raw (
    load_timestamp TIMESTAMP COMMENT 'When file was loaded',
    file_name STRING COMMENT 'Source file name',
    forecast_date DATE COMMENT 'Date for which rate is forecasted',
    base_currency STRING COMMENT 'Base currency code',
    quote_currency STRING COMMENT 'Quote currency code',
    forecasted_rate DECIMAL(18,6) COMMENT 'Forecasted exchange rate',
    scenario STRING COMMENT 'Scenario: Base, Upside, Downside',
    raw_record STRING COMMENT 'Original record as received'
  )
  USING DELTA
  COMMENT 'Forecasted exchange rates - forward curves for planning'
""")

print("✓ Created: bronze.fx_forecast_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Silver Tables (Standardized Data)

# COMMAND ----------

# FX rates standardized
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.fx_rates_std (
    rate_date DATE COMMENT 'Date for which rate is valid',
    base_currency STRING COMMENT 'Base currency code (always USD for reporting)',
    quote_currency STRING COMMENT 'Quote currency code',
    rate DECIMAL(18,6) COMMENT 'Standardized exchange rate',
    rate_type STRING COMMENT 'Rate type (spot, average, month-end)',
    is_latest BOOLEAN COMMENT 'Flag indicating most recent rate for this date',
    quality_score DECIMAL(3,2) COMMENT 'Data quality score (0.0-1.0)',
    source_provider STRING COMMENT 'Original provider',
    created_at TIMESTAMP COMMENT 'When record was created in Silver',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Cleaned and standardized FX rates - one rate per date per currency pair'
""")

print("✓ Created: silver.fx_rates_std")

# COMMAND ----------

# Close trial balance standardized
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.close_trial_balance_std (
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    bu_code STRING COMMENT 'Business unit code',
    cut_version STRING COMMENT 'Version: preliminary, cut1, cut2/final',
    version_timestamp TIMESTAMP COMMENT 'When this version was received',
    account_code STRING COMMENT 'Standardized GL account code',
    account_name STRING COMMENT 'GL account name',
    account_category STRING COMMENT 'Rollup category (Revenue, COGS, OpEx, etc.)',
    cost_center STRING COMMENT 'Cost center code',
    segment STRING COMMENT 'Segment code',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
    reporting_currency STRING COMMENT 'Reporting currency (USD)',
    reporting_amount DECIMAL(18,2) COMMENT 'Amount in reporting currency',
    fx_rate DECIMAL(18,6) COMMENT 'FX rate used for conversion',
    is_current_version BOOLEAN COMMENT 'Flag for latest version',
    created_at TIMESTAMP COMMENT 'When record was created in Silver',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Standardized trial balance with all versions and FX conversion'
""")

print("✓ Created: silver.close_trial_balance_std")

# COMMAND ----------

# Segmented close standardized
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.segmented_close_std (
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    bu_code STRING COMMENT 'Business unit code',
    segment STRING COMMENT 'Segment identifier',
    product STRING COMMENT 'Product line',
    region STRING COMMENT 'Geographic region',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx)',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Amount in local currency',
    reporting_currency STRING COMMENT 'Reporting currency (USD)',
    reporting_amount DECIMAL(18,2) COMMENT 'Amount in reporting currency',
    fx_rate DECIMAL(18,6) COMMENT 'FX rate used for conversion',
    created_at TIMESTAMP COMMENT 'When record was created in Silver',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Standardized segmented close data - reconciles to trial balance'
""")

print("✓ Created: silver.segmented_close_std")

# COMMAND ----------

# Forecast standardized
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.forecast_std (
    forecast_version STRING COMMENT 'Forecast version identifier',
    forecast_created_at TIMESTAMP COMMENT 'When forecast was created',
    bu_code STRING COMMENT 'Business unit code',
    forecast_period STRING COMMENT 'Period being forecasted (YYYY-MM)',
    scenario STRING COMMENT 'Scenario: Base, Upside, Downside',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx)',
    segment STRING COMMENT 'Segment identifier',
    local_currency STRING COMMENT 'Local currency code',
    local_amount DECIMAL(18,2) COMMENT 'Forecasted amount in local currency',
    reporting_currency STRING COMMENT 'Reporting currency (USD)',
    reporting_amount DECIMAL(18,2) COMMENT 'Amount in reporting currency',
    fx_rate DECIMAL(18,6) COMMENT 'FX rate used for conversion',
    is_current_version BOOLEAN COMMENT 'Flag for latest forecast version',
    created_at TIMESTAMP COMMENT 'When record was created in Silver',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Standardized forecast data with versioning and FX conversion'
""")

print("✓ Created: silver.forecast_std")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Gold Tables (Consumption-Ready)

# COMMAND ----------

# Close status tracking
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.close_status_gold (
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    phase_id INT COMMENT 'Phase number (1-5)',
    phase_name STRING COMMENT 'Phase name',
    task_id INT COMMENT 'Task identifier',
    task_name STRING COMMENT 'Task name',
    bu_code STRING COMMENT 'Business unit code (NULL for group-level tasks)',
    planned_due_date DATE COMMENT 'Planned completion date',
    actual_completion_timestamp TIMESTAMP COMMENT 'Actual completion timestamp',
    status STRING COMMENT 'Status: pending, in_progress, completed, blocked',
    owner_role STRING COMMENT 'Responsible role (FP&A, Accounting, Tech)',
    comments STRING COMMENT 'Status updates and agent-generated summaries',
    last_updated_by STRING COMMENT 'User or agent that last updated',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Real-time status tracking for all close phases and tasks'
""")

print("✓ Created: gold.close_status_gold")

# COMMAND ----------

# Close results
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.close_results_gold (
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    bu_code STRING COMMENT 'Business unit code (CONSOLIDATED for group)',
    segment STRING COMMENT 'Segment identifier (NULL for consolidated)',
    product STRING COMMENT 'Product line (NULL for consolidated)',
    region STRING COMMENT 'Geographic region (NULL for consolidated)',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx, Operating Profit)',
    actual_amount DECIMAL(18,2) COMMENT 'Actual amount from close (USD)',
    forecast_amount DECIMAL(18,2) COMMENT 'Forecasted amount (USD)',
    prior_period_amount DECIMAL(18,2) COMMENT 'Prior period actual (USD)',
    variance_vs_forecast DECIMAL(18,2) COMMENT 'Variance: actual - forecast',
    variance_vs_forecast_pct DECIMAL(5,2) COMMENT 'Variance % vs forecast',
    variance_vs_prior DECIMAL(18,2) COMMENT 'Variance: actual - prior period',
    variance_vs_prior_pct DECIMAL(5,2) COMMENT 'Variance % vs prior period',
    fx_impact DECIMAL(18,2) COMMENT 'FX impact vs local currency',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Final close results with variance analysis - segmented and consolidated'
""")

print("✓ Created: gold.close_results_gold")

# COMMAND ----------

# Forecast results
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.forecast_results_gold (
    forecast_version STRING COMMENT 'Forecast version identifier',
    forecast_period STRING COMMENT 'Period being forecasted (YYYY-MM)',
    bu_code STRING COMMENT 'Business unit code (CONSOLIDATED for group)',
    segment STRING COMMENT 'Segment identifier (NULL for consolidated)',
    scenario STRING COMMENT 'Scenario: Base, Upside, Downside',
    account_category STRING COMMENT 'P&L line (Revenue, COGS, OpEx, Operating Profit)',
    forecast_amount DECIMAL(18,2) COMMENT 'Forecasted amount (USD)',
    prior_forecast_amount DECIMAL(18,2) COMMENT 'Previous forecast for same period (USD)',
    variance_vs_prior_forecast DECIMAL(18,2) COMMENT 'Change from previous forecast',
    variance_vs_prior_forecast_pct DECIMAL(5,2) COMMENT 'Change % from previous forecast',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Consolidated forecast results with version-over-version variance'
""")

print("✓ Created: gold.forecast_results_gold")

# COMMAND ----------

# Close KPIs
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.close_kpi_gold (
    period STRING COMMENT 'Fiscal period (YYYY-MM)',
    kpi_name STRING COMMENT 'KPI name (e.g., Cycle Time, Timeliness Score)',
    kpi_category STRING COMMENT 'Category: efficiency, quality, compliance',
    kpi_value DECIMAL(18,2) COMMENT 'KPI value',
    kpi_unit STRING COMMENT 'Unit of measurement (days, %, count)',
    target_value DECIMAL(18,2) COMMENT 'Target/SLA value',
    bu_code STRING COMMENT 'Business unit code (NULL for group-level KPI)',
    calculation_notes STRING COMMENT 'How KPI was calculated',
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp'
  )
  USING DELTA
  COMMENT 'Key performance indicators for close process - quality and efficiency metrics'
""")

print("✓ Created: gold.close_kpi_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Agent Logging Tables

# COMMAND ----------

# Agent activity log
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.close_agent_logs (
    log_id STRING COMMENT 'Unique log entry identifier',
    log_timestamp TIMESTAMP COMMENT 'When agent action occurred',
    agent_name STRING COMMENT 'Agent that performed action',
    action_type STRING COMMENT 'Type: status_update, validation, calculation, alert',
    period STRING COMMENT 'Fiscal period context',
    task_id INT COMMENT 'Related task_id (if applicable)',
    bu_code STRING COMMENT 'Related BU (if applicable)',
    decision_rationale STRING COMMENT 'Why agent took this action',
    input_data STRING COMMENT 'Key input data (JSON)',
    output_data STRING COMMENT 'Key output data (JSON)',
    status STRING COMMENT 'Result: success, warning, error',
    execution_time_ms INT COMMENT 'Execution time in milliseconds'
  )
  USING DELTA
  COMMENT 'Audit log of all agent actions and decisions'
""")

print("✓ Created: gold.close_agent_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Initialize Configuration Data

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

# Insert business units
bus = [
    Row(bu_code="NA", bu_name="North America", region="Americas", 
        functional_currency="USD", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now()),
    Row(bu_code="EU", bu_name="Europe", region="EMEA", 
        functional_currency="EUR", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now()),
    Row(bu_code="UK", bu_name="United Kingdom", region="EMEA", 
        functional_currency="GBP", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now()),
    Row(bu_code="ASIA", bu_name="Asia Pacific", region="APAC", 
        functional_currency="CNY", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now()),
    Row(bu_code="LATAM", bu_name="Latin America", region="Americas", 
        functional_currency="BRL", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now()),
    Row(bu_code="INDIA", bu_name="India", region="APAC", 
        functional_currency="INR", reporting_currency="USD", is_active=True,
        created_at=datetime.now(), updated_at=datetime.now())
]

spark.createDataFrame(bus).write.mode("overwrite").saveAsTable("config.business_units")
print("✓ Inserted 6 business units")

# COMMAND ----------

# Insert close phase definitions
phases_tasks = [
    # Phase 1: Data Gathering
    (1, "Data Gathering", "Initial data collection for the close", 2, 101, "Receive exchange rates", "Upload FX rates to system", "Tech", False, [], datetime.now()),
    (1, "Data Gathering", "Initial data collection for the close", 2, 102, "Process exchange rates", "Validate and standardize FX rates", "Tech", False, [101], datetime.now()),
    (1, "Data Gathering", "Initial data collection for the close", 2, 103, "Receive BU preliminary files", "BUs submit preliminary trial balance", "FP&A", True, [], datetime.now()),
    
    # Phase 2: Adjustments
    (2, "Adjustments", "Process preliminary results and accounting cuts", 5, 201, "Process preliminary close", "Standardize and convert BU files", "Tech", False, [103], datetime.now()),
    (2, "Adjustments", "Process preliminary results and accounting cuts", 5, 202, "Publish preliminary results", "Make results available for review", "FP&A", False, [201], datetime.now()),
    (2, "Adjustments", "Process preliminary results and accounting cuts", 5, 203, "Preliminary review meeting", "FP&A and Tech review preliminary results", "FP&A", False, [202], datetime.now()),
    (2, "Adjustments", "Process preliminary results and accounting cuts", 5, 204, "Receive first accounting cut", "BUs submit first round of adjustments", "Accounting", True, [203], datetime.now()),
    (2, "Adjustments", "Process preliminary results and accounting cuts", 5, 205, "Receive second/final accounting cut", "BUs submit final adjustments", "Accounting", True, [204], datetime.now()),
    
    # Phase 3: Data Gathering
    (3, "Data Gathering", "Collect segmented and forecast data", 2, 301, "Receive segmented files", "BUs submit segmented P&L", "FP&A", True, [205], datetime.now()),
    (3, "Data Gathering", "Collect segmented and forecast data", 2, 302, "Receive forecast files", "BUs submit updated forecasts", "FP&A", True, [205], datetime.now()),
    (3, "Data Gathering", "Collect segmented and forecast data", 2, 303, "Receive forecast FX rates", "Upload forecast FX curves", "Tech", False, [205], datetime.now()),
    
    # Phase 4: Review
    (4, "Review", "Review segmented and forecast data", 2, 401, "Segmented close review meeting", "Review segment-level results", "FP&A", False, [301], datetime.now()),
    (4, "Review", "Review segmented and forecast data", 2, 402, "Forecast review meeting", "Review forecast vs actuals", "FP&A", False, [302, 303], datetime.now()),
    
    # Phase 5: Reporting & Sign-off
    (5, "Reporting & Sign-off", "Final publication and sign-off", 1, 501, "Publish final results", "Publish consolidated close package", "FP&A", False, [401, 402], datetime.now())
]

phase_df = spark.createDataFrame(phases_tasks, 
    ["phase_id", "phase_name", "phase_description", "typical_duration_days", 
     "task_id", "task_name", "task_description", "owner_role", "is_bu_specific", 
     "depends_on_tasks", "created_at"])

phase_df.write.mode("overwrite").saveAsTable("config.close_phase_definitions")
print("✓ Inserted 14 close tasks across 5 phases")

# COMMAND ----------

# Insert agent configuration
import json

agents = [
    Row(agent_name="close_supervisor", agent_type="orchestrator", is_enabled=True,
        run_frequency="hourly", 
        config_json=json.dumps({"check_interval_minutes": 60, "auto_transition": True}),
        last_run_timestamp=None, created_at=datetime.now(), updated_at=datetime.now()),
    
    Row(agent_name="fx_agent", agent_type="domain", is_enabled=True,
        run_frequency="hourly",
        config_json=json.dumps({"required_currencies": ["EUR", "GBP", "BRL", "INR", "CNY"], "quality_threshold": 0.95}),
        last_run_timestamp=None, created_at=datetime.now(), updated_at=datetime.now()),
    
    Row(agent_name="pre_close_agent", agent_type="domain", is_enabled=True,
        run_frequency="daily",
        config_json=json.dumps({"variance_threshold_pct": 10.0, "auto_flag_outliers": True}),
        last_run_timestamp=None, created_at=datetime.now(), updated_at=datetime.now()),
    
    Row(agent_name="segmented_forecast_agent", agent_type="domain", is_enabled=True,
        run_frequency="daily",
        config_json=json.dumps({"reconciliation_tolerance": 0.01}),
        last_run_timestamp=None, created_at=datetime.now(), updated_at=datetime.now()),
    
    Row(agent_name="reporting_agent", agent_type="reporting", is_enabled=True,
        run_frequency="on_demand",
        config_json=json.dumps({"generate_executive_summary": True, "auto_refresh_dashboards": True}),
        last_run_timestamp=None, created_at=datetime.now(), updated_at=datetime.now())
]

spark.createDataFrame(agents).write.mode("overwrite").saveAsTable("config.agent_configuration")
print("✓ Inserted 5 agent configurations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification and Summary

# COMMAND ----------

# Verify all tables were created
tables = spark.sql("""
  SELECT table_schema, table_name, comment 
  FROM financial_close_lakehouse.information_schema.tables 
  WHERE table_schema IN ('bronze', 'silver', 'gold', 'config')
  ORDER BY table_schema, table_name
""").toPandas()

print("\n" + "="*80)
print("LAKEHOUSE SETUP COMPLETE")
print("="*80)
print(f"\nTotal tables created: {len(tables)}")
print("\nBreakdown by layer:")
print(tables.groupby('table_schema').size())

# COMMAND ----------

# Display table summary
display(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Run `02_synthetic_data_generation.py` to populate Bronze tables
# MAGIC 2. Verify tables are ready for Genie by checking Unity Catalog UI
# MAGIC 3. Grant appropriate permissions to FP&A users
# MAGIC 
# MAGIC **Key Tables for Genie:**
# MAGIC - `gold.close_status_gold` - Close progress tracking
# MAGIC - `gold.close_results_gold` - Final results and variances
# MAGIC - `gold.forecast_results_gold` - Forecast analysis
# MAGIC - `gold.close_kpi_gold` - KPIs and metrics
