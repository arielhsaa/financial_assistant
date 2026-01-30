# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Phase 3 Data Ingestion and Standardization
# MAGIC 
# MAGIC This notebook processes Bronze data for **Phase 3 (Data Gathering - Segmented & Forecast)**:
# MAGIC - Standardizes segmented close to Silver (`segmented_close_std`)
# MAGIC - Standardizes forecast to Silver (`forecast_std`)
# MAGIC - Updates task tracking in Gold (`close_phase_tasks`)
# MAGIC - Updates close status in Gold (`close_status_gold`)
# MAGIC 
# MAGIC **Run this notebook** after Phase 1 & 2 processing is complete.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# Catalog and schema configuration
CATALOG = "financial_close_catalog"
BRONZE_SCHEMA = "bronze_layer"
SILVER_SCHEMA = "silver_layer"
GOLD_SCHEMA = "gold_layer"

# Period to process
CURRENT_PERIOD = 202601  # January 2026

print(f"Processing Phase 3 data for period: {CURRENT_PERIOD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Standardize Segmented Close Data

# COMMAND ----------

print("Standardizing segmented close data...")

# Read raw segmented data
segmented_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_segmented_raw") \
    .filter(col("period") == CURRENT_PERIOD)

# Calculate operating profit and FX impact
segmented_std = segmented_raw \
    .withColumn("operating_profit_local", 
                col("revenue_local") - col("cogs_local") - col("opex_local")) \
    .withColumn("operating_profit_reporting",
                col("revenue_reporting") - col("cogs_reporting") - col("opex_reporting")) \
    .withColumn("fx_impact_reporting", lit(0.0))  # Simplified - would calculate vs. budget/prior FX rates \
    .withColumn("data_quality_flag",
                when((col("revenue_local") < 0) | (col("operating_profit_reporting") / col("revenue_reporting") < -1), "WARN")
                .when(col("revenue_local") == 0, "FAIL")
                .otherwise("PASS")) \
    .select(
        "period",
        "bu",
        "segment",
        "product",
        "region",
        "local_currency",
        "reporting_currency",
        "revenue_local",
        "cogs_local",
        "opex_local",
        "operating_profit_local",
        "revenue_reporting",
        "cogs_reporting",
        "opex_reporting",
        "operating_profit_reporting",
        "fx_impact_reporting",
        col("load_timestamp"),
        "data_quality_flag",
        "metadata"
    )

# Write to Silver
segmented_std.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.segmented_close_std")

segmented_count = segmented_std.count()
quality_issues = segmented_std.filter(col("data_quality_flag").isin(["WARN", "FAIL"])).count()

print(f"✓ Standardized {segmented_count:,} segmented close records")
print(f"  - Data quality issues: {quality_issues}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize Forecast Data

# COMMAND ----------

print("Standardizing forecast data...")

# Read raw forecast data
forecast_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_forecast_raw")

# Get forecast FX rates
fx_forecast = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fx_forecast_raw")

# Calculate operating profit
forecast_std = forecast_raw \
    .withColumn("operating_profit_local",
                col("revenue_local") - col("cogs_local") - col("opex_local")) \
    .withColumn("operating_profit_reporting",
                col("revenue_reporting") - col("cogs_reporting") - col("opex_reporting")) \
    .withColumn("fx_rate",
                col("reporting_amount") / col("revenue_local")) \
    .withColumn("data_quality_flag",
                when((col("revenue_local") < 0), "WARN")
                .when((col("scenario").isNull()), "FAIL")
                .otherwise("PASS")) \
    .select(
        "forecast_period",
        "submission_date",
        "bu",
        "segment",
        "scenario",
        "local_currency",
        "reporting_currency",
        "revenue_local",
        "cogs_local",
        "opex_local",
        "operating_profit_local",
        "revenue_reporting",
        "cogs_reporting",
        "opex_reporting",
        "operating_profit_reporting",
        coalesce("fx_rate", lit(1.0)).alias("fx_rate"),
        "assumptions",
        col("load_timestamp"),
        "data_quality_flag",
        "metadata"
    )

# Write to Silver
forecast_std.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.forecast_std")

forecast_count = forecast_std.count()
print(f"✓ Standardized {forecast_count:,} forecast records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Phase 3 Tasks

# COMMAND ----------

print("Adding Phase 3 tasks...")

# Read existing tasks
existing_tasks = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks") \
    .filter(col("period") == CURRENT_PERIOD)

# Define Phase 3 tasks
phase3_tasks = [
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T1-NA", "task_name": "Receive segmented files from business units",
     "bu": "North America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T1-EU", "task_name": "Receive segmented files from business units",
     "bu": "Europe", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T1-AP", "task_name": "Receive segmented files from business units",
     "bu": "Asia Pacific", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T1-LA", "task_name": "Receive segmented files from business units",
     "bu": "Latin America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T1-ME", "task_name": "Receive segmented files from business units",
     "bu": "Middle East", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T2-NA", "task_name": "Receive forecast files from business units",
     "bu": "North America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 7, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T2-EU", "task_name": "Receive forecast files from business units",
     "bu": "Europe", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 7, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T2-AP", "task_name": "Receive forecast files from business units",
     "bu": "Asia Pacific", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 7, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T2-LA", "task_name": "Receive forecast files from business units",
     "bu": "Latin America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 7, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T2-ME", "task_name": "Receive forecast files from business units",
     "bu": "Middle East", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 7, 17, 0),
     "status": "completed", "priority": "high"},
    
    {"period": CURRENT_PERIOD, "phase_id": 3, "phase_name": "Data Gathering",
     "task_id": "P3-T3", "task_name": "Receive forecast FX rates",
     "bu": None, "owner_role": "Treasury", "planned_due_date": datetime(2026, 2, 7, 12, 0),
     "status": "completed", "priority": "medium"},
]

# Add common fields
for task in phase3_tasks:
    task["actual_start_timestamp"] = datetime.now() if task["status"] in ["completed", "in_progress"] else None
    task["actual_completion_timestamp"] = datetime.now() if task["status"] == "completed" else None
    task["blocking_reason"] = None
    task["comments"] = None
    task["agent_assigned"] = "Segmented & Forecast Agent" if "segmented" in task["task_name"].lower() or "forecast" in task["task_name"].lower() else None
    task["dependencies"] = None
    task["last_updated_timestamp"] = datetime.now()
    task["metadata"] = json.dumps({"created_by": "phase3_processing", "auto_generated": True})

# Create DataFrame for new tasks
new_tasks_df = spark.createDataFrame(phase3_tasks)

# Union with existing tasks and write
all_tasks = existing_tasks.union(new_tasks_df)
all_tasks.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks")

print(f"✓ Added {len(phase3_tasks)} Phase 3 tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update Close Status

# COMMAND ----------

print("Updating close status...")

# Recalculate status with Phase 3 tasks included
tasks_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks") \
    .filter(col("period") == CURRENT_PERIOD)

# Calculate status by BU
bu_status = tasks_df \
    .filter(col("bu").isNotNull()) \
    .groupBy("period", "bu") \
    .agg(
        max("phase_id").alias("phase_id"),
        max("phase_name").alias("phase_name"),
        count("*").alias("total_tasks"),
        sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_tasks"),
        sum(when(col("status") == "blocked", 1).otherwise(0)).alias("blocked_tasks")
    ) \
    .withColumn("pct_tasks_completed", 
                round(col("completed_tasks") / col("total_tasks") * 100, 2)) \
    .withColumn("overall_status",
                when(col("pct_tasks_completed") == 100, "completed")
                .when(col("blocked_tasks") > 0, "issues")
                .when(col("completed_tasks") > 0, "in_progress")
                .otherwise("not_started")) \
    .withColumn("days_since_period_end", lit(7))  # Day 7 \
    .withColumn("days_to_sla", lit(3))  # 3 days to SLA \
    .withColumn("sla_status", lit("on_track")) \
    .withColumn("key_issues", lit(None).cast("string")) \
    .withColumn("last_milestone", lit("Segmented and forecast data received")) \
    .withColumn("next_milestone", lit("Segmented close review meeting")) \
    .withColumn("agent_summary",
                concat(
                    lit("BU "), col("bu"),
                    lit(" has completed "), col("pct_tasks_completed"),
                    lit("% of tasks. Ready for Phase 4 review.")
                )) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"source": "phase3_processing"}'))

# Add consolidated status
consolidated_status = tasks_df \
    .groupBy("period") \
    .agg(
        max("phase_id").alias("phase_id"),
        lit("Review").alias("phase_name"),
        count("*").alias("total_tasks"),
        sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_tasks"),
        sum(when(col("status") == "blocked", 1).otherwise(0)).alias("blocked_tasks")
    ) \
    .withColumn("bu", lit("CONSOLIDATED")) \
    .withColumn("pct_tasks_completed",
                round(col("completed_tasks") / col("total_tasks") * 100, 2)) \
    .withColumn("overall_status", lit("in_progress")) \
    .withColumn("days_since_period_end", lit(7)) \
    .withColumn("days_to_sla", lit(3)) \
    .withColumn("sla_status", lit("on_track")) \
    .withColumn("key_issues", lit(None).cast("string")) \
    .withColumn("last_milestone", lit("Phase 3 data gathering completed")) \
    .withColumn("next_milestone", lit("Phase 4 review meetings")) \
    .withColumn("agent_summary",
                concat(
                    lit("Close progress: "),
                    col("pct_tasks_completed"),
                    lit("% complete. Entering Phase 4 - Review.")
                )) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"source": "phase3_processing"}'))

# Union and write
all_status = bu_status.union(consolidated_status)
all_status.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")

print(f"✓ Updated close status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Populate Close Results (Preliminary)

# COMMAND ----------

print("Populating preliminary close results...")

# Aggregate segmented close to various levels
segmented_std = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.segmented_close_std") \
    .filter(col("period") == CURRENT_PERIOD)

# Calculate gross profit
results = segmented_std \
    .withColumn("gross_profit_local", col("revenue_local") - col("cogs_local")) \
    .withColumn("gross_profit_reporting", col("revenue_reporting") - col("cogs_reporting"))

# Full granularity (BU + Segment + Product + Region)
detailed_results = results \
    .withColumn("variance_vs_prior_period", lit(0.0)) \
    .withColumn("variance_vs_prior_pct", lit(0.0)) \
    .withColumn("variance_vs_forecast", lit(0.0)) \
    .withColumn("variance_vs_forecast_pct", lit(0.0)) \
    .withColumn("gross_margin_pct", 
                round(col("gross_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("operating_margin_pct",
                round(col("operating_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"preliminary": true}')) \
    .select(
        "period", "bu", "segment", "product", "region",
        "local_currency", "reporting_currency",
        "revenue_local", "cogs_local", "gross_profit_local", "opex_local", "operating_profit_local",
        "revenue_reporting", "cogs_reporting", "gross_profit_reporting", "opex_reporting", "operating_profit_reporting",
        "fx_impact_reporting", "variance_vs_prior_period", "variance_vs_prior_pct",
        "variance_vs_forecast", "variance_vs_forecast_pct",
        "gross_margin_pct", "operating_margin_pct",
        "load_timestamp", "metadata"
    )

# BU + Segment level (aggregate products and regions)
bu_segment_results = results \
    .groupBy("period", "bu", "segment", "local_currency", "reporting_currency") \
    .agg(
        sum("revenue_local").alias("revenue_local"),
        sum("cogs_local").alias("cogs_local"),
        sum("opex_local").alias("opex_local"),
        sum("operating_profit_local").alias("operating_profit_local"),
        sum("revenue_reporting").alias("revenue_reporting"),
        sum("cogs_reporting").alias("cogs_reporting"),
        sum("opex_reporting").alias("opex_reporting"),
        sum("operating_profit_reporting").alias("operating_profit_reporting"),
        sum("fx_impact_reporting").alias("fx_impact_reporting")
    ) \
    .withColumn("product", lit("ALL")) \
    .withColumn("region", lit("ALL")) \
    .withColumn("gross_profit_local", col("revenue_local") - col("cogs_local")) \
    .withColumn("gross_profit_reporting", col("revenue_reporting") - col("cogs_reporting")) \
    .withColumn("variance_vs_prior_period", lit(0.0)) \
    .withColumn("variance_vs_prior_pct", lit(0.0)) \
    .withColumn("variance_vs_forecast", lit(0.0)) \
    .withColumn("variance_vs_forecast_pct", lit(0.0)) \
    .withColumn("gross_margin_pct",
                round(col("gross_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("operating_margin_pct",
                round(col("operating_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"aggregation": "bu_segment"}'))

# BU level (all segments)
bu_results = results \
    .groupBy("period", "bu", "local_currency", "reporting_currency") \
    .agg(
        sum("revenue_local").alias("revenue_local"),
        sum("cogs_local").alias("cogs_local"),
        sum("opex_local").alias("opex_local"),
        sum("operating_profit_local").alias("operating_profit_local"),
        sum("revenue_reporting").alias("revenue_reporting"),
        sum("cogs_reporting").alias("cogs_reporting"),
        sum("opex_reporting").alias("opex_reporting"),
        sum("operating_profit_reporting").alias("operating_profit_reporting"),
        sum("fx_impact_reporting").alias("fx_impact_reporting")
    ) \
    .withColumn("segment", lit("ALL")) \
    .withColumn("product", lit("ALL")) \
    .withColumn("region", lit("ALL")) \
    .withColumn("gross_profit_local", col("revenue_local") - col("cogs_local")) \
    .withColumn("gross_profit_reporting", col("revenue_reporting") - col("cogs_reporting")) \
    .withColumn("variance_vs_prior_period", lit(0.0)) \
    .withColumn("variance_vs_prior_pct", lit(0.0)) \
    .withColumn("variance_vs_forecast", lit(0.0)) \
    .withColumn("variance_vs_forecast_pct", lit(0.0)) \
    .withColumn("gross_margin_pct",
                round(col("gross_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("operating_margin_pct",
                round(col("operating_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"aggregation": "bu_total"}'))

# Consolidated (all BUs)
consolidated_results = results \
    .groupBy("period") \
    .agg(
        sum("revenue_reporting").alias("revenue_reporting"),
        sum("cogs_reporting").alias("cogs_reporting"),
        sum("opex_reporting").alias("opex_reporting"),
        sum("operating_profit_reporting").alias("operating_profit_reporting"),
        sum("fx_impact_reporting").alias("fx_impact_reporting")
    ) \
    .withColumn("bu", lit("CONSOLIDATED")) \
    .withColumn("segment", lit("ALL")) \
    .withColumn("product", lit("ALL")) \
    .withColumn("region", lit("ALL")) \
    .withColumn("local_currency", lit("USD")) \
    .withColumn("reporting_currency", lit("USD")) \
    .withColumn("revenue_local", col("revenue_reporting")) \
    .withColumn("cogs_local", col("cogs_reporting")) \
    .withColumn("opex_local", col("opex_reporting")) \
    .withColumn("operating_profit_local", col("operating_profit_reporting")) \
    .withColumn("gross_profit_local", col("revenue_local") - col("cogs_local")) \
    .withColumn("gross_profit_reporting", col("revenue_reporting") - col("cogs_reporting")) \
    .withColumn("variance_vs_prior_period", lit(0.0)) \
    .withColumn("variance_vs_prior_pct", lit(0.0)) \
    .withColumn("variance_vs_forecast", lit(0.0)) \
    .withColumn("variance_vs_forecast_pct", lit(0.0)) \
    .withColumn("gross_margin_pct",
                round(col("gross_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("operating_margin_pct",
                round(col("operating_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"aggregation": "consolidated"}'))

# Union all levels
all_results = detailed_results \
    .union(bu_segment_results.select(detailed_results.columns)) \
    .union(bu_results.select(detailed_results.columns)) \
    .union(consolidated_results.select(detailed_results.columns))

# Write to Gold
all_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_results_gold")

results_count = all_results.count()
print(f"✓ Populated {results_count:,} close result records (all aggregation levels)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Populate Forecast Results

# COMMAND ----------

print("Populating forecast results...")

# Get forecast data
forecast_std = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.forecast_std")

# Aggregate by BU + Segment + Scenario
forecast_results = forecast_std \
    .groupBy("forecast_period", "submission_date", "bu", "segment", "scenario", "local_currency", "reporting_currency") \
    .agg(
        sum("revenue_local").alias("revenue_forecast_local"),
        sum("cogs_local").alias("cogs_forecast_local"),
        sum("opex_local").alias("opex_forecast_local"),
        sum("operating_profit_local").alias("operating_profit_forecast_local"),
        sum("revenue_reporting").alias("revenue_forecast_reporting"),
        sum("cogs_reporting").alias("cogs_forecast_reporting"),
        sum("opex_reporting").alias("opex_forecast_reporting"),
        sum("operating_profit_reporting").alias("operating_profit_forecast_reporting"),
        max("assumptions").alias("assumptions")
    ) \
    .withColumn("revenue_actual_reporting", lit(None).cast("decimal(18,2)")) \
    .withColumn("operating_profit_actual_reporting", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_revenue", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_operating_profit", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_revenue_pct", lit(None).cast("decimal(10,2)")) \
    .withColumn("variance_operating_profit_pct", lit(None).cast("decimal(10,2)")) \
    .withColumn("forecast_accuracy_score", lit(None).cast("decimal(5,2)")) \
    .withColumn("variance_drivers", lit(None).cast("string")) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"actuals_available": false}'))

# Consolidated forecast
consolidated_forecast = forecast_std \
    .groupBy("forecast_period", "submission_date", "scenario") \
    .agg(
        sum("revenue_reporting").alias("revenue_forecast_reporting"),
        sum("cogs_reporting").alias("cogs_forecast_reporting"),
        sum("opex_reporting").alias("opex_forecast_reporting"),
        sum("operating_profit_reporting").alias("operating_profit_forecast_reporting")
    ) \
    .withColumn("bu", lit("CONSOLIDATED")) \
    .withColumn("segment", lit("ALL")) \
    .withColumn("local_currency", lit("USD")) \
    .withColumn("reporting_currency", lit("USD")) \
    .withColumn("revenue_forecast_local", col("revenue_forecast_reporting")) \
    .withColumn("cogs_forecast_local", col("cogs_forecast_reporting")) \
    .withColumn("opex_forecast_local", col("opex_forecast_reporting")) \
    .withColumn("operating_profit_forecast_local", col("operating_profit_forecast_reporting")) \
    .withColumn("assumptions", lit("Consolidated forecast across all BUs")) \
    .withColumn("revenue_actual_reporting", lit(None).cast("decimal(18,2)")) \
    .withColumn("operating_profit_actual_reporting", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_revenue", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_operating_profit", lit(None).cast("decimal(18,2)")) \
    .withColumn("variance_revenue_pct", lit(None).cast("decimal(10,2)")) \
    .withColumn("variance_operating_profit_pct", lit(None).cast("decimal(10,2)")) \
    .withColumn("forecast_accuracy_score", lit(None).cast("decimal(5,2)")) \
    .withColumn("variance_drivers", lit(None).cast("string")) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"aggregation": "consolidated"}'))

# Union all
all_forecast_results = forecast_results.union(consolidated_forecast)

# Write to Gold
all_forecast_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold")

forecast_results_count = all_forecast_results.count()
print(f"✓ Populated {forecast_results_count:,} forecast result records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Processing

# COMMAND ----------

print("\n=== Phase 3 Processing Summary ===\n")

# Silver tables
print("Silver Layer:")
print(f"  segmented_close_std: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.segmented_close_std').count():,} records")
print(f"  forecast_std: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.forecast_std').count():,} records")

# Gold tables
print("\nGold Layer:")
print(f"  close_phase_tasks: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks').count():,} tasks")
print(f"  close_status_gold: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_status_gold').count():,} status records")
print(f"  close_results_gold: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_results_gold').count():,} result records")
print(f"  forecast_results_gold: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold').count():,} forecast records")

# COMMAND ----------

# Display samples
print("\n=== Close Results (Consolidated) ===")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_results_gold")
    .filter(col("bu") == "CONSOLIDATED")
    .select("period", "revenue_reporting", "operating_profit_reporting", "operating_margin_pct")
)

print("\n=== Forecast Results (Base Scenario) ===")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold")
    .filter((col("scenario") == "Base") & (col("bu") == "CONSOLIDATED"))
    .select("forecast_period", "revenue_forecast_reporting", "operating_profit_forecast_reporting")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ Phase 3 Processing Complete!

Processed for period: {CURRENT_PERIOD}

Silver Tables Updated:
- segmented_close_std: Segmented P&L with operating profit and FX impact
- forecast_std: Multi-scenario forecasts with assumptions

Gold Tables Updated:
- close_phase_tasks: Phase 3 tasks added and tracked
- close_status_gold: Status updated for Phase 3 completion
- close_results_gold: Preliminary close results at all aggregation levels
- forecast_results_gold: Forecast results ready for variance analysis

Next Steps:
1. Run agent notebooks (05-07) for automated analysis and reporting
2. Schedule Phase 4 review meetings
3. Use Genie to analyze variances and investigate anomalies

Current Phase: Phase 4 - Review (ready to start)
Next Milestone: Segmented close review meeting
""")
