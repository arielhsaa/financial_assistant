# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Phase 1 & 2 Data Ingestion and Standardization
# MAGIC 
# MAGIC This notebook processes Bronze data for **Phase 1 (Data Gathering)** and **Phase 2 (Adjustments)**:
# MAGIC - Standardizes FX rates to Silver (`fx_rates_std`)
# MAGIC - Standardizes trial balance to Silver (`close_trial_balance_std`)
# MAGIC - Updates task tracking in Gold (`close_phase_tasks`)
# MAGIC - Updates close status in Gold (`close_status_gold`)
# MAGIC 
# MAGIC **Run this notebook** after generating synthetic data or receiving new BU submissions.

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

print(f"Processing Phase 1 & 2 data for period: {CURRENT_PERIOD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Standardize FX Rates (Phase 1)

# COMMAND ----------

print("Standardizing FX rates...")

# Read raw FX rates
fx_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw")

# Calculate data quality score and detect anomalies
fx_window = Window.partitionBy("from_currency", "to_currency").orderBy("rate_date")

fx_std = fx_raw \
    .withColumn("prev_rate", lag("exchange_rate").over(fx_window)) \
    .withColumn("rate_change_pct", 
                when(col("prev_rate").isNotNull(), 
                     (col("exchange_rate") - col("prev_rate")) / col("prev_rate") * 100)
                .otherwise(0)) \
    .withColumn("anomaly_flag", 
                when(abs(col("rate_change_pct")) > 5.0, True)  # >5% daily change = anomaly
                .otherwise(False)) \
    .withColumn("data_quality_score", 
                when(col("anomaly_flag"), 0.80)
                .otherwise(1.0)) \
    .withColumn("is_latest", 
                row_number().over(
                    Window.partitionBy("rate_date", "from_currency", "to_currency")
                    .orderBy(desc("load_timestamp"))
                ) == 1) \
    .select(
        "rate_date",
        "from_currency",
        "to_currency",
        "exchange_rate",
        "rate_type",
        "is_latest",
        "provider",
        col("load_timestamp"),
        "data_quality_score",
        "anomaly_flag",
        "metadata"
    )

# Write to Silver (overwrite for idempotency)
fx_std.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.fx_rates_std")

fx_count = fx_std.count()
anomaly_count = fx_std.filter(col("anomaly_flag") == True).count()

print(f"✓ Standardized {fx_count:,} FX rate records")
print(f"  - Anomalies detected: {anomaly_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Standardize Trial Balance (Phase 2)

# COMMAND ----------

print("Standardizing trial balance...")

# Read raw pre-close data
pre_close_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_pre_close_raw") \
    .filter(col("period") == CURRENT_PERIOD)

# Map account codes to categories
account_category_map = {
    "4000": "Revenue", "4100": "Revenue",
    "5000": "COGS", "5100": "COGS",
    "6000": "Operating Expense", "6100": "Operating Expense", 
    "6200": "Operating Expense", "7000": "Operating Expense",
    "8000": "Other"
}

# Create mapping expression
category_expr = when(col("account_code") == "4000", "Revenue") \
    .when(col("account_code") == "4100", "Revenue") \
    .when(col("account_code") == "5000", "COGS") \
    .when(col("account_code") == "5100", "COGS") \
    .when(col("account_code").startswith("6"), "Operating Expense") \
    .when(col("account_code") == "7000", "Operating Expense") \
    .otherwise("Other")

# Assign version number within each cut type
cut_version_window = Window.partitionBy("period", "bu", "cut_type", "account_code", "cost_center") \
    .orderBy("load_timestamp")

# Calculate variance vs. prior period (simplified - using random for demo)
tb_std = pre_close_raw \
    .withColumn("account_category", category_expr) \
    .withColumn("cut_version", row_number().over(cut_version_window)) \
    .withColumn("fx_rate", col("reporting_amount") / col("local_amount")) \
    .withColumn("fx_impact", lit(0.0))  # Simplified for demo - would calculate vs. prior period rate \
    .withColumn("variance_vs_prior_pct", 
                (rand() - 0.5) * 20)  # Random variance for demo \
    .withColumn("data_quality_flag", 
                when((col("local_amount") == 0) & (col("reporting_amount") != 0), "FAIL")
                .when(abs(col("variance_vs_prior_pct")) > 50, "WARN")
                .otherwise("PASS")) \
    .select(
        "period",
        "bu",
        "cut_type",
        "cut_version",
        "account_code",
        "account_name",
        "account_category",
        "cost_center",
        "segment",
        "region",
        "local_currency",
        "local_amount",
        "reporting_currency",
        "reporting_amount",
        "fx_rate",
        "fx_impact",
        col("load_timestamp"),
        "data_quality_flag",
        "variance_vs_prior_pct",
        "metadata"
    )

# Write to Silver
tb_std.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.close_trial_balance_std")

tb_count = tb_std.count()
quality_issues = tb_std.filter(col("data_quality_flag").isin(["WARN", "FAIL"])).count()

print(f"✓ Standardized {tb_count:,} trial balance records")
print(f"  - Data quality issues: {quality_issues}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize Phase Tasks (Phase 1 & 2)

# COMMAND ----------

print("Initializing phase tasks...")

# Define Phase 1 and Phase 2 tasks
tasks_data = [
    # Phase 1 - Data Gathering
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering", 
     "task_id": "P1-T1", "task_name": "Receive and upload exchange rates", 
     "bu": None, "owner_role": "Treasury", "planned_due_date": datetime(2026, 2, 1, 12, 0),
     "status": "completed", "priority": "high", "agent_assigned": "FX Agent"},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T2", "task_name": "Process exchange rates in Databricks",
     "bu": None, "owner_role": "FP&A Tech", "planned_due_date": datetime(2026, 2, 1, 14, 0),
     "status": "completed", "priority": "high", "agent_assigned": "FX Agent"},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T3-NA", "task_name": "Receive BU preliminary close files",
     "bu": "North America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 2, 17, 0),
     "status": "completed", "priority": "critical", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T3-EU", "task_name": "Receive BU preliminary close files",
     "bu": "Europe", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 2, 17, 0),
     "status": "completed", "priority": "critical", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T3-AP", "task_name": "Receive BU preliminary close files",
     "bu": "Asia Pacific", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 2, 17, 0),
     "status": "completed", "priority": "critical", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T3-LA", "task_name": "Receive BU preliminary close files",
     "bu": "Latin America", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 2, 17, 0),
     "status": "completed", "priority": "critical", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 1, "phase_name": "Data Gathering",
     "task_id": "P1-T3-ME", "task_name": "Receive BU preliminary close files",
     "bu": "Middle East", "owner_role": "BU Controller", "planned_due_date": datetime(2026, 2, 2, 17, 0),
     "status": "completed", "priority": "critical", "agent_assigned": None},
    
    # Phase 2 - Adjustments
    {"period": CURRENT_PERIOD, "phase_id": 2, "phase_name": "Adjustments",
     "task_id": "P2-T1", "task_name": "Process BU preliminary close files",
     "bu": None, "owner_role": "FP&A Tech", "planned_due_date": datetime(2026, 2, 3, 12, 0),
     "status": "completed", "priority": "high", "agent_assigned": "PreClose Agent"},
    
    {"period": CURRENT_PERIOD, "phase_id": 2, "phase_name": "Adjustments",
     "task_id": "P2-T2", "task_name": "Publish preliminary results",
     "bu": None, "owner_role": "FP&A", "planned_due_date": datetime(2026, 2, 3, 17, 0),
     "status": "in_progress", "priority": "high", "agent_assigned": "PreClose Agent"},
    
    {"period": CURRENT_PERIOD, "phase_id": 2, "phase_name": "Adjustments",
     "task_id": "P2-T3", "task_name": "Preliminary results review meeting (FP&A + Tech)",
     "bu": None, "owner_role": "FP&A Lead", "planned_due_date": datetime(2026, 2, 4, 10, 0),
     "status": "pending", "priority": "high", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 2, "phase_name": "Adjustments",
     "task_id": "P2-T4", "task_name": "Receive first accounting cut",
     "bu": None, "owner_role": "Accounting", "planned_due_date": datetime(2026, 2, 5, 17, 0),
     "status": "pending", "priority": "high", "agent_assigned": None},
    
    {"period": CURRENT_PERIOD, "phase_id": 2, "phase_name": "Adjustments",
     "task_id": "P2-T5", "task_name": "Receive second (final) accounting cut",
     "bu": None, "owner_role": "Accounting", "planned_due_date": datetime(2026, 2, 6, 17, 0),
     "status": "pending", "priority": "critical", "agent_assigned": None},
]

# Add common fields
for task in tasks_data:
    task["actual_start_timestamp"] = datetime.now() if task["status"] in ["completed", "in_progress"] else None
    task["actual_completion_timestamp"] = datetime.now() if task["status"] == "completed" else None
    task["blocking_reason"] = None
    task["comments"] = None
    task["dependencies"] = None
    task["last_updated_timestamp"] = datetime.now()
    task["metadata"] = json.dumps({"created_by": "system", "auto_generated": True})

# Create DataFrame and write to Gold
tasks_df = spark.createDataFrame(tasks_data)
tasks_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks")

print(f"✓ Initialized {len(tasks_data)} phase tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update Close Status

# COMMAND ----------

print("Updating close status...")

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
    .withColumn("days_since_period_end", lit(3))  # Simplified for demo \
    .withColumn("days_to_sla", lit(7))  # Simplified: 10 day SLA, 3 days elapsed \
    .withColumn("sla_status",
                when(col("days_to_sla") < 2, "at_risk")
                .when(col("days_to_sla") < 0, "overdue")
                .otherwise("on_track")) \
    .withColumn("key_issues", lit(None).cast("string")) \
    .withColumn("last_milestone", lit("Preliminary close data received")) \
    .withColumn("next_milestone", lit("Preliminary results review meeting")) \
    .withColumn("agent_summary", 
                concat(
                    lit("BU "), col("bu"), 
                    lit(" has completed "), col("pct_tasks_completed"), 
                    lit("% of tasks in Phase "), col("phase_id")
                )) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"source": "phase1_2_processing"}'))

# Add consolidated status
consolidated_status = tasks_df \
    .groupBy("period") \
    .agg(
        max("phase_id").alias("phase_id"),
        max("phase_name").alias("phase_name"),
        count("*").alias("total_tasks"),
        sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_tasks"),
        sum(when(col("status") == "blocked", 1).otherwise(0)).alias("blocked_tasks")
    ) \
    .withColumn("bu", lit("CONSOLIDATED")) \
    .withColumn("pct_tasks_completed", 
                round(col("completed_tasks") / col("total_tasks") * 100, 2)) \
    .withColumn("overall_status",
                when(col("pct_tasks_completed") == 100, "completed")
                .when(col("blocked_tasks") > 0, "issues")
                .when(col("completed_tasks") > 0, "in_progress")
                .otherwise("not_started")) \
    .withColumn("days_since_period_end", lit(3)) \
    .withColumn("days_to_sla", lit(7)) \
    .withColumn("sla_status", lit("on_track")) \
    .withColumn("key_issues", lit(None).cast("string")) \
    .withColumn("last_milestone", lit("Trial balance standardization completed")) \
    .withColumn("next_milestone", lit("Preliminary results review meeting")) \
    .withColumn("agent_summary", 
                concat(
                    lit("Overall close progress: "), 
                    col("pct_tasks_completed"), 
                    lit("% complete. Currently in Phase "), 
                    col("phase_id"), lit(" - "), col("phase_name")
                )) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"source": "phase1_2_processing"}'))

# Union BU and consolidated status
all_status = bu_status.union(consolidated_status)

# Write to Gold
all_status.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")

status_count = all_status.count()
print(f"✓ Updated close status for {status_count} entities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Preliminary KPIs

# COMMAND ----------

print("Calculating preliminary KPIs...")

# Calculate KPIs from trial balance
kpi_data = []

# Get final_cut data (or latest available)
final_tb = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.close_trial_balance_std") \
    .filter((col("period") == CURRENT_PERIOD) & (col("cut_type") == "final_cut"))

# If no final_cut yet, use preliminary
if final_tb.count() == 0:
    final_tb = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.close_trial_balance_std") \
        .filter((col("period") == CURRENT_PERIOD) & (col("cut_type") == "preliminary"))

# KPI 1: Total Revenue by BU
revenue_by_bu = final_tb \
    .filter(col("account_category") == "Revenue") \
    .groupBy("bu") \
    .agg(sum("reporting_amount").alias("kpi_value")) \
    .withColumn("period", lit(CURRENT_PERIOD)) \
    .withColumn("kpi_name", lit("Total Revenue")) \
    .withColumn("kpi_category", lit("Financial")) \
    .withColumn("kpi_unit", lit("currency")) \
    .withColumn("target_value", col("kpi_value") * 0.95)  # Target = 95% of actual for demo \
    .withColumn("variance_vs_target", col("kpi_value") - col("target_value")) \
    .withColumn("status", lit("on_target")) \
    .withColumn("trend", lit("stable")) \
    .withColumn("description", lit("Total revenue in reporting currency")) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("metadata", lit('{"preliminary": true}'))

# Write to Gold
revenue_by_bu.select(
    "period", "kpi_name", "kpi_category", "bu", "kpi_value", "kpi_unit",
    "target_value", "variance_vs_target", "status", "trend", "description",
    "load_timestamp", "metadata"
).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_kpi_gold")

kpi_count = revenue_by_bu.count()
print(f"✓ Calculated {kpi_count} preliminary KPIs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Processing

# COMMAND ----------

print("\n=== Phase 1 & 2 Processing Summary ===\n")

# Silver tables
print("Silver Layer:")
print(f"  fx_rates_std: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.fx_rates_std').count():,} records")
print(f"  close_trial_balance_std: {spark.table(f'{CATALOG}.{SILVER_SCHEMA}.close_trial_balance_std').count():,} records")

# Gold tables
print("\nGold Layer:")
print(f"  close_phase_tasks: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks').count():,} tasks")
print(f"  close_status_gold: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_status_gold').count():,} status records")
print(f"  close_kpi_gold: {spark.table(f'{CATALOG}.{GOLD_SCHEMA}.close_kpi_gold').count():,} KPIs")

# COMMAND ----------

# Display samples
print("\n=== Close Status by BU ===")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")
    .orderBy("bu")
)

print("\n=== Phase Tasks Status ===")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks")
    .groupBy("phase_name", "status")
    .count()
    .orderBy("phase_name", "status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ Phase 1 & 2 Processing Complete!

Processed for period: {CURRENT_PERIOD}

Silver Tables Updated:
- fx_rates_std: FX rates standardized with anomaly detection
- close_trial_balance_std: Trial balance with all cuts and data quality flags

Gold Tables Updated:
- close_phase_tasks: Task tracking initialized for Phase 1 & 2
- close_status_gold: Close status by BU and consolidated
- close_kpi_gold: Preliminary financial KPIs

Next Steps:
1. Review close status in Genie or dashboards
2. Run notebook 04_ingest_and_standardize_phase3.py to process segmented and forecast data
3. Run agent notebooks (05-07) for automated monitoring and analysis

Current Phase: Phase 2 - Adjustments (in progress)
Next Milestone: Preliminary results review meeting
""")
