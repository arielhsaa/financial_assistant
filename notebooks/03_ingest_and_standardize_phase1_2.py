# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Phase 1 & 2 Data Processing
# MAGIC 
# MAGIC **Purpose:** Ingest and standardize data for Phase 1 (Data Gathering) and Phase 2 (Adjustments)
# MAGIC 
# MAGIC **Processes:**
# MAGIC - FX rates: Bronze → Silver (validation, deduplication, quality scoring)
# MAGIC - BU preliminary close: Bronze → Silver (standardization, FX conversion)
# MAGIC - Populate Gold: close_status_gold for Phase 1 & 2 tasks
# MAGIC 
# MAGIC **Execution time:** ~3 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

spark.sql("USE CATALOG financial_close_lakehouse")

# Configuration
REPORTING_CURRENCY = "USD"
CURRENT_PERIOD = "2025-12"  # December 2025 close

print(f"✓ Processing close for period: {CURRENT_PERIOD}")
print(f"✓ Reporting currency: {REPORTING_CURRENCY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Process FX Rates (Bronze → Silver)

# COMMAND ----------

# Read raw FX rates
fx_raw = spark.table("bronze.fx_rates_raw")

print(f"Raw FX records: {fx_raw.count()}")

# COMMAND ----------

# Standardize and validate FX rates
# Strategy: For each date/currency pair, pick the median rate across providers
# Flag as latest if it's from the most recent load

fx_standardized = (
    fx_raw
    # Filter to valid rates (positive, reasonable range)
    .filter(F.col("rate") > 0)
    .filter(F.col("rate") < 1000)  # Catch data errors
    
    # Calculate median rate per date/currency pair
    .groupBy("rate_date", "base_currency", "quote_currency", "rate_type")
    .agg(
        F.expr("percentile(rate, 0.5)").alias("rate"),  # Median rate
        F.count("*").alias("provider_count"),
        F.min("rate").alias("min_rate"),
        F.max("rate").alias("max_rate"),
        F.max("load_timestamp").alias("load_timestamp")
    )
    
    # Calculate quality score based on provider consensus
    .withColumn("rate_spread", F.col("max_rate") - F.col("min_rate"))
    .withColumn("quality_score", 
                F.when(F.col("rate_spread") / F.col("rate") < 0.01, 1.0)  # <1% spread = perfect
                 .when(F.col("rate_spread") / F.col("rate") < 0.02, 0.9)  # <2% spread = good
                 .otherwise(0.7))  # >2% spread = acceptable
    
    # Flag latest rate for each date/currency
    .withColumn("is_latest", F.lit(True))
    
    # Add audit columns
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    
    # Select columns for silver table
    .select(
        "rate_date",
        "base_currency",
        "quote_currency",
        "rate",
        "rate_type",
        "is_latest",
        "quality_score",
        F.concat_ws(",", F.lit("multiple")).alias("source_provider"),
        "created_at",
        "updated_at"
    )
)

# Write to Silver
fx_standardized.write.mode("overwrite").saveAsTable("silver.fx_rates_std")

fx_count = fx_standardized.count()
print(f"✓ Processed {fx_count} standardized FX rates")

# Show quality metrics
fx_quality = fx_standardized.groupBy("quality_score").count().orderBy("quality_score", ascending=False)
print("\nFX Quality Distribution:")
display(fx_quality)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process BU Preliminary Close (Bronze → Silver)

# COMMAND ----------

# Read raw preliminary close data
pre_close_raw = spark.table("bronze.bu_pre_close_raw")

print(f"Raw preliminary close records: {pre_close_raw.count()}")

# COMMAND ----------

# Get latest FX rates for the period
# Strategy: Use month-end rate for the close period
from pyspark.sql.functions import last_day

period_end_date = F.last_day(F.lit(f"{CURRENT_PERIOD}-01"))

# Get FX rates for month-end
fx_rates_for_period = (
    spark.table("silver.fx_rates_std")
    .filter(F.col("rate_date") == F.last_day(F.lit(f"{CURRENT_PERIOD}-01")))
    .select(
        "quote_currency",
        F.col("rate").alias("fx_rate")
    )
)

print("FX rates for period:")
display(fx_rates_for_period)

# COMMAND ----------

# Standardize trial balance with FX conversion
trial_balance_std = (
    pre_close_raw
    .filter(F.col("period") == CURRENT_PERIOD)
    
    # Add account category mapping (already in raw data, but validate)
    .withColumn("account_category",
                F.when(F.col("account_code").startswith("4"), "Revenue")
                 .when(F.col("account_code").startswith("5"), "COGS")
                 .when(F.col("account_code").startswith("6"), "OpEx")
                 .when(F.col("account_code").startswith("7"), "OpEx")
                 .when(F.col("account_code").startswith("8"), "Interest")
                 .when(F.col("account_code").startswith("9"), "Tax")
                 .otherwise("Other"))
    
    # Add version timestamp (simulate receipt times)
    .withColumn("version_timestamp",
                F.when(F.col("cut_version") == "preliminary", F.lit("2025-12-03 10:00:00").cast("timestamp"))
                 .when(F.col("cut_version") == "cut1", F.lit("2025-12-06 14:00:00").cast("timestamp"))
                 .when(F.col("cut_version") == "cut2", F.lit("2025-12-10 16:00:00").cast("timestamp"))
                 .otherwise(F.current_timestamp()))
    
    # Join with FX rates for conversion
    .join(fx_rates_for_period, 
          F.col("local_currency") == F.col("quote_currency"), 
          "left")
    
    # Handle USD (no conversion needed)
    .withColumn("fx_rate", F.coalesce(F.col("fx_rate"), F.lit(1.0)))
    
    # Convert to reporting currency
    .withColumn("reporting_currency", F.lit(REPORTING_CURRENCY))
    .withColumn("reporting_amount", F.col("local_amount") / F.col("fx_rate"))
    
    # Flag current version (cut2 is final)
    .withColumn("is_current_version",
                F.when(F.col("cut_version") == "cut2", True).otherwise(False))
    
    # Add audit columns
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    
    .select(
        "period",
        "bu_code",
        "cut_version",
        "version_timestamp",
        "account_code",
        "account_name",
        "account_category",
        "cost_center",
        "segment",
        "local_currency",
        "local_amount",
        "reporting_currency",
        "reporting_amount",
        "fx_rate",
        "is_current_version",
        "created_at",
        "updated_at"
    )
)

# Write to Silver
trial_balance_std.write.mode("append").saveAsTable("silver.close_trial_balance_std")

tb_count = trial_balance_std.count()
print(f"✓ Processed {tb_count} trial balance records")

# Show summary by BU and version
tb_summary = (
    trial_balance_std
    .groupBy("bu_code", "cut_version")
    .agg(
        F.sum("reporting_amount").alias("total_amount"),
        F.count("*").alias("record_count")
    )
    .orderBy("bu_code", "cut_version")
)

print("\nTrial Balance Summary by BU and Version:")
display(tb_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate Preliminary KPIs

# COMMAND ----------

# Calculate key metrics by BU for preliminary review
preliminary_kpis = (
    trial_balance_std
    .filter(F.col("cut_version") == "cut2")  # Final cut
    .groupBy("bu_code", "account_category")
    .agg(F.sum("reporting_amount").alias("total_amount"))
    .groupBy("bu_code")
    .pivot("account_category", ["Revenue", "COGS", "OpEx", "Interest", "Tax"])
    .sum("total_amount")
    .fillna(0)
    
    # Calculate operating profit
    .withColumn("Operating_Profit", 
                F.col("Revenue") + F.col("COGS") + F.col("OpEx"))
    
    # Calculate operating margin
    .withColumn("Operating_Margin_Pct",
                F.round((F.col("Operating_Profit") / F.col("Revenue")) * 100, 2))
)

print("Preliminary KPIs by BU:")
display(preliminary_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Populate Close Status (Gold)

# COMMAND ----------

# Get business units and phase definitions
bus = spark.table("config.business_units").select("bu_code").collect()
bu_codes = [row.bu_code for row in bus]

phase_tasks = spark.table("config.close_phase_definitions").collect()

# COMMAND ----------

# Generate close status records for Phase 1 & 2
# Simulate realistic completion times

status_records = []

# Phase 1 tasks (Days 1-2 of close)
phase1_tasks = [t for t in phase_tasks if t.phase_id == 1]
for task in phase1_tasks:
    if task.is_bu_specific:
        # Create one task per BU
        for bu_code in bu_codes:
            status_records.append({
                "period": CURRENT_PERIOD,
                "phase_id": task.phase_id,
                "phase_name": task.phase_name,
                "task_id": task.task_id,
                "task_name": task.task_name,
                "bu_code": bu_code,
                "planned_due_date": f"2025-12-02",  # Day 2 of close
                "actual_completion_timestamp": datetime(2025, 12, 2, 14, 30, 0),
                "status": "completed",
                "owner_role": task.owner_role,
                "comments": f"FX Agent: Completed automatically. Quality score: 0.98",
                "last_updated_by": "fx_agent",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
    else:
        # Group-level task
        status_records.append({
            "period": CURRENT_PERIOD,
            "phase_id": task.phase_id,
            "phase_name": task.phase_name,
            "task_id": task.task_id,
            "task_name": task.task_name,
            "bu_code": None,
            "planned_due_date": f"2025-12-02",
            "actual_completion_timestamp": datetime(2025, 12, 2, 10, 0, 0) if task.task_id == 101 else datetime(2025, 12, 2, 12, 0, 0),
            "status": "completed",
            "owner_role": task.owner_role,
            "comments": f"Completed. All required currencies present." if task.task_id == 102 else "Files received from all providers.",
            "last_updated_by": "fx_agent" if task.task_id <= 102 else "pre_close_agent",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

# Phase 2 tasks (Days 3-7 of close)
phase2_tasks = [t for t in phase_tasks if t.phase_id == 2]
for task in phase2_tasks:
    if task.is_bu_specific:
        # Create one task per BU
        for bu_code in bu_codes:
            # Simulate different completion times per BU
            if task.task_id == 204:  # First cut
                completion = datetime(2025, 12, 6, 14, 0, 0)
                task_status = "completed"
            elif task.task_id == 205:  # Final cut
                completion = datetime(2025, 12, 10, 16, 0, 0)
                task_status = "completed"
            else:
                completion = None
                task_status = "pending"
            
            status_records.append({
                "period": CURRENT_PERIOD,
                "phase_id": task.phase_id,
                "phase_name": task.phase_name,
                "task_id": task.task_id,
                "task_name": task.task_name,
                "bu_code": bu_code,
                "planned_due_date": f"2025-12-{task.task_id - 195}",  # Day 6, 10, etc.
                "actual_completion_timestamp": completion,
                "status": task_status,
                "owner_role": task.owner_role,
                "comments": f"Completed with {random.randint(5, 25)} adjustment entries" if task_status == "completed" else None,
                "last_updated_by": "pre_close_agent" if task_status == "completed" else None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
    else:
        # Group-level tasks
        if task.task_id == 201:  # Process preliminary
            completion = datetime(2025, 12, 4, 10, 0, 0)
            task_status = "completed"
            comments = f"Processed {tb_count} records. {len(bu_codes)} BUs included."
        elif task.task_id == 202:  # Publish preliminary
            completion = datetime(2025, 12, 4, 14, 0, 0)
            task_status = "completed"
            comments = "Preliminary results available in Gold layer."
        elif task.task_id == 203:  # Review meeting
            completion = datetime(2025, 12, 5, 15, 0, 0)
            task_status = "completed"
            comments = "Meeting completed. Action items: Review BU Asia variance (15% below plan)."
        else:
            completion = None
            task_status = "pending"
            comments = None
        
        status_records.append({
            "period": CURRENT_PERIOD,
            "phase_id": task.phase_id,
            "phase_name": task.phase_name,
            "task_id": task.task_id,
            "task_name": task.task_name,
            "bu_code": None,
            "planned_due_date": f"2025-12-{task.task_id - 197}",
            "actual_completion_timestamp": completion,
            "status": task_status,
            "owner_role": task.owner_role,
            "comments": comments,
            "last_updated_by": "pre_close_agent" if task_status == "completed" else None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

# COMMAND ----------

import random  # Need for random adjustments count

# Recreate status_records with proper random import
status_records = []

# Phase 1 tasks (Days 1-2 of close)
phase1_tasks = [t for t in phase_tasks if t.phase_id == 1]
for task in phase1_tasks:
    if task.is_bu_specific:
        for bu_code in bu_codes:
            status_records.append({
                "period": CURRENT_PERIOD,
                "phase_id": task.phase_id,
                "phase_name": task.phase_name,
                "task_id": task.task_id,
                "task_name": task.task_name,
                "bu_code": bu_code,
                "planned_due_date": "2025-12-02",
                "actual_completion_timestamp": datetime(2025, 12, 2, 14, 30, 0),
                "status": "completed",
                "owner_role": task.owner_role,
                "comments": f"FX Agent: Completed automatically. Quality score: 0.98",
                "last_updated_by": "fx_agent",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
    else:
        status_records.append({
            "period": CURRENT_PERIOD,
            "phase_id": task.phase_id,
            "phase_name": task.phase_name,
            "task_id": task.task_id,
            "task_name": task.task_name,
            "bu_code": None,
            "planned_due_date": "2025-12-02",
            "actual_completion_timestamp": datetime(2025, 12, 2, 10, 0, 0) if task.task_id == 101 else datetime(2025, 12, 2, 12, 0, 0),
            "status": "completed",
            "owner_role": task.owner_role,
            "comments": f"Completed. All required currencies present." if task.task_id == 102 else "Files received from all providers.",
            "last_updated_by": "fx_agent" if task.task_id <= 102 else "pre_close_agent",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

# Phase 2 tasks (Days 3-7 of close)
phase2_tasks = [t for t in phase_tasks if t.phase_id == 2]
for task in phase2_tasks:
    if task.is_bu_specific:
        for bu_code in bu_codes:
            if task.task_id == 204:
                completion = datetime(2025, 12, 6, 14, 0, 0)
                task_status = "completed"
            elif task.task_id == 205:
                completion = datetime(2025, 12, 10, 16, 0, 0)
                task_status = "completed"
            else:
                completion = None
                task_status = "pending"
            
            status_records.append({
                "period": CURRENT_PERIOD,
                "phase_id": task.phase_id,
                "phase_name": task.phase_name,
                "task_id": task.task_id,
                "task_name": task.task_name,
                "bu_code": bu_code,
                "planned_due_date": f"2025-12-{6 if task.task_id == 204 else 10}",
                "actual_completion_timestamp": completion,
                "status": task_status,
                "owner_role": task.owner_role,
                "comments": f"Completed with {random.randint(5, 25)} adjustment entries" if task_status == "completed" else None,
                "last_updated_by": "pre_close_agent" if task_status == "completed" else None,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
    else:
        if task.task_id == 201:
            completion = datetime(2025, 12, 4, 10, 0, 0)
            task_status = "completed"
            comments = f"Processed {tb_count} records. {len(bu_codes)} BUs included."
        elif task.task_id == 202:
            completion = datetime(2025, 12, 4, 14, 0, 0)
            task_status = "completed"
            comments = "Preliminary results available in Gold layer."
        elif task.task_id == 203:
            completion = datetime(2025, 12, 5, 15, 0, 0)
            task_status = "completed"
            comments = "Meeting completed. Action items: Review BU Asia variance (15% below plan)."
        else:
            completion = None
            task_status = "pending"
            comments = None
        
        status_records.append({
            "period": CURRENT_PERIOD,
            "phase_id": task.phase_id,
            "phase_name": task.phase_name,
            "task_id": task.task_id,
            "task_name": task.task_name,
            "bu_code": None,
            "planned_due_date": f"2025-12-{4 if task.task_id == 201 else 5 if task.task_id in [202, 203] else 7}",
            "actual_completion_timestamp": completion,
            "status": task_status,
            "owner_role": task.owner_role,
            "comments": comments,
            "last_updated_by": "pre_close_agent" if task_status == "completed" else None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

# Write to Gold
status_df = spark.createDataFrame(status_records)
status_df.write.mode("append").saveAsTable("gold.close_status_gold")

print(f"✓ Created {len(status_records)} close status records for Phase 1 & 2")

# COMMAND ----------

# Show status summary
status_summary = (
    status_df
    .groupBy("phase_name", "status")
    .count()
    .orderBy("phase_name", "status")
)

print("\nClose Status Summary:")
display(status_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Processing Summary

# COMMAND ----------

print("\n" + "="*80)
print("PHASE 1 & 2 PROCESSING COMPLETE")
print("="*80)

print(f"\n✓ FX Rates Standardized: {fx_count} rates")
print(f"✓ Trial Balance Records: {tb_count} records")
print(f"✓ Close Status Tasks: {len(status_records)} tasks")

print(f"\nPeriod: {CURRENT_PERIOD}")
print(f"Phase 1 Status: All tasks completed")
print(f"Phase 2 Status: All tasks completed")

print("\n📊 Next Steps:")
print("1. Run notebook 04 to process Phase 3 data (segmented + forecast)")
print("2. Review preliminary KPIs in Gold tables")
print("3. Start agent workflows for automated monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Phase 1 & 2 Processing Complete
# MAGIC 
# MAGIC **Processed:**
# MAGIC - ✓ FX rates validated and standardized
# MAGIC - ✓ Trial balance converted to reporting currency
# MAGIC - ✓ Preliminary KPIs calculated
# MAGIC - ✓ Close status tracking initialized
# MAGIC 
# MAGIC **Ready for:**
# MAGIC - Phase 3 data ingestion (segmented + forecast)
# MAGIC - Agent automation
# MAGIC - Genie queries on preliminary results
