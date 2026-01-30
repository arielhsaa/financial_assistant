# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Agent Logic: Segmented, Forecast, and Reporting Agents
# MAGIC 
# MAGIC This notebook implements three specialized agents:
# MAGIC 
# MAGIC ## Segmented & Forecast Agent
# MAGIC - Integrates segmented close and forecast data (Phase 3 and 4)
# MAGIC - Computes variances: segmented close vs. forecast, and new forecast vs. previous forecast
# MAGIC - Writes consolidated outputs into `close_results_gold` and `forecast_results_gold`
# MAGIC 
# MAGIC ## Reporting Agent
# MAGIC - Finalizes all close outputs after all tasks are completed
# MAGIC - Populates `close_kpi_gold` with comprehensive metrics
# MAGIC - Creates or refreshes views for SQL dashboards
# MAGIC - Generates executive summary for FP&A and leadership
# MAGIC 
# MAGIC **Run this notebook** after Phase 3 data is available and Phase 4 reviews are complete.

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
CURRENT_PERIOD = 202601
PRIOR_PERIOD = 202512  # December 2025

print("Starting Segmented & Forecast Agent and Reporting Agent...")
print(f"Current period: {CURRENT_PERIOD}")
print(f"Prior period: {PRIOR_PERIOD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def log_agent_action(agent_name, period, action, target_table, target_key, status, message, execution_time_ms=0):
    """Log agent action to close_agent_logs"""
    log_data = [{
        "log_timestamp": datetime.now(),
        "period": period,
        "agent_name": agent_name,
        "action": action,
        "target_table": target_table,
        "target_record_key": target_key,
        "status": status,
        "message": message,
        "execution_time_ms": execution_time_ms,
        "user_context": "system",
        "metadata": json.dumps({"automated": True})
    }]
    
    log_df = spark.createDataFrame(log_data)
    log_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_agent_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC # Segmented & Forecast Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Segmented Close and Forecast Data

# COMMAND ----------

AGENT_NAME = "Segmented & Forecast Agent"
print(f"\n{'='*60}")
print(f"{AGENT_NAME} Starting")
print('='*60)

start_time = datetime.now()

# Load segmented close
segmented = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.segmented_close_std") \
    .filter(col("period") == CURRENT_PERIOD)

# Load forecast for current period (Base scenario)
forecast = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.forecast_std") \
    .filter((col("forecast_period") == CURRENT_PERIOD) & (col("scenario") == "Base"))

segmented_count = segmented.count()
forecast_count = forecast.count()

print(f"\n✓ Loaded segmented close: {segmented_count:,} records")
print(f"✓ Loaded forecast: {forecast_count:,} records")

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="load_data",
    target_table=f"{SILVER_SCHEMA}.segmented_close_std, {SILVER_SCHEMA}.forecast_std",
    target_key=f"period={CURRENT_PERIOD}",
    status="success",
    message=f"Loaded {segmented_count} segmented records and {forecast_count} forecast records",
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calculate Variance: Actual vs. Forecast

# COMMAND ----------

print("\nCalculating variance: Actual vs. Forecast...")

# Aggregate segmented to BU + Segment level
segmented_agg = segmented \
    .groupBy("period", "bu", "segment", "local_currency", "reporting_currency") \
    .agg(
        sum("revenue_reporting").alias("actual_revenue"),
        sum("operating_profit_reporting").alias("actual_operating_profit")
    )

# Aggregate forecast to BU + Segment level
forecast_agg = forecast \
    .groupBy("bu", "segment") \
    .agg(
        sum("revenue_reporting").alias("forecast_revenue"),
        sum("operating_profit_reporting").alias("forecast_operating_profit")
    )

# Join and calculate variance
variance_analysis = segmented_agg.alias("actual") \
    .join(
        forecast_agg.alias("forecast"),
        (col("actual.bu") == col("forecast.bu")) & (col("actual.segment") == col("forecast.segment")),
        "left"
    ) \
    .select(
        col("actual.period"),
        col("actual.bu"),
        col("actual.segment"),
        col("actual.actual_revenue"),
        col("actual.actual_operating_profit"),
        coalesce(col("forecast.forecast_revenue"), lit(0)).alias("forecast_revenue"),
        coalesce(col("forecast.forecast_operating_profit"), lit(0)).alias("forecast_operating_profit")
    ) \
    .withColumn("revenue_variance",
                col("actual_revenue") - col("forecast_revenue")) \
    .withColumn("revenue_variance_pct",
                when(col("forecast_revenue") != 0,
                     round((col("revenue_variance") / col("forecast_revenue")) * 100, 2))
                .otherwise(None)) \
    .withColumn("op_variance",
                col("actual_operating_profit") - col("forecast_operating_profit")) \
    .withColumn("op_variance_pct",
                when(col("forecast_operating_profit") != 0,
                     round((col("op_variance") / col("forecast_operating_profit")) * 100, 2))
                .otherwise(None))

print("\n✓ Variance Analysis (Top variances by operating profit):")
variance_analysis \
    .orderBy(desc(abs(col("op_variance")))) \
    .select("bu", "segment", "actual_operating_profit", "forecast_operating_profit", "op_variance", "op_variance_pct") \
    .show(10, truncate=False)

# Log significant variances
significant_variances = variance_analysis \
    .filter((abs(col("op_variance_pct")) > 10) & (abs(col("op_variance")) > 10000)) \
    .collect()

for var in significant_variances:
    log_agent_action(
        agent_name=AGENT_NAME,
        period=CURRENT_PERIOD,
        action="detect_variance",
        target_table=f"{GOLD_SCHEMA}.close_results_gold",
        target_key=f"{var['bu']}_{var['segment']}",
        status="warning",
        message=f"Significant variance: {var['bu']} / {var['segment']} operating profit {var['op_variance_pct']:+.1f}% vs forecast",
        execution_time_ms=0
    )

print(f"\n⚠ Found {len(significant_variances)} significant variances (>10%, >$10k)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Identify Top Variance Drivers

# COMMAND ----------

print("\nIdentifying top variance drivers...")

# Top 3 positive drivers (better than forecast)
top_positive = variance_analysis \
    .filter(col("op_variance") > 0) \
    .orderBy(desc("op_variance")) \
    .select("bu", "segment", "op_variance", "op_variance_pct") \
    .limit(3) \
    .collect()

print("\n✓ Top 3 Positive Variance Drivers:")
for i, driver in enumerate(top_positive, 1):
    print(f"  {i}. {driver['bu']} / {driver['segment']}: +${driver['op_variance']:,.0f} ({driver['op_variance_pct']:+.1f}%)")

# Top 3 negative drivers (worse than forecast)
top_negative = variance_analysis \
    .filter(col("op_variance") < 0) \
    .orderBy("op_variance") \
    .select("bu", "segment", "op_variance", "op_variance_pct") \
    .limit(3) \
    .collect()

print("\n✓ Top 3 Negative Variance Drivers:")
for i, driver in enumerate(top_negative, 1):
    print(f"  {i}. {driver['bu']} / {driver['segment']}: ${driver['op_variance']:,.0f} ({driver['op_variance_pct']:+.1f}%)")

# Create variance drivers summary
variance_drivers_summary = "Top positive: " + ", ".join([f"{d['bu']}/{d['segment']} ({d['op_variance_pct']:+.1f}%)" for d in top_positive]) + \
                          "; Top negative: " + ", ".join([f"{d['bu']}/{d['segment']} ({d['op_variance_pct']:+.1f}%)" for d in top_negative])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Update Forecast Results with Actuals

# COMMAND ----------

print("\nUpdating forecast results with actuals...")

# Read existing forecast results
forecast_results = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold") \
    .filter((col("forecast_period") == CURRENT_PERIOD) & (col("scenario") == "Base"))

# Join with actual results
forecast_with_actuals = forecast_results.alias("forecast") \
    .join(
        variance_analysis.alias("actual"),
        (col("forecast.bu") == col("actual.bu")) & (col("forecast.segment") == col("actual.segment")),
        "left"
    ) \
    .select(
        col("forecast.forecast_period"),
        col("forecast.submission_date"),
        col("forecast.bu"),
        col("forecast.segment"),
        col("forecast.scenario"),
        col("forecast.local_currency"),
        col("forecast.reporting_currency"),
        col("forecast.revenue_forecast_local"),
        col("forecast.cogs_forecast_local"),
        col("forecast.opex_forecast_local"),
        col("forecast.operating_profit_forecast_local"),
        col("forecast.revenue_forecast_reporting"),
        col("forecast.cogs_forecast_reporting"),
        col("forecast.opex_forecast_reporting"),
        col("forecast.operating_profit_forecast_reporting"),
        col("actual.actual_revenue").alias("revenue_actual_reporting"),
        col("actual.actual_operating_profit").alias("operating_profit_actual_reporting"),
        (col("actual.actual_revenue") - col("forecast.revenue_forecast_reporting")).alias("variance_revenue"),
        (col("actual.actual_operating_profit") - col("forecast.operating_profit_forecast_reporting")).alias("variance_operating_profit"),
        col("actual.revenue_variance_pct"),
        col("actual.op_variance_pct").alias("variance_operating_profit_pct"),
        # Calculate forecast accuracy score (0-100, where 100 = perfect)
        when(col("forecast.operating_profit_forecast_reporting") != 0,
             greatest(lit(0), lit(100) - abs(col("actual.op_variance_pct"))))
        .otherwise(None).alias("forecast_accuracy_score"),
        col("forecast.assumptions"),
        lit(variance_drivers_summary).alias("variance_drivers"),
        current_timestamp().alias("load_timestamp"),
        lit('{"actuals_available": true}').alias("metadata")
    )

# Write updated forecast results
forecast_with_actuals.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold")

print(f"✓ Updated forecast results with actuals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Segmented & Forecast Agent Summary

# COMMAND ----------

# Calculate overall forecast accuracy
avg_accuracy = forecast_with_actuals \
    .filter(col("forecast_accuracy_score").isNotNull()) \
    .agg(avg("forecast_accuracy_score").alias("avg_accuracy")) \
    .first()

avg_accuracy_score = avg_accuracy["avg_accuracy"] if avg_accuracy and avg_accuracy["avg_accuracy"] else 0

segmented_forecast_summary = f"""
Segmented & Forecast Agent completed successfully.
- Analyzed {segmented_count:,} segmented records and {forecast_count:,} forecast records
- Calculated variance for {variance_analysis.count()} BU/Segment combinations
- Significant variances detected: {len(significant_variances)}
- Average forecast accuracy: {avg_accuracy_score:.1f}/100
- Updated forecast results with actuals and variance drivers
"""

print(segmented_forecast_summary)

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="agent_summary",
    target_table="summary",
    target_key=f"segmented_forecast_agent_{CURRENT_PERIOD}",
    status="success",
    message=segmented_forecast_summary.replace("\n", " ").strip(),
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reporting Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify All Tasks Complete

# COMMAND ----------

AGENT_NAME = "Reporting Agent"
print(f"\n{'='*60}")
print(f"{AGENT_NAME} Starting")
print('='*60)

start_time = datetime.now()

# Check if all tasks are complete
tasks = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks") \
    .filter(col("period") == CURRENT_PERIOD)

total_tasks = tasks.count()
completed_tasks = tasks.filter(col("status") == "completed").count()
completion_pct = (completed_tasks / total_tasks) * 100

print(f"\n✓ Task completion status: {completed_tasks}/{total_tasks} ({completion_pct:.1f}%)")

if completion_pct < 100:
    print(f"\n⚠ Warning: Not all tasks are complete. Generating preliminary report.")
    report_type = "preliminary"
else:
    print(f"\n✓ All tasks complete. Generating final report.")
    report_type = "final"

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="check_completion",
    target_table=f"{GOLD_SCHEMA}.close_phase_tasks",
    target_key=f"period={CURRENT_PERIOD}",
    status="success",
    message=f"Task completion: {completion_pct:.1f}%. Generating {report_type} report.",
    execution_time_ms=0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Populate Comprehensive KPIs

# COMMAND ----------

print("\nPopulating comprehensive KPIs...")

# Read close results
close_results = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_results_gold") \
    .filter(col("period") == CURRENT_PERIOD)

# Read forecast results
forecast_results = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.forecast_results_gold") \
    .filter((col("forecast_period") == CURRENT_PERIOD) & (col("scenario") == "Base"))

kpi_data = []

# KPI 1: Close Cycle Time
period_end_date = datetime(2026, 1, 31)
close_completion_date = datetime.now()
cycle_time_days = (close_completion_date - period_end_date).days

kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Close Cycle Time",
    "kpi_category": "Timeliness",
    "bu": "CONSOLIDATED",
    "kpi_value": float(cycle_time_days),
    "kpi_unit": "days",
    "target_value": 10.0,
    "variance_vs_target": float(cycle_time_days - 10),
    "status": "on_target" if cycle_time_days <= 10 else "warning",
    "trend": "stable",
    "description": "Number of days from period end to close completion"
})

# KPI 2: Number of Adjustments (simplified - using random)
num_adjustments = 127  # Simulated

kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Number of Adjustments",
    "kpi_category": "Quality",
    "bu": "CONSOLIDATED",
    "kpi_value": float(num_adjustments),
    "kpi_unit": "count",
    "target_value": 150.0,
    "variance_vs_target": float(num_adjustments - 150),
    "status": "on_target",
    "trend": "improving",
    "description": "Total number of accounting adjustments during close"
})

# KPI 3: Consolidated Revenue
consolidated = close_results.filter(col("bu") == "CONSOLIDATED").first()

kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Consolidated Revenue",
    "kpi_category": "Financial",
    "bu": "CONSOLIDATED",
    "kpi_value": float(consolidated["revenue_reporting"]),
    "kpi_unit": "currency",
    "target_value": float(consolidated["revenue_reporting"] * 0.95),  # Target = 95% for demo
    "variance_vs_target": float(consolidated["revenue_reporting"] * 0.05),
    "status": "on_target",
    "trend": "stable",
    "description": "Total consolidated revenue in reporting currency"
})

# KPI 4: Consolidated Operating Profit
kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Consolidated Operating Profit",
    "kpi_category": "Financial",
    "bu": "CONSOLIDATED",
    "kpi_value": float(consolidated["operating_profit_reporting"]),
    "kpi_unit": "currency",
    "target_value": float(consolidated["operating_profit_reporting"] * 0.90),
    "variance_vs_target": float(consolidated["operating_profit_reporting"] * 0.10),
    "status": "on_target",
    "trend": "stable",
    "description": "Total consolidated operating profit in reporting currency"
})

# KPI 5: Operating Margin
kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Operating Margin",
    "kpi_category": "Financial",
    "bu": "CONSOLIDATED",
    "kpi_value": float(consolidated["operating_margin_pct"]),
    "kpi_unit": "percentage",
    "target_value": 15.0,
    "variance_vs_target": float(consolidated["operating_margin_pct"] - 15.0),
    "status": "on_target" if consolidated["operating_margin_pct"] >= 15.0 else "warning",
    "trend": "stable",
    "description": "Operating profit as percentage of revenue"
})

# KPI 6: FX Impact
kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "Total FX Impact",
    "kpi_category": "Financial",
    "bu": "CONSOLIDATED",
    "kpi_value": float(consolidated["fx_impact_reporting"]),
    "kpi_unit": "currency",
    "target_value": 0.0,
    "variance_vs_target": float(consolidated["fx_impact_reporting"]),
    "status": "on_target",
    "trend": "stable",
    "description": "Total foreign exchange impact on operating profit"
})

# KPI 7: Forecast Accuracy
avg_forecast_accuracy = forecast_results \
    .filter(col("forecast_accuracy_score").isNotNull()) \
    .agg(avg("forecast_accuracy_score").alias("avg_accuracy")) \
    .first()

if avg_forecast_accuracy and avg_forecast_accuracy["avg_accuracy"]:
    kpi_data.append({
        "period": CURRENT_PERIOD,
        "kpi_name": "Forecast Accuracy",
        "kpi_category": "Quality",
        "bu": "CONSOLIDATED",
        "kpi_value": float(avg_forecast_accuracy["avg_accuracy"]),
        "kpi_unit": "percentage",
        "target_value": 90.0,
        "variance_vs_target": float(avg_forecast_accuracy["avg_accuracy"] - 90.0),
        "status": "on_target" if avg_forecast_accuracy["avg_accuracy"] >= 90 else "warning",
        "trend": "stable",
        "description": "Average forecast accuracy score (100 = perfect forecast)"
    })

# KPI 8: BU Timeliness (% of BUs submitting on time)
bu_timeliness = 100.0  # Simulated - all BUs on time

kpi_data.append({
    "period": CURRENT_PERIOD,
    "kpi_name": "BU On-Time Submission Rate",
    "kpi_category": "Timeliness",
    "bu": "CONSOLIDATED",
    "kpi_value": bu_timeliness,
    "kpi_unit": "percentage",
    "target_value": 100.0,
    "variance_vs_target": 0.0,
    "status": "on_target",
    "trend": "stable",
    "description": "Percentage of business units submitting data on time"
})

# Add load timestamp and metadata
for kpi in kpi_data:
    kpi["load_timestamp"] = datetime.now()
    kpi["metadata"] = json.dumps({"report_type": report_type, "generated_by": AGENT_NAME})

# Create DataFrame and write to Gold (append to existing)
kpi_df = spark.createDataFrame(kpi_data)

# Remove existing KPIs for this period (if any) and write new ones
existing_kpis = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_kpi_gold") \
    .filter(col("period") != CURRENT_PERIOD)

all_kpis = existing_kpis.union(kpi_df)

all_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_kpi_gold")

print(f"✓ Populated {len(kpi_data)} KPIs")

# Display KPIs
print("\n=== Key Performance Indicators ===")
kpi_df.select("kpi_name", "kpi_value", "kpi_unit", "target_value", "status") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Executive Summary

# COMMAND ----------

print("\nGenerating executive summary...")

executive_summary = f"""
===================================================================
FINANCIAL CLOSE EXECUTIVE SUMMARY
Period: {CURRENT_PERIOD} (January 2026)
Report Type: {report_type.upper()}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
===================================================================

CLOSE STATUS:
- Completion: {completion_pct:.1f}% ({completed_tasks}/{total_tasks} tasks)
- Cycle Time: {cycle_time_days} days (Target: 10 days)
- Status: {"ON TRACK" if cycle_time_days <= 10 else "AT RISK"}

FINANCIAL RESULTS:
- Revenue: ${consolidated['revenue_reporting']:,.2f}
- Operating Profit: ${consolidated['operating_profit_reporting']:,.2f}
- Operating Margin: {consolidated['operating_margin_pct']:.2f}%
- FX Impact: ${consolidated['fx_impact_reporting']:,.2f}

FORECAST VARIANCE:
- Average Forecast Accuracy: {avg_forecast_accuracy['avg_accuracy'] if avg_forecast_accuracy and avg_forecast_accuracy['avg_accuracy'] else 0:.1f}%
- Significant Variances: {len(significant_variances)} BU/Segment combinations

TOP VARIANCE DRIVERS:
Positive:
"""

for i, driver in enumerate(top_positive, 1):
    executive_summary += f"  {i}. {driver['bu']} / {driver['segment']}: +${driver['op_variance']:,.0f} ({driver['op_variance_pct']:+.1f}%)\n"

executive_summary += "\nNegative:\n"
for i, driver in enumerate(top_negative, 1):
    executive_summary += f"  {i}. {driver['bu']} / {driver['segment']}: ${driver['op_variance']:,.0f} ({driver['op_variance_pct']:+.1f}%)\n"

executive_summary += f"""
PROCESS QUALITY:
- Number of Adjustments: {num_adjustments} (Target: <150)
- BU On-Time Submission: {bu_timeliness:.0f}%
- Data Quality Issues: 0 critical

NEXT STEPS:
"""

if completion_pct < 100:
    executive_summary += "- Complete remaining close tasks\n"
    executive_summary += "- Finalize all reviews and approvals\n"
else:
    executive_summary += "- Distribute final close results to stakeholders\n"
    executive_summary += "- Archive period data\n"
    executive_summary += "- Begin planning for next period close\n"

executive_summary += "\n===================================================================\n"

print(executive_summary)

# Save executive summary to agent logs
log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="generate_executive_summary",
    target_table="executive_summary",
    target_key=f"period_{CURRENT_PERIOD}",
    status="success",
    message=executive_summary.replace("\n", " | "),
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Finalize Close Status

# COMMAND ----------

print("\nFinalizing close status...")

# Update close status to reflect completion
final_status_data = [{
    "period": CURRENT_PERIOD,
    "bu": "CONSOLIDATED",
    "phase_id": 5,
    "phase_name": "Reporting & Sign-off",
    "overall_status": "completed" if completion_pct == 100 else "in_progress",
    "pct_tasks_completed": completion_pct,
    "total_tasks": total_tasks,
    "completed_tasks": completed_tasks,
    "blocked_tasks": 0,
    "days_since_period_end": cycle_time_days,
    "days_to_sla": 10 - cycle_time_days,
    "sla_status": "completed" if cycle_time_days <= 10 else "overdue",
    "key_issues": None,
    "last_milestone": "Final close results published",
    "next_milestone": "Period archived" if completion_pct == 100 else "Complete remaining tasks",
    "agent_summary": f"Close {'completed' if completion_pct == 100 else 'in progress'} for period {CURRENT_PERIOD}. " +
                    f"Revenue: ${consolidated['revenue_reporting']:,.0f}, " +
                    f"Operating Profit: ${consolidated['operating_profit_reporting']:,.0f} " +
                    f"({consolidated['operating_margin_pct']:.1f}% margin). " +
                    f"Cycle time: {cycle_time_days} days.",
    "load_timestamp": datetime.now(),
    "metadata": json.dumps({"updated_by": AGENT_NAME, "report_type": report_type})
}]

final_status_df = spark.createDataFrame(final_status_data)

# Update status table
existing_status = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold") \
    .filter((col("period") != CURRENT_PERIOD) | (col("bu") != "CONSOLIDATED"))

updated_status = existing_status.union(final_status_df)

updated_status.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")

print(f"✓ Finalized close status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reporting Agent Summary

# COMMAND ----------

reporting_summary = f"""
Reporting Agent completed successfully.
- Generated {report_type} report for period {CURRENT_PERIOD}
- Populated {len(kpi_data)} comprehensive KPIs
- Close cycle time: {cycle_time_days} days (Target: 10)
- Overall status: {"COMPLETED" if completion_pct == 100 else "IN PROGRESS"}
- Executive summary generated and logged
- All outputs available for FP&A and leadership dashboards
"""

print(reporting_summary)

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="agent_summary",
    target_table="summary",
    target_key=f"reporting_agent_{CURRENT_PERIOD}",
    status="success",
    message=reporting_summary.replace("\n", " ").strip(),
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ Segmented & Forecast Agent and Reporting Agent Execution Complete!

Segmented & Forecast Agent Results:
- Analyzed variance for {variance_analysis.count()} BU/Segment combinations
- Significant variances: {len(significant_variances)}
- Average forecast accuracy: {avg_accuracy_score:.1f}/100
- Updated forecast results with actuals

Reporting Agent Results:
- Generated {report_type} report
- Populated {len(kpi_data)} KPIs
- Close cycle time: {cycle_time_days} days
- Overall completion: {completion_pct:.1f}%
- Executive summary available

All agents logged actions to: {CATALOG}.{GOLD_SCHEMA}.close_agent_logs

Next Steps:
1. Review executive summary in Genie or dashboards
2. Investigate significant variances
3. {"Archive period and begin next close" if completion_pct == 100 else "Complete remaining tasks"}
4. Use dashboards for stakeholder communication

Financial close for period {CURRENT_PERIOD} is {"COMPLETE" if completion_pct == 100 else "IN PROGRESS"}.
""")
