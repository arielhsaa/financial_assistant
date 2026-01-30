# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Agent Logic: FX and Pre-Close Agents
# MAGIC 
# MAGIC This notebook implements two specialized agents:
# MAGIC 
# MAGIC ## FX Agent
# MAGIC - Watches for new FX files in Bronze
# MAGIC - Triggers normalization logic to update `fx_rates_std`
# MAGIC - Validates coverage (all required currencies/dates)
# MAGIC - Flags issues as tasks for humans
# MAGIC 
# MAGIC ## Pre-Close Agent
# MAGIC - Ingests BU preliminary close data
# MAGIC - Computes high-level KPIs: total revenue, operating profit, FX impact, unusual variances
# MAGIC - Updates `close_status_gold` with completion and summary comments
# MAGIC 
# MAGIC **Run this notebook** after receiving FX rates or BU preliminary close submissions.

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

print("Starting FX Agent and Pre-Close Agent...")
print(f"Processing period: {CURRENT_PERIOD}")

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

def get_period_dates(period):
    """Get start and end dates for a given period"""
    year = period // 100
    month = period % 100
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    return start_date, end_date

# COMMAND ----------

# MAGIC %md
# MAGIC # FX Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check for New FX Data

# COMMAND ----------

AGENT_NAME = "FX Agent"
print(f"\n{'='*60}")
print(f"{AGENT_NAME} Starting")
print('='*60)

start_time = datetime.now()

# Get period date range
period_start, period_end = get_period_dates(CURRENT_PERIOD)

# Check for new FX data
fx_raw = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw") \
    .filter((col("rate_date") >= period_start) & (col("rate_date") <= period_end))

fx_count = fx_raw.count()
unique_dates = fx_raw.select("rate_date").distinct().count()
unique_currencies = fx_raw.select("from_currency").distinct().count()

print(f"\n✓ Found {fx_count:,} FX rate records")
print(f"  - Unique dates: {unique_dates}")
print(f"  - Unique currencies: {unique_currencies}")

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="check_fx_data",
    target_table=f"{BRONZE_SCHEMA}.fx_rates_raw",
    target_key=f"period={CURRENT_PERIOD}",
    status="success",
    message=f"Found {fx_count} FX records for {unique_currencies} currencies across {unique_dates} dates",
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate FX Coverage

# COMMAND ----------

print("\nValidating FX coverage...")

# Required currencies
REQUIRED_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "BRL", "INR", "AED"]

# Calculate expected business days in period
from datetime import timedelta
expected_days = []
current_date = period_start
while current_date <= period_end:
    if current_date.weekday() < 5:  # Monday-Friday
        expected_days.append(current_date)
    current_date += timedelta(days=1)

expected_business_days = len(expected_days)

print(f"  Expected business days: {expected_business_days}")
print(f"  Required currencies: {len(REQUIRED_CURRENCIES)}")

# Check coverage by currency
coverage_issues = []

for currency in REQUIRED_CURRENCIES:
    if currency == "USD":
        continue  # USD is the base currency
    
    currency_dates = fx_raw \
        .filter((col("from_currency") == currency) & (col("to_currency") == "USD")) \
        .select("rate_date") \
        .distinct() \
        .count()
    
    coverage_pct = (currency_dates / expected_business_days) * 100
    
    print(f"  - {currency}: {currency_dates}/{expected_business_days} days ({coverage_pct:.1f}%)")
    
    if coverage_pct < 90:  # Flag if less than 90% coverage
        issue = f"{currency} has only {coverage_pct:.1f}% date coverage"
        coverage_issues.append(issue)
        
        log_agent_action(
            agent_name=AGENT_NAME,
            period=CURRENT_PERIOD,
            action="validate_fx_coverage",
            target_table=f"{BRONZE_SCHEMA}.fx_rates_raw",
            target_key=f"currency={currency}",
            status="warning",
            message=issue,
            execution_time_ms=0
        )

if len(coverage_issues) == 0:
    print("\n✓ FX coverage validation passed")
else:
    print(f"\n⚠ Found {len(coverage_issues)} coverage issues:")
    for issue in coverage_issues:
        print(f"  - {issue}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Detect FX Anomalies

# COMMAND ----------

print("\nDetecting FX anomalies...")

# Calculate day-over-day rate changes
fx_window = Window.partitionBy("from_currency", "to_currency").orderBy("rate_date")

fx_with_changes = fx_raw \
    .withColumn("prev_rate", lag("exchange_rate").over(fx_window)) \
    .withColumn("rate_change_pct",
                when(col("prev_rate").isNotNull(),
                     ((col("exchange_rate") - col("prev_rate")) / col("prev_rate") * 100))
                .otherwise(0))

# Flag anomalies (>5% daily change)
anomalies = fx_with_changes \
    .filter(abs(col("rate_change_pct")) > 5.0) \
    .select("rate_date", "from_currency", "to_currency", "exchange_rate", "prev_rate", "rate_change_pct") \
    .collect()

if len(anomalies) > 0:
    print(f"\n⚠ Found {len(anomalies)} FX anomalies (>5% daily change):")
    for anom in anomalies:
        print(f"  - {anom['from_currency']}/{anom['to_currency']} on {anom['rate_date']}: {anom['rate_change_pct']:+.2f}%")
        
        log_agent_action(
            agent_name=AGENT_NAME,
            period=CURRENT_PERIOD,
            action="detect_fx_anomaly",
            target_table=f"{BRONZE_SCHEMA}.fx_rates_raw",
            target_key=f"{anom['from_currency']}_{anom['rate_date']}",
            status="warning",
            message=f"Unusual FX movement: {anom['from_currency']}/{anom['to_currency']} changed {anom['rate_change_pct']:+.2f}% on {anom['rate_date']}",
            execution_time_ms=0
        )
else:
    print("  ✓ No significant FX anomalies detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. FX Agent Summary

# COMMAND ----------

fx_summary = f"""
FX Agent completed successfully.
- Processed {fx_count:,} FX records
- Coverage: {unique_currencies} currencies, {unique_dates} dates
- Issues: {len(coverage_issues)} coverage gaps, {len(anomalies)} anomalies
- All validations {"passed" if len(coverage_issues) == 0 else "completed with warnings"}
"""

print(fx_summary)

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="agent_summary",
    target_table="summary",
    target_key=f"fx_agent_{CURRENT_PERIOD}",
    status="success" if len(coverage_issues) == 0 else "warning",
    message=fx_summary.replace("\n", " ").strip(),
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-Close Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load BU Preliminary Close Data

# COMMAND ----------

AGENT_NAME = "PreClose Agent"
print(f"\n{'='*60}")
print(f"{AGENT_NAME} Starting")
print('='*60)

start_time = datetime.now()

# Load standardized trial balance
tb_std = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.close_trial_balance_std") \
    .filter(col("period") == CURRENT_PERIOD)

tb_count = tb_std.count()
bu_count = tb_std.select("bu").distinct().count()
cut_types = tb_std.select("cut_type").distinct().rdd.flatMap(lambda x: x).collect()

print(f"\n✓ Loaded {tb_count:,} trial balance records")
print(f"  - Business units: {bu_count}")
print(f"  - Cut types: {', '.join(cut_types)}")

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="load_trial_balance",
    target_table=f"{SILVER_SCHEMA}.close_trial_balance_std",
    target_key=f"period={CURRENT_PERIOD}",
    status="success",
    message=f"Loaded {tb_count} trial balance records from {bu_count} BUs",
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calculate High-Level KPIs by BU

# COMMAND ----------

print("\nCalculating high-level KPIs by BU...")

# Get the latest cut for each BU
latest_cut = tb_std \
    .groupBy("bu") \
    .agg(max("cut_type").alias("latest_cut_type"))

tb_latest = tb_std.join(latest_cut, ["bu"]) \
    .filter(col("cut_type") == col("latest_cut_type"))

# Calculate revenue by BU
revenue_by_bu = tb_latest \
    .filter(col("account_category") == "Revenue") \
    .groupBy("bu", "local_currency", "reporting_currency") \
    .agg(
        sum("local_amount").alias("revenue_local"),
        sum("reporting_amount").alias("revenue_reporting")
    )

# Calculate COGS by BU
cogs_by_bu = tb_latest \
    .filter(col("account_category") == "COGS") \
    .groupBy("bu") \
    .agg(
        sum("local_amount").alias("cogs_local"),
        sum("reporting_amount").alias("cogs_reporting")
    )

# Calculate OpEx by BU
opex_by_bu = tb_latest \
    .filter(col("account_category") == "Operating Expense") \
    .groupBy("bu") \
    .agg(
        sum("local_amount").alias("opex_local"),
        sum("reporting_amount").alias("opex_reporting")
    )

# Join all metrics
bu_kpis = revenue_by_bu \
    .join(cogs_by_bu, "bu", "left") \
    .join(opex_by_bu, "bu", "left") \
    .fillna(0, subset=["cogs_local", "cogs_reporting", "opex_local", "opex_reporting"]) \
    .withColumn("gross_profit_reporting", col("revenue_reporting") - col("cogs_reporting")) \
    .withColumn("operating_profit_reporting", col("revenue_reporting") - col("cogs_reporting") - col("opex_reporting")) \
    .withColumn("gross_margin_pct", round(col("gross_profit_reporting") / col("revenue_reporting") * 100, 2)) \
    .withColumn("operating_margin_pct", round(col("operating_profit_reporting") / col("revenue_reporting") * 100, 2))

print("\n✓ Calculated KPIs by BU:")
bu_kpis.select("bu", "revenue_reporting", "operating_profit_reporting", "operating_margin_pct").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calculate Consolidated KPIs

# COMMAND ----------

print("Calculating consolidated KPIs...")

consolidated = bu_kpis.agg(
    sum("revenue_reporting").alias("revenue_reporting"),
    sum("cogs_reporting").alias("cogs_reporting"),
    sum("opex_reporting").alias("opex_reporting"),
    sum("gross_profit_reporting").alias("gross_profit_reporting"),
    sum("operating_profit_reporting").alias("operating_profit_reporting")
).first()

consolidated_gross_margin = (consolidated["gross_profit_reporting"] / consolidated["revenue_reporting"]) * 100
consolidated_operating_margin = (consolidated["operating_profit_reporting"] / consolidated["revenue_reporting"]) * 100

print(f"\n✓ Consolidated Results:")
print(f"  Revenue: ${consolidated['revenue_reporting']:,.2f}")
print(f"  Gross Profit: ${consolidated['gross_profit_reporting']:,.2f} ({consolidated_gross_margin:.2f}%)")
print(f"  Operating Profit: ${consolidated['operating_profit_reporting']:,.2f} ({consolidated_operating_margin:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detect Unusual Variances

# COMMAND ----------

print("\nDetecting unusual variances...")

# Calculate variance distribution
variance_stats = tb_latest \
    .filter(col("variance_vs_prior_pct").isNotNull()) \
    .agg(
        avg("variance_vs_prior_pct").alias("avg_variance"),
        stddev("variance_vs_prior_pct").alias("stddev_variance")
    ).first()

avg_var = variance_stats["avg_variance"]
stddev_var = variance_stats["stddev_variance"]

# Flag outliers (>2 standard deviations)
outliers = tb_latest \
    .filter(
        (col("variance_vs_prior_pct").isNotNull()) &
        (abs(col("variance_vs_prior_pct") - avg_var) > 2 * stddev_var) &
        (abs(col("reporting_amount")) > 10000)  # Material amounts only
    ) \
    .select("bu", "account_code", "account_name", "reporting_amount", "variance_vs_prior_pct") \
    .orderBy(desc(abs(col("variance_vs_prior_pct")))) \
    .limit(10) \
    .collect()

if len(outliers) > 0:
    print(f"\n⚠ Found {len(outliers)} unusual variances (>2σ):")
    for outlier in outliers:
        print(f"  - {outlier['bu']} / {outlier['account_name']}: {outlier['variance_vs_prior_pct']:+.1f}%")
        
        log_agent_action(
            agent_name=AGENT_NAME,
            period=CURRENT_PERIOD,
            action="detect_unusual_variance",
            target_table=f"{SILVER_SCHEMA}.close_trial_balance_std",
            target_key=f"{outlier['bu']}_{outlier['account_code']}",
            status="warning",
            message=f"Unusual variance detected: {outlier['bu']} / {outlier['account_name']} changed {outlier['variance_vs_prior_pct']:+.1f}% vs prior period",
            execution_time_ms=0
        )
else:
    print("  ✓ No unusual variances detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate FX Impact

# COMMAND ----------

print("\nCalculating FX impact...")

# FX impact by BU (simplified - using random data in demo)
fx_impact_by_bu = tb_latest \
    .groupBy("bu") \
    .agg(sum("fx_impact").alias("total_fx_impact")) \
    .collect()

total_fx_impact = sum([row["total_fx_impact"] for row in fx_impact_by_bu])

print(f"\n✓ Total FX Impact: ${total_fx_impact:,.2f}")
for row in fx_impact_by_bu:
    if abs(row["total_fx_impact"]) > 1000:
        print(f"  - {row['bu']}: ${row['total_fx_impact']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Update Close Status with Pre-Close Summary

# COMMAND ----------

print("\nUpdating close status with pre-close summary...")

# Generate BU-level summaries
bu_summaries = []

for bu_row in bu_kpis.collect():
    summary = f"{bu_row['bu']}: Revenue ${bu_row['revenue_reporting']:,.0f}, " \
              f"Operating Profit ${bu_row['operating_profit_reporting']:,.0f} " \
              f"({bu_row['operating_margin_pct']:.1f}% margin)"
    
    bu_summaries.append({
        "period": CURRENT_PERIOD,
        "bu": bu_row['bu'],
        "phase_id": 2,
        "phase_name": "Adjustments",
        "overall_status": "in_progress",
        "pct_tasks_completed": 75.0,  # Simplified
        "total_tasks": 4,
        "completed_tasks": 3,
        "blocked_tasks": 0,
        "days_since_period_end": 5,
        "days_to_sla": 5,
        "sla_status": "on_track",
        "key_issues": None,
        "last_milestone": "Preliminary close data processed",
        "next_milestone": "Preliminary results review meeting",
        "agent_summary": summary,
        "load_timestamp": datetime.now(),
        "metadata": json.dumps({"updated_by": AGENT_NAME})
    })

# Create DataFrame
bu_status_df = spark.createDataFrame(bu_summaries)

# Update consolidated summary
consolidated_summary = f"Consolidated: Revenue ${consolidated['revenue_reporting']:,.0f}, " \
                      f"Operating Profit ${consolidated['operating_profit_reporting']:,.0f} " \
                      f"({consolidated_operating_margin:.1f}% margin). " \
                      f"FX Impact: ${total_fx_impact:,.0f}. " \
                      f"{len(outliers)} unusual variances detected."

consolidated_status = spark.createDataFrame([{
    "period": CURRENT_PERIOD,
    "bu": "CONSOLIDATED",
    "phase_id": 2,
    "phase_name": "Adjustments",
    "overall_status": "in_progress",
    "pct_tasks_completed": 75.0,
    "total_tasks": len(bu_summaries) + 5,  # BU tasks + central tasks
    "completed_tasks": len(bu_summaries) + 3,
    "blocked_tasks": 0,
    "days_since_period_end": 5,
    "days_to_sla": 5,
    "sla_status": "on_track",
    "key_issues": f"{len(outliers)} unusual variances" if len(outliers) > 0 else None,
    "last_milestone": "Preliminary close processed and analyzed",
    "next_milestone": "Preliminary results review meeting",
    "agent_summary": consolidated_summary,
    "load_timestamp": datetime.now(),
    "metadata": json.dumps({"updated_by": AGENT_NAME})
}])

# Merge with existing status
existing_status = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold") \
    .filter((col("period") != CURRENT_PERIOD))

updated_status = existing_status.union(bu_status_df).union(consolidated_status)

updated_status.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")

print(f"✓ Updated close status for {len(bu_summaries)} BUs and consolidated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pre-Close Agent Summary

# COMMAND ----------

preclose_summary = f"""
PreClose Agent completed successfully.
- Processed {tb_count:,} trial balance records from {bu_count} BUs
- Consolidated Revenue: ${consolidated['revenue_reporting']:,.0f}
- Consolidated Operating Profit: ${consolidated['operating_profit_reporting']:,.0f} ({consolidated_operating_margin:.1f}% margin)
- Total FX Impact: ${total_fx_impact:,.0f}
- Unusual variances detected: {len(outliers)}
- Close status updated with preliminary results summary
"""

print(preclose_summary)

log_agent_action(
    agent_name=AGENT_NAME,
    period=CURRENT_PERIOD,
    action="agent_summary",
    target_table="summary",
    target_key=f"preclose_agent_{CURRENT_PERIOD}",
    status="success" if len(outliers) == 0 else "warning",
    message=preclose_summary.replace("\n", " ").strip(),
    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ FX Agent and PreClose Agent Execution Complete!

FX Agent Results:
- Validated {fx_count:,} FX records
- Coverage issues: {len(coverage_issues)}
- Anomalies detected: {len(anomalies)}

PreClose Agent Results:
- Analyzed {tb_count:,} trial balance records
- Consolidated Revenue: ${consolidated['revenue_reporting']:,.2f}
- Consolidated Operating Profit: ${consolidated['operating_profit_reporting']:,.2f}
- Operating Margin: {consolidated_operating_margin:.2f}%
- Total FX Impact: ${total_fx_impact:,.2f}
- Unusual variances: {len(outliers)}

Both agents logged all actions to: {CATALOG}.{GOLD_SCHEMA}.close_agent_logs

Next Steps:
1. Review unusual variances in Genie or dashboards
2. Investigate FX anomalies if any
3. Prepare for preliminary results review meeting
4. Run Orchestrator Agent to advance tasks
""")
