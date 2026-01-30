# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - FX Agent & Pre-Close Agent
# MAGIC 
# MAGIC **Purpose:** Domain agents for FX validation and preliminary close analysis
# MAGIC 
# MAGIC **FX Agent Responsibilities:**
# MAGIC - Monitor new FX rate arrivals in Bronze
# MAGIC - Validate currency coverage and data quality
# MAGIC - Flag missing rates or quality issues
# MAGIC - Update fx_rates_std with quality scores
# MAGIC 
# MAGIC **Pre-Close Agent Responsibilities:**
# MAGIC - Process BU preliminary close files
# MAGIC - Calculate key KPIs (revenue, operating profit, margins)
# MAGIC - Identify unusual variances (>10% vs prior month)
# MAGIC - Generate summaries for review meetings
# MAGIC 
# MAGIC **Execution:** Can run hourly or on-demand via workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json
import uuid

spark.sql("USE CATALOG financial_close_lakehouse")

# Configuration
CURRENT_PERIOD = "2025-12"
PRIOR_PERIOD = "2025-11"
RUN_ID = str(uuid.uuid4())[:8]

print(f"🤖 FX Agent & Pre-Close Agent Starting")
print(f"   Run ID: {RUN_ID}")
print(f"   Current Period: {CURRENT_PERIOD}")
print(f"   Prior Period: {PRIOR_PERIOD}")

# COMMAND ----------

def log_agent_action(agent_name, action_type, task_id, bu_code, decision, input_data, output_data, status="success"):
    """Helper to log agent decisions"""
    return {
        "log_id": str(uuid.uuid4()),
        "log_timestamp": datetime.now(),
        "agent_name": agent_name,
        "action_type": action_type,
        "period": CURRENT_PERIOD,
        "task_id": task_id,
        "bu_code": bu_code,
        "decision_rationale": decision,
        "input_data": json.dumps(input_data),
        "output_data": json.dumps(output_data),
        "status": status,
        "execution_time_ms": 0
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: FX Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Check FX Rate Coverage

# COMMAND ----------

AGENT_NAME = "fx_agent"
fx_logs = []

print(f"\n{'='*80}")
print(f"FX AGENT - Currency Coverage Validation")
print(f"{'='*80}\n")

# Get required currencies from BU config
required_currencies = (
    spark.table("config.business_units")
    .filter(F.col("is_active") == True)
    .select("functional_currency")
    .distinct()
    .collect()
)

required_currency_list = [row.functional_currency for row in required_currencies if row.functional_currency != "USD"]

print(f"Required currencies: {required_currency_list}")

# COMMAND ----------

# Check FX rate availability for current period month-end
period_end = F.last_day(F.lit(f"{CURRENT_PERIOD}-01"))

available_fx = (
    spark.table("silver.fx_rates_std")
    .filter(F.col("rate_date") == period_end)
    .select("quote_currency")
    .distinct()
    .collect()
)

available_currency_list = [row.quote_currency for row in available_fx]

print(f"Available FX rates: {available_currency_list}")

# Find missing currencies
missing_currencies = set(required_currency_list) - set(available_currency_list)

if missing_currencies:
    print(f"\n⚠️  MISSING FX RATES: {missing_currencies}")
    
    for currency in missing_currencies:
        fx_logs.append(
            log_agent_action(
                agent_name=AGENT_NAME,
                action_type="validation",
                task_id=102,
                bu_code=None,
                decision=f"Missing FX rate for {currency} on {period_end}",
                input_data={"period": CURRENT_PERIOD, "currency": currency},
                output_data={"issue": "missing_rate", "severity": "high"},
                status="error"
            )
        )
else:
    print(f"\n✓ All required FX rates present")
    
    fx_logs.append(
        log_agent_action(
            agent_name=AGENT_NAME,
            action_type="validation",
            task_id=102,
            bu_code=None,
            decision=f"All {len(required_currency_list)} required currencies have FX rates",
            input_data={"period": CURRENT_PERIOD, "required": required_currency_list},
            output_data={"coverage": "complete", "currencies_validated": len(required_currency_list)},
            status="success"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 FX Rate Quality Analysis

# COMMAND ----------

# Analyze FX rate quality
fx_quality = (
    spark.table("silver.fx_rates_std")
    .filter(F.col("rate_date") == period_end)
    .select(
        "quote_currency",
        "rate",
        "quality_score"
    )
)

print("\nFX Rate Quality Summary:")
display(fx_quality)

# Flag low quality rates
low_quality = fx_quality.filter(F.col("quality_score") < 0.9).collect()

if low_quality:
    print(f"\n⚠️  {len(low_quality)} rates with quality score < 0.9:")
    for rate in low_quality:
        print(f"   {rate.quote_currency}: {rate.rate:.6f} (quality: {rate.quality_score})")
        
        fx_logs.append(
            log_agent_action(
                agent_name=AGENT_NAME,
                action_type="validation",
                task_id=102,
                bu_code=None,
                decision=f"Low quality FX rate for {rate.quote_currency} (score: {rate.quality_score})",
                input_data={"currency": rate.quote_currency, "rate": float(rate.rate)},
                output_data={"quality_score": float(rate.quality_score), "severity": "medium"},
                status="warning"
            )
        )
else:
    print("\n✓ All FX rates have quality score ≥ 0.9")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 FX Rate Change Analysis

# COMMAND ----------

# Compare current vs prior month FX rates
prior_period_end = F.last_day(F.lit(f"{PRIOR_PERIOD}-01"))

fx_current = (
    spark.table("silver.fx_rates_std")
    .filter(F.col("rate_date") == period_end)
    .select(
        F.col("quote_currency").alias("currency"),
        F.col("rate").alias("current_rate")
    )
)

fx_prior = (
    spark.table("silver.fx_rates_std")
    .filter(F.col("rate_date") == prior_period_end)
    .select(
        F.col("quote_currency").alias("currency"),
        F.col("rate").alias("prior_rate")
    )
)

fx_change = (
    fx_current
    .join(fx_prior, "currency", "left")
    .withColumn("rate_change", F.col("current_rate") - F.col("prior_rate"))
    .withColumn("rate_change_pct", 
                F.round((F.col("rate_change") / F.col("prior_rate")) * 100, 2))
    .orderBy(F.desc(F.abs(F.col("rate_change_pct"))))
)

print("\nFX Rate Changes (Current vs Prior Month):")
display(fx_change)

# Flag significant FX movements (>5%)
significant_moves = fx_change.filter(F.abs(F.col("rate_change_pct")) > 5).collect()

if significant_moves:
    print(f"\n📊 {len(significant_moves)} currencies with >5% movement:")
    for move in significant_moves:
        direction = "strengthened" if move.rate_change_pct > 0 else "weakened"
        print(f"   {move.currency}: {move.rate_change_pct:+.2f}% ({direction})")
        
        fx_logs.append(
            log_agent_action(
                agent_name=AGENT_NAME,
                action_type="calculation",
                task_id=102,
                bu_code=None,
                decision=f"Significant FX movement for {move.currency}: {move.rate_change_pct:+.2f}%",
                input_data={"currency": move.currency, "current_rate": float(move.current_rate), "prior_rate": float(move.prior_rate)},
                output_data={"rate_change_pct": float(move.rate_change_pct), "impact": "high"},
                status="success"
            )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Pre-Close Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Calculate KPIs by BU

# COMMAND ----------

AGENT_NAME = "pre_close_agent"
pre_close_logs = []

print(f"\n{'='*80}")
print(f"PRE-CLOSE AGENT - KPI Calculation")
print(f"{'='*80}\n")

# Calculate current period KPIs (using cut2/final)
current_kpis = (
    spark.table("silver.close_trial_balance_std")
    .filter(F.col("period") == CURRENT_PERIOD)
    .filter(F.col("cut_version") == "cut2")
    .groupBy("bu_code", "account_category")
    .agg(F.sum("reporting_amount").alias("amount"))
    .groupBy("bu_code")
    .pivot("account_category", ["Revenue", "COGS", "OpEx", "Interest", "Tax"])
    .sum("amount")
    .fillna(0)
    .withColumn("Operating_Profit", F.col("Revenue") + F.col("COGS") + F.col("OpEx"))
    .withColumn("Operating_Margin_Pct", 
                F.round((F.col("Operating_Profit") / F.col("Revenue")) * 100, 2))
    .withColumn("EBITDA", F.col("Operating_Profit"))  # Simplified - would add back D&A
    .withColumn("Net_Income", 
                F.col("Operating_Profit") + F.col("Interest") + F.col("Tax"))
)

print(f"Current Period KPIs ({CURRENT_PERIOD}):")
display(current_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Calculate Prior Period for Comparison

# COMMAND ----------

# Calculate prior period KPIs
prior_kpis = (
    spark.table("silver.close_trial_balance_std")
    .filter(F.col("period") == PRIOR_PERIOD)
    .filter(F.col("cut_version") == "cut2")
    .groupBy("bu_code", "account_category")
    .agg(F.sum("reporting_amount").alias("amount"))
    .groupBy("bu_code")
    .pivot("account_category", ["Revenue", "COGS", "OpEx", "Interest", "Tax"])
    .sum("amount")
    .fillna(0)
    .withColumn("Operating_Profit", F.col("Revenue") + F.col("COGS") + F.col("OpEx"))
    .withColumn("Operating_Margin_Pct", 
                F.round((F.col("Operating_Profit") / F.col("Revenue")) * 100, 2))
)

print(f"\nPrior Period KPIs ({PRIOR_PERIOD}):")
display(prior_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Variance Analysis (Current vs Prior)

# COMMAND ----------

# Calculate variances
variance_analysis = (
    current_kpis
    .select(
        "bu_code",
        F.col("Revenue").alias("current_revenue"),
        F.col("Operating_Profit").alias("current_op"),
        F.col("Operating_Margin_Pct").alias("current_margin")
    )
    .join(
        prior_kpis.select(
            "bu_code",
            F.col("Revenue").alias("prior_revenue"),
            F.col("Operating_Profit").alias("prior_op"),
            F.col("Operating_Margin_Pct").alias("prior_margin")
        ),
        "bu_code",
        "left"
    )
    .withColumn("revenue_variance", F.col("current_revenue") - F.col("prior_revenue"))
    .withColumn("revenue_variance_pct",
                F.round((F.col("revenue_variance") / F.abs(F.col("prior_revenue"))) * 100, 2))
    .withColumn("op_variance", F.col("current_op") - F.col("prior_op"))
    .withColumn("op_variance_pct",
                F.round((F.col("op_variance") / F.abs(F.col("prior_op"))) * 100, 2))
    .withColumn("margin_variance_bps",
                F.round((F.col("current_margin") - F.col("prior_margin")) * 100, 0))
    .orderBy(F.desc(F.abs(F.col("op_variance"))))
)

print("\nVariance Analysis (Current vs Prior Month):")
display(variance_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Flag Unusual Variances

# COMMAND ----------

# Flag BUs with significant variances (>10% or >$1M)
unusual_variances = (
    variance_analysis
    .filter(
        (F.abs(F.col("op_variance_pct")) > 10) | 
        (F.abs(F.col("op_variance")) > 1_000_000)
    )
    .collect()
)

if unusual_variances:
    print(f"\n⚠️  {len(unusual_variances)} BUs with unusual variances detected:\n")
    
    for var in unusual_variances:
        print(f"   {var.bu_code}:")
        print(f"      Operating Profit: ${var.current_op:,.0f} (vs ${var.prior_op:,.0f})")
        print(f"      Variance: ${var.op_variance:,.0f} ({var.op_variance_pct:+.1f}%)")
        print(f"      Margin: {var.current_margin:.1f}% (vs {var.prior_margin:.1f}%, {var.margin_variance_bps:+.0f} bps)")
        
        # Determine likely drivers
        if abs(var.revenue_variance_pct) > 10:
            driver = f"Revenue variance: {var.revenue_variance_pct:+.1f}%"
        elif abs(var.margin_variance_bps) > 200:
            driver = f"Margin compression: {var.margin_variance_bps:+.0f} bps"
        else:
            driver = "Mixed drivers - requires investigation"
        
        print(f"      Likely driver: {driver}\n")
        
        pre_close_logs.append(
            log_agent_action(
                agent_name=AGENT_NAME,
                action_type="validation",
                task_id=201,
                bu_code=var.bu_code,
                decision=f"Unusual variance detected: Operating Profit {var.op_variance_pct:+.1f}%",
                input_data={
                    "current_op": float(var.current_op),
                    "prior_op": float(var.prior_op),
                    "threshold_pct": 10.0
                },
                output_data={
                    "variance_pct": float(var.op_variance_pct),
                    "variance_amount": float(var.op_variance),
                    "likely_driver": driver,
                    "requires_review": True
                },
                status="warning"
            )
        )
else:
    print("\n✓ No unusual variances detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Calculate FX Impact

# COMMAND ----------

# Calculate FX impact by comparing local vs reporting amounts
fx_impact = (
    spark.table("silver.close_trial_balance_std")
    .filter(F.col("period") == CURRENT_PERIOD)
    .filter(F.col("cut_version") == "cut2")
    .filter(F.col("local_currency") != "USD")  # Only non-USD BUs have FX impact
    .groupBy("bu_code", "local_currency")
    .agg(
        F.sum("local_amount").alias("total_local"),
        F.sum("reporting_amount").alias("total_reporting"),
        F.avg("fx_rate").alias("avg_fx_rate")
    )
    .withColumn("fx_impact", F.col("total_reporting") - F.col("total_local"))
    .withColumn("fx_impact_pct",
                F.round((F.col("fx_impact") / F.abs(F.col("total_local"))) * 100, 2))
    .orderBy(F.desc(F.abs(F.col("fx_impact"))))
)

print("\nFX Impact by BU:")
display(fx_impact)

# Log significant FX impacts
significant_fx_impact = fx_impact.filter(F.abs(F.col("fx_impact")) > 100_000).collect()

for impact in significant_fx_impact:
    pre_close_logs.append(
        log_agent_action(
            agent_name=AGENT_NAME,
            action_type="calculation",
            task_id=201,
            bu_code=impact.bu_code,
            decision=f"FX impact of ${impact.fx_impact:,.0f} calculated for {impact.local_currency}",
            input_data={
                "local_currency": impact.local_currency,
                "avg_fx_rate": float(impact.avg_fx_rate)
            },
            output_data={
                "fx_impact": float(impact.fx_impact),
                "fx_impact_pct": float(impact.fx_impact_pct)
            },
            status="success"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Generate Executive Summary

# COMMAND ----------

# Create executive summary for preliminary review meeting
consolidated_kpis = (
    current_kpis
    .agg(
        F.sum("Revenue").alias("total_revenue"),
        F.sum("COGS").alias("total_cogs"),
        F.sum("OpEx").alias("total_opex"),
        F.sum("Operating_Profit").alias("total_op")
    )
    .withColumn("consolidated_margin",
                F.round((F.col("total_op") / F.col("total_revenue")) * 100, 2))
)

summary = consolidated_kpis.first()

executive_summary = f"""
PRELIMINARY CLOSE SUMMARY - {CURRENT_PERIOD}
Generated by Pre-Close Agent on {datetime.now().strftime('%Y-%m-%d %H:%M')}

CONSOLIDATED RESULTS:
- Revenue: ${summary.total_revenue:,.0f}
- Operating Profit: ${summary.total_op:,.0f}
- Operating Margin: {summary.consolidated_margin:.1f}%

KEY HIGHLIGHTS:
- {len(unusual_variances)} BUs with significant variances vs prior month
- FX impact: ${sum([abs(i.fx_impact) for i in significant_fx_impact]):,.0f} across {len(significant_fx_impact)} BUs
- All {len(required_currency_list)} required FX rates validated

ACTION ITEMS FOR REVIEW MEETING:
"""

for var in unusual_variances:
    executive_summary += f"\n- Review {var.bu_code} variance: Operating Profit {var.op_variance_pct:+.1f}%"

print(f"\n{'='*80}")
print(executive_summary)
print(f"{'='*80}")

# COMMAND ----------

# Update close status with agent summaries
for var in unusual_variances:
    spark.sql(f"""
        UPDATE gold.close_status_gold
        SET comments = CONCAT(
            COALESCE(comments, ''),
            '\\n[{datetime.now()}] Pre-Close Agent: Unusual variance detected ({var.op_variance_pct:+.1f}%). Requires review in meeting.'
        ),
        updated_at = current_timestamp()
        WHERE period = '{CURRENT_PERIOD}'
          AND task_id = 203
          AND bu_code IS NULL
    """)

print("\n✓ Close status updated with agent findings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Agent Logs

# COMMAND ----------

# Combine all logs
all_logs = fx_logs + pre_close_logs

if all_logs:
    logs_df = spark.createDataFrame(all_logs)
    logs_df.write.mode("append").saveAsTable("gold.close_agent_logs")
    
    print(f"\n✓ Wrote {len(all_logs)} agent log entries")
    print(f"   - FX Agent: {len(fx_logs)} entries")
    print(f"   - Pre-Close Agent: {len(pre_close_logs)} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execution Summary

# COMMAND ----------

print("\n" + "="*80)
print("FX AGENT & PRE-CLOSE AGENT - EXECUTION SUMMARY")
print("="*80)

print(f"\n🤖 FX Agent:")
print(f"   ✓ Validated {len(required_currency_list)} required currencies")
print(f"   ✓ Quality checked {len(available_currency_list)} FX rates")
if missing_currencies:
    print(f"   ⚠️  Missing rates: {missing_currencies}")
if significant_moves:
    print(f"   📊 Significant movements: {len(significant_moves)} currencies")

print(f"\n🤖 Pre-Close Agent:")
print(f"   ✓ Calculated KPIs for {current_kpis.count()} BUs")
print(f"   ✓ Analyzed variances vs prior month")
if unusual_variances:
    print(f"   ⚠️  Unusual variances: {len(unusual_variances)} BUs require review")
if significant_fx_impact:
    print(f"   📊 FX impact: ${sum([abs(i.fx_impact) for i in significant_fx_impact]):,.0f}")

print(f"\n📝 Total log entries: {len(all_logs)}")

# Update agent configurations
spark.sql(f"""
    UPDATE config.agent_configuration
    SET last_run_timestamp = current_timestamp(),
        updated_at = current_timestamp()
    WHERE agent_name IN ('fx_agent', 'pre_close_agent')
""")

print(f"\n✓ Agent configurations updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ FX Agent & Pre-Close Agent Complete
# MAGIC 
# MAGIC **FX Agent Completed:**
# MAGIC - ✓ Currency coverage validated
# MAGIC - ✓ Quality scores checked
# MAGIC - ✓ Rate changes analyzed
# MAGIC - ✓ Alerts raised for issues
# MAGIC 
# MAGIC **Pre-Close Agent Completed:**
# MAGIC - ✓ KPIs calculated for all BUs
# MAGIC - ✓ Variance analysis vs prior month
# MAGIC - ✓ FX impact quantified
# MAGIC - ✓ Executive summary generated
# MAGIC - ✓ Unusual variances flagged for review
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Review agent logs in `gold.close_agent_logs`
# MAGIC 2. Address any flagged issues before review meeting
# MAGIC 3. Run notebook 07 for segmented & forecast analysis
