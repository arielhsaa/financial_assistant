# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Synthetic Data Generation
# MAGIC 
# MAGIC This notebook generates realistic synthetic data for the financial close solution:
# MAGIC - Exchange rates with volatility and cross-rates
# MAGIC - BU preliminary close files with multiple versions
# MAGIC - Segmented close files by BU, segment, product, region
# MAGIC - Forecast files with scenarios
# MAGIC - Forecast FX rates
# MAGIC 
# MAGIC **Run this notebook** to populate Bronze tables with test data for a given period.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Catalog and schema configuration
CATALOG = "financial_close_catalog"
BRONZE_SCHEMA = "bronze_layer"

# Period to generate data for
CURRENT_PERIOD = 202601  # January 2026
PERIOD_YEAR = CURRENT_PERIOD // 100
PERIOD_MONTH = CURRENT_PERIOD % 100

# Business configuration
BUSINESS_UNITS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
SEGMENTS = ["Product A", "Product B", "Services", "Other"]
PRODUCTS = ["Widget Pro", "Widget Plus", "Widget Basic", "Service Premium", "Service Standard"]
REGIONS = ["North America", "Europe", "Asia", "Latin America", "Middle East", "Africa"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "BRL", "INR", "CNY", "AUD"]
REPORTING_CURRENCY = "USD"

print(f"Generating synthetic data for period: {CURRENT_PERIOD}")
print(f"Business Units: {len(BUSINESS_UNITS)}")
print(f"Segments: {len(SEGMENTS)}")
print(f"Currencies: {len(CURRENCIES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_period_dates(period):
    """Get start and end dates for a given period (YYYYMM)"""
    year = period // 100
    month = period % 100
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    return start_date, end_date

def get_bu_currency(bu):
    """Map BU to its primary local currency"""
    bu_currency_map = {
        "North America": "USD",
        "Europe": "EUR",
        "Asia Pacific": "JPY",
        "Latin America": "BRL",
        "Middle East": "AED"
    }
    return bu_currency_map.get(bu, "USD")

def generate_realistic_amount(base_amount, volatility=0.1):
    """Generate realistic amount with volatility"""
    return base_amount * (1 + random.uniform(-volatility, volatility))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Exchange Rates

# COMMAND ----------

print("Generating FX rates...")

# Base rates (vs USD)
base_fx_rates = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.79,
    "JPY": 148.50,
    "BRL": 5.75,
    "INR": 83.25,
    "CNY": 7.24,
    "AUD": 1.56,
    "AED": 3.67
}

# Generate daily FX rates for the period and previous month
start_date, end_date = get_period_dates(CURRENT_PERIOD)
prior_period = CURRENT_PERIOD - 1 if PERIOD_MONTH > 1 else (PERIOD_YEAR - 1) * 100 + 12
prior_start, prior_end = get_period_dates(prior_period)

fx_data = []
current_date = prior_start

while current_date <= end_date:
    # Skip weekends for simplicity
    if current_date.weekday() < 5:
        for from_curr in CURRENCIES:
            if from_curr != "USD":
                # Generate spot rate with daily volatility
                base_rate = base_fx_rates[from_curr]
                daily_volatility = random.uniform(-0.02, 0.02)  # ±2% daily volatility
                spot_rate = base_rate * (1 + daily_volatility)
                
                fx_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"fx_rates_{current_date.strftime('%Y%m%d')}.csv",
                    "provider": random.choice(["Bloomberg", "Reuters", "Internal"]),
                    "rate_date": current_date,
                    "from_currency": from_curr,
                    "to_currency": "USD",
                    "exchange_rate": round(spot_rate, 6),
                    "rate_type": "spot",
                    "metadata": '{"source": "synthetic", "quality": "high"}'
                })
                
                # Also generate reverse rate
                fx_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"fx_rates_{current_date.strftime('%Y%m%d')}.csv",
                    "provider": random.choice(["Bloomberg", "Reuters", "Internal"]),
                    "rate_date": current_date,
                    "from_currency": "USD",
                    "to_currency": from_curr,
                    "exchange_rate": round(1.0 / spot_rate, 6),
                    "rate_type": "spot",
                    "metadata": '{"source": "synthetic", "quality": "high"}'
                })
    
    current_date += timedelta(days=1)

# Create DataFrame and write to Bronze
fx_df = spark.createDataFrame(fx_data)
fx_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw")

print(f"✓ Generated {len(fx_data)} FX rate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate BU Preliminary Close Data

# COMMAND ----------

print("Generating BU preliminary close data...")

# Chart of accounts
accounts = [
    {"code": "4000", "name": "Product Revenue", "category": "Revenue"},
    {"code": "4100", "name": "Service Revenue", "category": "Revenue"},
    {"code": "5000", "name": "Cost of Goods Sold", "category": "COGS"},
    {"code": "5100", "name": "Direct Labor", "category": "COGS"},
    {"code": "6000", "name": "Sales & Marketing", "category": "Operating Expense"},
    {"code": "6100", "name": "Research & Development", "category": "Operating Expense"},
    {"code": "6200", "name": "General & Administrative", "category": "Operating Expense"},
    {"code": "7000", "name": "Depreciation", "category": "Operating Expense"},
    {"code": "8000", "name": "Other Income", "category": "Other"},
]

cost_centers = ["CC001", "CC002", "CC003", "CC004", "CC005"]

# Generate preliminary, first_cut, and final_cut for each BU
pre_close_data = []

for bu in BUSINESS_UNITS:
    local_currency = get_bu_currency(bu)
    
    # Get FX rate for this BU currency to USD
    fx_rate = base_fx_rates[local_currency] if local_currency in base_fx_rates else 1.0
    
    # Base revenue for BU (in millions, local currency)
    base_revenue = random.uniform(50, 200) * 1_000_000
    
    for cut_type in ["preliminary", "first_cut", "final_cut"]:
        # Add small adjustments for each cut
        cut_adjustment = {
            "preliminary": 1.0,
            "first_cut": random.uniform(0.98, 1.02),
            "final_cut": random.uniform(0.99, 1.01)
        }[cut_type]
        
        for account in accounts:
            for cost_center in cost_centers:
                # Generate amount based on account type
                if account["category"] == "Revenue":
                    local_amount = base_revenue * random.uniform(0.15, 0.25) * cut_adjustment
                elif account["category"] == "COGS":
                    local_amount = base_revenue * random.uniform(0.08, 0.12) * cut_adjustment * -1
                elif account["category"] == "Operating Expense":
                    local_amount = base_revenue * random.uniform(0.03, 0.08) * cut_adjustment * -1
                else:
                    local_amount = base_revenue * random.uniform(0.001, 0.005) * cut_adjustment
                
                reporting_amount = local_amount / fx_rate
                
                pre_close_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"{bu.replace(' ', '_')}_{cut_type}_{CURRENT_PERIOD}.csv",
                    "period": CURRENT_PERIOD,
                    "bu": bu,
                    "cut_type": cut_type,
                    "account_code": account["code"],
                    "account_name": account["name"],
                    "cost_center": cost_center,
                    "local_currency": local_currency,
                    "local_amount": round(local_amount, 2),
                    "reporting_currency": REPORTING_CURRENCY,
                    "reporting_amount": round(reporting_amount, 2),
                    "segment": random.choice(SEGMENTS),
                    "region": bu,  # Simplified: BU = Region
                    "metadata": f'{{"cut_type": "{cut_type}", "version": 1}}'
                })

# Create DataFrame and write to Bronze
pre_close_df = spark.createDataFrame(pre_close_data)
pre_close_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bu_pre_close_raw")

print(f"✓ Generated {len(pre_close_data)} preliminary close records across {len(BUSINESS_UNITS)} BUs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Segmented Close Data

# COMMAND ----------

print("Generating segmented close data...")

segmented_data = []

for bu in BUSINESS_UNITS:
    local_currency = get_bu_currency(bu)
    fx_rate = base_fx_rates[local_currency] if local_currency in base_fx_rates else 1.0
    
    for segment in SEGMENTS:
        for product in random.sample(PRODUCTS, 3):  # 3 products per segment
            for region in [bu]:  # Simplified: each BU reports for its own region
                # Base revenue for this combination (in local currency)
                revenue_local = random.uniform(5, 25) * 1_000_000
                cogs_local = revenue_local * random.uniform(0.40, 0.55)
                opex_local = revenue_local * random.uniform(0.20, 0.35)
                
                # Convert to reporting currency
                revenue_reporting = revenue_local / fx_rate
                cogs_reporting = cogs_local / fx_rate
                opex_reporting = opex_local / fx_rate
                
                segmented_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"{bu.replace(' ', '_')}_segmented_{CURRENT_PERIOD}.csv",
                    "period": CURRENT_PERIOD,
                    "bu": bu,
                    "segment": segment,
                    "product": product,
                    "region": region,
                    "revenue_local": round(revenue_local, 2),
                    "cogs_local": round(cogs_local, 2),
                    "opex_local": round(opex_local, 2),
                    "local_currency": local_currency,
                    "revenue_reporting": round(revenue_reporting, 2),
                    "cogs_reporting": round(cogs_reporting, 2),
                    "opex_reporting": round(opex_reporting, 2),
                    "reporting_currency": REPORTING_CURRENCY,
                    "metadata": '{"source": "BU_submission", "validated": true}'
                })

# Create DataFrame and write to Bronze
segmented_df = spark.createDataFrame(segmented_data)
segmented_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bu_segmented_raw")

print(f"✓ Generated {len(segmented_data)} segmented close records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Forecast Data

# COMMAND ----------

print("Generating forecast data...")

# Forecast for next 3 periods
forecast_data = []
scenarios = ["Base", "Upside", "Downside"]

for forecast_offset in range(1, 4):  # Next 3 months
    forecast_period = CURRENT_PERIOD + forecast_offset
    if (forecast_period % 100) > 12:
        forecast_period = ((forecast_period // 100) + 1) * 100 + ((forecast_period % 100) - 12)
    
    for bu in BUSINESS_UNITS:
        local_currency = get_bu_currency(bu)
        fx_rate = base_fx_rates[local_currency] if local_currency in base_fx_rates else 1.0
        
        for segment in SEGMENTS:
            for scenario in scenarios:
                # Base forecast revenue
                base_forecast_revenue = random.uniform(15, 35) * 1_000_000
                
                # Adjust by scenario
                scenario_multiplier = {
                    "Base": 1.0,
                    "Upside": random.uniform(1.10, 1.25),
                    "Downside": random.uniform(0.80, 0.95)
                }[scenario]
                
                revenue_local = base_forecast_revenue * scenario_multiplier
                cogs_local = revenue_local * random.uniform(0.42, 0.52)
                opex_local = revenue_local * random.uniform(0.22, 0.32)
                
                # Convert to reporting currency
                revenue_reporting = revenue_local / fx_rate
                cogs_reporting = cogs_local / fx_rate
                opex_reporting = opex_local / fx_rate
                
                forecast_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"{bu.replace(' ', '_')}_forecast_{forecast_period}.csv",
                    "forecast_period": forecast_period,
                    "submission_date": datetime.now().date(),
                    "bu": bu,
                    "segment": segment,
                    "scenario": scenario,
                    "revenue_local": round(revenue_local, 2),
                    "cogs_local": round(cogs_local, 2),
                    "opex_local": round(opex_local, 2),
                    "local_currency": local_currency,
                    "revenue_reporting": round(revenue_reporting, 2),
                    "cogs_reporting": round(cogs_reporting, 2),
                    "opex_reporting": round(opex_reporting, 2),
                    "reporting_currency": REPORTING_CURRENCY,
                    "assumptions": f"Scenario: {scenario}, Growth assumption: {scenario_multiplier:.2%}",
                    "metadata": f'{{"scenario": "{scenario}", "confidence": "medium"}}'
                })

# Create DataFrame and write to Bronze
forecast_df = spark.createDataFrame(forecast_data)
forecast_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.bu_forecast_raw")

print(f"✓ Generated {len(forecast_data)} forecast records for next 3 periods")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Forecast FX Rates

# COMMAND ----------

print("Generating forecast FX rates...")

fx_forecast_data = []

for forecast_offset in range(1, 4):  # Next 3 months
    forecast_period = CURRENT_PERIOD + forecast_offset
    if (forecast_period % 100) > 12:
        forecast_period = ((forecast_period // 100) + 1) * 100 + ((forecast_period % 100) - 12)
    
    for scenario in scenarios:
        for from_curr in CURRENCIES:
            if from_curr != "USD":
                base_rate = base_fx_rates[from_curr]
                
                # Adjust by scenario and add some drift
                scenario_adjustment = {
                    "Base": random.uniform(0.98, 1.02),
                    "Upside": random.uniform(0.95, 1.05),  # More volatility in upside
                    "Downside": random.uniform(0.90, 1.10)  # More volatility in downside
                }[scenario]
                
                forecast_rate = base_rate * scenario_adjustment
                
                fx_forecast_data.append({
                    "load_timestamp": datetime.now(),
                    "file_name": f"fx_forecast_{forecast_period}.csv",
                    "forecast_period": forecast_period,
                    "from_currency": from_curr,
                    "to_currency": "USD",
                    "exchange_rate": round(forecast_rate, 6),
                    "scenario": scenario,
                    "metadata": f'{{"scenario": "{scenario}", "source": "Treasury"}}'
                })

# Create DataFrame and write to Bronze
fx_forecast_df = spark.createDataFrame(fx_forecast_data)
fx_forecast_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.fx_forecast_raw")

print(f"✓ Generated {len(fx_forecast_data)} forecast FX rate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Data Generation

# COMMAND ----------

print("\n=== Data Generation Summary ===\n")

# Check record counts
tables_to_check = [
    "fx_rates_raw",
    "bu_pre_close_raw",
    "bu_segmented_raw",
    "bu_forecast_raw",
    "fx_forecast_raw"
]

for table in tables_to_check:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}").count()
    print(f"{table}: {count:,} records")

# COMMAND ----------

# Sample data from each table
print("\n=== Sample FX Rates ===")
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw").limit(5))

print("\n=== Sample BU Pre-Close ===")
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_pre_close_raw").limit(5))

print("\n=== Sample Segmented Close ===")
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_segmented_raw").limit(5))

print("\n=== Sample Forecast ===")
display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_forecast_raw").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ Synthetic Data Generation Complete!

Generated data for period: {CURRENT_PERIOD}

Bronze Tables Populated:
- fx_rates_raw: Daily FX rates with volatility
- bu_pre_close_raw: Preliminary, first_cut, and final_cut for {len(BUSINESS_UNITS)} BUs
- bu_segmented_raw: Segmented close by BU, segment, product, region
- bu_forecast_raw: Forecasts for next 3 periods with 3 scenarios
- fx_forecast_raw: Forecast FX rates for next 3 periods

Next Steps:
1. Run notebook 03_ingest_and_standardize_phase1_2.py to process Phase 1 & 2 data
2. Run notebook 04_ingest_and_standardize_phase3.py to process Phase 3 data
3. Run agent notebooks to automate close process

To generate data for additional periods, update CURRENT_PERIOD and re-run this notebook.
""")
