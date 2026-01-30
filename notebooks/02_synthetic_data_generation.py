# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Synthetic Data Generation
# MAGIC 
# MAGIC **Purpose:** Generate realistic synthetic data for all Bronze tables
# MAGIC 
# MAGIC **Generates:**
# MAGIC - 2 years of daily FX rates for 5 currency pairs
# MAGIC - 6 BUs × 3 versions × 12 months of trial balance data
# MAGIC - Segmented close data by BU, segment, product, region
# MAGIC - Forecast data with multiple scenarios
# MAGIC - Forecast FX curves
# MAGIC 
# MAGIC **Execution time:** ~5 minutes
# MAGIC 
# MAGIC **Output:** ~50K rows across Bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid

# Set random seed for reproducibility
random.seed(42)

# Use the financial close catalog
spark.sql("USE CATALOG financial_close_lakehouse")

print("✓ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate FX Rates (Bronze)

# COMMAND ----------

# Define currency pairs with base rates and volatility
currency_config = [
    {"base": "USD", "quote": "EUR", "base_rate": 0.85, "volatility": 0.02},
    {"base": "USD", "quote": "GBP", "base_rate": 0.73, "volatility": 0.015},
    {"base": "USD", "quote": "BRL", "base_rate": 5.0, "volatility": 0.05},
    {"base": "USD", "quote": "INR", "base_rate": 83.0, "volatility": 0.03},
    {"base": "USD", "quote": "CNY", "base_rate": 7.2, "volatility": 0.02}
]

# Generate 2 years of daily rates
start_date = datetime(2024, 1, 1)
end_date = datetime(2026, 1, 31)
date_range = [(start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days + 1)]

fx_records = []
providers = ["Bloomberg", "Reuters", "ECB"]

for date in date_range:
    for config in currency_config:
        # Generate rate with random walk
        days_from_start = (date - start_date.date()).days
        drift = random.gauss(0, config["volatility"])
        rate = config["base_rate"] * (1 + drift + 0.0001 * days_from_start)  # slight upward drift
        
        # Add some variation by provider (simulating bid/ask spread)
        for provider in providers:
            provider_adjustment = random.gauss(0, 0.001)
            final_rate = rate * (1 + provider_adjustment)
            
            fx_records.append({
                "load_timestamp": datetime.now(),
                "file_name": f"fx_{date.strftime('%Y%m%d')}_{provider.lower()}.csv",
                "provider": provider,
                "rate_date": date,
                "base_currency": config["base"],
                "quote_currency": config["quote"],
                "rate": round(final_rate, 6),
                "rate_type": "spot",
                "raw_record": f"{date}|{config['base']}{config['quote']}|{final_rate:.6f}"
            })

# Convert to DataFrame and save
fx_df = spark.createDataFrame(fx_records)
fx_df.write.mode("overwrite").saveAsTable("bronze.fx_rates_raw")

print(f"✓ Generated {len(fx_records)} FX rate records")
print(f"  - Date range: {start_date.date()} to {end_date.date()}")
print(f"  - Currency pairs: {len(currency_config)}")
print(f"  - Providers: {len(providers)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate BU Preliminary Close Data (Bronze)

# COMMAND ----------

# Get business units
bus_df = spark.table("config.business_units")
bus = [row.asDict() for row in bus_df.collect()]

# Define GL account structure
accounts = [
    {"code": "4000", "name": "Revenue - Product Sales", "category": "Revenue"},
    {"code": "4100", "name": "Revenue - Services", "category": "Revenue"},
    {"code": "4200", "name": "Revenue - Other", "category": "Revenue"},
    {"code": "5000", "name": "Cost of Goods Sold", "category": "COGS"},
    {"code": "5100", "name": "Direct Labor", "category": "COGS"},
    {"code": "6000", "name": "Sales & Marketing", "category": "OpEx"},
    {"code": "6100", "name": "Research & Development", "category": "OpEx"},
    {"code": "6200", "name": "General & Administrative", "category": "OpEx"},
    {"code": "7000", "name": "Depreciation & Amortization", "category": "OpEx"},
    {"code": "8000", "name": "Interest Expense", "category": "Interest"},
    {"code": "9000", "name": "Income Tax Expense", "category": "Tax"}
]

# Generate trial balance for 12 months (2025)
periods = [f"2025-{month:02d}" for month in range(1, 13)]
cut_versions = ["preliminary", "cut1", "cut2"]
cost_centers = ["CC100", "CC200", "CC300", "CC400"]
segments = ["Enterprise", "SMB", "Consumer", "Government"]

pre_close_records = []

for period in periods:
    for bu in bus:
        # Generate base amounts for this BU/period
        bu_revenue_base = random.uniform(5_000_000, 20_000_000)
        
        for version_idx, cut_version in enumerate(cut_versions):
            # Each cut version has slight adjustments
            version_adjustment = 1.0 + (version_idx * 0.02)  # 0%, 2%, 4% adjustments
            
            for account in accounts:
                for cost_center in cost_centers:
                    for segment in segments:
                        # Calculate amount based on account type
                        if account["category"] == "Revenue":
                            base_amount = bu_revenue_base * random.uniform(0.3, 0.4) * version_adjustment
                        elif account["category"] == "COGS":
                            base_amount = -bu_revenue_base * random.uniform(0.35, 0.45) * version_adjustment
                        elif account["category"] == "OpEx":
                            base_amount = -bu_revenue_base * random.uniform(0.05, 0.15) * version_adjustment
                        elif account["category"] == "Interest":
                            base_amount = -bu_revenue_base * random.uniform(0.01, 0.03) * version_adjustment
                        else:  # Tax
                            base_amount = -bu_revenue_base * random.uniform(0.02, 0.05) * version_adjustment
                        
                        # Add some randomness per cost center/segment
                        amount = base_amount * random.uniform(0.8, 1.2)
                        
                        pre_close_records.append({
                            "load_timestamp": datetime.now(),
                            "file_name": f"pre_close_{bu['bu_code']}_{period}_{cut_version}.csv",
                            "bu_code": bu["bu_code"],
                            "period": period,
                            "cut_version": cut_version,
                            "account_code": account["code"],
                            "account_name": account["name"],
                            "cost_center": cost_center,
                            "segment": segment,
                            "local_currency": bu["functional_currency"],
                            "local_amount": round(amount, 2),
                            "raw_record": f"{period}|{account['code']}|{amount:.2f}"
                        })

# Save to Bronze
pre_close_df = spark.createDataFrame(pre_close_records)
pre_close_df.write.mode("overwrite").saveAsTable("bronze.bu_pre_close_raw")

print(f"✓ Generated {len(pre_close_records)} preliminary close records")
print(f"  - Periods: {len(periods)}")
print(f"  - Business units: {len(bus)}")
print(f"  - Versions per period: {len(cut_versions)}")
print(f"  - Accounts: {len(accounts)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate BU Segmented Close Data (Bronze)

# COMMAND ----------

# Define segmentation dimensions
products = ["Product A", "Product B", "Product C", "Product D", "Product E"]
regions = ["North", "South", "East", "West", "Central"]
account_categories = ["Revenue", "COGS", "OpEx"]

segmented_records = []

for period in periods:
    for bu in bus:
        bu_revenue_base = random.uniform(5_000_000, 20_000_000)
        
        for segment in segments:
            for product in products:
                for region in regions:
                    for account_category in account_categories:
                        # Calculate amount based on category
                        if account_category == "Revenue":
                            amount = bu_revenue_base * random.uniform(0.01, 0.05)
                        elif account_category == "COGS":
                            amount = -bu_revenue_base * random.uniform(0.015, 0.06)
                        else:  # OpEx
                            amount = -bu_revenue_base * random.uniform(0.005, 0.02)
                        
                        segmented_records.append({
                            "load_timestamp": datetime.now(),
                            "file_name": f"segmented_{bu['bu_code']}_{period}.csv",
                            "bu_code": bu["bu_code"],
                            "period": period,
                            "segment": segment,
                            "product": product,
                            "region": region,
                            "account_category": account_category,
                            "local_currency": bu["functional_currency"],
                            "local_amount": round(amount, 2),
                            "raw_record": f"{period}|{segment}|{product}|{region}|{amount:.2f}"
                        })

# Save to Bronze
segmented_df = spark.createDataFrame(segmented_records)
segmented_df.write.mode("overwrite").saveAsTable("bronze.bu_segmented_raw")

print(f"✓ Generated {len(segmented_records)} segmented close records")
print(f"  - Segments: {len(segments)}")
print(f"  - Products: {len(products)}")
print(f"  - Regions: {len(regions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate BU Forecast Data (Bronze)

# COMMAND ----------

# Generate forecasts for 24 months forward (2025-2026)
forecast_periods = [f"{year}-{month:02d}" for year in [2025, 2026] for month in range(1, 13)]
scenarios = ["Base", "Upside", "Downside"]
forecast_version = "Jan26_v1"

forecast_records = []

for forecast_period in forecast_periods:
    for bu in bus:
        bu_revenue_base = random.uniform(5_000_000, 20_000_000)
        
        for scenario in scenarios:
            # Scenario adjustments
            if scenario == "Upside":
                scenario_mult = 1.15
            elif scenario == "Downside":
                scenario_mult = 0.85
            else:  # Base
                scenario_mult = 1.0
            
            for segment in segments:
                for account_category in ["Revenue", "COGS", "OpEx", "Operating Profit"]:
                    if account_category == "Revenue":
                        amount = bu_revenue_base * random.uniform(0.2, 0.3) * scenario_mult
                    elif account_category == "COGS":
                        amount = -bu_revenue_base * random.uniform(0.25, 0.35) * scenario_mult
                    elif account_category == "OpEx":
                        amount = -bu_revenue_base * random.uniform(0.08, 0.12) * scenario_mult
                    else:  # Operating Profit (calculated)
                        revenue = bu_revenue_base * random.uniform(0.2, 0.3) * scenario_mult
                        cogs = bu_revenue_base * random.uniform(0.25, 0.35) * scenario_mult
                        opex = bu_revenue_base * random.uniform(0.08, 0.12) * scenario_mult
                        amount = revenue - cogs - opex
                    
                    forecast_records.append({
                        "load_timestamp": datetime.now(),
                        "file_name": f"forecast_{bu['bu_code']}_{forecast_version}.csv",
                        "bu_code": bu["bu_code"],
                        "forecast_version": forecast_version,
                        "forecast_period": forecast_period,
                        "scenario": scenario,
                        "account_category": account_category,
                        "segment": segment,
                        "local_currency": bu["functional_currency"],
                        "local_amount": round(amount, 2),
                        "raw_record": f"{forecast_period}|{scenario}|{account_category}|{amount:.2f}"
                    })

# Save to Bronze
forecast_df = spark.createDataFrame(forecast_records)
forecast_df.write.mode("overwrite").saveAsTable("bronze.bu_forecast_raw")

print(f"✓ Generated {len(forecast_records)} forecast records")
print(f"  - Forecast periods: {len(forecast_periods)}")
print(f"  - Scenarios: {len(scenarios)}")
print(f"  - Forecast version: {forecast_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Forecast FX Rates (Bronze)

# COMMAND ----------

# Generate forecast FX curves for next 24 months
forecast_start = datetime(2025, 1, 1)
forecast_end = datetime(2026, 12, 31)
forecast_dates = [(forecast_start + timedelta(days=x)).date() 
                  for x in range(0, (forecast_end - forecast_start).days + 1, 30)]  # Monthly

fx_forecast_records = []

for forecast_date in forecast_dates:
    for config in currency_config:
        for scenario in scenarios:
            # Base forecast with scenario adjustments
            months_from_now = (forecast_date.year - 2025) * 12 + forecast_date.month
            
            if scenario == "Upside":  # Stronger USD (lower rates for foreign currencies)
                scenario_adj = 0.95
            elif scenario == "Downside":  # Weaker USD (higher rates)
                scenario_adj = 1.05
            else:
                scenario_adj = 1.0
            
            # Forecast with slight trend
            forecasted_rate = config["base_rate"] * (1 + 0.001 * months_from_now) * scenario_adj
            forecasted_rate *= random.uniform(0.98, 1.02)  # Add small random variation
            
            fx_forecast_records.append({
                "load_timestamp": datetime.now(),
                "file_name": f"fx_forecast_{forecast_date.strftime('%Y%m')}.csv",
                "forecast_date": forecast_date,
                "base_currency": config["base"],
                "quote_currency": config["quote"],
                "forecasted_rate": round(forecasted_rate, 6),
                "scenario": scenario,
                "raw_record": f"{forecast_date}|{config['base']}{config['quote']}|{forecasted_rate:.6f}|{scenario}"
            })

# Save to Bronze
fx_forecast_df = spark.createDataFrame(fx_forecast_records)
fx_forecast_df.write.mode("overwrite").saveAsTable("bronze.fx_forecast_raw")

print(f"✓ Generated {len(fx_forecast_records)} forecast FX records")
print(f"  - Forecast date range: {forecast_dates[0]} to {forecast_dates[-1]}")
print(f"  - Scenarios: {len(scenarios)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Generation Summary

# COMMAND ----------

# Get row counts for all Bronze tables
bronze_tables = [
    "fx_rates_raw",
    "bu_pre_close_raw",
    "bu_segmented_raw",
    "bu_forecast_raw",
    "fx_forecast_raw"
]

summary_data = []
for table in bronze_tables:
    count = spark.table(f"bronze.{table}").count()
    summary_data.append({"table": f"bronze.{table}", "row_count": count})

summary_df = spark.createDataFrame(summary_data)

print("\n" + "="*80)
print("SYNTHETIC DATA GENERATION COMPLETE")
print("="*80)

display(summary_df)

# COMMAND ----------

# Show sample data from each table
print("\n📊 Sample Data Preview:\n")

for table in bronze_tables:
    print(f"\n--- bronze.{table} ---")
    display(spark.table(f"bronze.{table}").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Data Generation Complete
# MAGIC 
# MAGIC **Generated Data Summary:**
# MAGIC - **FX Rates:** 2 years × 5 currencies × 3 providers = ~11K rows
# MAGIC - **Preliminary Close:** 12 months × 6 BUs × 3 versions × 11 accounts × 4 CCs × 4 segments = ~38K rows
# MAGIC - **Segmented Close:** 12 months × 6 BUs × 4 segments × 5 products × 5 regions × 3 categories = ~22K rows
# MAGIC - **Forecast:** 24 months × 6 BUs × 3 scenarios × 4 categories × 4 segments = ~7K rows
# MAGIC - **Forecast FX:** 24 months × 5 currencies × 3 scenarios = ~360 rows
# MAGIC 
# MAGIC **Total: ~78K rows across all Bronze tables**
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Run `03_ingest_and_standardize_phase1_2.py` to process this data into Silver/Gold
# MAGIC 2. Verify data quality and completeness
# MAGIC 3. Start agent workflows
