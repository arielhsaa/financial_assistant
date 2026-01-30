# Intelligent Financial Close Agentic Solution on Azure Databricks

## Overview

This solution streamlines and partially automates the monthly financial close process across multiple business units using Azure Databricks' native capabilities: Lakehouse architecture, Unity Catalog, Delta tables, Genie spaces, agents, and AI/BI dashboards.

## Business Context

**Goal**: Accelerate and improve the monthly financial close process by:
- Automating data gathering and consistency checks
- Tracking status across all phases and tasks
- Providing assisted review via Genie and intelligent agents
- Publishing close results and reporting automatically

**Key Stakeholders**:
- FP&A teams (close coordination, variance analysis)
- Business Unit Controllers (data submission, review)
- Tech/Accounting teams (adjustments, validation)
- Leadership (final reporting, KPIs)

## Architecture

### Lakehouse Design (Unity Catalog)

```
financial_close_catalog
├── bronze_layer (raw landing zone)
│   ├── fx_rates_raw
│   ├── bu_pre_close_raw
│   ├── bu_segmented_raw
│   ├── bu_forecast_raw
│   └── fx_forecast_raw
├── silver_layer (standardized, cleaned)
│   ├── fx_rates_std
│   ├── close_trial_balance_std
│   ├── segmented_close_std
│   └── forecast_std
└── gold_layer (consumption-ready)
    ├── close_status_gold
    ├── close_results_gold
    ├── forecast_results_gold
    ├── close_kpi_gold
    ├── close_phase_tasks
    └── close_agent_logs
```

### Financial Close Lifecycle

**Phase 1 - Data Gathering**
1. Receive and upload exchange rates
2. Process exchange rates in Databricks
3. Receive BU preliminary close files

**Phase 2 - Adjustments**
4. Process BU preliminary close files
5. Publish preliminary results
6. Preliminary results review meeting (FP&A + Tech)
7. Receive first accounting cut
8. Receive second (final) accounting cut

**Phase 3 - Data Gathering**
9. Receive segmented files from business units
10. Receive forecast files from business units
11. Receive forecast FX rates

**Phase 4 - Review**
12. Segmented close review meeting (FP&A + Tech)
13. Forecast review meeting (FP&A + Tech)

**Phase 5 - Reporting & Sign-off**
14. Publish final close results (Segmented + Forecast)

## Agentic Architecture

### Agent Roles

1. **Orchestrator Agent (Close Supervisor)**
   - Monitors `close_phase_tasks` and underlying tables
   - Advances task statuses based on prerequisites
   - Logs all progress to `close_agent_logs`

2. **FX Agent**
   - Watches for new FX files in Bronze
   - Triggers normalization to `fx_rates_std`
   - Validates coverage and flags missing data

3. **Pre-Close Agent**
   - Ingests BU preliminary close data
   - Computes high-level KPIs (revenue, operating profit, FX impact)
   - Updates `close_status_gold` with summaries

4. **Segmented & Forecast Agent**
   - Integrates segmented close and forecast data
   - Computes variances (actual vs. forecast)
   - Writes to `close_results_gold` and `forecast_results_gold`

5. **Reporting Agent**
   - Finalizes all close outputs
   - Populates `close_kpi_gold`
   - Refreshes dashboards for FP&A and leadership

## Technology Stack

- **Platform**: Azure Databricks (all-in-one)
- **Storage**: Delta Lake with Unity Catalog
- **Compute**: PySpark, Databricks SQL
- **Orchestration**: Databricks Workflows
- **Intelligence**: Genie spaces and agents
- **Visualization**: Databricks AI/BI dashboards
- **Dependencies**: Standard PySpark/SQL libraries only (no external tools)

## Getting Started

### Prerequisites

- Azure Databricks workspace (Premium or higher for Unity Catalog and Genie)
- Cluster with DBR 13.3+ (for Unity Catalog support)
- Permissions to create catalogs, schemas, and tables in Unity Catalog

### Setup Instructions

Run the notebooks in the following order:

#### 1. Initial Setup (One-time)

```bash
# Create the schema and all Delta tables
notebooks/01_setup_schema_and_tables.py
```

This notebook:
- Creates `financial_close_catalog` and schemas (bronze, silver, gold)
- Defines all Delta tables with proper schema and comments
- Sets up Unity Catalog permissions

#### 2. Generate Synthetic Data (Per Period)

```bash
# Generate synthetic data for testing and demo
notebooks/02_synthetic_data_generation.py
```

This notebook creates realistic synthetic data:
- Exchange rates with volatility
- BU preliminary close files (multiple versions)
- Segmented close files by BU, segment, product, region
- Forecast files with scenarios (Base, Upside, Downside)
- Forecast FX rates

#### 3. Run Close Process

Execute notebooks in sequence to simulate the monthly close:

```bash
# Phase 1 & 2: Data gathering and adjustments
notebooks/03_ingest_and_standardize_phase1_2.py

# Phase 3: Additional data gathering
notebooks/04_ingest_and_standardize_phase3.py

# Agent automation (can run continuously or on schedule)
notebooks/05_agent_logic_close_supervisor.py
notebooks/06_agent_logic_fx_and_pre_close.py
notebooks/07_agent_logic_segmented_and_forecast.py
```

#### 4. Setup Dashboards and Genie

```bash
# Create helper views and Genie instructions
notebooks/08_dashboards_and_genie_instructions.sql
```

### Databricks Workflows Setup

Create a Databricks workflow to orchestrate the monthly close:

1. **Daily FX Update Job**
   - Trigger: Daily at 8 AM
   - Tasks: Run `06_agent_logic_fx_and_pre_close.py` (FX section only)

2. **Monthly Close Workflow**
   - Trigger: Manual or scheduled (e.g., 1st business day after month-end)
   - Tasks:
     1. Run `03_ingest_and_standardize_phase1_2.py`
     2. Run `05_agent_logic_close_supervisor.py` (Phase 1-2 monitoring)
     3. Run `04_ingest_and_standardize_phase3.py`
     4. Run `05_agent_logic_close_supervisor.py` (Phase 3-5 monitoring)
     5. Run `07_agent_logic_segmented_and_forecast.py`

## Genie Space Configuration

### Creating the Financial Close Genie Space

1. Navigate to **Genie** in Databricks workspace
2. Create a new space: "Financial Close Assistant"
3. Add Gold tables:
   - `close_status_gold`
   - `close_results_gold`
   - `forecast_results_gold`
   - `close_kpi_gold`
   - `close_phase_tasks`

### Genie Space Instructions

Add the following instructions to the Genie space:

```
You are a financial close assistant for FP&A and controlling teams.

Context:
- Fiscal periods follow calendar months (January = Period 1, December = Period 12)
- All amounts are in reporting currency (USD) unless specified
- BUs include: North America, Europe, Asia Pacific, Latin America, Middle East
- Segments include: Product A, Product B, Services, Other

KPI Naming Conventions:
- "Operating Profit" = Revenue - COGS - Operating Expenses
- "FX Impact" = difference between local currency and reporting currency amounts
- "Variance" = Actual - Forecast (positive = favorable for revenue/profit)

Response Style:
- Start with executive summary (2-3 sentences)
- Then provide detailed breakdown with numbers
- Highlight anomalies or items requiring attention
- Use tables for multi-dimensional data

Available Data:
- close_status_gold: Overall close status by period and BU
- close_results_gold: Final P&L results (segmented and consolidated)
- forecast_results_gold: Forecast data and variance analysis
- close_kpi_gold: Key performance indicators for the close process
- close_phase_tasks: Detailed task tracking across all phases
```

### Example Genie Queries

Save these queries in the Genie space for quick access:

1. **Current Close Status**
   ```
   Show me the status of the current month close by BU and phase
   ```

2. **Variance Analysis**
   ```
   Explain the top 3 drivers of variance between this month's close and the previous forecast
   ```

3. **Task Management**
   ```
   List all tasks that are overdue or blocked and who owns them
   ```

4. **BU Deep Dive**
   ```
   Investigate why BU Europe's operating profit is below forecast this month, and summarize the root causes by segment
   ```

5. **FX Impact**
   ```
   What is the total FX impact on operating profit for the current period across all BUs?
   ```

6. **Close Performance**
   ```
   Compare the current close cycle time to the average of the last 6 months
   ```

## Dashboards

### 1. Close Cockpit Dashboard

**Purpose**: Real-time monitoring of close progress

**Key Metrics**:
- Overall close status (% tasks completed)
- Days since period end
- Days to completion vs. SLA
- Phase completion timeline
- Tasks by status (pending, in progress, completed, blocked)
- Overdue tasks by owner

**SQL Query Example**:
```sql
SELECT 
  period,
  phase_id,
  COUNT(*) as total_tasks,
  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
  ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_complete
FROM financial_close_catalog.gold_layer.close_phase_tasks
WHERE period = YEAR(CURRENT_DATE()) * 100 + MONTH(CURRENT_DATE()) - 1
GROUP BY period, phase_id
ORDER BY phase_id
```

### 2. Close Results Dashboard

**Purpose**: Financial results visualization

**Key Metrics**:
- P&L by BU (revenue, COGS, operating expenses, operating profit)
- Consolidated P&L
- Segmented view (by product, region)
- FX impact analysis
- Variance vs. prior period
- Variance vs. forecast

**Visualizations**:
- Waterfall chart: Prior period → Current period
- Heatmap: Variance by BU and segment
- Trend line: Key metrics over last 12 months

### 3. Forecast Dashboard

**Purpose**: Forward-looking analysis

**Key Metrics**:
- Forecast by BU and segment
- Scenario comparison (Base, Upside, Downside)
- Forecast vs. trailing actuals
- Forecast accuracy (if prior forecasts available)
- Key assumptions and drivers

## Usage Guide for FP&A Users

### Daily Operations

1. **Check Close Status**
   - Open "Close Cockpit Dashboard"
   - Review task completion percentage
   - Identify any blocked or overdue tasks

2. **Ask Genie Questions**
   - Use natural language to query close data
   - Example: "Which BUs haven't submitted their preliminary close yet?"

3. **Investigate Variances**
   - Use "Close Results Dashboard"
   - Drill down into specific BUs or segments
   - Ask Genie for root cause analysis

### Monthly Close Process

**Phase 1 (Day 1-2): Data Gathering**
- Monitor FX rates upload (automated)
- Track BU preliminary close submissions in `close_phase_tasks`
- FX Agent and Pre-Close Agent handle automation

**Phase 2 (Day 3-5): Adjustments**
- Review preliminary results in dashboard
- Attend review meeting (recorded in `close_phase_tasks`)
- Process first and second accounting cuts
- Pre-Close Agent updates summaries

**Phase 3 (Day 6-7): Additional Data**
- Track segmented and forecast file submissions
- Monitor forecast FX rates
- Segmented & Forecast Agent processes data

**Phase 4 (Day 8): Review**
- Attend segmented close review meeting
- Attend forecast review meeting
- Use Genie to investigate anomalies

**Phase 5 (Day 9-10): Reporting**
- Reporting Agent finalizes all outputs
- Review final dashboards
- Distribute results to leadership
- Archive period in `close_status_gold`

### Troubleshooting

**Issue**: Missing data for a BU
- Check `close_phase_tasks` for task status
- Query `close_agent_logs` for detailed errors
- Ask Genie: "Which BUs have missing data for period YYYYMM?"

**Issue**: FX rate not available
- FX Agent flags this automatically in `close_status_gold`
- Check `fx_rates_std` for coverage gaps
- Manual upload to `fx_rates_raw` if needed

**Issue**: Variance unexplained
- Use Genie research agent for deep dive
- Example: "Analyze the variance drivers for BU North America in Period 202601"
- Review segmented details in `close_results_gold`

## Key Benefits

### For FP&A Teams
- **Reduced cycle time**: Automation eliminates manual data gathering and validation
- **Improved accuracy**: Agents perform consistent checks and flag anomalies
- **Better insights**: Genie enables natural language analysis of close data
- **Real-time visibility**: Dashboards provide live status updates

### For Business Unit Controllers
- **Clear expectations**: Defined tasks with due dates in `close_phase_tasks`
- **Self-service status**: Query Genie for submission deadlines and requirements
- **Faster feedback**: Agents provide immediate validation of submitted data

### For Leadership
- **Executive dashboards**: High-level KPIs and trends
- **On-demand analysis**: Ask Genie complex questions without waiting for FP&A
- **Improved governance**: Full audit trail in `close_agent_logs`

## Data Governance

### Unity Catalog Security

- **Bronze layer**: Write access for ETL/agents, read access for data engineers
- **Silver layer**: Write access for agents, read access for FP&A and analysts
- **Gold layer**: Read access for all business users, write access for agents only
- **Agent logs**: Read access for FP&A leads and auditors

### Data Retention

- **Bronze**: Keep for 13 months (current + 12 prior)
- **Silver**: Keep for 24 months
- **Gold**: Keep indefinitely (or per company policy)
- **Agent logs**: Keep for 12 months

### Audit Trail

All agent actions logged to `close_agent_logs` with:
- Timestamp
- Agent name
- Action performed
- Tables affected
- User (if applicable)
- Status (success/failure)

## Performance Considerations

### Optimization Tips

1. **Partition strategy**: All tables partitioned by `period` for efficient queries
2. **Z-ordering**: Gold tables z-ordered by frequently filtered columns (BU, segment)
3. **Liquid clustering**: Consider for high-cardinality dimensions
4. **Caching**: Dashboard queries leverage Delta caching
5. **Compute**: Use separate clusters for agents (lightweight) vs. analytics (larger)

### Monitoring

- Track agent execution time in `close_agent_logs`
- Monitor close cycle time in `close_kpi_gold`
- Set alerts for overdue tasks or failed agent runs

## Future Enhancements

### Phase 1 (Near-term)
- Email notifications for task assignments and deadlines
- Slack integration for close status updates
- Machine learning for anomaly detection (unusual variances)

### Phase 2 (Medium-term)
- Predictive analytics for close completion date
- Automated commentary generation for variances
- Integration with source systems (ERP, planning tools)

### Phase 3 (Long-term)
- Continuous close (weekly or daily sub-processes)
- AI-powered variance explanations using LLMs
- Automated adjusting entries for known patterns

## Support and Maintenance

### Routine Maintenance

- **Weekly**: Review agent logs for errors or performance issues
- **Monthly**: Validate data quality in Silver layer
- **Quarterly**: Optimize table statistics and vacuum old versions
- **Annually**: Review and update Genie instructions and dashboard queries

### Contact

For questions or issues:
- FP&A Lead: [Name/Email]
- Technical Owner: [Name/Email]
- Databricks Support: [Company-specific contact]

## Appendix

### Glossary

- **BU**: Business Unit
- **FP&A**: Financial Planning & Analysis
- **FX**: Foreign Exchange
- **KPI**: Key Performance Indicator
- **P&L**: Profit & Loss
- **SLA**: Service Level Agreement

### Technical Specifications

- **Databricks Runtime**: 13.3 LTS or higher
- **Unity Catalog**: Enabled
- **Cluster Size**: 
  - Agents: Single node or small cluster (2-4 nodes)
  - Analytics: Medium cluster (4-8 nodes) with autoscaling
- **Data Volume** (estimated):
  - Bronze: ~1 GB per month
  - Silver: ~500 MB per month
  - Gold: ~100 MB per month

---

**Version**: 1.0  
**Last Updated**: January 2026  
**Maintained by**: FP&A Technology Team
