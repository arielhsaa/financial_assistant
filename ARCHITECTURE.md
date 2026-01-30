# Financial Close Solution - Architecture Overview

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AZURE DATABRICKS WORKSPACE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         UNITY CATALOG                                 │   │
│  │                   financial_close_catalog                             │   │
│  ├─────────────────────────────────────────────────────────────────────┤   │
│  │                                                                       │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │   │
│  │  │  BRONZE LAYER    │  │  SILVER LAYER    │  │  GOLD LAYER      │  │   │
│  │  │  (Raw Landing)   │  │  (Standardized)  │  │  (Consumption)   │  │   │
│  │  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤  │   │
│  │  │ fx_rates_raw     │─▶│ fx_rates_std     │  │ close_status_gold│  │   │
│  │  │ bu_pre_close_raw │─▶│ close_trial_     │─▶│ close_results_   │  │   │
│  │  │ bu_segmented_raw │─▶│   balance_std    │  │   gold           │  │   │
│  │  │ bu_forecast_raw  │─▶│ segmented_close_ │  │ forecast_results_│  │   │
│  │  │ fx_forecast_raw  │  │   std            │  │   gold           │  │   │
│  │  │                  │  │ forecast_std     │  │ close_kpi_gold   │  │   │
│  │  │                  │  │                  │  │ close_phase_tasks│  │   │
│  │  │                  │  │                  │  │ close_agent_logs │  │   │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │   │
│  │                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ▲                                         │
│                                    │                                         │
│  ┌─────────────────────────────────┼────────────────────────────────────┐  │
│  │              PROCESSING & ORCHESTRATION LAYER                         │  │
│  ├─────────────────────────────────┴────────────────────────────────────┤  │
│  │                                                                        │  │
│  │  ┌────────────────────────────────────────────────────────────┐      │  │
│  │  │              DATABRICKS NOTEBOOKS                           │      │  │
│  │  ├────────────────────────────────────────────────────────────┤      │  │
│  │  │ 01: Schema Setup                                           │      │  │
│  │  │ 02: Synthetic Data Generation                              │      │  │
│  │  │ 03: Phase 1 & 2 Processing (Data + Adjustments)           │      │  │
│  │  │ 04: Phase 3 Processing (Segmented + Forecast)             │      │  │
│  │  │ 05: Orchestrator Agent (Close Supervisor)                 │      │  │
│  │  │ 06: FX & PreClose Agents                                  │      │  │
│  │  │ 07: Segmented, Forecast & Reporting Agents                │      │  │
│  │  │ 08: Dashboard & Genie Setup                               │      │  │
│  │  └────────────────────────────────────────────────────────────┘      │  │
│  │                                                                        │  │
│  │  ┌────────────────────────────────────────────────────────────┐      │  │
│  │  │              DATABRICKS WORKFLOWS                           │      │  │
│  │  ├────────────────────────────────────────────────────────────┤      │  │
│  │  │ ▶ Daily FX Update Job (Scheduled)                          │      │  │
│  │  │ ▶ Monthly Close Workflow (Manual/Scheduled)                │      │  │
│  │  │ ▶ Agent Monitoring Job (Hourly)                            │      │  │
│  │  └────────────────────────────────────────────────────────────┘      │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                    ▲                                         │
│                                    │                                         │
│  ┌─────────────────────────────────┼────────────────────────────────────┐  │
│  │                    INTELLIGENT AGENTS LAYER                           │  │
│  ├─────────────────────────────────┴────────────────────────────────────┤  │
│  │                                                                        │  │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │  │
│  │  │  FX Agent        │  │  PreClose Agent  │  │  Segmented &     │   │  │
│  │  │                  │  │                  │  │  Forecast Agent  │   │  │
│  │  │ • Validate FX    │  │ • Calculate KPIs │  │ • Variance       │   │  │
│  │  │ • Detect         │  │ • Detect unusual │  │   analysis       │   │  │
│  │  │   anomalies      │  │   variances      │  │ • Update results │   │  │
│  │  │ • Update Silver  │  │ • Update status  │  │ • Identify       │   │  │
│  │  └──────────────────┘  └──────────────────┘  │   drivers        │   │  │
│  │                                               └──────────────────┘   │  │
│  │  ┌──────────────────┐  ┌──────────────────┐                         │  │
│  │  │  Orchestrator    │  │  Reporting Agent │                         │  │
│  │  │  Agent           │  │                  │                         │  │
│  │  │                  │  │ • Generate final │                         │  │
│  │  │ • Monitor tasks  │  │   KPIs           │                         │  │
│  │  │ • Advance status │  │ • Executive      │                         │  │
│  │  │ • Detect issues  │  │   summary        │                         │  │
│  │  │ • Send alerts    │  │ • Dashboard prep │                         │  │
│  │  └──────────────────┘  └──────────────────┘                         │  │
│  │                                                                        │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                    USER INTERFACE LAYER                                │ │
│  ├───────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────┐  ┌─────────────────────────────┐    │ │
│  │  │      GENIE SPACES           │  │   DATABRICKS AI/BI          │    │ │
│  │  │                             │  │   DASHBOARDS                │    │ │
│  │  ├─────────────────────────────┤  ├─────────────────────────────┤    │ │
│  │  │ Financial Close Assistant   │  │ 1. Close Cockpit Dashboard  │    │ │
│  │  │                             │  │    • Task progress          │    │ │
│  │  │ • Natural language queries  │  │    • SLA tracking           │    │ │
│  │  │ • Variance investigation    │  │    • Overdue items          │    │ │
│  │  │ • BU deep dives             │  │                             │    │ │
│  │  │ • Forecast analysis         │  │ 2. Close Results Dashboard  │    │ │
│  │  │ • Task status checking      │  │    • P&L by BU              │    │ │
│  │  │                             │  │    • Variance analysis      │    │ │
│  │  │ Example Prompts:            │  │    • FX impact              │    │ │
│  │  │ "Show current close status" │  │                             │    │ │
│  │  │ "Top 3 variance drivers?"   │  │ 3. Forecast Dashboard       │    │ │
│  │  │ "Why is Europe below        │  │    • Actual vs. forecast    │    │ │
│  │  │  forecast?"                 │  │    • Scenario comparison    │    │ │
│  │  └─────────────────────────────┘  │    • Accuracy tracking      │    │ │
│  │                                    │                             │    │ │
│  │                                    │ 4. KPI Scorecard Dashboard  │    │ │
│  │                                    │    • Cycle time             │    │ │
│  │                                    │    • Financial metrics      │    │ │
│  │                                    │    • Quality indicators     │    │ │
│  │                                    └─────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘

                                      ▲ ▼
                    ┌─────────────────────────────────────┐
                    │         USER PERSONAS               │
                    ├─────────────────────────────────────┤
                    │ • FP&A Analysts                     │
                    │ • BU Controllers                    │
                    │ • FP&A Leads                        │
                    │ • Finance Leadership                │
                    │ • Treasury Team                     │
                    │ • Accounting Team                   │
                    └─────────────────────────────────────┘
```

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FINANCIAL CLOSE LIFECYCLE                         │
└─────────────────────────────────────────────────────────────────────────┘

Phase 1: DATA GATHERING
    │
    ├─▶ FX rates uploaded → Bronze → FX Agent validates → Silver
    │
    └─▶ BU preliminary close → Bronze → PreClose Agent processes → Silver

Phase 2: ADJUSTMENTS
    │
    ├─▶ Process trial balance → Calculate KPIs → Update Gold
    │
    ├─▶ Preliminary results review (human)
    │
    └─▶ Accounting cuts (first, second) → Bronze → Silver → Gold

Phase 3: DATA GATHERING
    │
    ├─▶ Segmented files → Bronze → Segmented Agent → Silver → Gold
    │
    ├─▶ Forecast files → Bronze → Segmented Agent → Silver → Gold
    │
    └─▶ Forecast FX → Bronze → Silver

Phase 4: REVIEW
    │
    ├─▶ Segmented close review (human + Genie assistance)
    │
    ├─▶ Forecast review (human + Genie assistance)
    │
    └─▶ Variance analysis → Segmented Agent → Gold (with drivers)

Phase 5: REPORTING & SIGN-OFF
    │
    ├─▶ Reporting Agent → Populate KPIs → Gold
    │
    ├─▶ Generate executive summary
    │
    ├─▶ Refresh dashboards
    │
    └─▶ Close complete → Archive → Ready for next period

┌─────────────────────────────────────────────────────────────────────────┐
│                    CONTINUOUS ORCHESTRATION                              │
│  Orchestrator Agent runs every hour to:                                  │
│  • Monitor task prerequisites                                            │
│  • Advance task statuses                                                 │
│  • Detect blockers and overdue items                                     │
│  • Update overall close status                                           │
│  • Send notifications                                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Platform** | Azure Databricks Lakehouse | Unified analytics platform |
| **Catalog** | Unity Catalog | Centralized governance and metadata |
| **Storage** | Delta Lake | ACID transactions, time travel |
| **Compute** | Databricks Clusters | Scalable processing |
| **Orchestration** | Databricks Workflows | Job scheduling and automation |
| **Processing** | PySpark + SQL | Data transformation |
| **Intelligence** | Genie Spaces | Natural language analytics |
| **Agents** | Databricks Agents (Python) | Automated decision-making |
| **Visualization** | Databricks AI/BI Dashboards | Interactive reporting |
| **Security** | Unity Catalog RBAC | Role-based access control |

## Agent Architecture Detail

```
┌───────────────────────────────────────────────────────────────┐
│                    AGENT FRAMEWORK                             │
├───────────────────────────────────────────────────────────────┤
│                                                                │
│  Each Agent Has:                                               │
│  • Monitoring Function (check data/status)                     │
│  • Decision Logic (rules-based automation)                     │
│  • Action Function (update tables)                             │
│  • Logging Function (audit trail)                              │
│                                                                │
│  Agent Communication:                                          │
│  • Read from: Bronze/Silver/Gold tables                        │
│  • Write to: Silver/Gold tables                                │
│  • Log to: close_agent_logs                                    │
│  • Coordinate via: close_phase_tasks, close_status_gold        │
│                                                                │
│  Agent Triggers:                                               │
│  • Time-based (scheduled workflows)                            │
│  • Event-based (data arrival)                                  │
│  • Manual (user-initiated notebook run)                        │
│                                                                │
└───────────────────────────────────────────────────────────────┘
```

## Security & Governance Model

```
┌─────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG SECURITY                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Catalog: financial_close_catalog                               │
│                                                                  │
│  ┌────────────────┬─────────────────────────────────────────┐  │
│  │ User Group     │ Permissions                             │  │
│  ├────────────────┼─────────────────────────────────────────┤  │
│  │ FP&A Analysts  │ READ: All layers                        │  │
│  │                │ WRITE: None (via agents only)           │  │
│  ├────────────────┼─────────────────────────────────────────┤  │
│  │ BU Controllers │ READ: Gold (own BU only)                │  │
│  │                │ WRITE: None                             │  │
│  ├────────────────┼─────────────────────────────────────────┤  │
│  │ Leadership     │ READ: Gold (consolidated + all BUs)     │  │
│  │                │ WRITE: None                             │  │
│  ├────────────────┼─────────────────────────────────────────┤  │
│  │ Agents         │ READ: All layers                        │  │
│  │ (Service       │ WRITE: Silver, Gold                     │  │
│  │  Principal)    │ APPEND: Bronze, Logs                    │  │
│  ├────────────────┼─────────────────────────────────────────┤  │
│  │ Auditors       │ READ: All layers + agent_logs           │  │
│  │                │ WRITE: None                             │  │
│  └────────────────┴─────────────────────────────────────────┘  │
│                                                                  │
│  Row-Level Security (Optional):                                 │
│  • BU Controllers see only their BU data                        │
│  • Implemented via Unity Catalog row filters                    │
│                                                                  │
│  Column-Level Security (Optional):                              │
│  • Sensitive fields (e.g., detailed assumptions) masked         │
│  • Implemented via Unity Catalog column masks                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Deployment Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                    AZURE SUBSCRIPTION                              │
├───────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  Resource Group: rg-financial-close-prod                  │    │
│  ├──────────────────────────────────────────────────────────┤    │
│  │                                                            │    │
│  │  ┌─────────────────────────────────────────────────┐     │    │
│  │  │  Azure Databricks Workspace (Premium)           │     │    │
│  │  │  • Unity Catalog enabled                         │     │    │
│  │  │  • Genie enabled                                 │     │    │
│  │  │  • AI/BI dashboards enabled                      │     │    │
│  │  └─────────────────────────────────────────────────┘     │    │
│  │                         │                                 │    │
│  │                         ▼                                 │    │
│  │  ┌─────────────────────────────────────────────────┐     │    │
│  │  │  Azure Data Lake Storage Gen2                   │     │    │
│  │  │  • Container: financial-close                    │     │    │
│  │  │  • Delta Lake tables                             │     │    │
│  │  │  • Lifecycle management enabled                  │     │    │
│  │  └─────────────────────────────────────────────────┘     │    │
│  │                                                            │    │
│  │  ┌─────────────────────────────────────────────────┐     │    │
│  │  │  Azure Key Vault                                 │     │    │
│  │  │  • Service principal credentials                 │     │    │
│  │  │  • Connection strings (if needed)                │     │    │
│  │  └─────────────────────────────────────────────────┘     │    │
│  │                                                            │    │
│  │  ┌─────────────────────────────────────────────────┐     │    │
│  │  │  Azure Monitor                                   │     │    │
│  │  │  • Log Analytics workspace                       │     │    │
│  │  │  • Alerts for agent failures                     │     │    │
│  │  │  • Metrics for close performance                 │     │    │
│  │  └─────────────────────────────────────────────────┘     │    │
│  │                                                            │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                    │
└───────────────────────────────────────────────────────────────────┘

Cluster Configuration:
  • Agent Jobs: Single node (2-4 cores, 8-16 GB RAM)
  • Analytics: Autoscaling cluster (4-16 nodes, 32-128 GB RAM per node)
  • Runtime: DBR 13.3 LTS or higher (Unity Catalog support)
```

## Scalability Considerations

| Aspect | Current Design | Scale to 100+ BUs | Scale to 1000+ BUs |
|--------|---------------|-------------------|-------------------|
| **Data Volume** | ~1 GB/month | ~10 GB/month | ~100 GB/month |
| **Processing** | Single node for agents | Small cluster for agents | Medium cluster for agents |
| **Partitioning** | Period, BU | Period, BU, Region | Period, BU, Region, Date |
| **Z-Ordering** | BU, Segment | BU, Segment, Product | BU, Segment, Product, Date |
| **Agent Runtime** | Minutes | 10-30 minutes | 1-2 hours |
| **Optimization** | Standard Delta | Liquid Clustering | Liquid Clustering + Photon |

## Disaster Recovery & Business Continuity

```
Primary Region: East US 2
    │
    ├─▶ Continuous Delta table versioning (time travel)
    │   • 30 days retention
    │   • Point-in-time recovery
    │
    ├─▶ Daily backups to separate storage account
    │   • 90 days retention
    │   • Cross-region replication
    │
    └─▶ Agent logs archived to cold storage
        • 365 days retention
        • Audit compliance

Recovery Time Objective (RTO): 4 hours
Recovery Point Objective (RPO): 1 hour
```

## Integration Points (Future)

```
┌─────────────────────────────────────────────────────────────┐
│              POTENTIAL INTEGRATIONS                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Source Systems (Inbound):                                   │
│  • ERP (SAP, Oracle) → Extract to Bronze via API/SFTP       │
│  • Planning tools (Anaplan) → Forecast data                 │
│  • Treasury systems → FX rates                               │
│  • Shared drives → File landing zone                         │
│                                                              │
│  Notification Systems (Outbound):                            │
│  • Email (SendGrid, Azure Communication Services)            │
│  • Slack → Post close status updates                        │
│  • Microsoft Teams → Task notifications                      │
│                                                              │
│  Reporting Systems (Outbound):                               │
│  • Power BI → Embed dashboards                              │
│  • SharePoint → Publish executive summaries                  │
│  • Data warehouse → Export Gold tables                       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

**Document Version:** 1.0  
**Last Updated:** January 2026  
**Maintained by:** FP&A Technology Team
