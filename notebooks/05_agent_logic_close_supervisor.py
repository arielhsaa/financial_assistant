# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Agent Logic: Close Supervisor (Orchestrator)
# MAGIC 
# MAGIC This notebook implements the **Orchestrator Agent** (Close Supervisor) that:
# MAGIC - Monitors `close_phase_tasks` and underlying Bronze/Silver tables
# MAGIC - Advances task statuses from "pending" → "in_progress" → "completed" when prerequisites are met
# MAGIC - Writes progress logs to `close_agent_logs`
# MAGIC - Identifies blockers and notifies stakeholders (simulated)
# MAGIC 
# MAGIC **Run this notebook** continuously or on a schedule to automate close monitoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# Catalog and schema configuration
CATALOG = "financial_close_catalog"
BRONZE_SCHEMA = "bronze_layer"
SILVER_SCHEMA = "silver_layer"
GOLD_SCHEMA = "gold_layer"

# Agent configuration
AGENT_NAME = "Orchestrator Agent"
CURRENT_PERIOD = 202601

print(f"{AGENT_NAME} starting...")
print(f"Monitoring period: {CURRENT_PERIOD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def log_agent_action(period, action, target_table, target_key, status, message, execution_time_ms=0):
    """Log agent action to close_agent_logs"""
    log_data = [{
        "log_timestamp": datetime.now(),
        "period": period,
        "agent_name": AGENT_NAME,
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
    
    return log_df

def check_task_prerequisites(task_id, dependencies, tasks_df):
    """Check if task dependencies are met"""
    if not dependencies or dependencies == "":
        return True
    
    dep_list = [d.strip() for d in dependencies.split(",")]
    
    for dep_task_id in dep_list:
        dep_status = tasks_df.filter(col("task_id") == dep_task_id).select("status").first()
        if dep_status is None or dep_status[0] != "completed":
            return False
    
    return True

def check_data_availability(task_name, bu=None):
    """Check if required data is available for a task"""
    if "exchange rates" in task_name.lower():
        # Check if FX data exists
        fx_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.fx_rates_raw").count()
        return fx_count > 0
    
    elif "preliminary close" in task_name.lower() and bu:
        # Check if BU submitted preliminary close
        bu_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_pre_close_raw") \
            .filter((col("period") == CURRENT_PERIOD) & (col("bu") == bu)) \
            .count()
        return bu_count > 0
    
    elif "segmented" in task_name.lower() and bu:
        # Check if BU submitted segmented data
        seg_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_segmented_raw") \
            .filter((col("period") == CURRENT_PERIOD) & (col("bu") == bu)) \
            .count()
        return seg_count > 0
    
    elif "forecast" in task_name.lower() and bu:
        # Check if BU submitted forecast
        forecast_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bu_forecast_raw") \
            .filter(col("bu") == bu) \
            .count()
        return forecast_count > 0
    
    return True  # Default: assume data is available

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Current Tasks

# COMMAND ----------

print("Loading current tasks...")
start_time = datetime.now()

tasks_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks") \
    .filter(col("period") == CURRENT_PERIOD)

task_count = tasks_df.count()
pending_count = tasks_df.filter(col("status") == "pending").count()
in_progress_count = tasks_df.filter(col("status") == "in_progress").count()
completed_count = tasks_df.filter(col("status") == "completed").count()
blocked_count = tasks_df.filter(col("status") == "blocked").count()

print(f"✓ Loaded {task_count} tasks")
print(f"  - Pending: {pending_count}")
print(f"  - In Progress: {in_progress_count}")
print(f"  - Completed: {completed_count}")
print(f"  - Blocked: {blocked_count}")

execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
log_agent_action(
    period=CURRENT_PERIOD,
    action="load_tasks",
    target_table=f"{GOLD_SCHEMA}.close_phase_tasks",
    target_key=f"period={CURRENT_PERIOD}",
    status="success",
    message=f"Loaded {task_count} tasks for monitoring",
    execution_time_ms=execution_time
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Advance Pending Tasks

# COMMAND ----------

print("\nChecking pending tasks for advancement...")

# Get all pending tasks
pending_tasks = tasks_df.filter(col("status") == "pending").collect()

tasks_to_update = []

for task in pending_tasks:
    task_id = task["task_id"]
    task_name = task["task_name"]
    bu = task["bu"]
    dependencies = task["dependencies"]
    
    # Check prerequisites
    prereqs_met = check_task_prerequisites(task_id, dependencies, tasks_df)
    
    if not prereqs_met:
        print(f"  - {task_id}: Prerequisites not met")
        continue
    
    # Check data availability
    data_available = check_data_availability(task_name, bu)
    
    if not data_available:
        print(f"  - {task_id}: Data not available yet")
        continue
    
    # Check if task is overdue
    planned_due = task["planned_due_date"]
    is_overdue = datetime.now() > planned_due if planned_due else False
    
    # Advance to in_progress or completed based on task type
    # For automated tasks (with agent assigned), move directly to completed
    # For manual tasks, move to in_progress
    if task["agent_assigned"]:
        new_status = "completed"
        completion_time = datetime.now()
        message = f"Automated task {task_id} completed by agent"
    else:
        new_status = "in_progress"
        completion_time = None
        message = f"Task {task_id} ready to start (data available, prerequisites met)"
    
    tasks_to_update.append({
        "task_id": task_id,
        "new_status": new_status,
        "actual_start_timestamp": datetime.now(),
        "actual_completion_timestamp": completion_time,
        "message": message
    })
    
    print(f"  ✓ {task_id}: {task['status']} → {new_status}")
    
    # Log the advancement
    log_agent_action(
        period=CURRENT_PERIOD,
        action="advance_task",
        target_table=f"{GOLD_SCHEMA}.close_phase_tasks",
        target_key=task_id,
        status="success",
        message=message,
        execution_time_ms=0
    )

print(f"\n✓ Identified {len(tasks_to_update)} tasks to update")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Update Task Statuses

# COMMAND ----------

if len(tasks_to_update) > 0:
    print("Updating task statuses...")
    
    # Read current tasks
    tasks_current = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks")
    
    # Create updates DataFrame
    updates_df = spark.createDataFrame(tasks_to_update)
    
    # Join and update
    tasks_updated = tasks_current.alias("current") \
        .join(updates_df.alias("updates"), "task_id", "left") \
        .select(
            col("current.period"),
            col("current.phase_id"),
            col("current.phase_name"),
            col("current.task_id"),
            col("current.task_name"),
            col("current.bu"),
            col("current.owner_role"),
            col("current.planned_due_date"),
            coalesce(col("updates.actual_start_timestamp"), col("current.actual_start_timestamp")).alias("actual_start_timestamp"),
            coalesce(col("updates.actual_completion_timestamp"), col("current.actual_completion_timestamp")).alias("actual_completion_timestamp"),
            coalesce(col("updates.new_status"), col("current.status")).alias("status"),
            col("current.blocking_reason"),
            coalesce(col("updates.message"), col("current.comments")).alias("comments"),
            col("current.agent_assigned"),
            col("current.priority"),
            col("current.dependencies"),
            when(col("updates.task_id").isNotNull(), current_timestamp())
                .otherwise(col("current.last_updated_timestamp")).alias("last_updated_timestamp"),
            col("current.metadata")
        )
    
    # Write back to table
    tasks_updated.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks")
    
    print(f"✓ Updated {len(tasks_to_update)} task statuses")
else:
    print("No tasks to update at this time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identify Blockers and Issues

# COMMAND ----------

print("\nChecking for blockers and issues...")

# Reload tasks after updates
tasks_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_phase_tasks") \
    .filter(col("period") == CURRENT_PERIOD)

# Check for overdue tasks
overdue_tasks = tasks_df.filter(
    (col("status").isin(["pending", "in_progress"])) &
    (col("planned_due_date") < current_timestamp())
).collect()

if len(overdue_tasks) > 0:
    print(f"\n⚠ Found {len(overdue_tasks)} overdue tasks:")
    for task in overdue_tasks:
        hours_overdue = (datetime.now() - task["planned_due_date"]).total_seconds() / 3600
        print(f"  - {task['task_id']}: {task['task_name']} (overdue by {hours_overdue:.1f} hours)")
        
        # Log the issue
        log_agent_action(
            period=CURRENT_PERIOD,
            action="detect_overdue",
            target_table=f"{GOLD_SCHEMA}.close_phase_tasks",
            target_key=task["task_id"],
            status="warning",
            message=f"Task {task['task_id']} is overdue by {hours_overdue:.1f} hours",
            execution_time_ms=0
        )
else:
    print("  ✓ No overdue tasks")

# Check for explicitly blocked tasks
blocked_tasks = tasks_df.filter(col("status") == "blocked").collect()

if len(blocked_tasks) > 0:
    print(f"\n⚠ Found {len(blocked_tasks)} blocked tasks:")
    for task in blocked_tasks:
        print(f"  - {task['task_id']}: {task['task_name']} - Reason: {task['blocking_reason']}")
        
        # Log the issue
        log_agent_action(
            period=CURRENT_PERIOD,
            action="detect_blocked",
            target_table=f"{GOLD_SCHEMA}.close_phase_tasks",
            target_key=task["task_id"],
            status="error",
            message=f"Task {task['task_id']} is blocked: {task['blocking_reason']}",
            execution_time_ms=0
        )
else:
    print("  ✓ No blocked tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Update Overall Close Status

# COMMAND ----------

print("\nUpdating overall close status...")
start_time = datetime.now()

# Recalculate status metrics
status_metrics = tasks_df.groupBy("period").agg(
    max("phase_id").alias("current_phase"),
    count("*").alias("total_tasks"),
    sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_tasks"),
    sum(when(col("status") == "blocked", 1).otherwise(0)).alias("blocked_tasks"),
    sum(when(col("status") == "in_progress", 1).otherwise(0)).alias("in_progress_tasks")
).first()

completion_pct = (status_metrics["completed_tasks"] / status_metrics["total_tasks"]) * 100

# Determine overall status
if status_metrics["blocked_tasks"] > 0:
    overall_status = "issues"
elif completion_pct == 100:
    overall_status = "completed"
elif status_metrics["in_progress_tasks"] > 0:
    overall_status = "in_progress"
else:
    overall_status = "not_started"

# Get phase name
phase_names = {1: "Data Gathering", 2: "Adjustments", 3: "Data Gathering", 4: "Review", 5: "Reporting & Sign-off"}
current_phase_name = phase_names.get(status_metrics["current_phase"], "Unknown")

# Determine SLA status
days_since_period_end = (datetime.now() - datetime(2026, 1, 31)).days
sla_target_days = 10
days_to_sla = sla_target_days - days_since_period_end

if days_to_sla < 0:
    sla_status = "overdue"
elif days_to_sla < 2:
    sla_status = "at_risk"
else:
    sla_status = "on_track"

# Create status record
status_data = [{
    "period": CURRENT_PERIOD,
    "bu": "CONSOLIDATED",
    "phase_id": status_metrics["current_phase"],
    "phase_name": current_phase_name,
    "overall_status": overall_status,
    "pct_tasks_completed": round(completion_pct, 2),
    "total_tasks": status_metrics["total_tasks"],
    "completed_tasks": status_metrics["completed_tasks"],
    "blocked_tasks": status_metrics["blocked_tasks"],
    "days_since_period_end": days_since_period_end,
    "days_to_sla": days_to_sla,
    "sla_status": sla_status,
    "key_issues": f"{status_metrics['blocked_tasks']} blocked tasks" if status_metrics["blocked_tasks"] > 0 else None,
    "last_milestone": "Phase 3 data gathering completed" if status_metrics["current_phase"] >= 3 else "Trial balance standardization",
    "next_milestone": "Phase 4 review meetings" if status_metrics["current_phase"] == 3 else "Final reporting",
    "agent_summary": f"Overall close progress: {completion_pct:.1f}% complete. Currently in Phase {status_metrics['current_phase']} - {current_phase_name}. SLA status: {sla_status}.",
    "load_timestamp": datetime.now(),
    "metadata": json.dumps({"updated_by": AGENT_NAME, "automated": True})
}]

status_df = spark.createDataFrame(status_data)

# Read existing status and update or append
existing_status = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold") \
    .filter((col("period") != CURRENT_PERIOD) | (col("bu") != "CONSOLIDATED"))

updated_status = existing_status.union(status_df)

updated_status.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.close_status_gold")

execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

print(f"✓ Updated overall close status")
print(f"  - Phase: {current_phase_name}")
print(f"  - Progress: {completion_pct:.1f}%")
print(f"  - SLA Status: {sla_status}")

log_agent_action(
    period=CURRENT_PERIOD,
    action="update_status",
    target_table=f"{GOLD_SCHEMA}.close_status_gold",
    target_key=f"period={CURRENT_PERIOD},bu=CONSOLIDATED",
    status="success",
    message=f"Updated overall status: {completion_pct:.1f}% complete, SLA {sla_status}",
    execution_time_ms=execution_time
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Notifications (Simulated)

# COMMAND ----------

print("\nGenerating notifications...")

notifications = []

# Notify about overdue tasks
if len(overdue_tasks) > 0:
    for task in overdue_tasks:
        notifications.append({
            "type": "overdue_task",
            "recipient": task["owner_role"],
            "subject": f"Task Overdue: {task['task_name']}",
            "message": f"Task {task['task_id']} is overdue. Please complete or update status.",
            "priority": task["priority"]
        })

# Notify about blocked tasks
if len(blocked_tasks) > 0:
    for task in blocked_tasks:
        notifications.append({
            "type": "blocked_task",
            "recipient": "FP&A Lead",
            "subject": f"Task Blocked: {task['task_name']}",
            "message": f"Task {task['task_id']} is blocked: {task['blocking_reason']}. Intervention required.",
            "priority": "high"
        })

# Notify about SLA risk
if sla_status == "at_risk":
    notifications.append({
        "type": "sla_at_risk",
        "recipient": "FP&A Lead",
        "subject": "Close SLA At Risk",
        "message": f"Period {CURRENT_PERIOD} close is at risk of missing SLA. {days_to_sla} days remaining.",
        "priority": "high"
    })
elif sla_status == "overdue":
    notifications.append({
        "type": "sla_overdue",
        "recipient": "FP&A Lead",
        "subject": "Close SLA Overdue",
        "message": f"Period {CURRENT_PERIOD} close has exceeded SLA by {abs(days_to_sla)} days.",
        "priority": "critical"
    })

# Notify about phase completion
if completion_pct == 100:
    notifications.append({
        "type": "phase_complete",
        "recipient": "All Stakeholders",
        "subject": f"Close Phase {status_metrics['current_phase']} Complete",
        "message": f"Phase {status_metrics['current_phase']} - {current_phase_name} is complete. Ready to proceed to next phase.",
        "priority": "medium"
    })

# In a real implementation, these would be sent via email/Slack
# For now, just log them
for notif in notifications:
    print(f"  📧 [{notif['priority'].upper()}] To {notif['recipient']}: {notif['subject']}")
    
    log_agent_action(
        period=CURRENT_PERIOD,
        action="send_notification",
        target_table="notifications",
        target_key=notif["type"],
        status="success",
        message=f"Notification sent to {notif['recipient']}: {notif['subject']}",
        execution_time_ms=0
    )

if len(notifications) == 0:
    print("  ✓ No notifications needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Report

# COMMAND ----------

print("\n" + "="*60)
print(f"{AGENT_NAME} - Execution Summary")
print("="*60)

print(f"\nPeriod: {CURRENT_PERIOD}")
print(f"Execution Time: {datetime.now()}")

print(f"\nTask Status:")
print(f"  Total Tasks: {status_metrics['total_tasks']}")
print(f"  Completed: {status_metrics['completed_tasks']} ({completion_pct:.1f}%)")
print(f"  In Progress: {status_metrics['in_progress_tasks']}")
print(f"  Blocked: {status_metrics['blocked_tasks']}")
print(f"  Overdue: {len(overdue_tasks)}")

print(f"\nClose Progress:")
print(f"  Current Phase: {status_metrics['current_phase']} - {current_phase_name}")
print(f"  Overall Status: {overall_status}")
print(f"  Days Since Period End: {days_since_period_end}")
print(f"  Days to SLA: {days_to_sla}")
print(f"  SLA Status: {sla_status}")

print(f"\nActions Taken:")
print(f"  Tasks Advanced: {len(tasks_to_update)}")
print(f"  Issues Detected: {len(overdue_tasks) + len(blocked_tasks)}")
print(f"  Notifications Sent: {len(notifications)}")

print(f"\nNext Steps:")
if status_metrics['blocked_tasks'] > 0:
    print(f"  - Resolve {status_metrics['blocked_tasks']} blocked tasks")
if len(overdue_tasks) > 0:
    print(f"  - Follow up on {len(overdue_tasks)} overdue tasks")
if completion_pct < 100:
    print(f"  - Continue monitoring task progress")
    print(f"  - {status_metrics['total_tasks'] - status_metrics['completed_tasks']} tasks remaining")
else:
    print(f"  - All tasks complete! Ready for final sign-off")

print("\n" + "="*60)

# Log summary
log_agent_action(
    period=CURRENT_PERIOD,
    action="execution_summary",
    target_table="summary",
    target_key=f"execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    status="success",
    message=f"Orchestrator execution complete. Progress: {completion_pct:.1f}%, SLA: {sla_status}, Issues: {len(overdue_tasks) + len(blocked_tasks)}",
    execution_time_ms=0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✓ {AGENT_NAME} Execution Complete!

The Orchestrator Agent has:
- Monitored all tasks for period {CURRENT_PERIOD}
- Advanced {len(tasks_to_update)} tasks based on prerequisites and data availability
- Detected {len(overdue_tasks)} overdue tasks and {len(blocked_tasks)} blocked tasks
- Updated overall close status to {overall_status}
- Generated {len(notifications)} notifications for stakeholders

Current close status: {completion_pct:.1f}% complete, SLA status: {sla_status}

This notebook can be:
- Scheduled to run every hour via Databricks Workflows
- Triggered on-demand when investigating close progress
- Integrated with monitoring dashboards for real-time status

All actions logged to: {CATALOG}.{GOLD_SCHEMA}.close_agent_logs
""")
