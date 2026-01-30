# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Close - Close Supervisor Agent
# MAGIC 
# MAGIC **Purpose:** Orchestrate the entire financial close process across all phases
# MAGIC 
# MAGIC **Responsibilities:**
# MAGIC - Monitor `close_status_gold` for task completion
# MAGIC - Enforce dependencies between tasks and phases
# MAGIC - Auto-transition task statuses when conditions are met
# MAGIC - Log all orchestration decisions for audit trail
# MAGIC - Alert on blocked or overdue tasks
# MAGIC 
# MAGIC **Execution:** Can run hourly via Databricks workflow
# MAGIC 
# MAGIC **Agent Type:** Orchestrator

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
AGENT_NAME = "close_supervisor"
RUN_ID = str(uuid.uuid4())[:8]

print(f"🤖 Close Supervisor Agent Starting")
print(f"   Run ID: {RUN_ID}")
print(f"   Period: {CURRENT_PERIOD}")
print(f"   Timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Current Close Status

# COMMAND ----------

# Load current status for the period
close_status = (
    spark.table("gold.close_status_gold")
    .filter(F.col("period") == CURRENT_PERIOD)
)

total_tasks = close_status.count()
completed_tasks = close_status.filter(F.col("status") == "completed").count()
completion_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0

print(f"\n📊 Close Status Overview:")
print(f"   Total tasks: {total_tasks}")
print(f"   Completed: {completed_tasks}")
print(f"   Completion: {completion_pct:.1f}%")

# Show status by phase
status_by_phase = (
    close_status
    .groupBy("phase_name", "status")
    .count()
    .orderBy("phase_name")
)

print("\nStatus by Phase:")
display(status_by_phase)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Phase and Task Dependencies

# COMMAND ----------

# Load phase definitions with dependencies
phase_defs = spark.table("config.close_phase_definitions")

# Create dependency map
dependency_map = {}
for row in phase_defs.collect():
    dependency_map[row.task_id] = {
        "task_name": row.task_name,
        "phase_id": row.phase_id,
        "depends_on": row.depends_on_tasks,
        "is_bu_specific": row.is_bu_specific
    }

print(f"✓ Loaded {len(dependency_map)} task definitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Dependencies and Transition Tasks

# COMMAND ----------

def log_agent_action(action_type, task_id, bu_code, decision, input_data, output_data, status="success"):
    """Helper to log agent decisions"""
    log_record = {
        "log_id": str(uuid.uuid4()),
        "log_timestamp": datetime.now(),
        "agent_name": AGENT_NAME,
        "action_type": action_type,
        "period": CURRENT_PERIOD,
        "task_id": task_id,
        "bu_code": bu_code,
        "decision_rationale": decision,
        "input_data": json.dumps(input_data),
        "output_data": json.dumps(output_data),
        "status": status,
        "execution_time_ms": 0  # Would be calculated in production
    }
    
    return log_record

# COMMAND ----------

# Check each pending task to see if dependencies are met
pending_tasks = (
    close_status
    .filter(F.col("status").isin(["pending", "in_progress"]))
    .collect()
)

actions_taken = []
logs_to_write = []

print(f"\n🔍 Checking {len(pending_tasks)} pending/in-progress tasks...\n")

for task in pending_tasks:
    task_def = dependency_map.get(task.task_id, {})
    depends_on = task_def.get("depends_on", [])
    
    # Check if all dependencies are completed
    if depends_on:
        if task.bu_code:
            # BU-specific task - check dependencies for same BU
            dependencies_met = True
            for dep_task_id in depends_on:
                dep_status = (
                    close_status
                    .filter(F.col("task_id") == dep_task_id)
                    .filter(F.col("bu_code") == task.bu_code)
                    .select("status")
                    .first()
                )
                
                if not dep_status or dep_status.status != "completed":
                    dependencies_met = False
                    break
        else:
            # Group-level task - check dependencies (could be group or all BUs)
            dependencies_met = True
            for dep_task_id in depends_on:
                dep_task_def = dependency_map.get(dep_task_id, {})
                
                if dep_task_def.get("is_bu_specific"):
                    # Must check all BUs have completed
                    incomplete_bus = (
                        close_status
                        .filter(F.col("task_id") == dep_task_id)
                        .filter(F.col("status") != "completed")
                        .count()
                    )
                    
                    if incomplete_bus > 0:
                        dependencies_met = False
                        break
                else:
                    # Group-level dependency
                    dep_status = (
                        close_status
                        .filter(F.col("task_id") == dep_task_id)
                        .select("status")
                        .first()
                    )
                    
                    if not dep_status or dep_status.status != "completed":
                        dependencies_met = False
                        break
        
        # Transition task if dependencies met
        if dependencies_met and task.status == "pending":
            print(f"✓ Task {task.task_id} ({task.task_name}) - Dependencies met, transitioning to in_progress")
            
            actions_taken.append({
                "period": CURRENT_PERIOD,
                "task_id": task.task_id,
                "bu_code": task.bu_code,
                "old_status": "pending",
                "new_status": "in_progress",
                "reason": f"All {len(depends_on)} dependencies completed"
            })
            
            # Log action
            logs_to_write.append(
                log_agent_action(
                    action_type="status_update",
                    task_id=task.task_id,
                    bu_code=task.bu_code,
                    decision=f"Transitioned to in_progress - dependencies {depends_on} are completed",
                    input_data={"current_status": "pending", "depends_on": depends_on},
                    output_data={"new_status": "in_progress", "dependencies_met": True}
                )
            )
        elif not dependencies_met:
            print(f"⏳ Task {task.task_id} ({task.task_name}) - Waiting on dependencies: {depends_on}")
            
            # Log that we checked but dependencies not met
            logs_to_write.append(
                log_agent_action(
                    action_type="dependency_check",
                    task_id=task.task_id,
                    bu_code=task.bu_code,
                    decision=f"Dependencies not yet met: {depends_on}",
                    input_data={"current_status": task.status, "depends_on": depends_on},
                    output_data={"action": "none", "dependencies_met": False}
                )
            )
    else:
        # No dependencies - task should be in_progress or completed already
        if task.status == "pending":
            print(f"✓ Task {task.task_id} ({task.task_name}) - No dependencies, can start immediately")

print(f"\n📝 Actions to take: {len(actions_taken)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check for Overdue Tasks

# COMMAND ----------

# Find overdue tasks (planned due date in past, not completed)
overdue_tasks = (
    close_status
    .filter(F.col("status") != "completed")
    .filter(F.col("planned_due_date") < F.current_date())
    .withColumn("days_overdue", F.datediff(F.current_date(), F.col("planned_due_date")))
    .orderBy(F.desc("days_overdue"))
)

overdue_count = overdue_tasks.count()

if overdue_count > 0:
    print(f"\n⚠️  {overdue_count} OVERDUE TASKS DETECTED:\n")
    
    for task in overdue_tasks.collect():
        print(f"   - Task {task.task_id} ({task.task_name})")
        print(f"     BU: {task.bu_code or 'Group'}, Owner: {task.owner_role}")
        print(f"     Days overdue: {task.days_overdue}")
        
        # Log alert
        logs_to_write.append(
            log_agent_action(
                action_type="alert",
                task_id=task.task_id,
                bu_code=task.bu_code,
                decision=f"Task is {task.days_overdue} days overdue - alerting owner {task.owner_role}",
                input_data={"planned_due_date": str(task.planned_due_date), "current_status": task.status},
                output_data={"alert_type": "overdue", "days_overdue": task.days_overdue},
                status="warning"
            )
        )
else:
    print(f"\n✓ No overdue tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check Phase Completion

# COMMAND ----------

# Determine which phases are complete
phase_completion = (
    close_status
    .groupBy("phase_id", "phase_name")
    .agg(
        F.count("*").alias("total_tasks"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_tasks")
    )
    .withColumn("is_complete", F.col("total_tasks") == F.col("completed_tasks"))
    .orderBy("phase_id")
)

print("\n📊 Phase Completion Status:\n")
for phase in phase_completion.collect():
    status_icon = "✅" if phase.is_complete else "🔄"
    print(f"{status_icon} Phase {phase.phase_id} - {phase.phase_name}")
    print(f"   Progress: {phase.completed_tasks}/{phase.total_tasks} tasks")
    
    if phase.is_complete:
        # Log phase completion
        logs_to_write.append(
            log_agent_action(
                action_type="phase_completion",
                task_id=None,
                bu_code=None,
                decision=f"Phase {phase.phase_id} ({phase.phase_name}) is 100% complete",
                input_data={"phase_id": phase.phase_id, "total_tasks": phase.total_tasks},
                output_data={"completed_tasks": phase.completed_tasks, "phase_complete": True}
            )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Update Close Status and Write Logs

# COMMAND ----------

# Apply status transitions
if actions_taken:
    print(f"\n🔧 Applying {len(actions_taken)} status updates...")
    
    for action in actions_taken:
        # Update status in Gold table
        spark.sql(f"""
            UPDATE gold.close_status_gold
            SET status = '{action['new_status']}',
                comments = CONCAT(COALESCE(comments, ''), '\\n[{datetime.now()}] Supervisor: {action['reason']}'),
                last_updated_by = '{AGENT_NAME}',
                updated_at = current_timestamp()
            WHERE period = '{action['period']}'
              AND task_id = {action['task_id']}
              AND {'bu_code = "' + action['bu_code'] + '"' if action['bu_code'] else 'bu_code IS NULL'}
        """)
    
    print("✓ Status updates applied")
else:
    print("\nℹ️  No status updates needed")

# COMMAND ----------

# Write agent logs
if logs_to_write:
    logs_df = spark.createDataFrame(logs_to_write)
    logs_df.write.mode("append").saveAsTable("gold.close_agent_logs")
    
    print(f"✓ Wrote {len(logs_to_write)} log entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generate Executive Summary

# COMMAND ----------

# Calculate overall close metrics
close_metrics = {
    "period": CURRENT_PERIOD,
    "run_timestamp": datetime.now().isoformat(),
    "total_tasks": total_tasks,
    "completed_tasks": completed_tasks,
    "completion_pct": round(completion_pct, 1),
    "overdue_tasks": overdue_count,
    "phases_complete": phase_completion.filter(F.col("is_complete")).count(),
    "total_phases": phase_completion.count(),
    "actions_taken": len(actions_taken),
    "alerts_raised": len([log for log in logs_to_write if log["status"] == "warning"])
}

print("\n" + "="*80)
print("CLOSE SUPERVISOR AGENT - EXECUTION SUMMARY")
print("="*80)
print(f"\nPeriod: {close_metrics['period']}")
print(f"Run: {RUN_ID} at {close_metrics['run_timestamp']}")
print(f"\n📊 Progress:")
print(f"   Completed: {close_metrics['completed_tasks']}/{close_metrics['total_tasks']} tasks ({close_metrics['completion_pct']}%)")
print(f"   Phases complete: {close_metrics['phases_complete']}/{close_metrics['total_phases']}")
print(f"\n🔧 Actions:")
print(f"   Status transitions: {close_metrics['actions_taken']}")
print(f"   Alerts raised: {close_metrics['alerts_raised']}")
print(f"   Overdue tasks: {close_metrics['overdue_tasks']}")

# Update agent configuration with last run time
spark.sql(f"""
    UPDATE config.agent_configuration
    SET last_run_timestamp = current_timestamp(),
        updated_at = current_timestamp()
    WHERE agent_name = '{AGENT_NAME}'
""")

print(f"\n✓ Agent configuration updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Determine Next Actions

# COMMAND ----------

# Provide recommendations for next steps
recommendations = []

# Check if Phase 4 is ready to start
phase3_complete = phase_completion.filter(F.col("phase_id") == 3).first()
phase4_started = close_status.filter((F.col("phase_id") == 4) & (F.col("status") != "pending")).count()

if phase3_complete and phase3_complete.is_complete and phase4_started == 0:
    recommendations.append("✅ Phase 3 complete - Ready to start Phase 4 review meetings")

# Check if Phase 5 is ready
phase4_complete = phase_completion.filter(F.col("phase_id") == 4).first()
if phase4_complete and phase4_complete.is_complete:
    recommendations.append("✅ Phase 4 complete - Ready to publish final results (Phase 5)")

# Check if any phases are stuck
stuck_phases = phase_completion.filter(
    (F.col("is_complete") == False) & 
    (F.col("phase_id") < 5)
).collect()

for phase in stuck_phases:
    if phase.completed_tasks / phase.total_tasks < 0.5:
        recommendations.append(f"⚠️  Phase {phase.phase_id} ({phase.phase_name}) is <50% complete - may need attention")

# Overdue tasks
if overdue_count > 0:
    recommendations.append(f"🔴 {overdue_count} overdue tasks require immediate attention")

print("\n💡 Recommendations:\n")
if recommendations:
    for rec in recommendations:
        print(f"   {rec}")
else:
    print("   All tasks progressing as planned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Trigger Downstream Agents (if needed)

# COMMAND ----------

# Determine which domain agents should run based on current state
agents_to_trigger = []

# If Phase 1 or 2 has new data, trigger FX and Pre-Close agents
phase1_incomplete = close_status.filter((F.col("phase_id") <= 2) & (F.col("status") != "completed")).count()
if phase1_incomplete == 0:
    agents_to_trigger.append("fx_agent")
    agents_to_trigger.append("pre_close_agent")

# If Phase 3 has new data, trigger Segmented & Forecast agent
phase3_incomplete = close_status.filter((F.col("phase_id") == 3) & (F.col("status") != "completed")).count()
if phase3_incomplete == 0:
    agents_to_trigger.append("segmented_forecast_agent")

# If all phases complete, trigger Reporting agent
all_complete = close_status.filter(F.col("status") != "completed").count()
if all_complete == 0:
    agents_to_trigger.append("reporting_agent")

if agents_to_trigger:
    print(f"\n🤖 Downstream agents to trigger:")
    for agent in agents_to_trigger:
        print(f"   - {agent}")
    
    # Log trigger decisions
    for agent in agents_to_trigger:
        logs_to_write.append(
            log_agent_action(
                action_type="trigger_agent",
                task_id=None,
                bu_code=None,
                decision=f"Triggering {agent} based on phase completion status",
                input_data={"triggered_by": AGENT_NAME, "run_id": RUN_ID},
                output_data={"agent_triggered": agent, "trigger_time": datetime.now().isoformat()}
            )
        )
    
    # Write trigger logs
    if logs_to_write:
        logs_df = spark.createDataFrame(logs_to_write)
        logs_df.write.mode("append").saveAsTable("gold.close_agent_logs")
else:
    print("\nℹ️  No downstream agents need to be triggered at this time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Close Supervisor Agent Complete
# MAGIC 
# MAGIC **Summary:**
# MAGIC - Monitored close progress across all phases
# MAGIC - Checked task dependencies and transitioned statuses
# MAGIC - Identified overdue tasks and raised alerts
# MAGIC - Logged all decisions for audit trail
# MAGIC - Provided recommendations for next steps
# MAGIC 
# MAGIC **Scheduling:**
# MAGIC - Run this notebook hourly during close period via Databricks Workflow
# MAGIC - Agent will automatically orchestrate task progression
# MAGIC - Alerts sent to FP&A team for blocked/overdue tasks
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Schedule as recurring Databricks Job (hourly)
# MAGIC 2. Configure email alerts for overdue tasks
# MAGIC 3. Run domain agents (notebooks 06-07) based on triggers
