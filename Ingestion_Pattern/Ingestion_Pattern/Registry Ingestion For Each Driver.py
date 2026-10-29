# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Registry Ingestion For Each Driver
# MAGIC
# MAGIC First task of the Lakeflow Job: lists the active `pipeline_id`s from `governance_demo.control_plane.ctl_ingestion_registry` and emits them as a task value, which the job's **For Each** task consumes — fanning out one parameterized run of the child notebook per pipeline.

# COMMAND ----------

# DBTITLE 1,List Active Pipelines
# ==============================================================================
# LIST ACTIVE PIPELINES + EMIT TASK VALUE FOR THE FOR EACH TASK
# ==============================================================================
pipeline_ids = sorted(
    row["pipeline_id"]
    for row in spark.sql("""
        SELECT pipeline_id
        FROM governance_demo.control_plane.ctl_ingestion_registry
        WHERE is_active = true
    """).collect()
)

print(f"{len(pipeline_ids)} active pipelines:")
for pid in pipeline_ids:
    print(f"  - {pid}")

# For Each inputs: emit the list as a task value (available in job runs)
try:
    dbutils.jobs.taskValues.set(key="pipeline_ids", value=pipeline_ids)
    print("\nEmitted task value 'pipeline_ids' for the For Each task.")
except Exception as e:
    print(f"\n(Interactive run — taskValues not available: {e})")