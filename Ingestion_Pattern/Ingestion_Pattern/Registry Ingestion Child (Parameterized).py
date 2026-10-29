# Databricks notebook source
dbutils.widgets.text("pipeline_id", "*")

pipeline_id = dbutils.widgets.get("pipeline_id")

print(f"pipeline_id parameter = {pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Load Registry Configs

configs = spark.sql(f"""
    SELECT pipeline_id, entity_name, ingestion_method, source_path, source_format,
           format_options, target_catalog, target_schema, target_table,
           primary_key, cluster_by_cols, schema_evolution_mode,
           schema_location, checkpoint_location, dq_enabled
    FROM governance_demo.control_plane.ctl_ingestion_registry
    WHERE is_active = true
      AND (pipeline_id = '{pipeline_id}')
""").collect()

print(configs)

assert configs, f"No active registry entry found for pipeline_id = '{pipeline_id}'"
print(f"Selected {len(configs)} pipeline config(s): {[c['pipeline_id'] for c in configs]}")

# Extract the single config + its registered method so each use-case cell can guard itself
config = configs[0].asDict()
method = config["ingestion_method"]
run_result = None  # filled in by the use-case cell matching this method
print(f"Loaded config: {config['pipeline_id']} → method = {method}\n")

# COMMAND ----------

# DBTITLE 1,Use Case 1: LAKEFLOW Batch
from pyspark.sql.functions import current_timestamp, col

def ingest_lakeflow_batch(config):
    pipeline_id   = config["pipeline_id"]
    full_table    = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
    source_path   = config["source_path"]
    source_format = config["source_format"]
    format_opts   = dict(config["format_options"]) if config["format_options"] else {}

    print(f"[{pipeline_id}] {config['entity_name']}: {source_path} → {full_table}")

    reader = spark.read.format(source_format)
    for key, value in format_opts.items():
        reader = reader.option(key, value)

    df = (reader
          .load(source_path)
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingested_at", current_timestamp()))

    (df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(full_table))

    row_count = spark.table(full_table).count()
    print(f"  → {row_count} rows ingested\n")
    return {"pipeline_id": pipeline_id, "table": full_table, "rows": row_count}

# Run ONLY if this pipeline's registered method is LAKEFLOW
if method == "LAKEFLOW":
    run_result = ingest_lakeflow_batch(config)
else:
    print("(skipped — registered method is not LAKEFLOW)")

# COMMAND ----------

# DBTITLE 1,Use Case 4: Auto Loader
# ==============================================================================
# USE CASE 4: AUTO LOADER — STREAMING / INCREMENTAL BATCH WITH ROCKSDB STATE
# (same code as the monolithic demo — now driven by a single registry entry)
# ==============================================================================
def ingest_autoloader(config):
    pipeline_id   = config["pipeline_id"]
    full_table    = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
    source_path   = config["source_path"]
    source_format = config["source_format"]
    format_opts   = dict(config["format_options"]) if config["format_options"] else {}
    schema_loc    = config["schema_location"]
    checkpoint_loc = config["checkpoint_location"]
    schema_evol   = config["schema_evolution_mode"] or "addNewColumns"


    reader = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", source_format)
              .option("cloudFiles.schemaLocation", schema_loc)
              .option("cloudFiles.schemaEvolutionMode", schema_evol)
              .option("cloudFiles.inferColumnTypes", "true"))


    for key, value in format_opts.items():
        if key != "inferSchema":
            reader = reader.option(key, value)

    df = (reader
          .load(source_path)
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingested_at", current_timestamp()))

    # Stream write — availableNow trigger processes new files then stops
    stream = (df.writeStream
     .format("delta")
     .option("checkpointLocation", checkpoint_loc)
     .option("mergeSchema", "true")
     .trigger(availableNow=True)
     .toTable(full_table))
    stream.awaitTermination()

    row_count = spark.table(full_table).count()
    print(f"  → {row_count} rows ingested\n")
    return {"pipeline_id": pipeline_id, "table": full_table, "rows": row_count}

# Run ONLY if this pipeline's registered method is AUTOLOADER
if method == "AUTOLOADER":
    run_result = ingest_autoloader(config)
else:
    print("(skipped — registered method is not AUTOLOADER)")

# COMMAND ----------

# DBTITLE 0,Summary
# MAGIC %skip
# MAGIC # ==============================================================================
# MAGIC # DISPATCH — run the ingestion method registered for each config
# MAGIC # ==============================================================================
# MAGIC # Exactly ONE use-case cell above ran — the one matching the registry's ingestion_method
# MAGIC assert run_result is not None, (
# MAGIC     f"No ingestion ran for pipeline_id = '{pipeline_id}' "
# MAGIC     f"(ingestion_method = '{method}' is not LAKEFLOW / CTAS / COPY_INTO / AUTOLOADER)"
# MAGIC )
# MAGIC
# MAGIC print("=" * 60)
# MAGIC print(f"INGESTION SUMMARY (parameterized run) — {run_result['pipeline_id']} [{method}]")
# MAGIC print("=" * 60)
# MAGIC print(f"  {run_result['rows']} rows → {run_result['table']}")