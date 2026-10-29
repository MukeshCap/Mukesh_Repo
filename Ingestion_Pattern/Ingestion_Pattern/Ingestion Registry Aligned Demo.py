# Databricks notebook source
# DBTITLE 1,Demo Overview
# MAGIC %md
# MAGIC # Metadata-Driven Ingestion Framework — Demo
# MAGIC
# MAGIC This notebook demonstrates **four production ingestion patterns**, all driven by a single control-plane registry table (`ctl_ingestion_registry`).
# MAGIC
# MAGIC ## Use Case Overview
# MAGIC
# MAGIC | # | Use Case | Method | Pattern | Incremental | Idempotent |
# MAGIC |---|----------|--------|---------|------------|------------|
# MAGIC | 1 | Lakeflow Connect | `LAKEFLOW` | Metadata-driven batch Spark (`spark.read` + `saveAsTable` overwrite) | No (full refresh) | Yes (overwrite is deterministic) |
# MAGIC | 2 | CTAS | `CTAS` | `CREATE OR REPLACE TABLE AS SELECT` via `read_files` | No (full refresh) | Yes (replaces entire table) |
# MAGIC | 3 | COPY INTO | `COPY_INTO` | SQL idempotent incremental batch | Yes (tracks loaded files) | Yes (file-level dedup via Delta txn log) |
# MAGIC | 4 | Auto Loader | `AUTOLOADER` | `cloudFiles` streaming with `trigger(availableNow=True)` | Yes (checkpoint-based) | Yes (checkpoint tracks processed files) |
# MAGIC
# MAGIC ## Auto Loader Trigger Types
# MAGIC
# MAGIC | Trigger | Behavior | Best For |
# MAGIC |---------|----------|----------|
# MAGIC | `trigger(availableNow=True)` | Processes **all new files** since last checkpoint in one batch, then stops | Scheduled jobs, batch-style incremental loads (used in this demo) |
# MAGIC | `trigger(processingTime="10s")` | Micro-batch every N seconds — keeps streaming | Near-real-time dashboards, continuous ingestion |
# MAGIC | `trigger(once=True)` | Single micro-batch then stops — **deprecated**, use `availableNow` instead | Legacy one-shot loads |
# MAGIC | `trigger(continuous=True)` | Continuous low-latency mode (~1ms) — limited operator support | Ultra-low-latency streaming pipelines |
# MAGIC
# MAGIC ## Rescue Columns (Automatic)
# MAGIC
# MAGIC When Auto Loader's `schemaEvolutionMode` is set to `addNewColumns` (the default) or `rescue`, a **`_rescued_data`** column is **automatically** appended to the schema.
# MAGIC
# MAGIC - Captures any data that doesn't match the inferred schema — extra columns, type mismatches, malformed fields
# MAGIC - Acts as a safety net during schema evolution: **no data is lost** when the source adds new columns
# MAGIC - In `addNewColumns` mode: new columns are **promoted** to top-level on the next micro-batch; `_rescued_data` still captures anything that fails to parse
# MAGIC - In `rescue` mode: all unknown columns **stay** in `_rescued_data` and are NOT promoted to the schema
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Orchestration Pattern — Workflow For Each
# MAGIC %md
# MAGIC ## Orchestration Pattern — Workflow For Each + Parameterized Notebook
# MAGIC
# MAGIC The cells below demo each ingestion method monolithically (all pipelines in one session). The **production-corrected pattern** moves this to a Lakeflow Job:
# MAGIC
# MAGIC | Component | Asset | Role |
# MAGIC |---|---|---|
# MAGIC | Task 1 — Driver | `Registry Ingestion For Each Driver` notebook | Reads `ctl_ingestion_registry`, emits active `pipeline_id`s as a task value |
# MAGIC | Task 2 — For Each | For Each task (concurrency N) | Fans out one iteration per registry entry |
# MAGIC | Inner task — Child | `Registry Ingestion Child (Parameterized)` notebook | Runs ONE pipeline per iteration via the `pipeline_id` job parameter, writes its **bronze Delta table**, and validates source vs bronze row counts |
# MAGIC
# MAGIC **Why this is better than the monolithic loop:**
# MAGIC
# MAGIC * **Isolation & retry** — a failed pipeline (e.g. one bad source file) can be retried alone; others keep their results
# MAGIC * **Parallelism** — For Each runs up to N pipelines concurrently instead of a sequential `for` loop
# MAGIC * **Dynamic fan-out** — add a row to `ctl_ingestion_registry` and the job picks it up; no code change
# MAGIC * **One parameterized code path** — the child notebook dispatches on `ingestion_method` (`LAKEFLOW`, `CTAS`, `COPY_INTO`, `AUTOLOADER`) using the same ingestion functions from this notebook
# MAGIC
# MAGIC **Interactive fallback:** running the child notebook with `pipeline_id = '*'` (default) executes ALL active registry entries in one session — the behavior of this notebook's cells below.

# COMMAND ----------

# DBTITLE 1,Idempotency & Nuance Details
# MAGIC %md
# MAGIC ## Idempotency & Nuance Details
# MAGIC
# MAGIC ### USE CASE 1 — Lakeflow Connect (Batch)
# MAGIC - **Idempotency**: Uses `mode("overwrite")` — re-running produces the same result ( full refresh)
# MAGIC - **Nuance**: Simplest pattern. Reads all files, overwrites the target. No incremental capability. Best for small datasets or reference data that changes in full.
# MAGIC - **Trade-off**: Full re-read every run. No file-level tracking.
# MAGIC
# MAGIC ### USE CASE 2 — CTAS
# MAGIC - **Idempotency**: `CREATE OR REPLACE TABLE` drops and recreates the table each run — 
# MAGIC - **Nuance**: Pure SQL, no Python. Good for reference/dimension tables and one-time backfills. But loses table history (no Delta log continuity).
# MAGIC - **Trade-off**: Cannot do incremental loads. Every run is a full refresh.
# MAGIC
# MAGIC ### USE CASE 3 — COPY INTO
# MAGIC - **Idempotency**: Tracks loaded files in the Delta transaction log. Re-running the same COPY INTO will **not** re-load already-processed files — true file-level idempotency.
# MAGIC - **Nuance**: SQL-only, no Python. Good for scheduled incremental batch loads. But performance degrades with thousands of files (uses directory listing, not file notification).
# MAGIC - **Trade-off**: Limited schema evolution compared to Auto Loader. Cannot handle schema changes as gracefully.
# MAGIC
# MAGIC ### USE CASE 4 — Auto Loader
# MAGIC - **Idempotency**: Checkpoint-based. Re-running with `availableNow` will only pick up files not yet seen.
# MAGIC - **Nuance**: Best for streaming and large-scale file ingestion. Uses file notification (efficient for millions of files). Automatic schema inference and evolution with rescue columns. Requires checkpoint and schema location management.
# MAGIC - **Trade-off**: More setup (checkpoint + schema dirs). Streaming overhead even in batch mode. But the most production-ready for ongoing incremental ingestion.
# MAGIC
# MAGIC ## Source Data
# MAGIC
# MAGIC - **Catalog / Schema**: `governance_demo.bronze`
# MAGIC - **Customer CSV** (`/Volumes/governance_demo/landing/raw_data_csv/customer/`): 322 rows, 8 columns
# MAGIC - **Order CSV** (`/Volumes/governance_demo/landing/raw_data_csv/order/`): 664 rows, 8 columns
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG governance_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS control_plane;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- 1. ENTERPRISE INGESTION REGISTRY TABLE
# MAGIC -- ==============================================================================
# MAGIC CREATE OR REPLACE TABLE governance_demo.control_plane.ctl_ingestion_registry (
# MAGIC   pipeline_id STRING NOT NULL,
# MAGIC   source_system STRING,
# MAGIC   entity_name STRING,                  -- 'customer' or 'order'
# MAGIC   ingestion_method STRING,             -- 'LAKEFLOW', 'CTAS', 'COPY_INTO', 'AUTOLOADER'
# MAGIC   source_path STRING,                  -- UC Volume path
# MAGIC   source_format STRING,                -- 'csv', 'json', 'parquet'
# MAGIC   format_options MAP<STRING, STRING>,  -- Dynamic reader options (header, inferSchema, delimiter)
# MAGIC   target_catalog STRING,
# MAGIC   target_schema STRING,
# MAGIC   target_table STRING,
# MAGIC   primary_key STRING,
# MAGIC   cluster_by_cols STRING,              -- Liquid Clustering column(s)
# MAGIC   schema_evolution_mode STRING,        -- 'addNewColumns', 'rescue', 'failOnNewColumns'
# MAGIC   schema_location STRING,              -- Required for Auto Loader
# MAGIC   checkpoint_location STRING,          -- Required for Auto Loader
# MAGIC   dq_enabled BOOLEAN,                  -- Toggle to invoke your DQ Engine
# MAGIC   is_active BOOLEAN,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (ingestion_method, entity_name);
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- 2. POPULATE REGISTRY FOR ALL 4 DEMO USE CASES (CUSTOMER & ORDER)
# MAGIC -- ==============================================================================
# MAGIC INSERT INTO governance_demo.control_plane.ctl_ingestion_registry VALUES
# MAGIC   -- USE CASE 1: LAKEFLOW CONNECT / DECLARATIVE PIPELINE
# MAGIC   ('LF_CUST_01', 'CRM', 'customer', 'LAKEFLOW',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/customer', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_customer_lakeflow',
# MAGIC    'customer_id', 'customer_id', 'addNewColumns',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   ('LF_ORD_01', 'Web_Portal', 'order', 'LAKEFLOW',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/order', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_order_lakeflow',
# MAGIC    'order_id', 'order_date', 'addNewColumns',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   -- USE CASE 2: CTAS (FULL REFRESH VIA read_files)
# MAGIC   ('CTAS_CUST_02', 'CRM', 'customer', 'CTAS',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/customer', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_customer_ctas',
# MAGIC    'customer_id', 'customer_id', 'none',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   ('CTAS_ORD_02', 'Web_Portal', 'order', 'CTAS',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/order', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_order_ctas',
# MAGIC    'order_id', 'order_date', 'none',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   -- USE CASE 3: COPY INTO (IDEMPOTENT INCREMENTAL SQL BATCH)
# MAGIC   ('COPY_CUST_03', 'CRM', 'customer', 'COPY_INTO',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/customer', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true', 'mergeSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_customer_copyinto',
# MAGIC    'customer_id', 'customer_id', 'addNewColumns',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   ('COPY_ORD_03', 'Web_Portal', 'order', 'COPY_INTO',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/order', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true', 'mergeSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_order_copyinto',
# MAGIC    'order_id', 'order_date', 'addNewColumns',
# MAGIC    NULL, NULL, true, true, current_timestamp()),
# MAGIC
# MAGIC   -- USE CASE 4: AUTO LOADER (STREAMING / INCREMENTAL BATCH WITH ROCKSDB STATE)
# MAGIC   ('AL_CUST_04', 'CRM', 'customer', 'AUTOLOADER',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/customer', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_customer_autoloader',
# MAGIC    'customer_id', 'customer_id', 'addNewColumns',
# MAGIC    '/Volumes/governance_demo/landing/_schemas/al_customer',
# MAGIC    '/Volumes/governance_demo/landing/_checkpoints/al_customer',
# MAGIC    true, true, current_timestamp()),
# MAGIC
# MAGIC   ('AL_ORD_04', 'Web_Portal', 'order', 'AUTOLOADER',
# MAGIC    '/Volumes/governance_demo/landing/raw_data_csv/order', 'csv',
# MAGIC    map('header', 'true', 'inferSchema', 'true'),
# MAGIC    'governance_demo', 'bronze', 'bronze_order_autoloader',
# MAGIC    'order_id', 'order_date', 'addNewColumns',
# MAGIC    '/Volumes/governance_demo/landing/_schemas/al_order',
# MAGIC    '/Volumes/governance_demo/landing/_checkpoints/al_order',
# MAGIC    true, true, current_timestamp());
# MAGIC
# MAGIC SELECT pipeline_id, entity_name, ingestion_method, target_table, source_path 
# MAGIC FROM governance_demo.control_plane.ctl_ingestion_registry;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM governance_demo.control_plane.ctl_ingestion_registry;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS governance_demo.bronze.bronze_customer_copyinto (
# MAGIC   customer_id        STRING,
# MAGIC   customer_name      STRING,
# MAGIC   email              STRING,
# MAGIC   phone              STRING,
# MAGIC   city               STRING,
# MAGIC   country_code       STRING,
# MAGIC   credit_balance     STRING,
# MAGIC   source_system      STRING,
# MAGIC   _source_file       STRING,
# MAGIC   _ingested_at       TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (customer_id);
# MAGIC
# MAGIC --- 
# MAGIC
# MAGIC COPY INTO governance_demo.bronze.bronze_customer_copyinto
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     customer_id::STRING,
# MAGIC     customer_name::STRING,
# MAGIC     email::STRING,
# MAGIC     phone::STRING,
# MAGIC     city::STRING,
# MAGIC     country_code::STRING,
# MAGIC     credit_balance::STRING,
# MAGIC     source_system::STRING,
# MAGIC     _metadata.file_path AS _source_file,
# MAGIC     current_timestamp() AS _ingested_at
# MAGIC   FROM '/Volumes/governance_demo/landing/raw_data_csv/customer/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from governance_demo.bronze.bronze_customer_copyinto

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from governance_demo.bronze.bronze_customer_copyinto

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Delete From governance_demo.bronze.bronze_customer_copyinto Where customer_id  = 'CUST-1008'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY governance_demo.bronze.bronze_customer_copyinto;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO governance_demo.bronze.bronze_customer_copyinto
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     customer_id::STRING,
# MAGIC     customer_name::STRING,
# MAGIC     email::STRING,
# MAGIC     phone::STRING,
# MAGIC     city::STRING,
# MAGIC     country_code::STRING,
# MAGIC     credit_balance::STRING,
# MAGIC     source_system::STRING,
# MAGIC     _metadata.file_path AS _source_file,
# MAGIC     current_timestamp() AS _ingested_at
# MAGIC   FROM '/Volumes/governance_demo/landing/raw_data_csv/customer/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if Exists governance_demo.bronze.bronze_customer_copyinto

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace Table governance_demo.bronze.bronze_customer_copyinto (
# MAGIC   customer_id        STRING,
# MAGIC   customer_name      STRING,
# MAGIC   email              STRING,
# MAGIC   phone              STRING,
# MAGIC   city               STRING,
# MAGIC   country_code       STRING,
# MAGIC   credit_balance     STRING,
# MAGIC   source_system      STRING,
# MAGIC   _source_file       STRING,
# MAGIC   _ingested_at       TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (customer_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO governance_demo.bronze.bronze_customer_copyinto
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     customer_id::STRING,
# MAGIC     customer_name::STRING,
# MAGIC     email::STRING,
# MAGIC     phone::STRING,
# MAGIC     city::STRING,
# MAGIC     country_code::STRING,
# MAGIC     credit_balance::STRING,
# MAGIC     source_system::STRING,
# MAGIC     _metadata.file_path AS _source_file,
# MAGIC     current_timestamp() AS _ingested_at
# MAGIC   FROM '/Volumes/governance_demo/landing/raw_data_csv/customer/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true'
# MAGIC 'force' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE governance_demo.bronze.bronze_customer_ctas AS
# MAGIC SELECT
# MAGIC   customer_id::STRING        AS customer_id,
# MAGIC   customer_name::STRING      AS customer_name,
# MAGIC   email::STRING              AS email,
# MAGIC   phone::STRING              AS phone,
# MAGIC   city::STRING               AS city,
# MAGIC   country_code::STRING       AS country_code,
# MAGIC   credit_balance::STRING    AS credit_balance,
# MAGIC   source_system::STRING      AS source_system,
# MAGIC   _metadata.file_path                 AS _source_file,
# MAGIC   _metadata.file_modification_time     AS _file_timestamp,
# MAGIC   current_timestamp()                 AS _ingested_at
# MAGIC FROM read_files(
# MAGIC   '/Volumes/governance_demo/landing/raw_data_csv/customer/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );
# MAGIC
# MAGIC
# MAGIC SELECT * FROM governance_demo.bronze.bronze_customer_ctas;
# MAGIC SELECT * FROM governance_demo.bronze.bronze_order_ctas;

# COMMAND ----------

# ==============================================================================
# USE CASE 1: LAKEFLOW CONNECT — METADATA-DRIVEN BATCH INGESTION
# Reads ctl_ingestion_registry and dynamically ingests all LAKEFLOW entries
# Plain Spark batch — no declarative pipeline, no streaming table
# ==============================================================================

from pyspark.sql.functions import current_timestamp, col

# 1. Load active LAKEFLOW configs from the control-plane registry
lakeflow_configs = spark.sql("""
    SELECT pipeline_id, entity_name, source_path, source_format,
           format_options, target_catalog, target_schema, target_table,
           primary_key, cluster_by_cols, schema_evolution_mode
    FROM governance_demo.control_plane.ctl_ingestion_registry
    WHERE ingestion_method = 'LAKEFLOW' AND is_active = true
""").collect()

print(f"Found {len(lakeflow_configs)} active LAKEFLOW configs\n")

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

    # Full-refresh write (overwrite) — batch mode, not streaming
    (df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(full_table))

    row_count = spark.table(full_table).count()
    print(f"  → {row_count} rows ingested\n")
    return {"pipeline_id": pipeline_id, "table": full_table, "rows": row_count}


results = [ingest_lakeflow_batch(cfg.asDict()) for cfg in lakeflow_configs]

# 4. Summary
print("=" * 60)
print("LAKEFLOW CONNECT INGESTION SUMMARY")
print("=" * 60)
for r in results:
    print(f"  {r['pipeline_id']}: {r['rows']} rows → {r['table']}")

display(spark.sql("SHOW TABLES FROM governance_demo.bronze"))

# COMMAND ----------


from pyspark.sql.functions import current_timestamp, col

# 1. Load active AUTOLOADER configs from the control-plane registry
al_configs = spark.sql("""
    SELECT pipeline_id, entity_name, source_path, source_format,
           format_options, target_catalog, target_schema, target_table,
           primary_key, cluster_by_cols, schema_evolution_mode,
           schema_location, checkpoint_location
    FROM governance_demo.control_plane.ctl_ingestion_registry
    WHERE ingestion_method = 'AUTOLOADER' AND is_active = true
""").collect()

print(f"Found {len(al_configs)} active AUTOLOADER configs\n")

# 2. Reusable Auto Loader ingestion function
def ingest_autoloader(config):
    pipeline_id   = config["pipeline_id"]
    full_table    = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
    source_path   = config["source_path"]
    source_format = config["source_format"]
    format_opts   = dict(config["format_options"]) if config["format_options"] else {}
    schema_loc    = config["schema_location"]
    checkpoint_loc = config["checkpoint_location"]
    schema_evol   = config["schema_evolution_mode"] or "addNewColumns"

    print(f"[{pipeline_id}] {config['entity_name']}: {source_path} → {full_table}")

    # Build Auto Loader stream with dynamic options from registry
    reader = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", source_format)
              .option("cloudFiles.schemaLocation", schema_loc)
              .option("cloudFiles.schemaEvolutionMode", schema_evol)
              .option("cloudFiles.inferColumnTypes", "true"))

    # Pass through format-specific options (header, delimiter, etc.)
    # Skip 'inferSchema' — already handled by cloudFiles.inferColumnTypes
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

# 3. Execute Auto Loader ingestion for all configs
results = [ingest_autoloader(cfg.asDict()) for cfg in al_configs]

# 4. Summary
print("=" * 60)
print("AUTO LOADER INGESTION SUMMARY")
print("=" * 60)
for r in results:
    print(f"  {r['pipeline_id']}: {r['rows']} rows → {r['table']}")

display(spark.sql("SHOW TABLES FROM governance_demo.bronze"))