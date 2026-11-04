# India Project - Data Ingestion and CI/CD Runbook

Workspace: `https://adb-7405615726115651.11.azuredatabricks.net`
Bundle: `india_project_bundle` (Git folder `/Repos/indiasolution/repo_indiarawproject/india_project_bundle`)
Source system: Azure SQL Server database `sales-northindia-db` (AdventureWorksLT, schema `SalesLT`)
Storage: ADLS Gen2 account `indiaprojectstorage`

---

## Agenda

1. Ingestion from SQL Server using Lakeflow Connect (CDC)
2. Ingestion using Lakehouse Federation (connection + foreign catalog)
3. Ingestion of CSV / JSON / Parquet files using Auto Loader (config driven)
4. Ingestion from Kafka (Confluent Cloud) into Delta tables
5. Deployment with Declarative Automation Bundles (DABs) and Azure DevOps CI/CD

---

## 0. Platform overview (the `dev` catalog)

All demos land in the Unity Catalog catalog `dev`, organised as a medallion layout.

| Schema | Purpose | Key objects |
| --- | --- | --- |
| `dev.1_bronze` | Bronze tables from every ingestion pattern | `product`, `productcategory`, `productdescription`, `productmodel`, `productmodelproductdescription` (Lakeflow Connect), `customer`, `customeraddress` (Auto Loader), `salesorderdetail`, `salesorderheader` (Kafka), volume `checkpoints` |
| `dev.2_silver` / `dev.3_gold` | Reserved for curated layers (not covered today) | - |
| `dev.configs` | Framework metadata and secrets | table `ingestion_config`, volume `checkpoints`, UC secrets `kafka_bootstrap_server`, `kafka_api_key`, `kafka_api_secret` |
| `dev.events` | Lakeflow Connect internals | CDC staging table, staging volume, pipeline event log `event_log_20bc3e84_...` |

Supporting Unity Catalog objects:

| Object | Type | Value |
| --- | --- | --- |
| `cn_database` | Connection (SQLSERVER) | Credentials to Azure SQL `sales-northindia-db` |
| `sales-northindia-db` | Foreign catalog | Created on `cn_database`, option `database = sales-northindia-db` |
| `external_location_india_project_source` | External location | `abfss://indiaprojectraw@indiaprojectstorage.dfs.core.windows.net/` |
| `kafkainput` | External location | `abfss://kafkainput@indiaprojectstorage.dfs.core.windows.net/` |
| `indiaprojectstorage` | External location (metastore root) | `abfss://metastore@indiaprojectstorage.dfs.core.windows.net/unity_catalogs` |

Current row counts in bronze (snapshot taken while preparing this runbook):

| Table | Rows | Loaded by |
| --- | --- | --- |
| `dev.1_bronze.product` | 295 | Lakeflow Connect |
| `dev.1_bronze.productcategory` | 41 | Lakeflow Connect |
| `dev.1_bronze.productmodel` | 128 | Lakeflow Connect |
| `dev.1_bronze.customer` | 50 | Auto Loader (JSON) |
| `dev.1_bronze.customeraddress` | 40 | Auto Loader (CSV) - see known issue in section 3 |
| `dev.1_bronze.salesorderdetail` | 160 | Kafka |
| `dev.1_bronze.salesorderheader` | 32 | Kafka |

End-to-end flow:

```
Azure SQL (sales-northindia-db)
   |-- Lakeflow Connect (CDC) ------------------------------> dev.1_bronze.product*
   |-- Lakehouse Federation (foreign catalog) --> read live --+
                                                              |-- setup notebooks write files
                                                              v
        ADLS indiaprojectraw/customer, /customeraddress  --Auto Loader--> dev.1_bronze.customer / customeraddress
        ADLS kafkainput/topics --Confluent connector--> Kafka topics --Structured Streaming--> dev.1_bronze.salesorder*

All jobs/pipelines are defined in the bundle and deployed by Azure DevOps (DABs).
```

---

## 1. Ingestion from SQL Server using Lakeflow Connect

### What it shows
Managed, incremental change data capture (CDC) from SQL Server into streaming tables, with no custom code. Product tables are replicated; `Product` is kept as SCD Type 2 history.

### Assets
* Pipeline resource: `resources/ingest_sql_cdc_landing_lakeflow_connect.pipeline.yml` (pipeline name `ingest_sql_cdc_landing_lakeflow_connect`, id `20bc3e84-18ee-4fbd-a8b0-7b94e85aadd1`)
* Job resource: `resources/ingest_sql_cdc_landing_lakeflow_connect job.job.yml` - runs the pipeline every 24 hours (periodic trigger, unpaused)
* Connection: `cn_database` (SQLSERVER)

### Configuration highlights
| Setting | Value |
| --- | --- |
| `source_type` / `connector_type` | `SQLSERVER` / `CDC` |
| Source objects | `SalesLT.Product`, `ProductCategory`, `ProductDescription`, `ProductModel`, `ProductModelProductDescription` |
| Destination | `dev.1_bronze` |
| `Product` table | `scd_type: SCD_TYPE_2`, `sequence_by: ModifiedDate` |
| Staging | catalog `dev`, schema `events`, volume `__databricks_ingestion_gateway_staging_data-20bc3e84-...` |
| Compute | Serverless, channel `CURRENT` |

### Prerequisites (one time)
1. Enable CDC (or change tracking) on the SQL Server database and the source tables.
2. Allow the Databricks serverless egress to reach the Azure SQL server (firewall / networking).
3. Create the UC connection `cn_database` with a SQL login that can read the tables and CDC metadata.
4. Grant the pipeline owner `USE CONNECTION` on `cn_database`, and `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME` on `dev.1_bronze` and `dev.events`.

### Demo steps
1. Open the pipeline `ingest_sql_cdc_landing_lakeflow_connect` and show the source -> destination mapping.
2. Start an update (or run the job `ingest_sql_cdc_landing_lakeflow_connect job`).
3. Show the streaming tables in `dev.1_bronze` and the internals in `dev.events`.
4. Make a change in SQL Server (for example update a `Product.ListPrice`) and re-run to show CDC picking up only the change.
5. Show SCD2 history on `dev.1_bronze.product` (`__START_AT` / `__END_AT` columns).

### Validation queries
```sql
SELECT count(*) FROM dev.`1_bronze`.product;
SELECT * FROM dev.`1_bronze`.product WHERE ProductID = <id> ORDER BY __START_AT;
```

### Troubleshooting
* `FAILED_JDBC.CONNECTION`: the SQL Server is paused (serverless Azure SQL) or the firewall blocks Databricks. Resume the database / fix networking.
* Staging volume ownership error (403) when a different identity deploys the pipeline: each pipeline needs its own staging volume; do not reuse another pipeline's `volume_name`.

---

## 2. Ingestion using Lakehouse Federation

### What it shows
Query SQL Server tables live from Databricks, governed by Unity Catalog, without copying data first. The connection stores the credential once; the foreign catalog exposes the database as `catalog.schema.table`.

### Assets
* Connection `cn_database` (type SQLSERVER) - holds host, port and the SQL credential
* Foreign catalog `sales-northindia-db` (on `cn_database`, `database = sales-northindia-db`)
* Setup notebooks that use federation as the source:
  * `src/setup/ingest_customers_and_customer_address jsontoADLS` - reads `saleslt.customer` and `saleslt.customeraddress`, writes batch files to ADLS for the Auto Loader demo
  * `src/setup/ingest_salesorderdetailsjsonfokafka` - reads `saleslt.salesorderdetail`, writes JSON files that feed Kafka

### How it is set up (one time)
1. Catalog Explorer -> External data -> Connections -> Create connection, type SQL Server, enter host and SQL login. Name: `cn_database`.
2. Create a foreign catalog on that connection: name `sales-northindia-db`, database `sales-northindia-db`.
3. Grant users `USE CATALOG`, `USE SCHEMA`, `SELECT` on the foreign catalog (no access to the SQL credential itself is needed).

### Demo steps
1. In Catalog Explorer open `sales-northindia-db` -> `saleslt` and browse the tables.
2. Query live: ``SELECT * FROM `sales-northindia-db`.saleslt.customer LIMIT 10;``
3. Show pushdown: run a filtered query and point out in the query profile that the filter is pushed to SQL Server.
4. Materialise into the lakehouse (pattern used in this project):
   * Run the setup notebook to read `saleslt.customer` / `saleslt.customeraddress` and write batch files to `abfss://indiaprojectraw@.../customer` and `/customeraddress` (50 rows each, 5 rows per file).
   * Alternatively, `CREATE TABLE ... AS SELECT` from the foreign table for a one-off copy.

### When to use Federation vs Lakeflow Connect
| Need | Use |
| --- | --- |
| Ad-hoc / exploratory queries, small lookups, no copy | Lakehouse Federation |
| Continuous incremental replication with CDC and history | Lakeflow Connect |

### Troubleshooting
* `FAILED_JDBC.CONNECTION` (SQLSTATE HV000): the database is paused or unreachable. Every federated query depends on the source being online.

---

## 3. Ingestion of CSV and other file formats using Auto Loader

### What it shows
A metadata (config) driven framework: one generic notebook ingests any number of file sources (CSV, TSV, JSON, Parquet) incrementally with Auto Loader, controlled by rows in a config table.

### Assets
* Config table: `dev.configs.ingestion_config` (created/seeded by `src/setup/prepare_config`)
* Lookup notebook: `src/lookup_config_on_source_name` - filters active config rows by `source_name` and publishes them as task value `config_records`
* Ingestion notebook: `src/ingestion notebook` - processes one config record per run
* Job: `resources/bronze_ingest_by_autoloader.job.yml` (`bronze_ingest_by_autoloader`, parameter `source_name = adls_files`)

### Config table (active rows for this demo)
| config_id | source_name | source_folder | format | target | load_type |
| --- | --- | --- | --- | --- | --- |
| 1 | adls_files | `abfss://indiaprojectraw@indiaprojectstorage.dfs.core.windows.net/customer/` | json | `dev.1_bronze.customer` | incremental |
| 2 | adls_files | `abfss://indiaprojectraw@indiaprojectstorage.dfs.core.windows.net/customeraddress/` | csv | `dev.1_bronze.customeraddress` | incremental |

Checkpoints: `/Volumes/dev/configs/checkpoints/<target_schema>/<target_table>` (schema location in `_schema` below it).

### Job flow
```
lookup (source_name=adls_files)
   -> for_each_config_record (concurrency 2, input {{tasks.lookup.values.config_records}})
         -> ingestion notebook (config={{input}})
```

### What the ingestion notebook does
* Validates the config (required keys, supported formats `csv|tsv|json|parquet`, load types `incremental|overwrite`).
* `incremental`: Auto Loader (`cloudFiles`) with `schemaEvolutionMode = addNewColumns`, `inferColumnTypes`, `trigger(availableNow=True)`, append to Delta with `mergeSchema`.
* `overwrite`: batch read of the whole folder and full table replace.
* Adds audit columns `_source_file`, `_ingested_at`, `_config_id` and cleans invalid column characters.

### Demo steps
1. Show `dev.configs.ingestion_config` - adding a new source is just an `INSERT`.
2. Run the job `bronze_ingest_by_autoloader`; show the two for-each iterations.
3. Drop new files into the ADLS folder (re-run the setup notebook with a new row range) and run again - only new files are processed.
4. Show audit columns and `_rescued_data`.

### Validation queries
```sql
SELECT _source_file, count(*) FROM dev.`1_bronze`.customer GROUP BY ALL;
SELECT * FROM dev.configs.ingestion_config WHERE source_name = 'adls_files';
```

### Known issue to fix before the demo
`dev.1_bronze.customeraddress` has 40 rows instead of 50 and its column names are data values (for example `29485`, `Main_Office`). Cause: the setup notebook writes CSV files without a header row, while the ingestion notebook reads CSV with `header = true`, so the first row of each of the 10 files becomes a header. Fix: write the CSV with `.option("header", "true")` in the setup notebook, delete the target table and its checkpoint folder `/Volumes/dev/configs/checkpoints/1_bronze/customeraddress`, then re-run the job.

---

## 4. Ingestion from Kafka into Delta tables

### What it shows
Streaming ingestion from Confluent Cloud (Kafka on Azure) into Delta, using the same config-driven pattern and Unity Catalog secrets for credentials.

### Source side (how data gets into Kafka)
1. `src/setup/ingest_salesorderdetailsjsonfokafka` reads `sales-northindia-db`.saleslt.salesorderdetail via federation and writes JSON array files to `abfss://kafkainput@indiaprojectstorage.dfs.core.windows.net/topics`.
2. A Confluent Azure Blob Storage Source connector reads those files and publishes them to the topics `salesorderdetail` / `salesorderheader`.

### Assets
* Notebook: `src/kakfa_ingestion`
* Job: `resources/bronze_ingest_from_kafka.job.yml` (`bronze_ingest_from_kafka`, parameter `source_name = kafka`)
* Config rows 3 and 4 in `dev.configs.ingestion_config` (`source_folder` holds the topic name)
* Secrets (Unity Catalog, schema `dev.configs`): `kafka_bootstrap_server`, `kafka_api_key`, `kafka_api_secret`

| config_id | topic | target |
| --- | --- | --- |
| 3 | `salesorderdetail` | `dev.1_bronze.salesorderdetail` |
| 4 | `salesorderheader` | `dev.1_bronze.salesorderheader` |

### What the notebook does
* Reads secrets with `dbutils.secrets.get(catalog=..., schema="configs", key=...)`; connects with `SASL_SSL` / `PLAIN`.
* Samples up to 1000 messages and infers the JSON schema with `schema_of_json_agg` (strips the 5-byte Schema Registry header if present).
* Streams the topic, parses the value with `from_json`, flattens every key into a column and adds `_kafka_key`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_raw_value`, `_ingested_at`.
* Writes to Delta with `trigger(availableNow=True)`, `mergeSchema`, checkpoint under `/Volumes/dev/<target_schema>/checkpoints/kafka/...`.

### Demo steps
1. Show the topic and messages in Confluent Cloud.
2. Run job `bronze_ingest_from_kafka` - one for-each iteration per topic.
3. Query the bronze tables and show the Kafka metadata columns.
4. Produce more messages (run the setup notebook for a new row range) and re-run - only new offsets are read.

### Validation queries
```sql
SELECT _kafka_topic, _kafka_partition, max(_kafka_offset), count(*)
FROM dev.`1_bronze`.salesorderdetail GROUP BY ALL;
```

### Troubleshooting
* Authentication errors: check the three UC secrets and the API key permissions in Confluent.
* Missing offsets on trial clusters: `failOnDataLoss = false` is set because trial topics have short retention.
* Check cell 4 of `kakfa_ingestion`: `checkpoint_path` uses a variable `table`; make sure it resolves to the target table name (`target_table_load`) so each topic gets its own checkpoint.

---

## 5. Deployment with DABs and Azure DevOps CI/CD

### What it shows
Everything above (jobs and the Lakeflow Connect pipeline) is code in a Declarative Automation Bundle, deployed automatically from Git by Azure DevOps using a service principal.

### Bundle layout
```
india_project_bundle/
  databricks.yml            # bundle name, variables, targets (dev / prod)
  azure_pipeline.yml        # Azure DevOps pipeline
  resources/
    ingest_sql_cdc_landing_lakeflow_connect.pipeline.yml
    ingest_sql_cdc_landing_lakeflow_connect job.job.yml
    bronze_ingest_by_autoloader.job.yml
    bronze_ingest_from_kafka.job.yml
  src/                      # notebooks (lookup, ingestion, kafka, setup)
  docs/india_project_runbook.md
```

### Targets
| Target | Mode | Notes |
| --- | --- | --- |
| `dev` (default) | development | Resources prefixed `[dev <user>]`, schedules paused, deployed to the deployer's home folder |
| `prod` | production | Single shared copy; can use `run_as` a service principal (enable when ready) |

Variables: `catalog`, `schema`, `sp_application_id` (passed by the pipeline).

### Manual commands (from the bundle root)
```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run      bronze_ingest_by_autoloader --target dev
databricks bundle summary  --target dev
```

### Azure DevOps pipeline (`azure_pipeline.yml`)
* Trigger: push to `main` that changes `india_project_bundle/`; no PR trigger.
* Agent: `ubuntu-latest`.
* Steps: checkout -> install Python and Databricks CLI -> `AzureCLI@2` with service connection `service_connection1` -> `bundle validate` -> `bundle deploy --auto-approve` -> `bundle summary`.
* Auth: `DATABRICKS_AUTH_TYPE=azure-cli`; the Azure CLI task signs in with the service principal (workload identity federation), so no secrets are stored in the pipeline. `addSpnToEnvironment` exposes the SP application id to pass as `sp_application_id`.
* Timeouts: job 20 minutes, deploy step 10 minutes.

### One-time setup checklist
1. Azure DevOps: Azure Resource Manager service connection `service_connection1` (service principal); allow pipeline access.
2. Databricks: add the service principal (Application ID) to the workspace with Workspace access.
3. Grant the SP (directly or through an account group): `USE CATALOG` on `dev`, schema privileges, `READ FILES`/`WRITE FILES` on external locations, `USE CONNECTION` on `cn_database`, read on the Kafka secrets.
4. Create the pipeline in Azure DevOps from `/india_project_bundle/azure_pipeline.yml` on branch `main`.
5. Push to `main` and verify the run.

### Demo steps
1. Change a job (for example `concurrency`) in a resource YAML, commit and push to `main`.
2. Show the Azure DevOps run: validate -> deploy.
3. Show the updated job in Databricks (owner = service principal).

### Lessons learned while building this
| Symptom | Cause | Fix |
| --- | --- | --- |
| Wheel build failed (`Unable to determine which files to ship`) | Template `artifacts` block but no Python package in `src` | Removed the unused `artifacts` block |
| `prod: no such target` | `prod` target commented out | Uncomment `prod` or deploy `dev` |
| Run stuck at `az login` / waiting | Single free parallel job busy, or transient sign-in | Cancel older runs, rerun; added timeouts |
| 403 on staging volume | Pipeline reused another pipeline's staging volume | Unique staging volume per pipeline/identity |
| `/Workspace/Shared` root path rejected in strict validate | Folder writable by all users | Use `/Workspace/Users/${workspace.current_user.userName}/.bundle/...` |

### Improvement backlog
* Job `ingest_sql_cdc_landing_lakeflow_connect job` uses a hard-coded `pipeline_id`; reference the bundle pipeline instead (`${resources.pipelines.pipeline_ingest_sql_cdc_landing_lakeflow_connect.id}`).
* Job notebook paths point to `/Repos/...`; switch to bundle-relative paths (for example `../src/kakfa_ingestion`) so deployed copies are used.
* Add a dev stage for feature branches and an approval gate before prod.

---

## Appendix - demo order and timing (suggested)

| # | Topic | Time |
| --- | --- | --- |
| 0 | Platform overview (`dev` catalog) | 5 min |
| 1 | Lakeflow Connect | 15 min |
| 2 | Lakehouse Federation | 10 min |
| 3 | Auto Loader framework | 15 min |
| 4 | Kafka streaming | 15 min |
| 5 | DABs + Azure DevOps CI/CD | 15 min |

Pre-demo checklist: resume the Azure SQL database, confirm the Confluent cluster and connector are running, fix the `customeraddress` header issue, and run each job once.
