# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Create Sample Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1. Create a new Unity Catalog catalog
# MAGIC CREATE CATALOG IF NOT EXISTS my_catalog 
# MAGIC COMMENT 'This is my custom catalog';
# MAGIC
# MAGIC -- 2. Create a new schema (database) inside that catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema 
# MAGIC COMMENT 'This is my custom schema';
# MAGIC
# MAGIC use catalog my_catalog;
# MAGIC use schema my_schema;
# MAGIC
# MAGIC DROP TABLE IF EXISTS orders;
# MAGIC
# MAGIC CREATE TABLE  orders (
# MAGIC  order_id INT, customer_id INT, product_id INT, order_date DATE, amount DECIMAL(10,2)
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- 1. Insert your original 5 explicit records
# MAGIC INSERT INTO orders VALUES
# MAGIC (1, 101, 5001, '2026-01-01', 100.00),
# MAGIC (2, 102, 5002, '2026-01-01', 250.00),
# MAGIC (3, 103, 5003, '2026-01-02', 300.00),
# MAGIC (4, 101, 5004, '2026-01-02', 150.00),
# MAGIC (5, 104, 5005, '2026-01-03', 450.00);
# MAGIC
# MAGIC -- 2. Dynamically generate and append the remaining 9,995 records
# MAGIC INSERT INTO orders
# MAGIC SELECT 
# MAGIC     id AS order_id,
# MAGIC     -- Randomly assigns a customer ID between 101 and 150
# MAGIC     CAST(101 + (rand(1) * 50) AS INT) AS customer_id,
# MAGIC     -- Randomly assigns a salesman ID between 5001 and 5020
# MAGIC     CAST(5001 + (rand(2) * 20) AS INT) AS salesman_id,
# MAGIC     -- Distributes order dates across early 2026
# MAGIC     DATE_ADD('2026-01-03', CAST(rand(3) * 30 AS INT)) AS order_date,
# MAGIC     -- Randomizes purchase amounts between $10.00 and $1,000.00
# MAGIC     ROUND(10.00 + (rand(4) * 990.00), 2) AS purchase_amt
# MAGIC FROM (
# MAGIC     SELECT explode(sequence(6, 10000)) AS id
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDER
# MAGIC Improves filtering by clustering related values together.

# COMMAND ----------

# Python
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name,
# MAGIC min(order_id),
# MAGIC max(order_id)
# MAGIC from orders
# MAGIC group by _metadata.file_name

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE orders ZORDER BY (order_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name,
# MAGIC min(order_id),
# MAGIC max(order_id)
# MAGIC from orders
# MAGIC group by _metadata.file_name

# COMMAND ----------

# MAGIC %md
# MAGIC PARTITION BY (The Directory Level): Splits data into separate physical folders on cloud storage based on a high-cardinality or high-level category (e.g., date=2026-06-01/)
# MAGIC
# MAGIC .OPTIMIZE (The File Compaction Level): Runs inside those partition folders (or the whole table if unpartitioned) to mash tiny files into big files
# MAGIC
# MAGIC .ZORDER (The Internal File Sorting Level): An optional sorting algorithm applied during the OPTIMIZE process. It sorts data across multiple columns inside those newly packed files so Spark can skip reading specific parts of a file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring
# MAGIC 1. DESCRIBE DETAIL orders
# MAGIC 2. DESCRIBE HISTORY orders
# MAGIC 3. Query Profile
# MAGIC 4. Spark UI
# MAGIC 5. EXPLAIN FORMATTED