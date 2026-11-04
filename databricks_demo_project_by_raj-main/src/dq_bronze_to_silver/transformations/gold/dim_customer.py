from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog_name")
SILVER = f"`{CATALOG}`.`2_silver`"
GOLD = f"`{CATALOG}`.`3_gold`"


@dp.materialized_view(
    name=f"{GOLD}.dim_customer",
    comment="Customer dimension: one row per customer (latest ingested version). "
            "Phone and password columns are intentionally excluded (sensitive / governed in silver).",
)
@dp.expect_or_fail("customer_id_not_null", "customer_id IS NOT NULL")
def dim_customer():
    latest = Window.partitionBy("CustomerID").orderBy(F.col("_ingested_at").desc())
    return (
        spark.read.table(f"{SILVER}.customer")
        .withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .select(
            F.col("CustomerID").alias("customer_id"),
            F.col("Title").alias("title"),
            F.col("FirstName").alias("first_name"),
            F.col("MiddleName").alias("middle_name"),
            F.col("LastName").alias("last_name"),
            F.col("Suffix").alias("suffix"),
            F.concat_ws(" ", "FirstName", "MiddleName", "LastName").alias("full_name"),
            F.col("CompanyName").alias("company_name"),
            F.col("EmailAddress").alias("email_address"),
            F.col("SalesPerson").alias("sales_person"),
            F.to_timestamp("ModifiedDate").alias("modified_at"),
        )
    )
