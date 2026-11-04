from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")
GOLD = f"`{CATALOG}`.`3_gold`"


@dp.materialized_view(
    name=f"{GOLD}.agg_customer_sales",
    comment="Per-customer order KPIs from fact_sales_order, enriched with dim_customer attributes "
            "(attributes are NULL when the customer is not yet in dim_customer).",
)
@dp.expect("customer_in_dim", "full_name IS NOT NULL")
def agg_customer_sales():
    customers = spark.read.table(f"{GOLD}.dim_customer").select(
        "customer_id", "full_name", "company_name", "sales_person"
    )
    return (
        spark.read.table(f"{GOLD}.fact_sales_order")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("sub_total").alias("sub_total"),
            F.sum("total_due").alias("total_due"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("avg_order_value", (F.col("total_due") / F.col("order_count")).cast("decimal(19,4)"))
        .join(customers, "customer_id", "left")
        .select(
            "customer_id", "full_name", "company_name", "sales_person",
            "order_count", "sub_total", "total_due", "avg_order_value",
            "first_order_date", "last_order_date",
        )
    )
