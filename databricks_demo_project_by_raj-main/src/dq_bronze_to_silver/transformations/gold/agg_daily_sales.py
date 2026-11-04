from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")
GOLD = f"`{CATALOG}`.`3_gold`"


@dp.materialized_view(
    name=f"{GOLD}.agg_daily_sales",
    comment="Daily sales KPIs by order date (order-level amounts from fact_sales_order).",
)
def agg_daily_sales():
    return (
        spark.read.table(f"{GOLD}.fact_sales_order")
        .groupBy("order_date")
        .agg(
            F.count("*").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.sum(F.col("is_online_order").cast("int")).alias("online_order_count"),
            F.sum("sub_total").alias("sub_total"),
            F.sum("tax_amt").alias("tax_amt"),
            F.sum("freight").alias("freight"),
            F.sum("total_due").alias("total_due"),
            F.avg("days_to_ship").alias("avg_days_to_ship"),
        )
        .withColumn("avg_order_value", (F.col("total_due") / F.col("order_count")).cast("decimal(19,4)"))
    )
