from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")
GOLD = f"`{CATALOG}`.`3_gold`"


@dp.materialized_view(
    name=f"{GOLD}.agg_sales_by_product_category",
    comment="Units, revenue, cost and gross margin by top-level and sub product category "
            "(line grain from fact_sales_order_line).",
)
def agg_sales_by_product_category():
    products = spark.read.table(f"{GOLD}.dim_product").select(
        "product_id", "parent_category_name", "category_name"
    )
    return (
        spark.read.table(f"{GOLD}.fact_sales_order_line")
        .join(products, "product_id", "left")
        .groupBy(
            F.coalesce("parent_category_name", F.lit("Unknown")).alias("parent_category_name"),
            F.coalesce("category_name", F.lit("Unknown")).alias("category_name"),
        )
        .agg(
            F.countDistinct("sales_order_id").alias("order_count"),
            F.countDistinct("product_id").alias("product_count"),
            F.sum("order_qty").alias("units_sold"),
            F.sum("line_total").alias("revenue"),
            F.sum("line_cost").alias("cost"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .withColumn(
            "gross_margin_pct",
            F.when(F.col("revenue") != 0, F.round(F.col("gross_margin") / F.col("revenue") * 100, 2)),
        )
    )
