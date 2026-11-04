from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog_name")
SILVER = f"`{CATALOG}`.`2_silver`"
GOLD = f"`{CATALOG}`.`3_gold`"

MONEY = "decimal(19,4)"


@dp.materialized_view(
    name=f"{GOLD}.fact_sales_order_line",
    comment="Sales order line fact: one row per SalesOrderDetailID (Kafka duplicates removed), "
            "with order date/customer from fact_sales_order and cost/margin from dim_product.",
)
@dp.expect_or_fail("sales_order_detail_id_not_null", "sales_order_detail_id IS NOT NULL")
@dp.expect("order_found", "order_date IS NOT NULL")
@dp.expect("product_found", "standard_cost IS NOT NULL")
def fact_sales_order_line():
    latest = Window.partitionBy("SalesOrderDetailID").orderBy(
        F.col("_kafka_timestamp").desc(), F.col("_kafka_offset").desc()
    )
    lines = (
        spark.read.table(f"{SILVER}.salesorderdetail")
        .withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .select(
            F.col("SalesOrderDetailID").alias("sales_order_detail_id"),
            F.col("SalesOrderID").alias("sales_order_id"),
            F.col("ProductID").cast("int").alias("product_id"),
            F.col("OrderQty").alias("order_qty"),
            F.col("UnitPrice").cast(MONEY).alias("unit_price"),
            F.col("UnitPriceDiscount").cast(MONEY).alias("unit_price_discount"),
            F.col("LineTotal").cast(MONEY).alias("line_total"),
        )
    )
    orders = spark.read.table(f"{GOLD}.fact_sales_order").select(
        "sales_order_id", "order_date", "customer_id"
    )
    products = spark.read.table(f"{GOLD}.dim_product").select("product_id", "standard_cost")

    return (
        lines.join(orders, "sales_order_id", "left")
        .join(products, "product_id", "left")
        .withColumn("line_cost", (F.col("order_qty") * F.col("standard_cost")).cast(MONEY))
        .withColumn("gross_margin", (F.col("line_total") - F.col("line_cost")).cast(MONEY))
        .select(
            "sales_order_detail_id", "sales_order_id", "order_date", "customer_id", "product_id",
            "order_qty", "unit_price", "unit_price_discount", "line_total",
            "standard_cost", "line_cost", "gross_margin",
        )
    )
