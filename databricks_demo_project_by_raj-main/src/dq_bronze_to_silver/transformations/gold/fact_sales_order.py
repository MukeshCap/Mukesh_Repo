from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog_name")
SILVER = f"`{CATALOG}`.`2_silver`"
GOLD = f"`{CATALOG}`.`3_gold`"

MONEY = "decimal(19,4)"


@dp.materialized_view(
    name=f"{GOLD}.fact_sales_order",
    comment="Sales order fact (order grain): latest Kafka version per SalesOrderID, typed dates and "
            "order-level amounts from the order header.",
)
@dp.expect_or_fail("sales_order_id_not_null", "sales_order_id IS NOT NULL")
@dp.expect("total_due_reconciles", "abs(total_due - (sub_total + tax_amt + freight)) <= 0.01")
@dp.expect("ship_after_order", "ship_date IS NULL OR ship_date >= order_date")
def fact_sales_order():
    latest = Window.partitionBy("SalesOrderID").orderBy(
        F.col("_kafka_timestamp").desc(), F.col("_kafka_offset").desc()
    )
    return (
        spark.read.table(f"{SILVER}.salesorderheader")
        .withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .select(
            F.col("SalesOrderID").alias("sales_order_id"),
            F.col("SalesOrderNumber").alias("sales_order_number"),
            F.col("CustomerID").alias("customer_id"),
            F.to_date(F.to_timestamp("OrderDate")).alias("order_date"),
            F.to_date(F.to_timestamp("DueDate")).alias("due_date"),
            F.to_date(F.to_timestamp("ShipDate")).alias("ship_date"),
            F.col("Status").alias("status"),
            F.col("ShipMethod").alias("ship_method"),
            F.col("OnlineOrderFlag").alias("is_online_order"),
            F.col("PurchaseOrderNumber").alias("purchase_order_number"),
            F.col("AccountNumber").alias("account_number"),
            F.col("BillToAddressID").alias("bill_to_address_id"),
            F.col("ShipToAddressID").alias("ship_to_address_id"),
            F.col("SubTotal").cast(MONEY).alias("sub_total"),
            F.col("TaxAmt").cast(MONEY).alias("tax_amt"),
            F.col("Freight").cast(MONEY).alias("freight"),
            F.col("TotalDue").cast(MONEY).alias("total_due"),
        )
        .withColumn("days_to_ship", F.datediff("ship_date", "order_date"))
    )
