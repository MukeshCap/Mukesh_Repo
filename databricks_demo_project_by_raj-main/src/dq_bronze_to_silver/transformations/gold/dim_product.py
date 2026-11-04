from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog_name")
SILVER = f"`{CATALOG}`.`2_silver`"
GOLD = f"`{CATALOG}`.`3_gold`"


@dp.materialized_view(
    name=f"{GOLD}.dim_product",
    comment="Product dimension: current product version (SCD2 __END_AT IS NULL) enriched with "
            "subcategory, top-level category, product model and English model description.",
)
@dp.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
@dp.expect("category_resolved", "category_name IS NOT NULL")
def dim_product():
    product = spark.read.table(f"{SILVER}.product").where(F.col("__END_AT").isNull())

    category = spark.read.table(f"{SILVER}.productcategory").select(
        F.col("ProductCategoryID").alias("category_id"),
        F.col("Name").alias("category_name"),
        F.col("ParentProductCategoryID").alias("parent_category_id"),
    )
    parent_category = spark.read.table(f"{SILVER}.productcategory").select(
        F.col("ProductCategoryID").alias("parent_category_id"),
        F.col("Name").alias("parent_category_name"),
    )
    model = spark.read.table(f"{SILVER}.productmodel").select(
        F.col("ProductModelID").alias("product_model_id"),
        F.col("Name").alias("model_name"),
    )
    model_description_en = (
        spark.read.table(f"{SILVER}.productmodelproductdescription")
        .where(F.trim("Culture") == "en")
        .join(spark.read.table(f"{SILVER}.productdescription"), "ProductDescriptionID")
        .select(
            F.col("ProductModelID").alias("product_model_id"),
            F.col("Description").alias("model_description"),
        )
    )

    return (
        product.select(
            F.col("ProductID").alias("product_id"),
            F.col("Name").alias("product_name"),
            F.col("ProductNumber").alias("product_number"),
            F.col("Color").alias("color"),
            F.col("Size").alias("size"),
            F.col("Weight").alias("weight"),
            F.col("StandardCost").alias("standard_cost"),
            F.col("ListPrice").alias("list_price"),
            F.col("ProductCategoryID").alias("category_id"),
            F.col("ProductModelID").alias("product_model_id"),
            F.col("SellStartDate").cast("date").alias("sell_start_date"),
            F.col("SellEndDate").cast("date").alias("sell_end_date"),
            F.col("DiscontinuedDate").cast("date").alias("discontinued_date"),
        )
        .join(category, "category_id", "left")
        .join(parent_category, "parent_category_id", "left")
        .join(model, "product_model_id", "left")
        .join(model_description_en, "product_model_id", "left")
        .select(
            "product_id", "product_name", "product_number", "color", "size", "weight",
            "standard_cost", "list_price",
            (F.col("list_price") - F.col("standard_cost")).alias("unit_margin"),
            "category_id", "category_name",
            "parent_category_id",
            F.coalesce("parent_category_name", "category_name").alias("parent_category_name"),
            "product_model_id", "model_name", "model_description",
            "sell_start_date", "sell_end_date", "discontinued_date",
            (F.col("sell_end_date").isNull() & F.col("discontinued_date").isNull()).alias("is_active"),
        )
    )
