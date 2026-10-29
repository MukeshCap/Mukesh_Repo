from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Read active entities from metadata
df = spark.read.table("uat.mukesh.metadata_ingestion").filter(col("active_flag") == "Y")

for row in df.collect():

    def create_bronze(
        target_table=row.bronze_target_table,
        source_path=row.source_path,
        file_format=row.file_format,
    ):
        @dp.table(
            name=target_table, table_properties={"delta.enableChangeDataFeed": "true"}
        )
        def bronze():
            return (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("header", "true")
                .option("inferSchema", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("rescuedDataColumn", "_rescued_data")
                .load(source_path)
                .withColumn("file_name", col("_metadata.file_name"))
                .withColumn("file_path", col("_metadata.file_path"))
                .withColumn("file_size", col("_metadata.file_size"))
                .withColumn(
                    "file_modification_time", col("_metadata.file_modification_time")
                )
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("load_date", current_date())
            )

    create_bronze()
