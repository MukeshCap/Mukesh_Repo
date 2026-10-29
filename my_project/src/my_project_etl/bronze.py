from pyspark import pipelines as dp
from pyspark.sql.functions import *

df = spark.read.table("uat.mukesh.metadata_ingestion")

for c in df.collect():

    @dp.table(
        name=c.target_table, table_properties={"delta.enableChangeDataFeed": "true"}
    )
    def bronze(source_path=c.source_path, table_name=c.target_table):
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
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
