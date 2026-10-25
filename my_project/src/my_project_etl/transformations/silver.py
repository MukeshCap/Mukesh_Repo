from pyspark import pipelines as dp
from pyspark.sql.functions import *
import re


# Common Silver transformations
def silver_transform(df):
    # Rename columns to snake_case
    new_cols = [re.sub(r"[^a-zA-Z0-9]", "_", c).lower().strip("_") for c in df.columns]
    df = df.toDF(*new_cols)
    # Remove duplicates
    df = df.dropDuplicates()
    # Add audit column
    df = df.withColumn("processed_ts", current_timestamp())
    return df


metadata = spark.read.table("uat.mukesh.metadata_ingestion").collect()
for row in metadata:

    @dp.table(name=f"silver_{row.entity_name}")
    def silver(entity_name=row.entity_name):
        bronze_df = spark.readStream.option("readChangeFeed", "true").table(
            f"bronze_{entity_name}"
        )
        return bronze_df

    # .filter("_change_type != 'delete'")
    # .drop("_change_type", "_commit_version", "_commit_timestamp")
