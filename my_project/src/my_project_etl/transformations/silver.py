from pyspark import pipelines as dp
df = spark.read.table("uat.mukesh.metadata_ingestion")
data = df.collect()
for c in data:
    @dp.table()
    def silver():
            return(spark.read.table('bronze_agent'))
        