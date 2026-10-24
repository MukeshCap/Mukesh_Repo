from pyspark import pipelines as dp

df = spark.read.table("uat.mukesh.metadata_ingestion")
data = df.collect()
for c in data:

    @dp.table(name=f"silver_{c.entity_name}")
    @dp.expect('')
    def silver(entity_name=c.entity_name):
        return spark.read.table(f"bronze_{entity_name}").dropDuplicates()