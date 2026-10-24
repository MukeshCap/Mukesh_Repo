from pyspark import pipelines as dp
df=spark.read.table('uat.mukesh.metadata_ingestion')
data=df.collect()
for c in data:
    @dp.table(name=c.target_table)
    def bronze():
        return(
        spark.readStream.format("cloudFiles") 
        .option("cloudFiles.format", "csv") 
        .option('header',True)
        .option('inferSchema',True)
        .load(c.source_path)
        )