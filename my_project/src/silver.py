from pyspark import pipelines as dp
@dp.table()
def order_silver():
    return(
        spark.readStream.table('order_bronze')
    )