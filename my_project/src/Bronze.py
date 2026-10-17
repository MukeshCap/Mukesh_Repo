from pyspark import pipelines as dp

@dp.table(
    table_properties={'delta.enableChangeDataFeed':'true'}
)
def order_bronze():
    return(
        spark.readStream.format('cloudFiles')
        .option('cloudFiles.format','csv')
        .option('header',True)
        .option('inferSchema',True)
        .load('/Volumes/autoloader/mukesh_dhiman/order/Orders/csv/')
    )