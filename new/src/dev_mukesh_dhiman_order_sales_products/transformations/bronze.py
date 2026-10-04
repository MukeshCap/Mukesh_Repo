from pyspark import pipelines as dp
from pyspark.sql.functions import col
##LOAD ORDER TABLES
@dp.table
def orders_bronze():
    return (
        spark.readStream.format('cloudFiles')
        .option ('cloudFiles.format','csv')
        .option('header',True)
        .option('inferSchema',True)
        .load('/Volumes/autoloader/default/orders')
        .withColumn('File_name',col('_metadata.file_name'))
    )
##LOAD products TABLES
@dp.table
def products_bronze():
    return (
        spark.readStream.format('cloudFiles')
        .option ('cloudFiles.format','csv')
        .option('header',True)
        .option('inferSchema',True)
        .load('/Volumes/autoloader/default/products')
        .withColumn('File_name',col('_metadata.file_name'))
    )
##LOAD sales TABLES
@dp.table
def sales_bronze():
    return (
        spark.readStream.format('cloudFiles')
        .option ('cloudFiles.format','csv')
        .option('header',True)
        .option('inferSchema',True)
        .load('/Volumes/autoloader/default/sales')
        .withColumn('File_name',col('_metadata.file_name'))
    )
