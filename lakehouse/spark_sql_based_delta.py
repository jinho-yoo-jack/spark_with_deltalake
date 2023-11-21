import pyspark
from lakehouse import *
from pyspark import SparkConf
from pyspark.sql.catalog import Table
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a spark session with Delta
spark_config = (
    SparkConf()
    .setAppName("execute-spark-sql-for-check")
    .setMaster('yarn')
    .set('deploy-mode', 'client')
    .set('spark.executors.instances', 2)
    .set('spark.executors.cores', 3)
    .set('spark.memory.fraction', '0.5')
    .set('spark.executor.memory', '1g')
)

builder = pyspark.sql.SparkSession.builder.appName("DeltaLakeTutorial_3") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark/warehouse") \
    .config("spark.sql.extensions", "io.lakehouse.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakehouse.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(spark.catalog.listTables('throne'))

spark.sql("SHOW DATABASES").show()
spark.sql("USE THRONE")
spark.sql("show tables").show()
spark.sql("select * from family").show(100)
spark.sql("DESCRIBE TABLE EXTENDED family").show(100)
# (
#     spark.read.format("lakehouse")
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .table("family")
#     .show(truncate=False)
# )
