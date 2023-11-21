import pyspark
from lakehouse import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaLakeTutorial_2") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark/warehouse") \
    .config("spark.sql.extensions", "io.lakehouse.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakehouse.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


spark.sql("SELECT * FROM throne.family").show()
