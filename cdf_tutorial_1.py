import pyspark
from delta import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark/warehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame([("Bob", 23), ("Sue", 25), ("Jim", 27)]).toDF(
    "first_name", "age"
)

df.write.mode("append").format("delta").saveAsTable("people")

delta_table = DeltaTable.forName(spark, "people")
delta_table.delete(col("first_name") == "Sue")

(
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("people")
    .show(truncate=False)
)