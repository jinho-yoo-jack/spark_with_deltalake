import pyspark
from delta import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

hdfs_path = "hdfs://localhost:9000/data/delta-table"

# Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("DeltaLakeTutorial_1") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark/warehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Starting Delta table creation")

data = [
    ("Robert", "Baratheon", "Baratheon", "Storms End", 48),
    ("Eddard", "Stark", "Stark", "Winterfell", 46),
    ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29),
]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("house", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True)
])

spark.sql("CREATE DATABASE IF NOT EXISTS throne")

sample_dataframe = spark.createDataFrame(data=data, schema=schema)
# Internal Table
sample_dataframe.write.mode(saveMode="overwrite").format("delta").saveAsTable("throne.family")
# External Table
sample_dataframe.write.mode(saveMode="overwrite").format("delta").save("throne.family_1")
#
# # Read Data
# print("Reading delta file ... !")
#
# got_df = spark.read.format("delta").load("hdfs://localhost:9000/data/delta-table")
# got_df.show()
#
#
# # Update data
# print("Updating Delta table ... !")
# data = [
#     ("Robert", "Baratheon", "Baratheon", "Storms End", 30),
#     ("Eddard", "Stark", "Stark", "Winterfell", 46),
#     ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29),
# ]
# sample_dataframe = spark.createDataFrame(data=data, schema=schema)
# # sample_dataframe.write.mode(saveMode="overwrite").format("delta").save("data/delta-table")
# sample_dataframe.write.mode(saveMode="overwrite").format("delta").save("hdfs://localhost:9000/data/delta-table")
#
#
# got_df = spark.read.format("delta").load("hdfs://localhost:9000/data/delta-table")
# got_df.show()
#
# # Update data in Delta
# print("Update delta table ... !")
#
# deltaTable = DeltaTable.forPath(spark, hdfs_path)
# deltaTable.toDF().show()
#
# deltaTable.update(
#     condition=expr("firstname == 'Jamie'"),
#     set={
#         "firstname": lit("Jamie"),
#         "lastname": lit("Lannister"),
#         "house": lit("Lannister"),
#         "location": lit("Kings Landing"),
#         "age": lit(31)
#     }
# )
#
#
# deltaTable.toDF().show(1000)
#
#
# # Upsert Data
# print("Upserting Data...!")
# # delta table path
# deltaTable = DeltaTable.forPath(spark, path=hdfs_path)
# deltaTable.toDF().show()
#
# # define new data
# data = [("Gendry", "Baratheon", "Baratheon", "Kings Landing", 19),
#         ("Jon", "Snow", "Stark", "Winterfell", 21),
#         ("Jamie", "Lannister", "Lannister", "Casterly Rock", 36)
#         ]
# schema = StructType([
#     StructField("firstname", StringType(), True),
#     StructField("lastname", StringType(), True),
#     StructField("house", StringType(), True),
#     StructField("location", StringType(), True),
#     StructField("age", IntegerType(), True)
# ])
#
# newData = spark.createDataFrame(data=data, schema=schema)
#
# deltaTable.alias("oldData") \
#     .merge(
#     newData.alias("newData"),
#     "oldData.firstname = newData.firstname") \
#     .whenMatchedUpdate(
#     set={"firstname": col("newData.firstname"), "lastname": col("newData.lastname"), "house": col("newData.house"),
#          "location": col("newData.location"), "age": col("newData.age")}) \
#     .whenNotMatchedInsert(
#     values={"firstname": col("newData.firstname"), "lastname": col("newData.lastname"), "house": col("newData.house"),
#             "location": col("newData.location"), "age": col("newData.age")}) \
#     .execute()
#
# deltaTable.toDF().show()
#
#
# # Delete Data
# print("Deleting data...!")
#
# # delta table path
# deltaTable = DeltaTable.forPath(spark, path=hdfs_path)
# deltaTable.toDF().show()
#
# deltaTable.delete(condition=expr("firstname == 'Gendry'"))
#
# deltaTable.toDF().show(1000)
# deltaTable.toDF().write.format("delta").saveAsTable("delta_as_table")
