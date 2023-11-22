import pyspark
from lakehouse import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# Create a spark session with Delta
builder = pyspark.sql.SparkSession.builder.appName("appended-datasource-ingest-into-datalake") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/spark/warehouse") \
    .config("spark.sql.extensions", "io.lakehouse.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakehouse.catalog.DeltaCatalog")

# Create spark context
spark = configure_spark_with_delta_pip(builder) \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Starting Delta table creation")

newlyArrivedData = [
    ("Catelyn", "Stark", "Stark", "Stark House", 58),
    ("Jon", "Snow", "Stark", "Winterfell", 30)
]

schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("house", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True)
])

print("Starting Delta table creation")
hdfs_path = "hdfs://localhost:9000/raw-data"
today = datetime.today().strftime('%Y_%m_%d')
database = 'family'
# dest_path = f"{hdfs_path}/{database}/{today}"
dest_path = f"{hdfs_path}/{database}/2023_11_15"

df = spark.createDataFrame(data=newlyArrivedData, schema=schema)
df.write.mode(saveMode="overwrite") \
    .format("lakehouse") \
    .save(dest_path)

# sample_dataframe.write.mode(saveMode="overwrite").format("lakehouse").save(hdfs_path+"/family_1")
#
# # Read Data
# print("Reading lakehouse file ... !")
#
# got_df = spark.read.format("lakehouse").load("hdfs://localhost:9000/data/lakehouse-table")
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
# sample_dataframe = spark.createDataFrame(data=data, source_data=source_data)
# # sample_dataframe.write.mode(saveMode="overwrite").format("lakehouse").save("data/lakehouse-table")
# sample_dataframe.write.mode(saveMode="overwrite").format("lakehouse").save("hdfs://localhost:9000/data/lakehouse-table")
#
#
# got_df = spark.read.format("lakehouse").load("hdfs://localhost:9000/data/lakehouse-table")
# got_df.show()
#
# # Update data in Delta
# print("Update lakehouse table ... !")
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
# # lakehouse table path
# deltaTable = DeltaTable.forPath(spark, path=hdfs_path)
# deltaTable.toDF().show()
#
# # define new data
# data = [("Gendry", "Baratheon", "Baratheon", "Kings Landing", 19),
#         ("Jon", "Snow", "Stark", "Winterfell", 21),
#         ("Jamie", "Lannister", "Lannister", "Casterly Rock", 36)
#         ]
# source_data = StructType([
#     StructField("firstname", StringType(), True),
#     StructField("lastname", StringType(), True),
#     StructField("house", StringType(), True),
#     StructField("location", StringType(), True),
#     StructField("age", IntegerType(), True)
# ])
#
# newData = spark.createDataFrame(data=data, source_data=source_data)
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
# # lakehouse table path
# deltaTable = DeltaTable.forPath(spark, path=hdfs_path)
# deltaTable.toDF().show()
#
# deltaTable.delete(condition=expr("firstname == 'Gendry'"))
#
# deltaTable.toDF().show(1000)
# deltaTable.toDF().write.format("lakehouse").saveAsTable("delta_as_table")
