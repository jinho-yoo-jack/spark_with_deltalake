import pyspark
from delta import *

from spark.session_manager import SparkSessionManager

if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("main") \
        .config("hive.metastore.uris", "thrift://localhost:9083")

    spark = configure_spark_with_delta_pip(builder) \
        .enableHiveSupport() \
        .getOrCreate()

    r = spark.conf.get("spark.sql.catalogImplementation")
    print(r)
