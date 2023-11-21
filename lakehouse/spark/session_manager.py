from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(self):
        self.spark: SparkSession = None

    def start_spark_session(self, config):
        self.spark = SparkSession.builder.config(conf=config) \
            .getOrCreate()
        print('Started SparkSession.')

    def stop_spark_session(self):
        if self.spark is not None:
            self.spark.stop()
           
    def getSparkConfig(self):
        pass
