from datetime import datetime

import hdfs.client as h
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


class BronzeLayer:
    def __init__(self, src_format, src_driver_name, src_url, src_id=None, src_passwd=None):
        self.src_format = src_format
        self.src_driver_name = src_driver_name
        self.src_url = src_url
        self.src_id = src_id
        self.src_passwd = src_passwd

    def isExistedDirectory(self, path: str):
        h.Client(url='http://localhost:50070')
        isExistedFile = False
        response = client.status(hdfs_path=path, strict=False)
        if response is not None:
            isExistedFile = True
        return isExistedFile

    def rawdataLanding2Datalake(self, df: DataFrame, database: str, tableName: str):
        hdfs_path = "hdfs://localhost:9000/raw-data"
        today = datetime.today().strftime('%Y_%m_%d')
        dest_path = f"{hdfs_path}/{database}/{tableName}/{today}"
        df.write.format('delta') \
            .mode(saveMode="overwrite") \
            .save(dest_path)

        pass

    def ingestRawData(self, spark: SparkSession, dbTableName, partitionColumnName) -> DataFrame:
        return spark.read \
            .format(self.src_format) \
            .option('driver', self.src_driver_name) \
            .option('url', self.src_url) \
            .option('user', self.src_id) \
            .option('password', self.src_passwd) \
            .option('dbtable', dbTableName) \
            .option('partitionColumn', partitionColumnName) \
            .option('lowerBound', 1) \
            .option('upperBound', 10) \
            .option('numPartitions', 3) \
            .load()


if __name__ == '__main__':
    client = h.Client(url='http://localhost:50070')
    is_existed_file = client.status(hdfs_path='/spark/warehouse/throne.db/2023-11-13', strict=False)
    if is_existed_file is None:
        print('Not existed File')
    is_existed_file = True
