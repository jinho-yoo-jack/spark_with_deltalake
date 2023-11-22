#!/bin/bash
SPARK_SUBMIT_HOME=/Users/black/dev/bigdata/spark-3.2.4
execute_shell=$1
echo "Execution => ${execute_shell}"


${SPARK_SUBMIT_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode client \
--packages io.delta:delta-core_2.12:2.1.0 \
--conf hive.metastore.uris="thrift://localhost:9083" \
--conf spark.sql.warehouse.dir="hdfs://localhost:9000/spark/warehouse" \
--conf spark.sql.extensions="io.delta.sql.DeltaSparkSessionExtension" \
--conf spark.sql.catalog.spark_catalog="org.apache.spark.sql.lakehouse.catalog.DeltaCatalog" \
${execute_shell}
#insert-src-into-oracle.py
#data_ingestion_by_date.py
