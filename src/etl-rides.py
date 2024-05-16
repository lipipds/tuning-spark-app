"""
local
spark-submit src/etl-rides.py

docker
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/etl-rides.py
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder \
    .appName("etl-rides") \
    .getOrCreate()

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("WARN")

file_df_fhvhv = "./storage/fhvhv_tripdata_2023-01.parquet"
df_fhvhv = spark.read.parquet(file_df_fhvhv)

print(df_fhvhv.rdd.getNumPartitions())

df_fhvhv.printSchema()
df_fhvhv.count()
df_fhvhv.show()
