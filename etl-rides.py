from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder \
    .appName("etl-rides") \
    .getOrCreate()

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("WARN")

file_df_fhvhv = "storage/fhvhv_tripdata_2023-01.parquet"
df_fhvhv = spark.read.parquet(file_df_fhvhv)

print(df_fhvhv.rdd.getNumPartitions())

df_fhvhv.printSchema()
df_fhvhv.count()
df_fhvhv.show()
