"""
Utility Functions for PySpark
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
from py4j.java_gateway import java_import


def init_spark_session(app_name):
    """Initialize Spark session."""

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("WARN")

    return spark


def list_files(spark, file_pattern):
    """List Files using Hadoop FileSystem API."""

    hadoop_conf = spark._jsc.hadoopConfiguration()
    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

    fs = spark._jvm.FileSystem.get(hadoop_conf)
    path = spark._jvm.Path(file_pattern)
    file_statuses = fs.globStatus(path)

    for status in file_statuses:
        print(status.getPath().toString())
