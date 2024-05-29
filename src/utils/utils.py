"""
Utility Functions for PySpark

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --version

Spark = 3.5.1
Scala = 2.12
"""

import pyspark

from delta import *
from pyspark import SparkConf
from py4j.java_gateway import java_import


def init_spark_session(app_name):
    """Initialize Spark session."""

    builder = (
        pyspark.sql.SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "3g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("INFO")

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
