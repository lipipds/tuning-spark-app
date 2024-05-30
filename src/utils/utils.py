"""
Utility Functions for PySpark

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --version

Spark = 3.5.1
Scala = 2.12
"""

import logging
import pyspark
from delta import *
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

logger = logging.getLogger('owshq')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.propagate = True


def init_spark_session(app_name):
    """Initialize Spark session with log4j configuration."""

    builder = (
        pyspark.sql.SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j2.properties")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j2.properties")
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info("spark config: %s", spark.sparkContext.getConf().getAll())
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
        logger.info("file: %s", status.getPath().toString())
