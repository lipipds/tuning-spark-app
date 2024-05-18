"""
Mode: Local
Spark Measure: Stage & Task Metrics

spark-submit \
    --packages ch.cern.sparkmeasure:spark-measure_2.12:0.23 \
    spark-measure.py
"""

import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from sparkmeasure import StageMetrics, TaskMetrics

base_path = os.path.abspath(os.getcwd())

conf = SparkConf()
conf.set("spark.app.name", "etl-rides")
conf.set("spark.master", "local[*]")
conf.set("spark.executor.memory", "4g")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
taskmetrics = TaskMetrics(spark)
stagemetrics = StageMetrics(spark)

print(SparkConf().getAll())
print(conf.toDebugString())
spark.sparkContext.setLogLevel("WARN")

file_df_fhvhv = "storage/fhvhv/*.parquet"
df_fhvhv = spark.read.parquet(file_df_fhvhv)

print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")
df_fhvhv.printSchema()
print(f"number of records: {df_fhvhv.count()}")
df_fhvhv.show()

#
stagemetrics.begin()
df_fhvhv.groupBy("hvfhs_license_num").count().show()
stagemetrics.end()
stagemetrics.print_report()

stage_metrics = stagemetrics.aggregate_stagemetrics()
print(f"stage metrics elapsedTime = {stage_metrics.get('elapsedTime')}")

stage_metrics_perf_state_metrics_df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
stagemetrics.save_data(stage_metrics_perf_state_metrics_df.orderBy("jobId", "stageId"), base_path + "/json/stage_metrics_report")

stage_metrics_spark_measure_agg_df = stagemetrics.aggregate_stagemetrics_DF("PerfStageMetrics")
stagemetrics.save_data(stage_metrics_spark_measure_agg_df, base_path + "/json/stage_metrics_agg_report")

taskmetrics.runandmeasure(globals(), 'df_fhvhv.groupBy("hvfhs_license_num").count().show()')


spark.stop()
