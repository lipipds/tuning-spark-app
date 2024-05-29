"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-strawberry.py
"""

from pyspark.sql.functions import current_timestamp, col, sum, avg, udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from utils.utils import init_spark_session, list_files
from utils.transformers import hvfhs_license_num


def categorize_trip(distance):
    if distance > 10:
        return "Long"
    elif distance > 5:
        return "Medium"
    else:
        return "Short"


categorize_trip_udf = udf(categorize_trip, StringType())


def main():

    # TODO [0]: add KyroSerializer to SparkConf.
    spark = init_spark_session("elt-rides-fhvhv-py-strawberry")

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)

    # TODO [1]: coalesce to 18 partitions & later change partition size.
    partition_number = 18
    df_fhvhv = spark.read.parquet(file_fhvhv).repartition(partition_number)
    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

    print(f"number of rows: {df_fhvhv.count()}")
    df_fhvhv.show()

    df_fhvhv = hvfhs_license_num(df_fhvhv)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    # TODO [3]: optimize joins and aggregations to reduce shuffles.
    df_rides = spark.sql("""
        SELECT hvfhs_license_num,
               zones_pu.Borough AS PU_Borough,
               zones_pu.Zone AS PU_Zone,
               zones_do.Borough AS DO_Borough,
               zones_do.Zone AS DO_Zone,
               request_datetime,
               pickup_datetime,
               dropoff_datetime,
               trip_miles,
               trip_time,
               base_passenger_fare,
               tolls,
               bcf,
               sales_tax,
               congestion_surcharge,
               tips,
               driver_pay,
               shared_request_flag,
               shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
        DISTRIBUTE BY zones_pu.Borough, zones_pu.Zone
    """)

    df_rides = df_rides.withColumn("ingestion_timestamp", current_timestamp())
    df_rides = df_rides.withColumn("trip_category", categorize_trip_udf(col("trip_miles")))
    window_spec = Window.partitionBy("PU_Borough", "PU_Zone").orderBy("pickup_datetime")

    df_rides = df_rides.withColumn("running_total_fare", sum("base_passenger_fare").over(window_spec))
    df_rides = df_rides.withColumn("average_trip_time", avg("trip_time").over(window_spec))

    df_rides.createOrReplaceTempView("rides")

    df_total_trip_time = spark.sql("""
        SELECT 
            PU_Borough,
            PU_Zone,
            DO_Borough,
            DO_Zone,
            trip_category,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time,
            MAX(ingestion_timestamp) AS ingestion_timestamp,
            MAX(running_total_fare) AS running_total_fare,
            MAX(average_trip_time) AS average_trip_time
        FROM 
            rides
        GROUP BY 
            PU_Borough, 
            PU_Zone,
            DO_Borough,
            DO_Zone,
            trip_category
    """)

    df_hvfhs_license_num = spark.sql("""
        SELECT 
            hvfhs_license_num,
            trip_category,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time,
            MAX(ingestion_timestamp) AS ingestion_timestamp,
            MAX(running_total_fare) AS running_total_fare,
            MAX(average_trip_time) AS average_trip_time
        FROM 
            rides
        GROUP BY 
            hvfhs_license_num,
            trip_category
    """)

    storage = "./storage/rides/parquet/"
    df_total_trip_time.write.mode("append").partitionBy("PU_Borough").parquet(storage + "total_trip_time")
    df_hvfhs_license_num.write.mode("append").partitionBy("hvfhs_license_num").parquet(storage + "hvfhs_license_num")


if __name__ == "__main__":
    main()
