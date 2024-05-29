"""
PySpark: elt-rides-fhvhv-py-strawberry-owshq
Author: Luan Moreno

common problems:
- select columns to reduce footprint

issues to be addressed:
- pandas udf = licensed_num
- order by = request_datetime in rides table
- partition by = using parquet file format

executing job:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-strawberry-owshq.py
"""

from pyspark.sql.functions import current_date, udf
from pyspark.sql.types import StringType

from utils.utils import init_spark_session, list_files


def license_num(num):
    """
    :param num: The license number of the ride-sharing service
    :return: The name of the ride-sharing service associated with the given license number
    """

    if num == 'HV0002':
        return 'Juno'
    elif num == 'HV0003':
        return 'Uber'
    elif num == 'HV0004':
        return 'Via'
    elif num == 'HV0005':
        return 'Lyft'
    else:
        return 'Unknown'


def main():

    spark = init_spark_session("elt-rides-fhvhv-py-strawberry-owshq")

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)

    # TODO [1]: select columns to reduce footprint.
    fhvhv_cols = [
        "hvfhs_license_num", "PULocationID", "DOLocationID",
        "request_datetime", "pickup_datetime", "dropoff_datetime",
        "trip_miles", "trip_time", "base_passenger_fare", "tolls",
        "bcf", "sales_tax", "congestion_surcharge", "tips"]
    df_fhvhv = spark.read.parquet(file_fhvhv).select(*fhvhv_cols)
    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)
    print(f"number of rows: {df_fhvhv.count()}")

    udf_license_num = udf(license_num, StringType())
    spark.udf.register("license_num", udf_license_num)
    df_fhvhv = df_fhvhv.withColumn('hvfhs_license_num', udf_license_num(df_fhvhv['hvfhs_license_num']))

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

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
               tips
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
        ORDER BY request_datetime DESC
    """)

    df_rides = df_rides.withColumn("ingestion_timestamp", current_date())
    df_rides.createOrReplaceTempView("rides")

    df_total_trip_time = spark.sql("""
        SELECT 
            ingestion_timestamp,
            PU_Borough,
            PU_Zone,
            DO_Borough,
            DO_Zone,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time
        FROM 
            rides
        GROUP BY 
            ingestion_timestamp,
            PU_Borough, 
            PU_Zone,
            DO_Borough,
            DO_Zone
    """)

    df_hvfhs_license_num = spark.sql("""
        SELECT 
            ingestion_timestamp,
            hvfhs_license_num,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time
        FROM 
            rides
        GROUP BY 
            ingestion_timestamp,
            hvfhs_license_num
    """)

    storage = "./storage/rides/parquet/"
    df_total_trip_time.write.mode("append").partitionBy("ingestion_timestamp").parquet(storage + "total_trip_time")
    df_hvfhs_license_num.write.mode("append").partitionBy("hvfhs_license_num").parquet(storage + "hvfhs_license_num")


if __name__ == "__main__":
    main()
