"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-butter.py
"""

from utils.utils import init_spark_session, list_files
from utils.transformers import hvfhs_license_num


def main():
    spark = init_spark_session("elt-rides-fhvhv-py-butter")

    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)
    df_fhvhv = spark.read.parquet(file_fhvhv)

    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")
    df_fhvhv.printSchema()

    print(f"number of rows: {df_fhvhv.count()}")
    df_fhvhv.show()

    df_fhvhv = hvfhs_license_num(df_fhvhv)

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
               tips,
               driver_pay,
               shared_request_flag,
               shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
    """)

    df_rides.createOrReplaceTempView("rides")

    df_total_trip_time = spark.sql("""
                SELECT 
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
                    PU_Borough, 
                    PU_Zone,
                    DO_Borough,
                    DO_Zone
            """)

    df_hvfhs_license_num = spark.sql("""
            SELECT 
                hvfhs_license_num,
                SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
                SUM(trip_miles) AS total_trip_miles,
                SUM(trip_time) AS total_trip_time
            FROM 
                rides
            GROUP BY 
                hvfhs_license_num
        """)

    # TODO write in delta [liquid clustering] | partitioned by vs. cluster by
    spark.sql("""
        CREATE TABLE hvfhs_license_num(
            PU_Borough string, 
            PU_Zone string,
            DO_Borough string,
            DO_Zone string,
            total_fare double,
            total_trip_miles double,
            total_trip_time long
        ) 
        USING DELTA 
        LOCATION './storage/rides/cluster/hvfhs_license_num'
        CLUSTER BY (PU_Borough);
    """)


if __name__ == "__main__":
    main()
