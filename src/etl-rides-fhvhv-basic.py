"""
ETL Rides FHVHV
"""

from utils.utils import init_spark_session, list_files
from utils.transformers import transform_hvfhs_license_num


def main():
    spark = init_spark_session("etl-rides-fhvhv-basic")

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

    df_fhvhv = transform_hvfhs_license_num(df_fhvhv)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    df_rides = spark.sql("""
        SELECT hvfhs_license_num,
               zones_pu.Borough AS PU_Borough,
               zones_pu.Zone AS PU_Zone,
               zones_do.Borough AS DO_Borough,
               zones_do.Zone AS DO_Zone,
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
            DO_Borough,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time
        FROM 
            rides
        GROUP BY 
            PU_Borough, 
            DO_Borough
        ORDER BY 
            total_fare DESC
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
        ORDER BY 
            total_fare DESC
    """)

    df_rides.write.parquet("./storage/rides/hvfhs", mode="overwrite")
    df_total_trip_time.write.parquet("./storage/rides/total_trip_time", mode="overwrite")
    df_hvfhs_license_num.write.parquet("./storage/rides/hvfhs_license_num", mode="overwrite")


if __name__ == "__main__":
    main()
