"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-chocolate.py
"""

import logging
from utils.utils import init_spark_session, list_files
from utils.transformers import hvfhs_license_num

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

logger = logging.getLogger('my_custom_logger')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.propagate = True


def main():
    logger.info("init spark session")
    spark = init_spark_session("elt-rides-fhvhv-py-chocolate")

    logger.info("list fhvhv files")
    file_fhvhv = "./storage/fhvhv/2022/*.parquet"
    list_files(spark, file_fhvhv)
    df_fhvhv = spark.read.parquet(file_fhvhv)

    logger.info("list zones file")
    file_zones = "./storage/zones.csv"
    list_files(spark, file_zones)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

    num_partitions = df_fhvhv.rdd.getNumPartitions()
    logger.info("number of partitions: %d", num_partitions)
    df_fhvhv.printSchema()

    num_rows = df_fhvhv.count()
    logger.info("number of rows: %d", num_rows)
    df_fhvhv.show()

    # TODO use of: built-in spark function
    df_fhvhv = hvfhs_license_num(df_fhvhv)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    # TODO remove order by clause
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

    # TODO remove show & display


if __name__ == "__main__":
    main()
