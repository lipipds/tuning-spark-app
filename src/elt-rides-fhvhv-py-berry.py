"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/elt-rides-fhvhv-py-berry.py
"""

import pandas as pd

from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf

from utils.utils import init_spark_session, list_files


@pandas_udf(StringType())
def license_num(license_num: pd.Series) -> pd.Series:
    """
    :param license_num: The license number of the ride-sharing service
    :return: The name of the ride-sharing service associated with the given license number
    """

    return license_num.map({
        'HV0002': 'Juno',
        'HV0003': 'Uber',
        'HV0004': 'Via',
        'HV0005': 'Lyft'
    }).fillna('Unknown')


def main():
    spark = init_spark_session("elt-rides-fhvhv-py-berry")

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

    df_fhvhv = df_fhvhv.withColumn('hvfhs_license_num', license_num(df_fhvhv['hvfhs_license_num']))

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
        ORDER BY request_datetime DESC
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

    df_total_trip_time.show()
    df_hvfhs_license_num.show()


if __name__ == "__main__":
    main()
