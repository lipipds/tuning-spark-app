"""
PySpark Functions:
- Built-in
- User-Defined [UDFs]
- Pandas UDFs [Vectorized UDFs]
"""

import time
import pandas as pd

from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import DoubleType

from utils.utils import init_spark_session, list_files
from utils.transformers import transform_hvfhs_license_num


def main():
    spark = init_spark_session("etl-rides-green")

    file_fhvhv = "./storage/fhvhv/*.parquet"
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

    # Built-In
    start_time = time.time()
    df_with_km_built_in = df_fhvhv.withColumn("trip_km", col("trip_miles") * 1.60934)
    df_with_km_built_in.count()
    print(f"built-in: {time.time() - start_time} seconds")

    # User-Defined
    def miles_to_km(miles):
        return miles * 1.60934

    miles_to_km_udf = udf(miles_to_km, DoubleType())

    start_time = time.time()
    df_with_km_udf = df_fhvhv.withColumn("trip_km", miles_to_km_udf(col("trip_miles")))
    df_with_km_udf.count()
    print(f"udf: {time.time() - start_time} seconds")

    # Pandas UDF
    @pandas_udf(DoubleType())
    def miles_to_km_pandas_udf(miles: pd.Series) -> pd.Series:
        return miles * 1.60934

    start_time = time.time()
    df_with_km_pandas_udf = df_fhvhv.withColumn("trip_km", miles_to_km_pandas_udf(col("trip_miles")))
    df_with_km_pandas_udf.count()
    print(f"pandas udf: {time.time() - start_time} seconds")

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
               airport_fee,
               tips,
               driver_pay,
               shared_request_flag,
               shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON hvfhs.PULocationID = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
    """)

    df_rides.show()
        

if __name__ == "__main__":
    main()
