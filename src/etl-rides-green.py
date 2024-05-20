from src.utils.utils import init_spark_session, list_files


def main():
    spark = init_spark_session("etl-rides-green")

    file_df_fhvhv = "./storage/fhvhv/*/*.parquet"
    list_files(spark, file_df_fhvhv)

    df_fhvhv = spark.read.parquet(file_df_fhvhv)

    print(f"number of partitions: {df_fhvhv.rdd.getNumPartitions()}")
    df_fhvhv.printSchema()

    print(f"number of rows: {df_fhvhv.count()}")
    df_fhvhv.show()

    df_fhvhv.createOrReplaceTempView("fhvhv_table")

    shuffle_result = spark.sql("""
        SELECT dispatching_base_num, COUNT(*) AS count
        FROM fhvhv_table
        GROUP BY dispatching_base_num
        ORDER BY count DESC
    """)

    shuffle_result.show()

    print(f"number of partitions after shuffle: {shuffle_result.rdd.getNumPartitions()}")


if __name__ == "__main__":
    main()
