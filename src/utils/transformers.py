"""
Functions for PySpark with Business Logic

Best: Native Spark Function [Large DataSets]
Moderate: Vectorized Pandas Function [PyArrow]
Low: Normal UDF Function [Python]
"""

from pyspark.sql.functions import col, when


def hvfhs_license_num(df):
    """
    Transform the hvfhs_license_num field based on the following logic:

    - HV0002: Juno
    - HV0003: Uber
    - HV0004: Via
    - HV0005: Lyft

    :param df: Input DataFrame with Hvfhs_license_num field
    :return: DataFrame with transformed Hvfhs_license_num field
    """

    transformed_df = df.withColumn("hvfhs_license_num",
         when(col("hvfhs_license_num") == "HV0002", "Juno")
        .when(col("hvfhs_license_num") == "HV0003", "Uber")
        .when(col("hvfhs_license_num") == "HV0004", "Via")
        .when(col("hvfhs_license_num") == "HV0005", "Lyft")
        .otherwise(col("hvfhs_license_num"))
    )

    return transformed_df
