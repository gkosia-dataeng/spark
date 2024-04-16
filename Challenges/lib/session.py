from pyspark.sql import SparkSession


def get_spark_session(_appName) -> SparkSession:

    spark = SparkSession. \
            builder. \
            appName(_appName). \
            getOrCreate()

    return spark