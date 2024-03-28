from pyspark.sql import SparkSession


def get_spark_session(_appName) -> SparkSession:

    spark = SparkSession. \
            builder. \
            appName(_appName). \
            config("spark.sql.warehouse.dir",f"/home/jovyan/data/warehouse"). \
            enableHiveSupport(). \
            getOrCreate()

    return spark
    