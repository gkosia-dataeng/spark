from pyspark.sql import SparkSession


def get_spark_session(_appName) -> SparkSession:

    spark = SparkSession. \
            builder. \
            appName(_appName). \
            config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0"). \
            config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"). \
            config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"). \
            config("spark.sql.warehouse.dir",f"/home/jovyan/data/warehouse"). \
            enableHiveSupport(). \
            getOrCreate()

    return spark
    