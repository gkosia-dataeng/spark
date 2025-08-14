from pyspark.sql import SparkSession


def get_spark_session(_appName) -> SparkSession:

    spark = SparkSession. \
            builder. \
            appName(_appName). \
            getOrCreate()

    return spark


def connect_to_minIO(spark):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")        # MinIO service name and port
    hadoop_conf.set("fs.s3a.access.key", "minIOuser")             # MinIO root user
    hadoop_conf.set("fs.s3a.secret.key", "minIOpass")             # MinIO root password
    hadoop_conf.set("fs.s3a.path.style.access", "true")            # Required for MinIO
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")