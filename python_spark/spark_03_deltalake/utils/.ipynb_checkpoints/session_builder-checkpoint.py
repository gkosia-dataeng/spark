from pyspark.sql import SparkSession



def get_session_deltalake(appName, master="local[*]"):
    
    return (    
             SparkSession.
             builder.
             appName(appName).
             master(master).
             config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").
             config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").
             config("spark.sql.catalogImplementation", "hive").
             config("spark.sql.warehouse.dir", "s3a://lakehouse/warehouse/delta").
             getOrCreate()     
          )
