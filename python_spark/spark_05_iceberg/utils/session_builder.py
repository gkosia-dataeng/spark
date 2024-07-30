from pyspark.sql import SparkSession



def get_session_iceberglake(appName, master="local[*]"):
    
    return (    
             SparkSession.
             builder.
             appName(appName).
             master(master).
             config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"). 
             config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog"). 
             config("spark.sql.catalogImplementation", "hive"). 
             config("spark.sql.warehouse.dir", "s3a://lakehouse/warehouse/iceberg").
             getOrCreate()     
          )
