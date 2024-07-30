from pyspark.sql import SparkSession



def get_session_hudilake(appName, master="local[*]"):
    
    return (    
             SparkSession.
             builder.
             appName(appName).
             master(master).
             config("spark.serializer", "org.apache.spark.serializer.KryoSerializer"). 
             config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"). 
             config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"). 
             config("spark.sql.catalogImplementation", "hive"). 
             config("spark.sql.warehouse.dir", "s3a://lakehouse/warehouse/hudi").
             getOrCreate()     
          )
