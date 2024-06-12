from pyspark.sql import SparkSession

def get_session_on_local(appName):

    return (
            SparkSession.
            builder.
            enableHiveSupport().
            getOrCreate()
           )




def get_session_on_cluster(master, appName):
    return (    
             SparkSession.
             builder.
             appName(appName).
             master(master).
             enableHiveSupport().
             getOrCreate()     
          )
