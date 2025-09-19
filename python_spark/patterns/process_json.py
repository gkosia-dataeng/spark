from common.session import get_spark_session

spark = get_spark_session('Process json')

json_txt = '{"Institute_Name":"ABC_Coaching_Center","Course_type":"Best_seller","branches":[{"State":"Maharashtra","City":"Mumbai","address":"XYZ"},{"State":"Gujrat","City":"Surat","address":"PQRX"}],"Head_Office_Contact":8787878787}'


rdd = spark.sparkContext.parallelize([json_txt])

# 
df_json = spark.read.json(rdd)
df_json.show(truncate=False)
df_json.printSchema()


'''
    explode: explode a list and convert multiple rows with each value
'''

from pyspark.sql.functions import explode, col

df_json_exploded = df_json.select("Course_type", "Head_Office_Contact", "Institute_Name", explode(col("branches")).alias('branche'))
df_json_exploded.show(truncate=False)
df_json_exploded.printSchema()



'''
    When is struct i can said branche.key to generate a new column or branche.* to generate all columns
'''

df_json_exploded.select("Course_type", "Head_Office_Contact", "Institute_Name", "branche.address").show()

df_json_exploded.selectExpr("Course_type as type", "branche.*").show()



df_json_as_text = spark.createDataFrame([(json_txt,)], ["json_as_text"])
df_json_as_text.show(truncate=False)
df_json_as_text.printSchema()




'''
    from_json: convert a column that contains JSON as string to Struct type based on a json schema
    schema_of_json: return the schema of json sample as string
    get_json_object: extract an information from JSON string using the path
'''

from pyspark.sql.functions import from_json, schema_of_json, get_json_object

df_json_as_struct = df_json_as_text.withColumn("json_as_struct", from_json("json_as_text", schema_of_json(json_txt)))
df_json_as_struct = df_json_as_struct.withColumn("schema", schema_of_json(json_txt))
df_json_as_struct.printSchema()
df_json_as_struct.show(truncate=False)




df_json_as_struct.withColumn("info_using_get_json_object", get_json_object("json_as_text", "$.branches[0].State")).show()
