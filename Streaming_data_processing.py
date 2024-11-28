# Databricks notebook source
# MAGIC %md
# MAGIC ### Read streaming data from Kinesis

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import urllib
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType # help with schemas set up

# Define the path to the Delta table for credential information
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
     

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning methods

# COMMAND ----------

def cleaned_pin_df(df_to_clean):
    pin_df = df_to_clean 
    # Replace empty entries and entries with no relevant data in each column with Nones
    pin_df = pin_df.replace({
        'No description available Story format': None,
        'No description available': None,
        'Untitled': None}, subset=['description'])
    pin_df = pin_df.replace({'User Info Error': None}, subset=['follower_count'])
    pin_df = pin_df.replace({'Image src error.': None}, subset=['image_src'])
    pin_df = pin_df.replace({'User Info Error': None}, subset=['poster_name'])
    pin_df = pin_df.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset=['tag_list'])
    pin_df = pin_df.replace({"No Title Data Available": None}, subset=["title"])

    # Perform transformations to ensure every entry is a number
    pin_df = pin_df.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
    pin_df = pin_df.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

    # Clean the data to include only the save location path
    pin_df = pin_df.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

    # Rename/drop column
    pin_df = pin_df.withColumnRenamed("index", "ind")
    pin_df = pin_df.drop("index")

    # Cast to datatype
    pin_df = pin_df.withColumn("follower_count", pin_df["follower_count"].cast("int"))
    # pin_df = pin_df.withColumn("downloaded", pin_df["downloaded"].cast("int"))
    # pin_df = pin_df.withColumn("downloaded",col("downloaded").cast("int"))
    pin_df = pin_df.withColumn("ind", pin_df["ind"].cast("int"))

    # Reorder the DataFrame columns
    pin_df = pin_df.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

    return pin_df

def cleaned_geo_df(df_to_clean):
    geo_df = df_to_clean 
    # Create array column based on the latitude and longitude columns
    geo_df = geo_df.withColumn("coordinates", array("latitude", "longitude"))

    # Drop the latitude and longitude columns from the DataFrame
    geo_df = geo_df.drop("latitude", "longitude")

    # Cast the timestamp column to a timestamp data type
    geo_df = geo_df.withColumn("timestamp", to_timestamp("timestamp"))

    # Reorder the DataFrame columns
    geo_df = geo_df.select("ind", "country", "coordinates", "timestamp")

    return geo_df


def cleaned_user_df(df_to_clean):
    user_df = df_to_clean 
    # Create a new column user_name that concatenates the information found in the first_name and last_name columns
    user_df = user_df.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

    # Drop the first_name and last_name columns from the DataFrame
    user_df = user_df.drop("first_name", "last_name")

    # Cast the date_joined column to a timestamp data type
    user_df = user_df.withColumn("date_joined", to_timestamp("date_joined"))

    # Reorder the DataFrame columns
    user_df = user_df.select("ind", "user_name", "age", "date_joined")

    return user_df
     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull [PIN stream]
# MAGIC - Defining schema for stream table
# MAGIC - Reading in streamed data
# MAGIC - Deserialising stream data
# MAGIC - Cleaning streamed data
# MAGIC - Writing streams data into delta tables
# MAGIC - NOTE: the writeStreams must be interrupted before the next one can run

# COMMAND ----------

# Pin schema

# Define structured streaming schema using StructType
pin_df_schema = StructType([\
    StructField("category", StringType(), True),\
    StructField("description", StringType(), True),\
    StructField("downloaded", IntegerType(), True),\
    StructField("follower_count", StringType(), True),\
    StructField("image_src", StringType(), True),\
    StructField("index", IntegerType(), True),\
    StructField("is_image_or_video", StringType(), True),\
    StructField("poster_name", StringType(), True),\
    StructField("save_location", StringType(), True),\
    StructField("tag_list", StringType(), True),\
    StructField("title", StringType(), True),\
    StructField("unique_id", StringType(), True),\
])

# Pin stream
# Now using the ACCESS_KEY and SECRET_KEY we can read the streaming data from Kinesis using the format below (make sure you are sending data to your stream before running the code cells below):

pin_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe4008223-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

##to look at the data
# display(pin(df)) 

# Cast the data column to string to deserialise
pin_df = pin_df.selectExpr("CAST(data AS STRING) AS jsonData")

##to look at the data
# display(pin(df)) 

# Deserialise the JSON data using the schema
# https://www.databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html to help with transformation
pin_df = pin_df.select(from_json("jsonData", pin_df_schema).alias("parsed_data"))

# Explode the parsed_data struct column to get individual columns
pin_df = pin_df.select("parsed_data.*")

pin_df = cleaned_pin_df(pin_df)

display(pin_df)

# Remove the checkpoint folder first
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

pin_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe4008223_pin_table")
     

# COMMAND ----------

#dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull [GEO stream]
# MAGIC - Defining schema for stream table
# MAGIC - Reading in streamed data
# MAGIC - Deserialising stream data
# MAGIC - Cleaning streamed data
# MAGIC - Writing streams data into delta tables
# MAGIC - NOTE: the writeStreams must be interrupted before the next one can run

# COMMAND ----------

# Define a streaming schema using StructType
# Geo schema
geo_df_schema = StructType([\
    StructField("country", StringType(), True),\
    StructField("ind", IntegerType(), True),\
    StructField("latitude", StringType(), True),\
    StructField("longitude", StringType(), True),\
    StructField("timestamp", StringType(), True),\
])

# Geo stream
geo_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe4008223-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Cast the data column to string to deserialize
geo_df = geo_df.selectExpr("CAST(data AS STRING) AS jsonData")

# Deserialize the JSON data using the schema
geo_df = geo_df.select(from_json("jsonData", geo_df_schema).alias("parsed_data"))

# Explode the parsed_data struct column to get individual columns
geo_df = geo_df.select("parsed_data.*")

geo_df = cleaned_geo_df(geo_df)

display(geo_df)

# Remove the checkpoint folder first
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

geo_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe4008223_geo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull [USER stream]
# MAGIC - Defining schema for stream table
# MAGIC - Reading in streamed data
# MAGIC - Deserialising stream data
# MAGIC - Cleaning streamed data
# MAGIC - Writing streams data into delta tables
# MAGIC - NOTE: the writeStreams must be interrupted before the next one can run

# COMMAND ----------

# Define a streaming schema using StructType
# User schema
user_df_schema = StructType([\
    StructField("age", StringType(), True), \
    StructField("category", StringType(), True), \
    StructField("date_joined", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("downloaded", IntegerType(), True), \
    StructField("first_name", StringType(), True), \
    StructField("follower_count", StringType(), True), \
    StructField("image_src", StringType(), True), \
    StructField("ind", IntegerType(), True), \
    StructField("index", IntegerType(), True), \
    StructField("is_image_or_video", StringType(), True), \
    StructField("last_name", StringType(), True), \
    StructField("poster_name", StringType(), True), \
    StructField("save_location", StringType(), True), \
    StructField("tag_list", StringType(), True), \
    StructField("title", StringType(), True), \
    StructField("unique_id", StringType(), True), \
])

# User stream
user_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affe4008223-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# Cast the data column to string to deserialize
user_df = user_df.selectExpr("CAST(data AS STRING) AS jsonData")

# Deserialize the JSON data using the schema
user_df = user_df.select(from_json("jsonData", user_df_schema).alias("parsed_data"))

# Explode the parsed_data struct column to get individual columns
user_df = user_df.select("parsed_data.*")

user_df = cleaned_user_df(user_df)

display(user_df)

# Remove the checkpoint folder first
# dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

user_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affe4008223_user_table")
