# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import array, concat, lit, to_timestamp
# from pyspark.sql.functions import regexp_replace

# COMMAND ----------

print("To check if the S3 bucket was mounted successfully: \n")
display(dbutils.fs.ls("s3n://user-0affe4008223-bucket/topics/0affe4008223.pin/partition=0/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
pin_location = "s3n://user-0affe4008223-bucket/topics/0affe4008223.pin/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"

# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(pin_location)
# Display Spark dataframe to check its content

display(df_pin)

# COMMAND ----------

# MAGIC %md
# MAGIC ### To clean the df_pin DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Replace empty entries and entries with no relevant data in each column with Nones
# MAGIC - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# MAGIC - Ensure that each column containing numeric data has a numeric data type
# MAGIC - Clean the data in the save_location column to include only the save location path
# MAGIC - Rename the index column to ind.
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - unique_id
# MAGIC   - title
# MAGIC   - description
# MAGIC   - follower_count
# MAGIC   - poster_name
# MAGIC   - tag_list
# MAGIC   - is_image_or_video
# MAGIC   - image_src
# MAGIC   - save_location
# MAGIC   - category
# MAGIC

# COMMAND ----------

# Replace empty entries and entries with no relevant data in each column with Nones
df_pin = df_pin.replace({'No description available Story format': None, 'No description available': None, 'Untitled': None}, subset=['description'])
df_pin = df_pin.replace({'User Info Error': None}, subset=['follower_count'])
df_pin = df_pin.replace({'Image src error.': None}, subset=['image_src'])
df_pin = df_pin.replace({'User Info Error': None}, subset=['poster_name'])
df_pin = df_pin.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset=['tag_list'])
df_pin = df_pin.replace({"No Title Data Available": None}, subset=["title"])

# Perform transformations to ensure every entry is a number
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

# Clean the data to include only the save location path
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# rename/drop column
df_pin = df_pin.withColumnRenamed("index", "ind")
df_pin = df_pin.drop("index")

# Cast to datatype
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))
df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("int"))
df_pin = df_pin.withColumn("ind", df_pin["ind"].cast("int"))

# Reorder the DataFrame columns
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
     

# COMMAND ----------

# Writing a DataFrame to a table in Parquet format
df_pin.write.format("delta").mode("overwrite").saveAsTable("0affe4008223_pin_table")

# COMMAND ----------

#%sql
#DROP TABLE IF EXISTS _pin_table;

# COMMAND ----------

display(df_pin)

# COMMAND ----------

print("To check if the S3 bucket was mounted successfully: \n")
display(dbutils.fs.ls("s3n://user-0affe4008223-bucket/topics/0affe4008223.geo/partition=0/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
geo_location = "s3n://user-0affe4008223-bucket/topics/0affe4008223.geo/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"

# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(geo_location)
# Display Spark dataframe to check its content
display(df_geo)
     

# COMMAND ----------

# MAGIC %md
# MAGIC ### To clean the df_geo DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Create a new column coordinates that contains an array based on the latitude and longitude columns
# MAGIC - Drop the latitude and longitude columns from the DataFrame
# MAGIC - Convert the timestamp column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - country
# MAGIC   - coordinates
# MAGIC   - timestamp

# COMMAND ----------

# Create array column based on the latitude and longitude columns
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop the latitude and longitude columns from the DataFrame
df_geo = df_geo.drop("latitude", "longitude")

# Cast the timestamp column to a timestamp data type
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder the DataFrame columns
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

# Writing a DataFrame to a table in Parquet format
df_geo.write.format("delta").mode("overwrite").saveAsTable("0affe4008223_geo_table")

# COMMAND ----------

print("To check if the S3 bucket was mounted successfully: \n")
display(dbutils.fs.ls("s3n://user-0affe4008223-bucket/topics/0affe4008223.user/partition=0/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
user_location = "s3n://user-0affe4008223-bucket/topics/0affe4008223.user/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"

# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(user_location)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC ### To clean the df_user DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC - Drop the first_name and last_name columns from the DataFrame
# MAGIC - Convert the date_joined column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC   - ind
# MAGIC   - user_name
# MAGIC   - age
# MAGIC   - date_joined

# COMMAND ----------

# Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))

# Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop("first_name", "last_name")

# Cast the date_joined column to a timestamp data type
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder the DataFrame columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

# Writing a DataFrame to a table in Parquet format
df_user.write.format("delta").mode("overwrite").saveAsTable("0affe4008223_user_table")

# COMMAND ----------

display(df_user)

# COMMAND ----------


