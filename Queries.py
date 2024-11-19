# Databricks notebook source
# MAGIC %md
# MAGIC ### Milestone 7

# COMMAND ----------

df_pin = spark.read.format("delta").load('dbfs:/user/hive/warehouse/0affe4008223_pin_table')
df_geo = spark.read.format("delta").load('dbfs:/user/hive/warehouse/0affe4008223_geo_table')
df_user = spark.read.format("delta").load('dbfs:/user/hive/warehouse/0affe4008223_user_table')

display(df_pin.limit(20))
display(df_geo.limit(20))
display(df_user.limit(20))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4:
# MAGIC - Find the most popular Pinterest category people post to based on their country

# COMMAND ----------

# First, create the joined DataFrame
df_pin_geo = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Register the DataFrame as a temporary table for SQL querying
df_pin_geo.createOrReplaceTempView("df_pin_geo")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH category_counts AS (
# MAGIC     SELECT 
# MAGIC         country,
# MAGIC         category,
# MAGIC         COUNT(*) AS category_count,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY country ORDER BY COUNT(*) DESC) AS row_num
# MAGIC     FROM 
# MAGIC         df_pin_geo
# MAGIC     GROUP BY 
# MAGIC         country, 
# MAGIC         category
# MAGIC )
# MAGIC SELECT 
# MAGIC     country,
# MAGIC     category,
# MAGIC     category_count
# MAGIC FROM 
# MAGIC     category_counts
# MAGIC WHERE 
# MAGIC     row_num = 1
# MAGIC ORDER BY 
# MAGIC     country;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: 
# MAGIC Find which was the most popular category between 2018 and 2022 and the number of posts.

# COMMAND ----------

from pyspark.sql.functions import year, col

# Extract year from the timestamp column
df_pin_geo = df_pin_geo.withColumn("post_year", year("timestamp"))

# Filter for years between 2018 and 2022
df_pin_geo_filtered = df_pin_geo.filter((col("post_year") >= 2018) & (col("post_year") <= 2022))

# Group by post_year and category to get the category count
df_category_counts = df_pin_geo_filtered.groupBy("post_year", "category").count().withColumnRenamed("count", "category_count")

# Show the result
#df_category_counts.show()
display(df_category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6:
# MAGIC - Find the user with the most followers in each country.

# COMMAND ----------

## Step 1

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Define a window partitioned by country and ordered by follower count in descending order
window_spec = Window.partitionBy("country").orderBy(col("follower_count").desc())

# Select the top user per country based on follower count
df_top_user_per_country = df_pin_geo.withColumn("row_num", row_number().over(window_spec)) \
                                    .filter(col("row_num") == 1) \
                                    .select("country", "poster_name", "follower_count")

# Show the result
#df_top_user_per_country.show()
display(df_top_user_per_country)

# COMMAND ----------

# Step 2

# Find the country with the user with the most followers overall
df_country_max_followers = df_top_user_per_country.orderBy(col("follower_count").desc()).limit(1)

# Show the result
#df_country_max_followers.select("country", "follower_count").show()
display(df_country_max_followers.select("country", "follower_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7:
# MAGIC - Find the most popular category for different age groups

# COMMAND ----------

from pyspark.sql.functions import when, col, row_number
from pyspark.sql.window import Window

# Step 1: Join df_pin and df_user DataFrames
df_pin_user = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")

# Step 2: Create age_group column based on age ranges
df_pin_user = df_pin_user.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "+50")
)

# Step 3: Count occurrences of each category within each age group
df_category_count = df_pin_user.groupBy("age_group", "category").count().withColumnRenamed("count", "category_count")

# Step 4: Define a window to get the top category for each age group
window_spec = Window.partitionBy("age_group").orderBy(col("category_count").desc())

# Step 5: Apply row number to get the top category for each age group and filter for the top row
df_top_category_by_age_group = df_category_count.withColumn("row_num", row_number().over(window_spec)) \
                                                .filter(col("row_num") == 1) \
                                                .select("age_group", "category", "category_count")

# Show the result
#df_top_category_by_age_group.show()
display(df_top_category_by_age_group)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8:
# MAGIC - Find the median follower count for different age groups

# COMMAND ----------

from pyspark.sql.functions import when, col, percentile_approx

# Step 1: Join df_pin and df_user DataFrames if necessary (if follower count is in df_user)
df_pin_user = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")

# Step 2: Create age_group column based on age ranges
df_pin_user = df_pin_user.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "+50")
)

# Step 3: Calculate the median follower count for each age group
df_median_follower_count = df_pin_user.groupBy("age_group") \
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))

# Show the result
#df_median_follower_count.show()
display(df_median_follower_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9:
# MAGIC - Find how many users have joined each year
# MAGIC - Find how many users have joined between 2015 and 2020.

# COMMAND ----------

#df_pin_geo.columns
#df_pin_user.columns
df_pin.columns

# COMMAND ----------

from pyspark.sql.functions import year, col
## 88
# Step 1: Join df_user and df_geo on the common 'ind' column
df_user_geo = df_user.join(df_geo, df_user["ind"] == df_geo["ind"], how="inner")

# Step 2: Extract year from the 'date_joined' column in the joined DataFrame
df_user_geo = df_user_geo.withColumn("post_year", year("date_joined"))

# Step 3: Filter for users who joined between 2015 and 2020
df_user_geo_filtered = df_user_geo.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

# Step 4: Group by post_year and count the number of users who joined in each year
df_user_geo_joined = df_user_geo_filtered.groupBy("post_year") \
    .count() \
    .withColumnRenamed("count", "number_users_joined")

# Show the result
#df_user_geo_joined.show()
display(df_user_geo_joined)

# COMMAND ----------

display(df_pin.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 10:
# MAGIC - Find the median follower count of users have joined between 2015 and 2020.

# COMMAND ----------

from pyspark.sql.functions import year, col, when, percentile_approx

# Step 1: Join df_pin, df_user, and df_geo DataFrames (assume follower count is in df_user and timestamp is in df_geo)
df_pin_user_geo = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner") \
                        .join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Step 2: Extract year from the 'timestamp' column in df_geo and create a new column called 'post_year'
df_pin_user_geo = df_pin_user_geo.withColumn("post_year", year("timestamp"))

# Step 3: Filter for users who joined between 2015 and 2020
df_pin_user_geo_filtered = df_pin_user_geo.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

# Step 4: Calculate the median follower count for each post_year
df_median_follower_count = df_pin_user_geo_filtered.groupBy("post_year") \
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))

# Show the result
#df_median_follower_count.show()
display(df_median_follower_count)

# COMMAND ----------

df_user.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 11:
# MAGIC
# MAGIC - Find the median follower count of users based on their joining year and age group.

# COMMAND ----------

from pyspark.sql.functions import year, col, when, percentile_approx

# Step 1: Join df_pin, df_user, and df_geo DataFrames (assume follower count is in df_user and timestamp is in df_geo)
df_pin_user_geo = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner") \
                        .join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Step 2: Create age_group column based on the original age column
df_pin_user_geo = df_pin_user_geo.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "+50")
)

# Step 3: Extract year from the 'timestamp' column in df_geo and create a new column called 'post_year'
df_pin_user_geo = df_pin_user_geo.withColumn("post_year", year("timestamp"))

# Step 4: Filter for users who joined between 2015 and 2020
df_pin_user_geo_filtered = df_pin_user_geo.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

# Step 5: Calculate the median follower count for each age_group and post_year
df_median_follower_count = df_pin_user_geo_filtered.groupBy("age_group", "post_year") \
    .agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))

# Show the result
#df_median_follower_count.show()
display(df_median_follower_count)
