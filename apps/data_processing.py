from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from includes import SparkFactory
from includes.Loader import IniLoader, YamlLoader
from models.ECommerceSchema import ecommerce_schema

spark:SparkSession = SparkFactory.Factory(
    YamlLoader("config.yaml").get_data(), 
    IniLoader("app.ini").get_data()
).get()

df = spark.readStream\
   .format("delta")\
   .load("path", "s3a://datalake/Ecommerce/all_sessions")

kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", IniLoader("app.ini").get_data()["KAFKA"]["kafka_topic_name"]) \
  .option("subscribe", IniLoader("app.ini").get_data()["KAFKA"]["kafka_bootstrap_server"]) \
  .load()

transformed_data = kafka_df \
    .selectExpr("cast (value as STRING) jsonData") \
    .select (from_json("jsonData", ecommerce_schema).alias("data")) \
    .select("data.*")

# Replace empty fields with null values in all columns
transformed_df = transformed_data.na.fill("") \
    .select([when(col(c).isin(""), None).otherwise(col(c)).alias(c) for c in kinesis_df.columns])

# Create an expression to check if all of the columns are "None"
all_none_expression = reduce(lambda x, y: x & y, [col(c) == "None" for c in transformed_df.columns])

# Remove rows that contain only the value "None"
filtered_data = transformed_df.where(~all_none_expression)

# Deduplicate the data by grouping by the unique identifier and selecting the first row
deduplicated_df = filtered_data.groupBy("id") \
    .agg(first("timestamp").alias("timestamp"), first("value").alias("value"))

transformed_df = deduplicated_df.withColumn("country", when(col("country") == "Isareal", "Palestine").otherwise(col("country")))

# Create a new column that contains time on minute
transformed_df = transformed_df.withColumn("timeOnSiteMinute", col("timeOnSite") / 60)

# Start the streaming pipeline
query = transformed_df.writeStream\
   .outputMode("append")\
   .format("delta")\
   .option("checkpointLocation", "s3a://logs/checkpoint")\
   .option("path", "s3a://datalake/Ecommerce/all_sessions")\
   .start()

# Block the current thread until the streaming query terminates
query.awaitTerminate()
