from includes import SparkFactory
from includes.Loader import IniLoader, YamlLoader
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark:SparkSession = SparkFactory.Factory(
    YamlLoader("config.yaml").get_data(), 
    IniLoader("app.ini").get_data()
).get()

df = spark.readStream\
   .format("delta")\
   .load("path", "s3a://datalake/Ecommerce/all_sessions")

# Indique le nombre total de visiteurs uniques
query1 = df.select(count("*").alias("product_views"), count(col("fullVisitorId").distinct()).alias("unique_visitors"))\
        .writeStream\
        .format("console")\
        .start()

# Indique le nombre total de visiteurs uniques (fullVisitorID) sur le site référent (channelGrouping)
query2 = df.groupBy("channelGrouping").agg(countDistinct(col("fullVisitorId")).alias("unique_visitors"))\
        .orderBy(col("channelGrouping").desc())\
        .writeStream.format("console")\
        .start()

# Liste les cinq produits avec le plus de vues (product_views) de visiteurs uniques
query3 = df.filter(col("type") == 'PAGE') \
    .groupBy("v2ProductName") \
    .agg(count("*").alias("product_views")) \
    .orderBy(col("product_views").desc()) \
    .select("product_views", "v2ProductName") \
    .withColumn("rank", rank().over(Window.partitionBy().orderBy(col("product_views").desc()))) \
    .filter(col("rank") <= 5)\
    .writeStream.format("console").start().awaitTermination()

# Elimine les vues de produit en double pour les visiteurs qui ont consulté un produit plusieurs fois
query4 = df.filter(col("type") == "PAGE")\
        .groupBy("fullVisitorId", "v2ProductName").agg()\
        .groupBy("ProductName")\
        .agg(count("*").alias("unique_view_count"))\
        .orderBy(col("unique_view_count").desc())\
        .limit(5)\
        .writeStream.format("console").start().awaitTermination()
