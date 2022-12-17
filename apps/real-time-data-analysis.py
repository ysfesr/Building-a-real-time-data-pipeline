from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import configparser
from pathlib import Path
import os

# Create a ConfigParser object
config = config = configparser.ConfigParser()

# Read the configuration file
path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "streaming_app.ini")
config.read(config_path)

# Get the values of the variables from the configuration file
KAFKA_TOPIC_NAME = config.get("KAFKA", "KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVER = config.get("KAFKA", "KAFKA_BOOTSTRAP_SERVER")

AWS_ACCESS_KEY = config.get("aws", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = config.get("aws", "AWS_SECRET_KEY")
AWS_S3_ENDPOINT = config.get("aws", "AWS_S3_ENDPOINT")

APP_NAME = config.get("APP", "APP_NAME")

# Create a SparkSession object
spark = SparkSession.builder \
    .appName(APP_NAME) \
<<<<<<< HEAD
    .config("spark.sql.warehouse.dir","s3a://data/warehouse")\
=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("fs.s3a.endpoint", AWS_S3_ENDPOINT)\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.connection.ssl.enabled", "false")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream\
   .format("delta")\
   .load("path", "s3a://datalake/Ecommerce/all_sessions")

# une requête indique le nombre total de visiteurs uniques
query1 = df.select(count("*").alias("product_views"), count(col("fullVisitorId").distinct()).alias("unique_visitors"))\
        .writeStream\
        .format("console")\
        .start().awaitTermination()

# une requête indique le nombre total de visiteurs uniques (fullVisitorID) sur le site référent (channelGrouping)
query2 = df.groupBy("channelGrouping").agg(countDistinct(col("fullVisitorId")).alias("unique_visitors"))\
        .orderBy(col("channelGrouping").desc())\
        .writeStream.format("console")\
<<<<<<< HEAD
        .start()
=======
        .start().awaitTermination()
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664

# une requête pour lister les cinq produits avec le plus de vues (product_views) de visiteurs uniques
query3 = df.filter(col("type") == 'PAGE') \
    .groupBy("v2ProductName") \
    .agg(count("*").alias("product_views")) \
    .orderBy(col("product_views").desc()) \
    .select("product_views", "v2ProductName") \
    .withColumn("rank", rank().over(Window.partitionBy().orderBy(col("product_views").desc()))) \
    .filter(col("rank") <= 5)\
    .writeStream.format("console").start().awaitTermination()


## La requête ne compte plus les vues de produit en double pour les visiteurs qui ont consulté un produit plusieurs fois
query4 = df.filter(col("type") == "PAGE")\
        .groupBy("fullVisitorId", "v2ProductName").agg()\
        .groupBy("ProductName")\
        .agg(count("*").alias("unique_view_count"))\
        .orderBy(col("unique_view_count").desc())\
        .limit(5)\
        .writeStream.format("console").start().awaitTermination()
