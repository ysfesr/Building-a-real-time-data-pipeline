from importlib.resources import Package
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

KAFKA_TOPIC_NAME  = "sessions"
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"

AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "123456789"
AWS_S3_ENDPOINT = "http://minio:9000"

spark = SparkSession.builder \
    .appName("Ecommerce session log analysis") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")\
    .config("spark.sql.warehouse.dir","s3a://data/warehouse")\
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("fs.s3a.endpoint", AWS_S3_ENDPOINT)\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.connection.ssl.enabled", "false")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ecommerce_schema = StructType([
    StructField('fullVisitorId',DoubleType()),
    StructField('channelGrouping' ,StringType()),
    StructField('time' ,TimestampType()),
    StructField('country',StringType()),
    StructField('city',StringType()),
    StructField('totalTransactionRevenue',DoubleType()),
    StructField('transactions' ,DoubleType()),
    StructField('timeOnSite',DoubleType()),
    StructField('pageviews' ,IntegerType()),
    StructField('sessionQualityDim',DoubleType()),
    StructField('date', DateType()),
    StructField('visitId',IntegerType()),
    StructField('type',StringType()),
    StructField('productRefundAmount' ,DoubleType()),
    StructField('productQuantity' ,DoubleType()),
    StructField('productPrice', IntegerType()),
    StructField('productRevenue',DoubleType()),
    StructField('productSKU',StringType()),
    StructField('v2ProductName',StringType()),
    StructField('v2ProductCategory',StringType()),
    StructField('productVariant',StringType()),
    StructField('currencyCode',StringType()),
    StructField('itemQuantity',DoubleType()),
    StructField('itemRevenue',DoubleType()),
    StructField('transactionRevenue',DoubleType()),
    StructField('transactionId',StringType()),
    StructField('pageTitle',StringType()),
    StructField('searchKeyword',DoubleType()),
    StructField('pagePathLevel1',StringType()),
    StructField('eCommerceAction_type',IntegerType()),
    StructField('eCommerceAction_step', IntegerType()),
    StructField('eCommerceAction_option',StringType())]
)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", KAFKA_TOPIC_NAME ) \
  .load()

transformed_data = df \
    .selectExpr("cast (value as STRING) jsonData") \
    .select (from_json("jsonData", ecommerce_schema).alias("data")) \
    .select("data.*")

# Filter out rows that only contain null values
filtered_data = transformed_data.filter(~transformed_data.columns.map(lambda c: isnull(col(c))).all())

# Replace null values with "?"
replaced_data = filtered_data.select([when(col(c).isNull(), "?").otherwise(col(c)) for c in filtered_data.columns])

filtered_data.writeStream\
   .outputMode("append")\
   .format("json")\
   .option("path", "s3a://data/Ecommerce/all_sessions")\
   .option("checkpointLocation", "s3a://logs/checkpoint")\
   .toTable("all_sessions")\
   .awaitTermination()