from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import configparser

# Create a ConfigParser object
config = config = configparser.ConfigParser()

# Read the configuration file
config.read("streaming_app.ini")

# Get the values of the variables from the configuration file
KAFKA_TOPIC_NAME = config.get("KAFKA", "KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVER = config.get("KAFKA", "KAFKA_BOOTSTRAP_SERVER")

AWS_ACCESS_KEY = config.get("aws", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = config.get("aws", "AWS_SECRET_KEY")
AWS_S3_ENDPOINT = config.get("aws", "AWS_S3_ENDPOINT")

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("Ecommerce session log processing") \
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

kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", KAFKA_TOPIC_NAME ) \
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
