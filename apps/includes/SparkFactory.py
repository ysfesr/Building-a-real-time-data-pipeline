from pyspark.sql import SparkSession


class Factory:

    def __init__(self, config:list, app:dict) -> None:
        self.config:list = config
        self.app:dict = app


    def configurate(self, spark:SparkSession) -> SparkSession:
        for params in self.config["client"]:
            spark = spark.config(*params)
        return spark


    def auth(self, spark) -> SparkSession:
        spark = spark.config("spark.hadoop.fs.s3a.access.key", self.app["AWS"]["aws_access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.app["AWS"]["aws_secret_key"]) \
            .config("fs.s3a.endpoint", self.app["AWS"]["aws_s3_endpoint"])
        return spark


    def additional_config(self, spark):
        spark = spark.enableHiveSupport()
        return spark
        

    def get(self):
        spark = SparkSession.builder.appName(self.app["APP"]["app_name"])
        
        spark = self.auth(spark)
        spark = self.configurate(spark)
        spark = self.additional_config(spark)
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark.getOrCreate()
