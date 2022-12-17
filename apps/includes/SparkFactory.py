from pyspark.sql import SparkSession

<<<<<<< HEAD

=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
class Factory:

    def __init__(self, config:list, app:dict) -> None:
        self.config:list = config
        self.app:dict = app

<<<<<<< HEAD

=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
    def configurate(self, spark:SparkSession) -> SparkSession:
        for params in self.config["client"]:
            spark = spark.config(*params)
        return spark

<<<<<<< HEAD

=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
    def auth(self, spark) -> SparkSession:
        spark = spark.config("spark.hadoop.fs.s3a.access.key", self.app["AWS"]["aws_access_key"]) \
            .config("spark.hadoop.fs.s3a.secret.key", self.app["AWS"]["aws_secret_key"]) \
            .config("fs.s3a.endpoint", self.app["AWS"]["aws_s3_endpoint"])
        return spark

<<<<<<< HEAD

=======
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
    def additional_config(self, spark):
        spark = spark.enableHiveSupport()
        return spark
        
<<<<<<< HEAD

    def create(self):
=======
    def get(self):
>>>>>>> c88d118f940fa44c615524f06d5ed6628bf40664
        spark = SparkSession.builder.appName(self.app["APP"]["app_name"])
        
        spark = self.auth(spark)
        spark = self.configurate(spark)
        spark = self.additional_config(spark)
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark.getOrCreate()
