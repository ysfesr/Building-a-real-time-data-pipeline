import configparser

# Create a ConfigParser object
config = config = configparser.ConfigParser()

# Read the configuration file
config.read("./streaming_app.ini")

# Get the values of the variables from the configuration file
KAFKA_TOPIC_NAME = config.get("KAFKA", "KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVER = config.get("KAFKA", "KAFKA_BOOTSTRAP_SERVER")

AWS_ACCESS_KEY = config.get("aws", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = config.get("aws", "AWS_SECRET_KEY")
AWS_S3_ENDPOINT = config.get("aws", "AWS_S3_ENDPOINT")


params = {
    # Kafka configuration
    "topic_name": config.get("KAFKA", "KAFKA_TOPIC_NAME"),
    "bootstrap_server": config.get("KAFKA", "KAFKA_BOOTSTRAP_SERVER"),
    # AWS configuration
    "access_key": config.get("aws", "AWS_ACCESS_KEY"),
    "secret_key": config.get("aws", "AWS_SECRET_KEY"),
    "endpoint": config.get("aws", "AWS_S3_ENDPOINT"),
    # APP configuration
    "app_name": config.get("app","APP_NAME")
}