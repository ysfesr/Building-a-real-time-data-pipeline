from kafka import KafkaProducer
from ..Loader import YamlLoader
import kafka
import json


class ProducerFactory:

    """
        Serves as a Factory for the 'KafkaProducer' objects.
        
        ### Parameters:
            config_path: str
                The path for kafka_config.yaml configuration file
    """

    def __init__(self, config_path:str) -> None:
        self.config_path = config_path
        self.init_config()


    def generate(self) -> KafkaProducer:
        """
            Returns a freshly created 'KafkaProducer'
        """
        return KafkaProducer(
            bootstrap_servers=[self.config["host"]+":"+self.config["port"]],
            value_serializer=lambda v: json.dumps(v).encode(self.config["encoding"]),
            api_version=tuple(map(int, kafka.__version__.split(".")))
        )


    def init_config(self):
        """
            Overrides the default configuration with the one in the 'assets/kafka_config.yaml'
        """

        self.config:dict = {
            **self.get_default_config(),
            **YamlLoader(self.config_path).get_data()
        }


    def get_default_config(self) -> dict:
        """
            defines a default configuration for the 'KafkaProducer' object
        """

        return {
            "host": "localhost",
            "port": "9093",
            "encoding": 'utf-8'
        }
