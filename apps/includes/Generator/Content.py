from kafka.errors import KafkaError
from apps.includes.Loader import CsvLoader
import time

from .ProducerFactory import ProducerFactory


class ContentGenerator:

    def __init__(self):
        self.config:dict = {}
        self.producer = ProducerFactory("assets/kafka_config.yaml").generate()
        self.content = CsvLoader("../data/mini_session.csv").get_data()

        
    def generate(self, row:dict) -> None:
        try:
            future = self.producer.send('sessions', row)
            self.producer.flush()
            record_metadata = future.get(timeout=10)

            print(record_metadata)

        except KafkaError as e:
            print(e)


    def loop(self):
        for row in self.content:
            self.generate(row)
            time.sleep(5)