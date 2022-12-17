from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def kafka_producer(data):
    try:
       future = producer.send('sessions', data)
       producer.flush()
       record_metadata = future.get(timeout=10)
       print(record_metadata)
    except KafkaError as e:
       print(e)

tmp = 0
with open("data/all_session.csv") as f:
    fdict = csv.DictReader(f, delimiter=",")
    for row in fdict:
        data = dict(row)
        kafka_producer(data)
        time.sleep(5)
        print(data)
        # tmp = tmp + 1
        # if tmp > 30: break