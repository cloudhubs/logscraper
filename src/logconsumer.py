from kafka import KafkaConsumer
from json import loads
consumer = KafkaConsumer('test', auto_offset_reset='earliest', enable_auto_commit=True, bootstrap_servers='localhost:9092')
for msg in consumer:
        print(msg)
