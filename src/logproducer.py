from kafka import KafkaProducer
from json import dumps
from time import sleep

producer = KafkaProducer(value_serializer=lambda m: dumps(m).encode('utf-8'),
bootstrap_servers='localhost:9092')
for i in range(4):
    print('Sending')
    sleep(1)
    producer.send('test', value={"offset": i + 100})
