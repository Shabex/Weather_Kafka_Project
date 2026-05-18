from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('open_weather_data',
                         bootstrap_servers='localhost:9092',
                         value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='latest'
                         )

for message in consumer:
    print(f"Consumer: {message.value}")