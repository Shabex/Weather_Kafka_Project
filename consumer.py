from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')

consumer = KafkaConsumer('open_weather_data',
                         bootstrap_servers='localhost:9092',
                         value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='latest'
                         )

for message in consumer:
    data = message.value

    print(f"Consumer received: {data}")

    session.execute("""
        INSERT INTO weather (
            city,
            temperature,
            humidity,
            description,
            wind_speed,
            last_updated
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data['city'],
        data['temperature'],
        data['humidity'],
        data['description'],
        data['wind_speed'],
        data['last_updated']
    ))

    print("Inserted into Cassandra:", data['city'])