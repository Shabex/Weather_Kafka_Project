from kafka import KafkaProducer
import requests
import json
import time
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = '65b0d6aa9ac69d16bc57dff4c11eeb1d'
def get_weather_data():
    cities = ['Nairobi', 'Mombasa', 'Kitale', 'Johannesburg','Cairo', 'Lagos', 'Accra', 'Kampala', 'Addis Ababa', 'Dar es Salaam']
    # CITY_NAME = 'Nairobi'
    city_list = []
    for city in cities:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}'

        response = requests.get(url)

        data = response.json()

        city_list.append(
            {
                'city': city,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'last_updated': data['dt']
            }
        )
    return city_list

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer = lambda p:  json.dumps(p).encode('utf-8')
                            )


while True:
    weather_data = get_weather_data()
    topic = 'open_weather_data'
    producer.send(topic, value=weather_data)
    print(f"Producer: {weather_data}")
    time.sleep(5)