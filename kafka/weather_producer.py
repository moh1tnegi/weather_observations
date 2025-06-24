import requests
import json
from confluent_kafka import Producer


config = {
    'bootstrap.servers': 'localhost:9092'
}
topic = 'weather_obs'
producer = Producer(**config)

def fetch_weather(api_url):
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f'Error fetching weather data: {e}')

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

def stream_weather_data(api_url):
    weather_data = fetch_weather(api_url)
    if weather_data:
        payload = json.dumps(weather_data)
        producer.produce(topic, value=payload, callback=delivery_report)
        producer.flush()

if __name__ == '__main__':
    station_id = "KLAX"  # LA International Airport
    api_url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    headers = {'user-agent': 'KafkaProducer notmohitnegi@gmail.com'}
    print(f'Starting weather producer for {station_id}')
    stream_weather_data(api_url)
