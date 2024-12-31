import requests
from confluent_kafka import Producer
import json
import schedule
import time
import logging

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# default producer configuration
producer_config = {
    "bootstrap.servers": "localhost:29092",
    # to be adjusted
    "batch.size": 200000,
    "linger.ms": 100,
    # 'compression.type': 'lz4',
    # 'acks': '1',
    # 'max.request.size': 10000000,
    # 'buffer.memory': 33554432
}

# create the producer
producer = Producer(producer_config)

latitude = 23.5
longitude = 46.625


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def get_message_data(latitude, longitude):
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current_weather": "true",
            },
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None


def send_message_to_kafka(topic, data):
    try:
        value = json.dumps(data)
        producer.produce(topic, value=value, callback=delivery_report)
        producer.poll(0)
    except BufferError:
        logger.warning("Local producer queue is full, waiting for free space")
        producer.poll(1)


# this is to be replaced once we have a cronjob to trigger the producer
def job():
    try:
        weather_data = get_message_data(latitude, longitude)
        if weather_data:
            send_message_to_kafka("weather-topic", weather_data)
    except Exception as e:
        logger.error(f"Error in job execution: {e}")


schedule.every(1).second.do(job)


# just for testing, 60 messages
def main():
    try:
        i = 0
        while i <= 10:
            schedule.run_pending()
            time.sleep(1)
            i += 1
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
