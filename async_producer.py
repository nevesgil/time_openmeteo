from confluent_kafka import Producer
import json
import logging
import aiohttp
import asyncio

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# default producer configuration
producer_config = {
    "bootstrap.servers": "localhost:29092",
    "batch.size": 500000,
    "linger.ms": 500,
    "compression.type": "lz4",
}

# create the producer
producer = Producer(producer_config)

locations = [
    {"city": "sao paulo", "latitude": 23.5, "longitude": 46.625},
    {"city": "new york", "latitude": 40.7128, "longitude": -74.0060},
]


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


async def fetch_weather(session, location):
    try:
        async with session.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "current_weather": "true",
            },
        ) as response:
            response.raise_for_status()
            data = await response.json()
            data["city"] = location["city"]
            return data
    except Exception as e:
        logger.error(f"Error fetching data for {location['city']}: {e}")
        return None


async def fetch_all_weather(locations):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, loc) for loc in locations]
        return await asyncio.gather(*tasks)


def send_message_to_kafka(topic, key, data):
    try:
        value = json.dumps(data)
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.poll(0)
    except BufferError:
        logger.warning("Local producer queue is full, waiting for free space")
        producer.poll(1)


async def job():
    weather_data = await fetch_all_weather(locations)
    for data in weather_data:
        if data:
            send_message_to_kafka("weather-topic", key=data["city"], data=data)


async def main():
    try:
        i = 0
        while i <= 100:  # True
            await job()
            # await asyncio.sleep(1)
            i += 1
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except asyncio.exceptions.CancelledError:
        logger.info("Cancelled")
    finally:
        producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
