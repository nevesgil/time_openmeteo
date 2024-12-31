import signal
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from db.db import Session, KafkaOffset, WeatherData, engine
import logging
from time import time

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Kafka Consumer Configuration
consumer_config = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "weather_consumer_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Global flag for graceful shutdown
shutdown_flag = False


def transform_message(message):
    data = json.loads(message.value().decode("utf-8"))
    current_weather = data.get("current_weather", {})
    return WeatherData(
        timestamp=datetime.now(),
        temperature=current_weather.get("temperature"),
        windspeed=current_weather.get("windspeed"),
        winddirection=current_weather.get("winddirection"),
        weathercode=current_weather.get("weathercode"),
    )


def get_latest_offset(session, topic, partition):
    offset_record = (
        session.query(KafkaOffset)
        .filter_by(topic=topic, partition=partition)
        .order_by(KafkaOffset.offset.desc())
        .first()
    )
    # Return the offset or OFFSET_BEGINNING if no record exists
    return offset_record.offset if offset_record else OFFSET_BEGINNING


def update_offset(session, topic, partition, offset):
    kafka_offset = KafkaOffset(
        topic=topic, partition=partition, offset=offset, timestamp=datetime.now()
    )
    session.add(kafka_offset)
    session.commit()


def process_data(session, weather_data):
    session.add(weather_data)
    session.commit()


def consume_messages():
    consumer = Consumer(consumer_config)
    session = Session()
    idle_threshold = 10  # seconds
    max_empty_polls = 5
    last_message_time = time()
    empty_poll_count = 0

    try:
        # Get partitions and assign specific offsets
        partitions = (
            consumer.list_topics("weather-topic").topics["weather-topic"].partitions
        )
        topic_partitions = []

        for partition in partitions:
            tp = TopicPartition("weather-topic", partition)
            last_offset = get_latest_offset(session, "weather-topic", partition)
            print(f"Last offset for partition {partition}: {last_offset}")
            if last_offset is not None:
                tp.offset = last_offset  # Start from the last unprocessed offset
            else:
                tp.offset = OFFSET_BEGINNING  # Start from the beginning
            topic_partitions.append(tp)

        # Assign partitions to the consumer
        consumer.assign(topic_partitions)

        # Seek to the desired offsets
        for tp in topic_partitions:
            consumer.seek(tp)

        while not shutdown_flag:
            msg = consumer.poll(5.0)
            if msg is None:
                empty_poll_count += 1
                if empty_poll_count >= max_empty_polls:
                    if time() - last_message_time > idle_threshold:
                        logging.info(
                            "Consumer idle. No new messages. Closing consumer."
                        )
                        break
                continue

            empty_poll_count = 0
            last_message_time = time()

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logging.error(f"Kafka error: {msg.error()}")
                continue

            print(
                f"Processing message from topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}"
            )

            try:
                weather_data = transform_message(msg)
                process_data(session, weather_data)
                # Update the last processed offset
                update_offset(session, msg.topic(), msg.partition(), msg.offset() + 1)
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info("Finalizing consumer...")
        consumer.close()
        session.close()


def signal_handler(signum, frame):
    global shutdown_flag
    print("Shutdown signal received. Closing consumer...")
    shutdown_flag = True


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        consume_messages()
    except Exception as e:
        print(f"Unexpected error: {e}")
