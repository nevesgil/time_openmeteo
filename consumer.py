import signal
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from db.db import Session, KafkaOffset, engine
from db.td import TDengineSetup
import logging
from time import time

# Logging
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

# TDengine setup
tdengine = TDengineSetup()
tdengine.set_database("weather_db", KEEP=365, WAL_LEVEL=1)

# Create the super table
tdengine.create_super_table(
    table_name="weather_data",
    schema="ts TIMESTAMP, temperature FLOAT, windspeed FLOAT, winddirection INT, weathercode INT",
    tags="city VARCHAR(50)",
)


def validate_message(data):
    required_keys = ["city", "current_weather"]
    weather_keys = ["time", "temperature", "windspeed", "winddirection", "weathercode"]

    if not all(key in data for key in required_keys):
        raise ValueError("Invalid message format: Missing required keys.")
    if not all(key in data["current_weather"] for key in weather_keys):
        raise ValueError("Invalid weather format: Missing required keys.")


def transform_message(message):
    """
    Transform a Kafka message into a format suitable for TDengine insertion.
    """
    data = json.loads(message.value().decode("utf-8"))

    validate_message(data)

    city = data["city"]
    current_weather = data["current_weather"]

    timestamp = datetime.strptime(current_weather["time"], "%Y-%m-%dT%H:%M")
    timestamp = timestamp.replace(microsecond=int((time() % 1) * 1e6)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    temperature = current_weather["temperature"]
    windspeed = current_weather["windspeed"]
    winddirection = current_weather["winddirection"]
    weathercode = current_weather["weathercode"]

    subtable_name = (
        f"weather_{city.lower().replace(' ', '_')}"  # e.g., weather_new_york
    )
    tags = f"'{city}'"
    values = (
        f"'{timestamp}', {temperature}, {windspeed}, {winddirection}, {weathercode}"
    )

    return {
        "city": city,
        "subtable_name": subtable_name,
        "tags": tags,
        "values": values,
    }


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


def process_data(weather_data):
    try:
        tdengine.create_subtable(
            subtable_name=weather_data["subtable_name"],
            tags=weather_data["tags"],
            values=weather_data["values"],
        )
    except Exception as e:
        logging.error(f"Error inserting data into TDengine: {e}")


def consume_messages():
    consumer = Consumer(consumer_config)
    session = Session()
    idle_threshold = 10
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
                tp.offset = last_offset
            else:
                tp.offset = OFFSET_BEGINNING
            topic_partitions.append(tp)

        # assign partitions
        consumer.assign(topic_partitions)

        for tp in topic_partitions:
            consumer.seek(tp)

        while not shutdown_flag:
            msg = consumer.poll(5.0)
            if msg is None:
                empty_poll_count += 1
                if (
                    empty_poll_count >= max_empty_polls
                    and time() - last_message_time > idle_threshold
                ):
                    logging.info("Consumer idle. No new messages. Exiting.")
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
                process_data(weather_data)
                update_offset(session, msg.topic(), msg.partition(), msg.offset() + 1)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
    finally:
        logging.info("Closing consumer and session.")
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
