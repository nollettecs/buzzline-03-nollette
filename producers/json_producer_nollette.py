"""
json_producer_nollette.py

Stream dynamically generated JSON data to a Kafka topic related to weather alerts.

Example JSON message:
{"alert_id": "WX12345", "severity": "Severe", "event": "Thunderstorm", "location": "Downtown", "timestamp": "2025-01-28T14:30:00Z"}

Example serialized to Kafka message:
"{\"alert_id\": \"WX12345\", \"severity\": \"Severe\", \"event\": \"Thunderstorm\", \"location\": \"Downtown\", \"timestamp\": \"2025-01-28T14:30:00Z\"}"
"""

#####################################
# Import Modules
#####################################

import os
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("WEATHER_TOPIC", "weather_alerts")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("WEATHER_INTERVAL_SECONDS", 5))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Generate Dynamic Messages
#####################################

def generate_messages():
    """
    Generate sample weather alert messages dynamically.

    Yields:
        dict: A dictionary containing weather alert data.
    """
    alert_id_counter = 1
    weather_events = ["Thunderstorm", "Tornado", "Flood", "Heatwave", "Snowstorm"]
    severities = ["Minor", "Moderate", "Severe", "Extreme"]
    locations = ["Downtown", "Suburbs", "Rural Area", "Coastal Region", "Mountainous Area"]

    while True:
        message = {
            "alert_id": f"WX{alert_id_counter:05}",
            "severity": severities[alert_id_counter % len(severities)],
            "event": weather_events[alert_id_counter % len(weather_events)],
            "location": locations[alert_id_counter % len(locations)],
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        logger.debug(f"Generated JSON: {message}")
        yield message

        alert_id_counter += 1
        time.sleep(5)  # Simulate message interval

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams dynamically generated JSON messages to the Kafka topic.
    """

    logger.info("START weather alert producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages():
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END weather alert producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()