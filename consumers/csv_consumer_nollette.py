"""
csv_consumer_nollette.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 85.0}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque  # Used for rolling window temperature tracking

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("OUTSIDE_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group ID from environment or use default."""
    group_id = os.getenv("OUTSIDE_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group ID: {group_id}")
    return group_id

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("TEMPERATURE_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Function to Process a Message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON message, log temperature trends, and detect heat alerts.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")

        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Check for too hot alert
        if temperature > 95:
            logger.warning(f"🔥 TOO HOT ALERT at {timestamp}: Temperature is {temperature}°F! 🔥")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Main Consumer Function
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # Fetch environment variables
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
