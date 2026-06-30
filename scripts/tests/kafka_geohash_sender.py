#!/usr/bin/env python3
"""
Test script for sending GeoHashRoutedMsg protobuf messages to Kafka.
This script generates test messages with different DSRC message types and geohashes.
"""

import os
import time
import json
import random
from kafka import KafkaProducer
from google.protobuf.timestamp_pb2 import Timestamp
import sys
from geoHashRoutedMsg_pb2 import GeoHashRoutedMsg

TEST_MESSAGES = {
  "TIM": "",
}

# SAMPLE_GEOHASHES = [
#     "9xu1mx",
#     "9xu1t9",
#     "9xu1t8",
#     "9xu1tc",
#     "9xu1t3"
# ]

SAMPLE_GEOHASHES = [
    "9qcgytf", # caltrans tim
    # "9xjq72" # neaera office
    # "9xjq725"
]


# SAMPLE_GEOHASHES = [
#     "9xu1mxy",
#     "9xu1t8q",
#     "9xu1t8t",
#     "9xu1t8s",
#     "9xu1t8v",
#     "9xu1t8u",
#     "9xu1t8x",
#     "9xu1t87",
#     "9xu1t8w",
#     "9xu1t8z",
#     "9xu1t8y",
#     "9xu1t8b",
#     "9xu1t8d",
#     "9xu1t8c",
#     "9xu1t8f",
#     "9xu1t8e",
#     "9xu1t8h",
#     "9xu1t8g",
#     "9xu1tc2",
#     "9xu1t8j",
#     "9xu1t8k",
#     "9xu1t8n",
#     "9xu1tc8",
#     "9xu1t8m",
#     "9xu1t91",
#     "9xu1t9q",
#     "9xu1t90",
#     "9xu1t9p",
#     "9xu1t93",
#     "9xu1t9r",
#     "9xu1t95",
#     "9xu1t94",
#     "9xu1t97",
#     "9xu1t9w",
#     "9xu1t3p",
#     "9xu1t96",
#     "9xu1t9x",
#     "9xu1t9h",
#     "9xu1t9k",
#     "9xu1t9j",
#     "9xu1t9m",
#     "9xu1t9n"
# ]

def create_geohash_routed_msg(message_type, hex_payload, geohash=None):
    """Create a GeoHashRoutedMsg protobuf message."""
    # Convert hex string to bytes
    msg_bytes = bytes.fromhex(hex_payload)
    
    # Create timestamp (current time)
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    
    # Create the GeoHashRoutedMsg
    geo_hash_msg = GeoHashRoutedMsg()
    geo_hash_msg.msgBytes = msg_bytes
    geo_hash_msg.time.CopyFrom(timestamp)
    geo_hash_msg.geohash = geohash

    return geo_hash_msg

def main():
    # Get configuration from environment variables
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('ETX_DEPOSITORS_GEOHASH_MQTT_KAFKA_TOPIC', 'topic.GeoHashRoutedMsg')
    frequency = int(os.getenv('MESSAGE_FREQUENCY', '5'))  # messages per second
    
    print(f"Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    print(f"Kafka Topic: {kafka_topic}")
    print(f"Message Frequency: {frequency} messages per second")
    print(f"Available message types: {list(TEST_MESSAGES.keys())}")
    print(f"Sample geohashes: {len(SAMPLE_GEOHASHES)} locations")
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print("Connected to Kafka successfully!")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return
    
    message_count = 0
    
    try:
        while True:
            # Select a random message type and geohash
            message_type = random.choice(list(TEST_MESSAGES.keys()))
            geohash = random.choice(SAMPLE_GEOHASHES)
            hex_payload = TEST_MESSAGES[message_type]
            
            # Create the GeoHashRoutedMsg
            geo_hash_msg = create_geohash_routed_msg(message_type, hex_payload, geohash)
            
            # Send to Kafka
            key = f"{message_type}_{message_count}_{int(time.time())}"
            producer.send(kafka_topic, value=geo_hash_msg, key=key)
            
            message_count += 1
            print(f"Sent message #{message_count}: {message_type} with geohash {geohash}")
            
            # Wait before sending next message
            # time.sleep(1.0 / frequency)
            exit()
            
    except KeyboardInterrupt:
        print("\nStopping message sender...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
        print(f"Sent {message_count} messages total")

if __name__ == "__main__":
    main()
