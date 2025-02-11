import paho.mqtt.client as mqtt
import threading
import time
import random
import json
import csv
from itertools import islice

# MQTT Broker details
broker_address = "192.168.0.16"  # Replace with your broker address
port = 1883  # Default MQTT port
base_topic = "zigbee2mqtt/uart4"  # Base topic for publishers

# Message template
message_template = {
    "action": {
        "node": None,  # This will be updated dynamically for each publisher
        "line": None,
        "stime": None
    },
    "linkquality": 255
}

# Barrier to synchronize threads
num_publishers = 2  # Number of publishers (threads)
barrier = threading.Barrier(num_publishers)
m = 1  # Initialize row counter outside of threads

# Function to publish messages
def publish_message(client_id, topic, node_value):
    global m  # To access and update the global row counter
    client = mqtt.Client(client_id)  # Create a new MQTT client with a unique client ID
    client.connect(broker_address, port)
    
    while True:
        with open("ecg_data.csv", "r") as f:
            reader = csv.reader(f)
            # Use islice to fetch only the m-th row
            for row in islice(reader, m - 1, m):
                print(f"Node {node_value}, Row: {row}")
                break  # Read only one row
        
        # Convert the row to integers
        line_value = [float(x) for x in row[:]]  # Use first 11 elements
        # Update node value in the message
        st = time.time() * 1000
        message_template["action"]["line"] = line_value
        message_template["action"]["node"] = node_value
        message_template["action"]["stime"] = st
        message = json.dumps(message_template)  # Convert the message to JSON format

        # Publish the message to the topic
        client.publish(topic, message, retain=False, qos=0)
        print(f"Published by {client_id}: {message}")

        # Wait for other threads before proceeding to the next row
        barrier.wait()  # Synchronize all threads

        if node_value == f"X{num_publishers}":  # Only one thread should increment the row
            m += 1  # Move to the next row after all nodes have published
            print(f"Moving to next row: {m}")

        time.sleep(5)  # Delay before publishing the next message

# Create multiple publishers with unique IDs and dynamic node values
def create_publishers():
    threads = []

    for node_value in range(1, num_publishers + 1):  # Create publishers with node values from 1 to 3
        client_id = f"Publisher_{node_value}"  # Unique client ID for each publisher
        topic = base_topic
        node_value = f"X{node_value}"
        t = threading.Thread(target=publish_message, args=(client_id, topic, node_value))
        threads.append(t)
        t.start()

    # Keep the main thread alive to allow publishers to run
    for t in threads:
        t.join()

# Start multiple publishers with node values from 1 to num_publishers
if __name__ == "__main__":
    create_publishers()
