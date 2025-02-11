import paho.mqtt.client as mqtt
import time
import csv
import argparse
import struct
from itertools import islice

# Parse command-line arguments
node_value = "X05"
# Display received arguments
# MQTT Broker details
broker_address = "10.40.65.101"  # IP address from arguments
port = 1883  # Default MQTT port
topic = "uart1"  # MQTT topic
td = [0.01,0.1,0.2,0.5,1]
scale = [1,10,20,50,100]
num_runs = 5
# Global variables
m = 1  # Row counter
message_count = 0  # Total message counter
max_rows=100


  

def publish_message():
    """Publish messages to the MQTT broker."""
    
    # Create a new MQTT client
    client = mqtt.Client( node_value)
    client.connect(broker_address, port)
    run = 0
    for run in range(0,num_runs):
        print("run number ",run)
        for i in range(100):
           # Get the current timestamp in milliseconds
            st = [time.time() * 1000]*scale[run]
            # Create format string dynamically
            format_string = f"{'d' * scale[run]}"  # 'B' for byte, 'd' repeated `sizer` times
            # Pack the data
            packed_message = struct.pack(format_string, *st)
            client.publish(topic, packed_message, retain=False, qos=0)
            time.sleep(td[run])
        time.sleep(5)

if __name__ == "__main__":
    print("Starting single publisher...")
    publish_message()
