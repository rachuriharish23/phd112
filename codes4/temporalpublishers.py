import paho.mqtt.client as mqtt
import time
import csv
import argparse
import struct
from itertools import islice
node_value="X05"

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Single Point Publisher")
parser.add_argument("ip_address", type=str, help="IP address to connect to")
parser.add_argument("delay1", type=float, help="Delay between messages (seconds)")
parser.add_argument("sizer", type=int, help="Number of floating-point values to include")
args = parser.parse_args()

# Display received arguments
print(f"Received arguments: IP Address={args.ip_address}, Delay={args.delay1}, "
      f"Number of floats={args.sizer}")

# MQTT Broker details
broker_address = args.ip_address  # IP address from arguments
port = 1883  # Default MQTT port
topic = "zigbee2mqtt/uart2"  # MQTT topic

# Global variables
m = 1  # Row counter
message_count = 0  # Total message counter

def pack_message(node_value, timestamp, sizer):
    """
    Pack the message dynamically based on the number of floating-point values.

    :param node_value: Node identifier in hexadecimal format (e.g., "X01")
    :param timestamp: Current timestamp (float)
    :param sizer: Number of floating-point values to include
    :return: Packed message
    """
    # Convert the node value to a byte
    byte_value = bytes.fromhex(node_value[1:])  # Strip the 'X' and convert

    # Generate dynamic float values based on the timestamp
    float_values = [timestamp] * sizer  # Repeat timestamp `sizer` times

    # Create format string dynamically
    format_string = f"B{'d' * sizer}"  # 'B' for byte, 'd' repeated `sizer` times

    # Pack the data
    packed_message = struct.pack(format_string, byte_value[0], *float_values)
    
    return packed_message, format_string, float_values

def publish_message():
    """Publish messages to the MQTT broker."""
    global m, message_count  # Access global variables

    # Create a new MQTT client
    client = mqtt.Client(node_value)
    client.connect(broker_address, port)

    while True:
        # Read a single row from the CSV file
        with open("cardio_datatest.csv", "r") as f:
            reader = csv.reader(f)
            for row in islice(reader, m - 1, m):  # Fetch only the m-th row
                break  # Only one row is needed

        # Convert the row to integers (use first 11 elements)
        line_value = [int(x) for x in row[:11]]

        # Get the current timestamp in milliseconds
        st = time.time() * 1000
        st = round(st, 3)

        # Prepare node value
        

        # Pack the message dynamically
        packed_message, format_string, float_values = pack_message(node_value, st, args.sizer)

        # Debug: Print packed data and its size
        print("Before packing:", [node_value] + float_values)
        print(f"Packed data: {packed_message}")
        print(f"Size of packed_message: {len(packed_message)} bytes")

        # Publish to MQTT broker
        client.publish(topic, packed_message, retain=False, qos=0)
        print(f"Published: {packed_message}")

        # Unpack the data to verify
        unpacked_data = struct.unpack(format_string, packed_message)
        unpacked_node_value = f"X{unpacked_data[0]:02X}"  # Convert back to "X01" format
        unpacked_float_values = unpacked_data[1:]  # Extract unpacked floats
        final_unpacked_data = (unpacked_node_value, *unpacked_float_values)
        print("Unpacked data:", final_unpacked_data)

        # Increment the message counter
        message_count += 1
        print(f"Total messages published so far: {message_count}")

        # Increment row counter for the next iteration
        m += 1

        # Delay before publishing the next message
        time.sleep(args.delay1)

if __name__ == "__main__":
    print("Starting single publisher...")
    publish_message()
