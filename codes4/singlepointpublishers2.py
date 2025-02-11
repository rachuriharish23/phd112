import paho.mqtt.client as mqtt
import time
import csv
import argparse
import struct
from itertools import islice

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Single Point Publisher")
parser.add_argument("ip_address", type=str, help="IP address to connect to")
parser.add_argument("delay", type=float, help="Delay between messages (seconds)")
parser.add_argument("variable", type=str, help="A custom variable")
args = parser.parse_args()

# Display received arguments
print(f"Received arguments: IP Address={args.ip_address}, Delay={args.delay}, "
      f"Variable={args.variable}")

# MQTT Broker details
broker_address = args.ip_address  # IP address from arguments
port = 1883  # Default MQTT port
topic = "zigbee2mqtt/uart1"  # MQTT topic

# Global variables
m = 1  # Row counter
message_count = 0  # Total message counter

def publish_message():
    """Publish messages to the MQTT broker."""
    global m, message_count  # Access global variables

    # Create a new MQTT client
    client = mqtt.Client("Publisher_1")
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

        # Prepare node value and message
        node_value1 = "X01"
        values = [node_value1, st,st,st,st,st]
        print("before packing:",values)
        # Convert the hexadecimal string to a byte
        byte_value = bytes.fromhex(values[0][1:])  # Strip the 'X' and convert
        float_value = values[1]
        float_value1=values[2]# Use the float value
        float_value2=values[3]
        float_value3=values[4]
        float_value4=values[5]
        # Define the format string
        format_string = 'Bddddd'

        # Pack the values into a bytes object
        packed_message = struct.pack(format_string, byte_value[0], float_value,float_value1,float_value2,float_value3,float_value4)
        # Print packed data
        print("Packed data:", packed_message)
        size_in_bytes = len(packed_message)       
        print(f"size of packed_message : {size_in_bytes} bytes")
        # Send the packed message to the broker
        client.publish(topic, packed_message, retain=False, qos=0)
        print(f"Published: {packed_message}")

        # Unpack the data to verify
        unpacked_data = struct.unpack(format_string, packed_message)
        unpacked_node_value = f"X{unpacked_data[0]:02X}"  # Convert back to "X01" format
        unpacked_float_value = unpacked_data[1]  # Float remains as is
        unpacked_float_value1=unpacked_data[2]
        unpacked_float_value2=unpacked_data[3]
        unpacked_float_value3=unpacked_data[4]
        unpacked_float_value4=unpacked_data[5]
        # Combine unpacked values into desired format
        final_unpacked_data = (unpacked_node_value, unpacked_float_value,unpacked_float_value1,unpacked_float_value2,unpacked_float_value3,unpacked_float_value4)
        print("Unpacked data:", final_unpacked_data)

        # Increment the message counter
        message_count += 1
        print(f"Total messages published so far: {message_count}")

        # Increment row counter for the next iteration
        m += 1

        # Delay before publishing the next message
        time.sleep(args.delay)

if __name__ == "__main__":
    print("Starting single publisher...")
    publish_message()
