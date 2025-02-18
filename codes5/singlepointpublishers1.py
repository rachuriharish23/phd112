import paho.mqtt.client as mqtt
import time
import struct

# Parse command-line arguments
node_value = "X01"
# Display received arguments
# MQTT Broker details
broker_address = "10.40.65.101"  # IP address from arguments
port = 1883  # Default MQTT port
topic = "uart1"  # MQTT topic
td = [0.01,0.1,0.2,0.5,1]
scale = [1,10,20,50,100]
num_runs = 5
trans=[100,10,5,2,1]
# Global variables
m = 1  # Row counter
message_count = 0  # Total message counter
max_rows = 100

def publish_message():
    """Publish messages to the MQTT broker."""
    
    # Create a new MQTT client
    
    client = mqtt.Client(node_value)
    client.max_inflight_messages_set(200)
    client.connect(broker_address, port)
    run = 0
    
    for run in range(0, num_runs):
        print("run number ", run)
        client.publish("topicrun", run, retain=False, qos=0)
        time.sleep(30)
        for i in range(trans[run]):
            # Get the current timestamp in milliseconds
            st = [time.time() * 1000] * scale[run]
            # Create format string dynamically
            format_string = f"{'d' * scale[run]}"  # 'd' repeated `scale[run]` times
            # Pack the data
            packed_message = struct.pack(format_string, *st)
            client.publish(topic, packed_message, retain=False, qos=2)
            time.sleep(td[run])
        time.sleep(60)

    # Disconnect from the MQTT broker after publishing is complete
    print("Publishing complete, disconnecting from broker...")
    client.disconnect()

if __name__ == "__main__":
    print("Starting single publisher...")
    publish_message()
    print("Execution complete, exiting script.")
