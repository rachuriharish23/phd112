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
trans = [100,10,5,2,1]
# Global variables
m = 1  # Row counter
message_count = 0  # Total message counter
max_rows = 100

def on_connect(client, userdata, flags, rc):
    """Callback function when connected to the broker."""
    print(f"Connected to broker with result code {rc}")
    client.subscribe(topic)  # Subscribe to the topic if needed

def publish_message():
    """Publish messages to the MQTT broker."""
    
    # Create a new MQTT client and set up callbacks
    client = mqtt.Client(node_value)
    client.on_connect = on_connect
    client.max_inflight_messages_set(200)
    
    # Connect to the broker
    print("Connecting to broker...")
    client.connect(broker_address, port, 60)
    
    # Start the MQTT client loop
    client.loop_start()

    run = 0
    for run in range(0, num_runs):
        print("Run number ", run)
        client.publish("topicrun", run, retain=False, qos=0)
        time.sleep(30)
        
        for i in range(trans[run]):
            # Get the current timestamp in milliseconds
            st = [time.time() * 1000] * scale[run]
            # Create format string dynamically
            format_string = f"{'d' * scale[run]}"  # 'd' repeated `scale[run]` times
            # Pack the data
            packed_message = struct.pack(format_string, *st)
            # Publish with QoS 2 (exactly once delivery)
            client.publish(topic, packed_message, retain=False, qos=2)
            time.sleep(td[run])
        
        time.sleep(60)
    
    # Stop the MQTT client loop and disconnect after publishing
    print("Publishing complete, disconnecting from broker...")
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    print("Starting single publisher...")
    publish_message()
    print("Execution complete, exiting script.")
