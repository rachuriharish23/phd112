import csv
import json
import paho.mqtt.client as mqtt
import time 
import struct
# MQTT broker details
BROKER = "10.40.65.101"  # updated broker address
PORT = 1883  # MQTT default port
TOPIC = "outputtopic1"  # updated topic


pkt_count = 0
latency = 0

# Callback for when the client receives a connection acknowledgment from the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected successfully to broker at {BROKER}")
        client.subscribe(TOPIC)

    else:
        print(f"Connection failed with code {rc}")

# Callback for when a PUBLISH message is received from the broker
def on_message(client, userdata, msg):
    global pkt_count,latency
    arecived_time = time.time()*1000
    pkt_count = pkt_count + 1
    unpacked=struct.unpack("d",msg.payload)[0]
    latency = latency + arecived_time - unpacked
    if pkt_count == 100:
        print(latency/100)
        pkt_count = 0

    
# Initialize MQTT client
client = mqtt.Client()
print(time.time()*1000)
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
client.connect(BROKER, PORT)

# Start the MQTT client loop
client.loop_forever()
