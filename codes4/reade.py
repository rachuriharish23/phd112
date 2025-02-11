import csv
import json
import paho.mqtt.client as mqtt
import time 

# MQTT broker details
BROKER = "10.40.64.161"  # updated broker address
PORT = 1883  # MQTT default port
TOPIC = "zigbee2mqtt/uart3/set/action"  # updated topic

# Generate a CSV file path with the current date and time
timestamp = time.time()
CSV_FILE = f"mqtt_data_{timestamp}.csv"

# Define CSV headers (must match JSON keys you expect)
csv_headers = [ "n", "idno", "o", "cl", "st","etime","sonme","eonme","srt","ert", "raequst","arecived_time","edle","totallatency"]



# Initialize CSV file with headers
with open(CSV_FILE, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(csv_headers)

# Callback for when the client receives a connection acknowledgment from the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected successfully to broker at {BROKER}")
        client.subscribe(TOPIC)

    else:
        print(f"Connection failed with code {rc}")

# Callback for when a PUBLISH message is received from the broker
def on_message(client, userdata, msg):
    try:
        # Decode and parse message payload as JSON
        message = json.loads(msg.payload.decode())
        print(message)
        arecived_time = time.time()*1000
        #print(type(arecived_time))
        # Extract fields individually
        n = message.get("n", "")
        id_ = message.get("idno", "")
        o = message.get("o", "")
        cl = message.get("cl", "")
        st = message.get("stime", "")
        etime=message.get("etime","")
        sonme=message.get("sonme","")
        onmes = message.get("onmes", "")
        eonme=message.get("eonme","")
        srt=message.get("srt","")
        ert=message.get("ert","")
        edle=message.get("edle","")

        onta = message.get("onta", "")
        #raeqs = message.get("raeqs", "")
        raequst = message.get("raequst", "")
        edle=message.get("edle","")
        
        totallatency=arecived_time-float (st)
        # Save each field to CSV file
        with open(CSV_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([n, id_, o, cl, st, etime,sonme, onmes,srt,ert, raequst, arecived_time,edle,totallatency])
        
        print(f"Saved to CSV: {[ n, id_, o, cl, st,etime,sonme,eonme,srt,ert, raequst,arecived_time,edle,totallatency]}")

    except json.JSONDecodeError:
        print("Failed to decode JSON from payload:", msg.payload.decode())

# Initialize MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
client.connect(BROKER, PORT, 60)

# Start the MQTT client loop
client.loop_forever()
