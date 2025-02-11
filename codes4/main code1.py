import configparser
import paho.mqtt.client as mqtt
import subprocess
import threading
import time
import json

# MQTT settings
MQTT_BROKER = "10.40.65.101"  # Default broker address
MQTT_TOPIC = "coderun"
MQTT_QOS = 1  # QoS level for reliable delivery

# Variable to keep track of the running subprocess
current_process = None
process_lock = threading.Lock()  # Lock to manage subprocess access

# Function to stop the running process
def stop_process():
    global current_process
    with process_lock:
        if current_process and current_process.poll() is None:
            print("Stopping the current process...")
            current_process.terminate()
            current_process.wait()  # Ensure the process has fully terminated
            current_process = None
            print("Process terminated.")

def start_process(script_name, args=None):
    global current_process
    with process_lock:
        # Ensure there is no running process before starting a new one
        if current_process is None or current_process.poll() is not None:
            print(script_name, args)
            command = ["python3", script_name]
            if args:
                # Convert all arguments to strings
                command.extend(map(str, args))
            try:
                current_process = subprocess.Popen(command)
                print(f"Started process for {script_name} with args: {args}")
            except Exception as e:
                print(f"Error starting process: {e}")
        else:
            print("Another process is currently running; stop it first.")

# Load configurations from the .ini file
def load_configuration(section, filename="configurations.ini"):
    config = configparser.ConfigParser()
    config.read(filename)
    if section in config:
        return {
            "ip_address": config[section].get("ip_address", None),
            "delay1": config[section].getfloat("delay1", None),
            "sizer": config[section].getint("sizer", None),
            "command":config[section].get("command", None),
        }
    else:
        print(f"Section [{section}] not found in {filename}.")
        return None

# Callback function when a message is received
def on_message(client, userdata, message):
    try:
        # Decode and parse the message payload
        msg = message.payload.decode("utf-8")
        print(f"Received message: {msg}")

        # Try to parse the message as JSON for structured data
        #msg_data = json.loads(msg)
        section=msg
        # Handle "stop" command
        if msg == "stop":
            threading.Thread(target=stop_process).start()
            return

        # Load the configuration for the specified section
        config = load_configuration(section)
        if not config:
            print("Invalid or missing configuration section.")
            return
        
        ip_address = config["ip_address"]
        delay1 = config["delay1"]
        sizer = config["sizer"]
        command=config["command"]
        print(ip_address,delay1,sizer,command)
        # Map commands to script names
        script_name = None
        if command == "run_script_1":
            script_name = "singlepointpublishers1.py"
        elif command == "run_script_2":
            script_name = "temporalpublishers.py"
        elif command == "run_script_3":
            script_name = "spatialpublishers1.py"
        elif command == "run_script_4":
            script_name = "saptio-temporal.py"

        # Start the specified script with parameters
        if script_name and ip_address and delay1 is not None and sizer is not None:
            script_args = [ip_address, delay1, sizer]  # Arguments must be in order
            print(f"Script Args: {script_args}")
            time.sleep(5)
            threading.Thread(target=start_process, args=(script_name, script_args)).start()
        else:
            print("Invalid or missing arguments for the command.")
    except json.JSONDecodeError:
        print("Invalid message format. Expected JSON.")

# Setup MQTT client
client = mqtt.Client()
client.on_message = on_message

# Connect to the broker and subscribe to the topic with the specified QoS level
client.connect(MQTT_BROKER,1883,10)
client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
print(f"Listening for messages on topic: {MQTT_TOPIC} with QoS {MQTT_QOS}")

# Start listening indefinitely
client.loop_forever()
