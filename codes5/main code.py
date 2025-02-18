import paho.mqtt.client as mqtt
import subprocess
import threading
import time
import json  # Use JSON to pass arguments conveniently

# MQTT settings
MQTT_BROKER = "10.40.64.248"  # Replace with your broker's IP address
MQTT_TOPIC = "coderun"
MQTT_QOS = 1  # Set QoS to 1 for more reliable delivery

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

# Function to capture and print subprocess output
def capture_output(process):
    for line in iter(process.stdout.readline, b""):
        print(line.decode().strip())  # Print each line of output from the subprocess

# Function to start a new process with a 5-second delay
def start_process_with_delay(script_name, *args):
    global current_process
    try:
        with process_lock:
            # Stop any existing process before starting a new one
            if current_process and current_process.poll() is None:
                print("A process is already running; stopping it.")
                stop_process()

            # Delay before starting the process
            print("Delaying process start by 5 seconds...")
            time.sleep(5)

            # Start the new process
            command = ["python", script_name] + list(args)
            current_process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,  # Ensure outputs are in text mode for Python 3.6+
            )
            print(f"Started process: {script_name} with arguments: {args}")

            # Start a thread to capture and print the output of the subprocess
            threading.Thread(target=capture_output, args=(current_process,)).start()

    except Exception as e:
        print(f"Error starting process: {e}")

# Callback function when a message is received
def on_message(client, userdata, message):
    msg = message.payload.decode("utf-8")
    print(f"Received message: {msg}")

    # Handle "stop" message in a separate thread to ensure immediate handling
    if msg == "stop":
        threading.Thread(target=stop_process).start()
        return

    # Parse the message as JSON
    try:
        data = json.loads(msg)  # Expected JSON format: {"script": "script_name", "args": ["arg1", "arg2", ...]}
        script_name = data.get("script")
        args = data.get("args", [])

        if script_name:
            # Start the specified script in a new thread with a delay
            threading.Thread(target=start_process_with_delay, args=(script_name, *args)).start()
        else:
            print("Invalid message: 'script' key is missing.")
    except json.JSONDecodeError:
        print("Message is not in valid JSON format.")

# Setup MQTT client
client = mqtt.Client()
client.on_message = on_message

# Connect to the broker and subscribe to the topic with the specified QoS level
client.connect(MQTT_BROKER)
client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
print(f"Listening for messages on topic: {MQTT_TOPIC} with QoS {MQTT_QOS}")

# Start listening indefinitely
client.loop_forever()
