import paho.mqtt.client as mqtt
import json
import os
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
from threading import Thread
import queue

# MWTT CONFIG
MQTT_BROKER = "localhost"
MQTT_TOPIC = "NSW_fuel/all"
OUTPUT_FILE = "received_fuel_data.json"

# STREAMLIT CONFIG
if 'connected' not in st.session_state: # Check if connected to MQTT
    st.session_state.connected = False
if 'first_call' not in st.session_state: # Check if it's the first call or not
    st.session_state.first_call = False
if 'data' not in st.session_state: # Data To be Drawn 
    st.session_state.data = []
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-31.2532, 146.9211]

################################################# MAJOR CHANGES NEEDED
fuel_records = []

if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'r') as f:
        try:
            fuel_records = json.load(f)
        except json.JSONDecodeError:
            print(" Existing JSON file is empty or corrupted. Starting fresh.")

def save_to_file(data):
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to {OUTPUT_FILE}")

############################################################# UNTIL HERE

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
    else:
        print("Connection failed. Code:", rc)

##################################### NEED TO CHANGE SOME - DATA
def on_message(client, userdata, msg):
    global fuel_records
    print(f"\nMessage received on topic '{msg.topic}':")
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        # If batch (list), store all
        if isinstance(data, list):
            print(f"Received {len(data)} records (batch).")
            fuel_records.extend(data)
        else:
            print("Single updated record:")
            print(data)
            fuel_records.append(data)

        seen = set()
        unique_records = []
        for record in reversed(fuel_records):  # reverse to keep 
            key = (record.get("stationcode"), record.get("fueltype"))
            if key not in seen:
                unique_records.append(record)
                seen.add(key)
        fuel_records = list(reversed(unique_records))  # restore original order

        save_to_file(fuel_records)

    except json.JSONDecodeError:
        print("Error decoding JSON payload:", msg.payload)

#################################################################### UNTIL HERE

def start_mqtt():
    # Create client and bind callbacks
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect and listen
    client.connect(MQTT_BROKER, 1883, 60)
    print("Listening for fuel updates... Press Ctrl+C to exit.")
    client.loop_forever()

# Visualization of the Map
def create_map():
    if st.session_state.first_call:
        pass
    else:
        pass

# Precaution for Threading
def main():
    # Start the MQTT Subcriber (Blocking OS) running on the background
    if not st.session_state['connected']:
        Thread(target=start_mqtt, daemon=True).start()
        st.session_state.mqtt_started = True

if __name__ == "__main__":
    main()