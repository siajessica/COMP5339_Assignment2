import paho.mqtt.client as mqtt
import json
import os
import streamlit as st
import folium
from streamlit_folium import st_folium

# CONFIG
MQTT_BROKER = "localhost"
MQTT_TOPIC = "NSW_fuel/all"
OUTPUT_FILE = "received_fuel_data.json"

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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
    else:
        print("Connection failed. Code:", rc)

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

# Create client and bind callbacks
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect and listen
client.connect(MQTT_BROKER, 1883, 60)
print("Listening for fuel updates... Press Ctrl+C to exit.")
client.loop_forever()

# m = folium.Map(location=[-31.2532, 146.9211], zoom_start=8)
# fg = folium.FeatureGroup(name="Markers")
# for marker in st.session_state["markers"]:
#     fg.add_child(marker)

# st_folium(
#     m,
#     center=st.session_state["center"],
#     zoom=st.session_state["zoom"],
#     key="new",
#     feature_group_to_add=fg,
#     height=400,
#     width=700,
# )
