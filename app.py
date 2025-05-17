import paho.mqtt.client as mqtt
import json
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
from datetime import datetime

# MWTT CONFIG
MQTT_BROKER = "localhost"
MQTT_TOPIC = "NSW_fuel/all"
OUTPUT_FILE = "received_geojson.json"

feature_map = {} # Key: (stationid, fueltype), Value: feature 
geojson = {"type": "FeatureCollection", "features": []}

# STREAMLIT CONFIG
if 'connected' not in st.session_state: # Check if connected to MQTT
    st.session_state.connected = False
if 'first_call' not in st.session_state:
    st.session_state.first_call = True
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-31.2532, 146.9211]
if 'fuel_option' not in st.session_state:
    st.session_state.fuel_option = {}
if 'selected_fuel' not in st.session_state:
    st.session_state.selected_fuel = '"E10'

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

def parse_timestamp(ts):
    return datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")

def update_feature_map(record):
    key = (record['stationid'], record['fueltype'])
    new_time = parse_timestamp(record['lastupdated'])

    existing = feature_map.get(key)
    if existing:
        old_time = parse_timestamp(existing['properties']['lastupdated'])
        if new_time <= old_time:
            return
    
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [record["location.longitude"], record["location.latitude"]]
        },
        "properties": {k: v for k, v in record.items() if not k.startswith("location.")}
    }
    feature_map[key] = feature

def rebuild_geojson():
    geojson['features'] = list(feature_map.values())

    if not st.session_state.fuel_option:
        data = geojson.get("features", [])
        st.session_state.fuel_option = sorted({f["properties"]["fueltype"] for f in data})

def on_message(client, userdata, msg):
    print(f"\nMessage received on topic '{msg.topic}':")
    payload = msg.payload.decode()
    data = json.loads(payload)
    records = data if isinstance(data, list) else [data]
    for rec in records:
        update_feature_map(rec)
    rebuild_geojson()
    save_to_file(geojson)

def start_mqtt():
    # Create client and bind callbacks
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect and listen
    client.connect(MQTT_BROKER, 1883, 60)
    print("Listening for fuel updates... Press Ctrl+C to exit.")
    client.loop_forever()

# Precaution for Threading
def main():
    st.set_page_config(layout="wide")
    st.title("Fuel Check NSW")

    # Start the MQTT Subcriber (Blocking OS) running on the background
    if not st.session_state.connected:
        start_mqtt()
        st.session_state.connected = True
    
    m = folium.Map(location=st.session_state.center, zoom_start=14)
    st.folium(m, height=600, width=1000)

main()