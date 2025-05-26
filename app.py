import paho.mqtt.client as mqtt
import time
import json
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
from folium.features import DivIcon
from datetime import datetime

# MWTT CONFIG
MQTT_BROKER = "localhost"
MQTT_TOPIC = "NSW_fuel/all"
OUTPUT_FILE = "received_geojson.json"

feature_map = {} # Key: (stationid, fueltype), Value: feature 
geojson = {"type": "FeatureCollection", "features": []}
has_new_data = False

# STREAMLIT CONFIG
if 'connected' not in st.session_state: # Check if connected to MQTT
    st.session_state.connected = False
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-31.2532, 147.9211]
if 'fuel_option' not in st.session_state:
    st.session_state.fuel_option = ['DL', 'E10', 'E85', 'P95', 'P98', 'PDL', 'U91', 'LPG', 'B20', 'EV']
if 'default_fuel' not in st.session_state:
    st.session_state.default_fuel = '"E10'

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
    properties = record.get('properties', {})
    if 'stationid' not in properties or 'fueltype' not in properties:
        return 
    
    key = (properties['stationid'], properties['fueltype'])
    new_time = parse_timestamp(properties['lastupdated'])

    existing = feature_map.get(key)
    if existing:
        old_time = parse_timestamp(existing['properties']['lastupdated'])
        if new_time <= old_time:
            return

    feature = record
    feature_map[key] = feature

def rebuild_geojson():
    global has_new_data
    geojson['features'] = list(feature_map.values())
    has_new_data = True

def on_message(client, userdata, msg):
    print(f"\nMessage received on topic '{msg.topic}':")
    if not msg.payload:
        return
    
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

def main():
    st.set_page_config(layout="wide")
    st.title("Fuel Check NSW")

    if not st.session_state.connected:
        start_mqtt()
        st.session_state.connected = True
    
    m = folium.Map(location=st.session_state.map_center, zoom_start=5)
    
    fuel_groups = {}
    for feature in geojson['features']:
        coordinate = feature["geometry"]["coordinates"] # long, lat
        props = feature["properties"]
        fuel_type = props['fueltype']
        price = props["price"] 

        # Pop-Up When Clicked
        popup_html = f"""
        <div style="width:200px;font-family:sans-serif">
        <h4 style="margin:0">{props["name"]}</h4>
        <small>{props["address"]}</small><br>
        </div>
        """

        # Price Icon
        price_marker = DivIcon(
            icon_size=(40, 20),
            icon_anchor=(0, 0),
            html=f"""
            <div style="
                background: white;
                border: 1px solid #555;
                border-radius: 3px;
                padding: 2px 4px;
                font-size: 12px;
                font-weight: bold;
                color: #004;
                box-shadow: 1px 1px 2px rgba(0,0,0,0.3);
            ">{price}</div>
            """
        )

        if fuel_type not in fuel_groups:
            fuel_groups[fuel_type] = folium.FeatureGroup(
                name=fuel_type,
                show=(fuel_type==st.session_state.default_fuel)
            )
        folium.Marker(
            location=[coordinate[1], coordinate[0]], # lat, long
            icon=price_marker,
            popup=folium.Popup(popup_html, max_width=250)
        ).add_to(fuel_groups[fuel_type])

    for fg in fuel_groups.values():
        fg.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    st_data = st_folium(m, height=600, width=1000)

    if has_new_data:
        has_new_data = False
        time.sleep(0.1)
        st.experimental_rerun()

    # For debugging
    print(st_data)

main()