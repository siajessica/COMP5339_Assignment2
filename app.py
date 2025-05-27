import paho.mqtt.client as mqtt
import json
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
from datetime import datetime
from folium.features import DivIcon
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
import threading
import time
import queue

# MWTT CONFIG
MQTT_BROKER = "localhost"
MQTT_TOPIC = "NSW_fuel/all"
OUTPUT_FILE = "received_geojson.json"

# STREAMLIT CONFIG
if 'connected' not in st.session_state: # Check if connected to MQTT
    st.session_state.connected = False
# if 'first_call' not in st.session_state:
#     st.session_state.first_call = True
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-33.8688, 151.2093]
if 'fuel_option' not in st.session_state:
    st.session_state.fuel_option = []
if 'selected_fuel' not in st.session_state:
    st.session_state.selected_fuel = 'E10'
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'stations_data' not in st.session_state:
    st.session_state.stations_data = {}
if 'previous_map_data' not in st.session_state:
    st.session_state.previous_map_data = {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
        st.session_state.connected = True
    else:
        print("Connection failed. Code:", rc)
        st.session_state.connected = False

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    data = json.loads(payload)
    records = data if isinstance(data, list) else [data]
    
    for rec in records:
        st.session_state.message_queue.put(rec)

def start_mqtt():
    thread = threading.Thread(target=run_mqtt, daemon=True)
    ctx = get_script_run_ctx()
    add_script_run_ctx(thread, ctx)
    thread.start()

def run_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever() 

def update_feature_map(record):
    props = record['properties']
    coords = record['geometry']['coordinates']
    
    station_id = f"{props['stationid']}_{props['fueltype']}"
    
    st.session_state.stations_data[station_id] = {
        'lat': coords[1],
        'lon': coords[0], 
        'name': props['name'],
        'brand': props.get('brand', ''),
        'address': props['address'],
        'fueltype': props['fueltype'],
        'price': float(props['price']),
        'lastupdated': props['lastupdated'],
        'stationid': props['stationid'],
        'isAdBlueAvailable': props['isAdBlueAvailable']
    }
    
    if props['fueltype'] not in st.session_state.fuel_option:
        st.session_state.fuel_option.append(props['fueltype'])
        st.session_state.fuel_option.sort()

def is_within_map(lat, lon, map_data):
    if map_data and map_data['bounds']['_southWest']['lat'] and map_data['bounds']['_northEast']['lat']:
        min_lat = map_data['bounds']['_southWest']['lat']
        min_lon = map_data['bounds']['_southWest']['lng']
        max_lat = map_data['bounds']['_northEast']['lat']
        max_lon = map_data['bounds']['_northEast']['lng']
        return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)
    else:        
        return True

def get_all_fuels_for_station(base_station_id):
    station_fuels = []
    for station_id, station_data in st.session_state.stations_data.items():
        if station_data['stationid'] == base_station_id:
            station_fuels.append(station_data)
    return sorted(station_fuels, key=lambda x: x['fueltype'])

def create_popup(primary_station_data, all_fuels_at_station):
    popup_html = f"""
    <div style="font-size: 12px;">
        <h6><b>{primary_station_data['name']}<b></h6>
        <p><b>Brand:</b> {primary_station_data['brand']}</p>
        <p><b>Address:</b> {primary_station_data['address']}</p>
        {('<p><b>AdBlue Sold Here</b></p>' if any(fuel.get('isAdBlueAvailable', False) for fuel in all_fuels_at_station) else '')}
        <p><b>Available Fuels:</b></p>
    """
    
    for fuel_data in all_fuels_at_station:
        popup_html += f"""
        <p><b>{fuel_data['fueltype']}:</b> ${fuel_data['price']:.1f}<br>
        Updated: {fuel_data['lastupdated']}</p>
        """
    
    popup_html += "</div>"
    return popup_html


def create_feature_group(map_data):
    feature_group = folium.FeatureGroup(name="Fuel Stations")
    processed_stations = set()
    
    for station_id, station_data in st.session_state.stations_data.items():
        base_station_id = station_data['stationid']
        
        if (station_data.get('fueltype') == st.session_state.selected_fuel and 
            base_station_id not in processed_stations and 
            is_within_map(station_data["lat"], station_data["lon"], map_data)):
            
            all_fuels_at_station = get_all_fuels_for_station(base_station_id)
            popup_html = create_popup(station_data, all_fuels_at_station)
            
            price_marker = DivIcon(
                icon_size=(90, 20),
                icon_anchor=(0, 0),
                html=f"""
                <div style="
                    background: white;
                    border: 1px solid #555;
                    border-radius: 3px;
                    font-size: 12px;
                    font-weight: bold;
                    color: #004;
                    box-shadow: 1px 1px 2px rgba(0,0,0,0.3);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    flex-direction: column;
                "><div>{station_data['brand']}</div><div>{station_data['price']:.1f}</div></div>
                """
            )
            
            folium.Marker(
                location=[station_data['lat'], station_data['lon']],
                icon=price_marker,
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(feature_group)
            
            processed_stations.add(base_station_id)
    
    return feature_group


@st.experimental_fragment(run_every=0.1)
def draw_map():
    if not st.session_state.message_queue.empty():
        record = st.session_state.message_queue.get_nowait()
        update_feature_map(record)

    base_map = folium.Map(
        location=st.session_state.map_center,
        zoom_start=14,
        tiles='OpenStreetMap'
    )

    previous_map_data = st.session_state.get('previous_map_data', {})
    feature_group = create_feature_group(previous_map_data)

    map_data = st_folium(
        base_map, 
        height=600, 
        width=1000,
        center=st.session_state.map_center,
        zoom=14,
        feature_group_to_add=feature_group,
        returned_objects=["bounds"],
        key="fuel_map"
    )
    st.session_state.previous_map_data = map_data

    if map_data and map_data.get('center'):
        st.session_state.map_center = [
            map_data['center']['lat'], 
            map_data['center']['lng']
        ]



def main():    
    st.set_page_config(layout="wide")
    st.title("Fuel Check NSW")

    if not st.session_state.connected:
        start_mqtt()

    print(st.session_state.fuel_option)
    if st.session_state.fuel_option:
        st.session_state.selected_fuel = st.selectbox(
            "Select Fuel Type:",
            st.session_state.fuel_option,
            index=st.session_state.fuel_option.index(st.session_state.selected_fuel)
        )

    draw_map()


main()