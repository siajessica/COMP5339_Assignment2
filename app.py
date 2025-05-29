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
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-33.8688, 151.2093]
if 'fuel_option' not in st.session_state:
    st.session_state.fuel_option = ['B20', 'DL', 'E10', 'E85', 'EV', 'LPG', 'P95', 'P98', 'PDL', 'U91']
if 'selected_fuel' not in st.session_state:
    st.session_state.selected_fuel = 'E10'
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'stations_data' not in st.session_state:
    st.session_state.stations_data = {}
if 'previous_map_data' not in st.session_state:
    st.session_state.previous_map_data = {}
if 'open_popup' not in st.session_state:
    st.session_state.open_popup = None

def on_connect(client, userdata, flags, rc):
    """
    Callback function for when the client connects to the MQTT broker.
    - rc == 0 indicates a successful connection.
    - Subscribes to the MQTT topic upon successful connection.
    """
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
        st.session_state.connected = True
    else:
        print("Connection failed. Code:", rc)
        st.session_state.connected = False

def on_message(client, userdata, msg):
    """
    Callback function for when a message is received from the MQTT broker.
    - Decodes the JSON payload
    - Enqueues each record for processing
    """
    payload = msg.payload.decode()
    data = json.loads(payload)
    records = data if isinstance(data, list) else [data]
    
    for rec in records:
        st.session_state.message_queue.put(rec)

def start_mqtt():
    """
    Starts the MQTT client in a separate backgound daemon thread
    to avoid blocking the Streamlit app.
    """
    thread = threading.Thread(target=run_mqtt, daemon=True)
    ctx = get_script_run_ctx()
    add_script_run_ctx(thread, ctx)
    thread.start()

def run_mqtt():
    """
    Initializes the MQTT client, sets up the connection and message callbacks,
    and starts the MQTT loop to listen for messages.
    """
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.loop_forever() 

def update_feature_map(record):
    """
    Given the GEOJson record, extracts the geospatial properties and station metadata,
    then update or insert station data into stations_data in the session state,
    keyed by station code and keep track of fuel types
    """

    props = record['properties']
    coords = record['geometry']['coordinates']
    
    station_code = props['stationcode']
    
    if station_code not in st.session_state.stations_data:
        st.session_state.stations_data[station_code] = {
            'lat': coords[1],
            'lon': coords[0], 
            'name': props['name'],
            'brand': props.get('brand', ''),
            'address': props['address'],
            'stationcode': props['stationcode'],
            'isAdBlueAvailable': props['isAdBlueAvailable'],
            'fuels': {}
        }
    
    st.session_state.stations_data[station_code]['fuels'][props['fueltype']] = {
        'price': float(props['price']),
        'lastupdated': props['lastupdated']
    }
    
    if props['fueltype'] not in st.session_state.fuel_option:
        st.session_state.fuel_option.append(props['fueltype'])
        st.session_state.fuel_option.sort()


def is_within_map(lat, lon, map_data):
    """
    Check if the given latitude and longitude are within the bounds of the map_data.
    Returns True if the coordinates are within the bounds or if no bounds exists, False otherwise.
    """

    if map_data and map_data['bounds']['_southWest']['lat'] and map_data['bounds']['_northEast']['lat']:
        min_lat = map_data['bounds']['_southWest']['lat']
        min_lon = map_data['bounds']['_southWest']['lng']
        max_lat = map_data['bounds']['_northEast']['lat']
        max_lon = map_data['bounds']['_northEast']['lng']
        return (min_lat <= lat <= max_lat) and (min_lon <= lon <= max_lon)
    else:        
        return True

def get_all_fuels_for_station(station_code):
    """
    Given a station code, retrieves all fuel types and their prices for that station.
    Returns a sorted list of dictionaries containing fuel type, price, and last updated time.
    """

    if station_code in st.session_state.stations_data:
        station_data = st.session_state.stations_data[station_code]
        station_fuels = []
        for fuel_type, fuel_info in station_data['fuels'].items():
            station_fuels.append({
                'fueltype': fuel_type,
                'price': fuel_info['price'],
                'lastupdated': fuel_info['lastupdated'],
            })
        return sorted(station_fuels, key=lambda x: x['fueltype'])
    return []


def create_popup(primary_station_data, all_fuels_at_station):
    """
    Build and return an HTML string for the folium.Popup,
    showing station name, brand, address, AdBlue availability,
    and listing each fuel type with its price and timestamp.
    """

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

def is_current_popup(station_data):
    """
    Check if the given station geosparial data matches the currently open popup in the session state.
    Returns True if it matches, False otherwise.
    """
    if st.session_state.open_popup:
        ret =  (st.session_state.open_popup['lat'] == station_data['lat'] and
                st.session_state.open_popup['lng'] == station_data['lon'])
        return ret
    return False

def create_feature_group(map_data):
    """
    Create a folium.FeatureGroup for containing a DivIcon marker and a popup
    for each station that offers the selected fuel type and is within the map bounds.
    Returns the feature group containing all markers.
    """

    feature_group = folium.FeatureGroup(name="Fuel Stations")
    
    for station_code, station_data in st.session_state.stations_data.items():
        if (st.session_state.selected_fuel in station_data['fuels'] and 
            is_within_map(station_data["lat"], station_data["lon"], map_data)):
            
            all_fuels_at_station = get_all_fuels_for_station(station_code)
            popup_html = create_popup(station_data, all_fuels_at_station)
            
            selected_fuel_price = station_data['fuels'][st.session_state.selected_fuel]['price']
            
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
                "><div>{station_data['brand']}</div><div>{selected_fuel_price:.1f}</div></div>
                """
            )
            
            feature_group.add_child(folium.Marker(
                location=[station_data['lat'], station_data['lon']],
                icon=price_marker,
                popup=folium.Popup(popup_html, max_width=300, sticky=True, show=is_current_popup(station_data))
            ))
    
    return feature_group


@st.fragment(run_every=1)
def draw_map():
    """
    A Streamlit fragment that draws the map with fuel stations which runs every second.
    1. Checks and processes messages from the MQTT queue.
    2. Creates a base folium.Map centered on the session_state.map_center.
    3. Creates a feature group with markers for each fuel station that matches the
    selected fuel type and within the map bounds.
    4. Renders the map with the feature group and updates the session state with the last clicked object.
    """
    
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
        width="100%",
        feature_group_to_add=feature_group,
        returned_objects=["bounds", "last_object_clicked"],
        key="fuel_map"
    )

    st.session_state.open_popup = map_data.get('last_object_clicked')
    st.session_state.previous_map_data = map_data


def main():    
    """
    Configure Streamlit layout, ensure MQTT is running,
    render fuel type selector, and draw the live map.
    """

    st.set_page_config(layout="wide")
    st.title("Fuel Check NSW")

    if not st.session_state.connected:
        start_mqtt()

    if st.session_state.fuel_option:
        selected_fuel = st.selectbox(
            "Select Fuel Type:",
            options=st.session_state.fuel_option,
            key="selected_fuel"
        )

    draw_map()


main()