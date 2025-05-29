"""
Retrieve data using API, save it as .csv, preprocess and publish it to MQTT Broker 
Make sure MQTT is running 
"""

import os
import requests
import pandas as pd
import json
import time
import paho.mqtt.client as mqtt
from datetime import datetime

# Make sure MQTT is running !!!

# CONFIGURATION
API_KEY_FUEL = "66e33UefJsXEALZf7cKeTjGP17qXdOx8"
API_SECRET_FUEL = "Nost2rQ2z5iPcmar"
AUTH_FUEL = "Basic NjZlMzNVZWZKc1hFQUxaZjdjS2VUakdQMTdxWGRPeDg6Tm9zdDJyUTJ6NWlQY21hcg=="
MQTT_TOPIC = "NSW_fuel/all"
MQTT_BROKER = "localhost"
FILENAME = "all_fuel_data.csv"
MQTT_CLIENT_ID = 'test'
FETCH_INTERVAL = 60  # seconds
PUBLISH_DELAY = 0.1  # seconds
FIRST_RUN = True
# MQTT SETUP
client = mqtt.Client(client_id=MQTT_CLIENT_ID)
client.connect(MQTT_BROKER, 1883, 60)
client.loop_start() 


# SPARK SESSION
#spark = SparkSession.builder.appName("FuelData").getOrCreate()

# HELPER FUNCTIONS

def crawler(url, params=None, headers=None, to_json=False):
    """
    Getting the accesstoken from the Fuel API security https://api.nsw.gov.au/Documentation/GenerateHar/22
    Returns:
        tuple: access token generated from the Fuel API security
    """
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        if to_json:
            response = response.json()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def GetFuelAccessToken(AUTH_FUEL):
    """
    Getting the access token from the Fuel API security
    Returns:
        str: access token generated from the Fuel API security
    """
    GRANT_TYPE = "client_credentials"
    URL_SECURITY = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
    
    headers = {
        'content-type': "application/json",
        'authorization': AUTH_FUEL
    }

    querystring = {"grant_type": GRANT_TYPE}
    response = crawler(URL_SECURITY, params=querystring, headers=headers, to_json=True)
    return response["access_token"]

def FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price=True):
    """
    Getting the latitude and longitude from the Fuel API
    Returns:
        DataFrame: containing the station details
    """
    if not get_new_price:
        url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"
    else:
        url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices/new"
    
    headers = {
        'content-type': "application/json; charset=utf-8",
        'authorization': f"Bearer {access_token}",
        'apikey': API_KEY_FUEL,
        'transactionid': "1234567890",
        'requesttimestamp': datetime.utcnow().strftime('%d/%m/%Y %I:%M:%S %p')
    }

    print(headers)
    resp = crawler(url=url, headers=headers, to_json=True)
    
    stations = resp.get("stations", [])
    prices = resp.get("prices", [])

    if not stations or not prices:
        print("No data found in the response.")
        return pd.DataFrame()
    
    df_station_api = pd.json_normalize(stations)
    df_prices_api = pd.json_normalize(prices)

    if df_station_api.empty or df_prices_api.empty:
        print("No data found in the API response.")
        return pd.DataFrame()

    merged_df = pd.merge(df_station_api, df_prices_api, left_on="code", right_on="stationcode", how="inner")
    return merged_df

def cleaning(df):
    df = df.dropna()
    if 'code' in df.columns and (df['code'] == df['stationcode']).all():
        df = df.drop(columns=['code'])
    df = df.drop_duplicates(subset=['brand', 'stationid', 'address', 'fueltype', 'lastupdated'])
    #df['lastupdated'] = pd.to_datetime(df['lastupdated'], dayfirst=True)
    df = df[df['price'] > 0]
    return df

def upsert(df_new, df_existing, filename=FILENAME):
    """
    Upserts new data into the existing CSV and returns only the rows that
    are new or have changed.
    """
    keys = ['stationcode', 'fueltype']
    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        # Combine and keep the latest records
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_updated_all = df_combined.drop_duplicates(subset=keys, keep='last')
    else:
        df_updated_all = df_new

    df_updated_all.to_csv(filename, index=False)
    print(f"Saved {len(df_updated_all)} total records to {filename}")
    # For publishing, we only send new or changed data
    return df_new

def to_geojson(row):
    """
    Converts a single DataFrame row into a GeoJSON Feature dictionary.
    """
    latitude = row.get('location.latitude')
    longitude = row.get('location.longitude')

    # check geometry
    if pd.isna(latitude) or pd.isna(longitude):
        return None

    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(longitude), float(latitude)]
        },
        "properties": {
            # Extract all other columns into the properties dict
            **row.drop(['location.latitude', 'location.longitude']).to_dict()
        }
    }
    return feature

def fetch_publish():
    """This function retrieves fuel station data from the API, cleans it, and publishes it to an MQTT topic.
    IF FIRST_RUN: get all data /prices API
    ELSE: get only new/changed data from prices/new API
    as GeoJSON
    """
    global FIRST_RUN
    access_token = GetFuelAccessToken(AUTH_FUEL)
    df_api = FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price=not FIRST_RUN)
    if df_api.empty:
        print("No data retrieved from API. Waiting for next interval.")
        return
    df_cleaned = cleaning(df_api)
    
    if FIRST_RUN:
        # On the first run, all data is new.
        df_to_publish = df_cleaned
        df_cleaned.to_csv(FILENAME, index=False)
        print(f"First run: Saved {len(df_cleaned)} initial records to {FILENAME}")
        FIRST_RUN = False
    else:
        # get only the new/changed data to publish
        df_to_publish = upsert(df_cleaned, FILENAME)

    if df_to_publish.empty:
        print("No new price updates to publish.")
        return
        
    print(f"Publishing {len(df_to_publish)} records to topic '{MQTT_TOPIC}'...")
    for index, row in df_to_publish.iterrows():
        # Convert the row to a GeoJSON 
        feature = to_geojson(row)
        if feature:
            # Serialize the object to a JSON string
            payload = json.dumps(feature)
            client.publish(MQTT_TOPIC, payload, qos=2) # Qualitiy of Service 2 for reliable publishing
            print(f"Published record {index + 1}: \n {payload} \n")
            time.sleep(PUBLISH_DELAY)

    print("Finished publishing all data. Waiting for the next API call...\n")
# Run
if __name__ == "__main__":
    try:    
        while True:
            fetch_publish()
            print(f"Waiting for {FETCH_INTERVAL} seconds before next API call.")
            time.sleep(FETCH_INTERVAL)
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting...")
        client.loop_stop()
        client.disconnect()
