"""
Better code for the Fuel API data retrieval and publishing to MQTT
Main features (ongoing): 
  Running on 2 APIs, all fuel data and newer one
  First API iteration fetch all, publish to broker
  Second API iteration just update the newest one, publish to broker
  Ongoing (pyspark), too lazy to install java
"""

import requests
import pandas as pd
import json
import time
import paho.mqtt.client as mqtt
from datetime import datetime
#import pytz
#sydney_tz = pytz.timezone("Australia/Sydney")

#from pyspark.sql import SparkSession
#import pyspark.sql.functions as ps

# CONFIGURATION
API_KEY_FUEL = "66e33UefJsXEALZf7cKeTjGP17qXdOx8"
API_SECRET_FUEL = "Nost2rQ2z5iPcmar"
AUTH_FUEL = "Basic NjZlMzNVZWZKc1hFQUxaZjdjS2VUakdQMTdxWGRPeDg6Tm9zdDJyUTJ6NWlQY21hcg=="
MQTT_TOPIC = "NSW_fuel/all"
MQTT_BROKER = "localhost"
FILENAME = "all_fuel_data.csv"
FETCH_INTERVAL = 60  # seconds
PUBLISH_DELAY = 0.1  # seconds
FIRST_RUN = True
# MQTT SETUP
client = mqtt.Client()
client.connect(MQTT_BROKER, 1883, 60)


# SPARK SESSION
#spark = SparkSession.builder.appName("FuelData").getOrCreate()

# HELPER FUNCTIONS

def crawler(url, params=None, headers=None, to_json=False):
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

    merged_df = pd.merge(df_station_api, df_prices_api, left_on="code", right_on="stationcode", how="right")
    return merged_df

def cleaning(df):
    df = df.dropna()
    if 'code' in df.columns and (df['code'] == df['stationcode']).all():
        df = df.drop(columns=['code'])
    df = df.drop_duplicates(subset=['brand', 'stationid', 'address', 'fueltype', 'lastupdated'])
    #df['lastupdated'] = pd.to_datetime(df['lastupdated'], dayfirst=True)
    df = df[df['price'] > 0]
    return df

def upsert(df, current_df):
    combined_df = pd.concat([current_df, df], ignore_index=True)
    merged_df = combined_df.drop_duplicates(subset=['stationcode', 'fueltype'], keep='last')
    merged_df.to_csv(FILENAME, index=False)
    return df

def upsert1(df_new, df_existing, filename=FILENAME):
    keys = ['stationcode', 'fueltype']
    df_new = df_new.dropna(subset=keys)

    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_updated_all = df_combined.drop_duplicates(subset=keys, keep='last')

    df_updated_all.to_csv(filename, index=False)

    # Find rows that were changed 
    df_existing_keyed = df_existing.set_index(keys)
    df_new_keyed = df_new.set_index(keys)

    changed_keys = []
    for key in df_new_keyed.index:
        if key in df_existing_keyed.index:
            old_row = df_existing_keyed.loc[key]
            new_row = df_new_keyed.loc[key]
            if (old_row['price'] != new_row['price']) or (old_row['lastupdated'] != new_row['lastupdated']):
                changed_keys.append(key)
    
    df_changes = df_updated_all.set_index(keys).loc[changed_keys].reset_index()

    return df_changes

def fetch_publish():
    global FIRST_RUN
    try:
        access_token = GetFuelAccessToken(AUTH_FUEL)
        df = FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price=not FIRST_RUN)
        #df.to_csv(FILENAME, index=False)
        if df.empty:
            print("No new data found, Waiting for API call...")
            return
        else: 
            print(df.head)
            if FIRST_RUN:
                df = cleaning(df)
                df.to_csv(FILENAME, index=False)
                print(f"Saved {len(df)} records to {FILENAME}")
                batch_size = len(df)
                # Create a list to hold all the records
                batch_records = []
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    batch_records.append(row_dict)

                # Publish the entire batch as a single message
                client.publish(MQTT_TOPIC, json.dumps(batch_records))
                print(f"Published {len(batch_records)} records in one batch")
                time.sleep(PUBLISH_DELAY)
            else:
                current_df = pd.read_csv(FILENAME)
                df = upsert1(df, current_df)
                print(f"Publishing {len(df)} records to topic '{MQTT_TOPIC}' in 0.1 delay...")
                # Publish in delay
                for index, row in df.iterrows():
                    row_dict = row.to_dict()
                    payload = json.dumps(row_dict)
                    client.publish(MQTT_TOPIC, payload)
                    print(f" Published record {index + 1}/{len(df)}")
                    time.sleep(PUBLISH_DELAY)

            print(" Finished publishing all data. Waiting for the next API call...\n")
            FIRST_RUN = False

    except Exception as e:
        print(f"Error occurred: {e}")

# Run
if __name__ == "__main__":
    while True:
        fetch_publish()
        time.sleep(FETCH_INTERVAL)
