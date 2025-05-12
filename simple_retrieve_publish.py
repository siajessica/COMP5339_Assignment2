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

def upsert(df, current_df):
    combined_df = pd.concat([current_df, df], ignore_index=True)
    merged_df = combined_df.drop_duplicates(subset=['stationcode', 'fueltype'], keep='last')
    merged_df.to_csv(FILENAME, index=False)
    return df

def fetch_publish():
    """
    Main function for MTQQ broker
    Run GetFuelAccessToken, safe the output to new dataframe as new .csv in df
    Send output from df to MTQQ broker in batch with 0.1 delay
    """
    access_token = GetFuelAccessToken(AUTH_FUEL)
    df = FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price=not FIRST_RUN)
    #df.to_csv(FILENAME, index=False)
    if df.empty:
        print("No new data found, Waiting for API call...")
        return
    else: 
        print(df.head)
        df = cleaning(df)
        df.to_csv(FILENAME, index=False)
        print(f"Saved {len(df)} records to {FILENAME}")
        batch_size = max(1, len(df) // 1)
        # Publish in batches
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size].copy()
            batch_records = []
            for _, row in batch_df.iterrows():
                row_dict = row.to_dict()
                batch_records.append(row_dict)
            client.publish(MQTT_TOPIC, json.dumps(batch_records))
            print(f" Published batch {i // batch_size + 1}: {len(batch_records)} records")
            time.sleep(PUBLISH_DELAY)
        print(" Finished publishing all data. Waiting for the next API call...\n")
# Run
if __name__ == "__main__":
    while True:
        fetch_publish()
        time.sleep(FETCH_INTERVAL)
