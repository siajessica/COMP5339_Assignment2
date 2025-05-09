import requests
import pandas as pd
import argparse
import os
import pandas as pd
import numpy
import csv
import requests
from bs4 import BeautifulSoup
import re

from datetime import datetime
def crawler(url , params = None, headers = None, to_json = False):
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
    Getting the accesstoken from the Fuel API security https://api.nsw.gov.au/Documentation/GenerateHar/22
    Returns:
        tuple: access token generated from the Fuel API security
    """
    
    GRANT_TYPE = "client_credentials"
    URL_SECURITY = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
    
    headers = {
        'content-type': "application/json",
        'authorization': AUTH_FUEL
    }

    querystring = {"grant_type":GRANT_TYPE}
    response = crawler(URL_SECURITY, params=querystring, headers=headers, to_json=True)
    return response["access_token"]

def FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price = True):
    """
    Getting the Lattitude and Longitude from the Fuel API security https://api.nsw.gov.au/Documentation/GenerateHar/22
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
    df_station_api = pd.json_normalize(stations) 
    
    prices = resp.get("prices", []) 
    df_prices_api = pd.json_normalize(prices) 
    
    merged_df = pd.merge(df_station_api, df_prices_api, left_on="code", right_on="stationcode", how="inner")
    # merged_df = merged_df.drop(columns=["state_y"]).rename(columns={"state_x": "state"})

    return merged_df

def cleaning(df):
    # Drop rows with any missing values
    df = df.dropna()

    # Drop 'code' column if identical to 'stationcode'
    if 'code' in df.columns and (df['code'] == df['stationcode']).all():
        df = df.drop(columns=['code'])

    # Drop exact duplicate rows on selected subset
    df = df.drop_duplicates(subset=['brand', 'stationid', 'address', 'fueltype', 'lastupdated'])

    # Convert date column format
    df['lastupdated'] = pd.to_datetime(df['lastupdated'], dayfirst=True)

    # Remove rows where 'price' is non-positive
    df = df[df['price'] > 0]

    return df



if __name__ == "__main__":
    API_KEY_FUEL = "66e33UefJsXEALZf7cKeTjGP17qXdOx8"
    API_SECRET_FUEL = "Nost2rQ2z5iPcmar"
    AUTH_FUEL = "Basic NjZlMzNVZWZKc1hFQUxaZjdjS2VUakdQMTdxWGRPeDg6Tm9zdDJyUTJ6NWlQY21hcg=="
    
    parser = argparse.ArgumentParser(description="Get Data from Fuel Price API")
    parser.add_argument('--get_new_price', default=True, help='Whether to get all the data or get the newest data')
    parser.add_argument('--output', default="combined_stations_prices.csv", help='Path to the output CSV file after data integration')
    
    args = parser.parse_args()
    
    access_token = GetFuelAccessToken(AUTH_FUEL)
    
    df_station_details = FuelStationIntegration(access_token, API_KEY_FUEL, get_new_price=args.get_new_price)
    cleaning(df_station_details)
    df_station_details.to_csv(args.output, index=False)