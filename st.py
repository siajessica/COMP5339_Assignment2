import os
import json
import streamlit as st
import folium
from streamlit_folium import st_folium
from folium.features import DivIcon

OUTPUT_FILE = "received_geojson.json"
geojson = []
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'r') as f:
        try:
            geojson = json.load(f)
        except json.JSONDecodeError:
            print(" Existing JSON file is empty or corrupted. Starting fresh.")

# STREAMLIT
if 'map_center' not in st.session_state:
    st.session_state.map_center = [-31.2532, 147.9211]

st.set_page_config(layout="wide")
st.title("Fuel Check NSW")

m = folium.Map(location=st.session_state.map_center, zoom_start=5)

fuel_options = sorted(list({f["properties"]["fueltype"] for f in geojson["features"]}))
default_fuel = "E10"
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
            show=(fuel_type==default_fuel)
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

# For debugging
print(st_data)