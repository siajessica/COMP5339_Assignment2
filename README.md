## NSW Fuel Price Data Integration

This script retrieves fuel station and price data from the NSW Government API and exports it into a CSV file. We use the NSW Fuel Check API:

- **API Endpoint**: [https://api.nsw.gov.au/Product/Index/22](https://api.nsw.gov.au/Product/Index/22)
- **API Documentation**: [View Official Docs](https://api.nsw.gov.au/Product/Index/22#v-pills-doc)

### Data Flow

1. Authenticate and get an access token.
2. Retrieve fuel price data via API.
3. Save the merged station and price information into a structured file.

## Available API Endpoints

- `prices`: Returns all existing fuel prices.
- `prices/new`: Returns only the latest updated prices since the last API call to `prices` or `prices/new`.

## How to Use

### Prerequisites

Make sure you have:

- Python 3.x installed
- MQTT Broker e.g. Mosquitto Broker 🦟 from [Mosquitto Eclipse](https://mosquitto.org/download/)
- Required dependencies from requirements.txt(e.g. `requests`, `pandas`, `paho-mqtt`, `streamlit` ...)

### Run Independent API Call 
```bash
cd <Code Location>
python DataIntegration.py --get_new_price <True|False> --output <output_filename.csv>
```

### MQTT
#### Run MQTT Broker
Add mosquitto path to environment path or call it directly using
```bash
mosquitto -v
```
#### Run Publisher Function
Make sure broker is already running
```bash
python publisher.py
```
#### Run Subscriber and Data Visualisation
Run it in different terminal
```bash
streamlit run app.py
```
