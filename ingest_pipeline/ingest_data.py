import os
import pandas as pd
import requests
import yaml
from google.cloud import bigquery

# To read the yaml configs
with open('config-dev.yaml','r') as f:
    config=yaml.safe_load(f)

PROJECT_ID =config['gcp']['project_id']
DATASET_ID = config['bigquery']['dataset_id']
TABLE_ID = config['bigquery']['table_id']
API_KEY = config['api']['api_key']


def fetch_and_ingest_data():
    client = bigquery.Client(project=PROJECT_ID)

    cities = [
        {"name": "New Delhi", "lat": "28.61", "lon": "77.20"},
        {"name": "Los Angeles", "lat": "34.05", "lon": "-118.24"},
        {"name": "London", "lat": "51.50", "lon": "-0.12"},
        {"name": "Cairo", "lat": "30.04", "lon": "31.23"},
        {"name": "Tokyo", "lat": "35.67", "lon": "139.65"},
        {"name": "Sydney", "lat": "-33.87", "lon": "151.21"},
        {"name": "Beijing", "lat": "39.90", "lon": "116.41"},
        {"name": "Sao Paulo", "lat": "-23.55", "lon": "-46.63"},
        {"name": "Reykjavik", "lat": "64.14", "lon": "-21.94"},
        {"name": "Johannesburg", "lat": "-26.20", "lon": "28.04"}
    ]

    def cal_aqi_target(pm25_value):
        if pm25_value <= 12.0:
            return 'low'
        elif pm25_value <= 35.4:
            return 'medium'
        elif pm25_value <= 55.4:
            return 'high'
        else:
            return 'hazardous'

    processed_rows = []

    for i in cities:
        pol_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={i['lat']}&lon={i['lon']}&appid={API_KEY}"
        we_url = f"http://api.openweathermap.org/data/2.5/weather?lat={i['lat']}&lon={i['lon']}&appid={API_KEY}&units=metric"

        pol_res = requests.get(pol_url).json()
        we_res = requests.get(we_url).json()
        pm25 = float(pol_res["list"][0]["components"]["pm2_5"])


        data = {
            "city": i["name"],
            "temp": float(we_res["main"]["temp"]),
            "feels_like": float(we_res["main"]["feels_like"]),
            "temp_max": float(we_res["main"]["temp_max"]),
            "temp_min": float(we_res["main"]["temp_min"]),
            "pressure": int(we_res["main"]["pressure"]),
            "humidity": int(we_res["main"]["humidity"]),
            "sea_level": int(we_res["main"]["sea_level"]) if "sea_level" in we_res["main"] else None,
            "precipitation": float(we_res.get("rain", {}).get("1h", 0.0)),
            "wind_speed": int(float(we_res["wind"]["speed"])),  # Fixed key name
            "wind_direction": float(we_res["wind"]["deg"]),
            "pm25_value": pm25,
            "target_value": cal_aqi_target(pm25),
            "update_timestamp": pd.Timestamp.utcnow()
        }

        processed_rows.append(data)

    df = pd.DataFrame(processed_rows)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("temp", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("feels_like", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("temp_max", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("temp_min", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("pressure", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("humidity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("sea_level", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("precipitation", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("wind_speed", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("wind_direction", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("pm25_value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("target_value", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("update_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Successfully ingested {len(df)} rows into {table_ref}.")


if __name__ == "__main__":
    fetch_and_ingest_data()

