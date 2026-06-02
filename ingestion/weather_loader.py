"""
Weather Loader
Calls the OpenWeather API (current + 5-day forecast) for Quito's bounding
box, normalises the response, and writes Parquet to
s3://quito-transport-raw/weather/year=.../month=.../day=...
"""

import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

from ingestion.constants import (
    OPENWEATHER_BASE_URL,
    QUITO_LAT,
    QUITO_LON,
    S3_WEATHER_PREFIX,
    S3_BUCKET_NAME,
)

load_dotenv()


def handler(event, context):
    """Lambda entry point."""
    api_key = os.environ["OPENWEATHER_API_KEY"]
    bucket = S3_BUCKET_NAME
    now = datetime.now(timezone.utc)

    current_raw = fetch_current(api_key)
    forecast_raw = fetch_forecast(api_key)

    df = pd.concat(
        [normalize_current(current_raw, now), normalize_forecast(forecast_raw, now)],
        ignore_index=True,
    )

    key = f"raw/{S3_WEATHER_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather.parquet"
    write_to_s3(df, bucket, key)

    return {"statusCode": 200, "records_written": len(df), "s3_key": key}


def fetch_current(api_key: str) -> dict:
    response = requests.get(
        f"{OPENWEATHER_BASE_URL}/weather",
        params={"lat": QUITO_LAT, "lon": QUITO_LON, "appid": api_key, "units": "metric"},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def fetch_forecast(api_key: str) -> dict:
    response = requests.get(
        f"{OPENWEATHER_BASE_URL}/forecast",
        params={"lat": QUITO_LAT, "lon": QUITO_LON, "appid": api_key, "units": "metric"},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def normalize_current(data: dict, fetched_at: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "record_type": "current",
                "fetched_at": fetched_at.isoformat(),
                "timestamp": datetime.fromtimestamp(data["dt"], tz=timezone.utc).isoformat(),
                "temp_c": data["main"]["temp"],
                "feels_like_c": data["main"]["feels_like"],
                "humidity_pct": data["main"]["humidity"],
                "pressure_hpa": data["main"]["pressure"],
                "weather_main": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "wind_speed_ms": data["wind"]["speed"],
                "wind_deg": data["wind"].get("deg"),
                "rain_1h_mm": data.get("rain", {}).get("1h", 0.0),
                "clouds_pct": data["clouds"]["all"],
                "visibility_m": data.get("visibility"),
            }
        ]
    )


def normalize_forecast(data: dict, fetched_at: datetime) -> pd.DataFrame:
    records = [
        {
            "record_type": "forecast",
            "fetched_at": fetched_at.isoformat(),
            "timestamp": datetime.fromtimestamp(item["dt"], tz=timezone.utc).isoformat(),
            "temp_c": item["main"]["temp"],
            "feels_like_c": item["main"]["feels_like"],
            "humidity_pct": item["main"]["humidity"],
            "pressure_hpa": item["main"]["pressure"],
            "weather_main": item["weather"][0]["main"],
            "weather_description": item["weather"][0]["description"],
            "wind_speed_ms": item["wind"]["speed"],
            "wind_deg": item["wind"].get("deg"),
            "rain_1h_mm": item.get("rain", {}).get("3h", 0.0),
            "clouds_pct": item["clouds"]["all"],
            "visibility_m": item.get("visibility"),
        }
        for item in data["list"]
    ]
    return pd.DataFrame(records)


def write_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


if __name__ == "__main__":
    result = handler({}, None)
    print(result)
