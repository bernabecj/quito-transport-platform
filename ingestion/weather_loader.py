"""
Weather Loader
Calls the OpenWeather API (current + 5-day forecast) for Quito's bounding
box, normalises the response, and writes Parquet to
raw/weather/year=.../month=.../day=... — in the S3 bucket, or under
data/local/ when STORAGE_BACKEND=local (see ingestion/storage.py).

Secret handling
  The API key is read from Secrets Manager at runtime, never injected as a
  Terraform-managed environment variable. Resolving it in Terraform would write
  the plaintext key into terraform.tfstate (and into any plan file), which
  defeats the point of storing it in Secrets Manager at all. Terraform passes
  only the secret's *name*; this module fetches the value.
"""

import json
import os
import re
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.constants import (
    OPENWEATHER_BASE_URL,
    OPENWEATHER_SECRET_KEY,
    QUITO_LAT,
    QUITO_LON,
    S3_WEATHER_PREFIX,
)
from ingestion.storage import get_storage

load_dotenv()


def get_api_key() -> str:
    """Resolve the OpenWeather key.

    In Lambda, OPENWEATHER_SECRET_NAME points at the Secrets Manager entry and
    the value is fetched on each cold start. Locally, the key comes from .env
    so development needs no AWS access.
    """
    secret_name = os.environ.get("OPENWEATHER_SECRET_NAME")
    if secret_name:
        response = boto3.client("secretsmanager").get_secret_value(
            SecretId=secret_name
        )
        return json.loads(response["SecretString"])[OPENWEATHER_SECRET_KEY]

    try:
        return os.environ[OPENWEATHER_SECRET_KEY]
    except KeyError:
        raise RuntimeError(
            f"No credentials: set OPENWEATHER_SECRET_NAME (Lambda) or "
            f"{OPENWEATHER_SECRET_KEY} (local .env)."
        ) from None


def _get(path: str, api_key: str) -> dict:
    """GET an OpenWeather endpoint, keeping the key out of any error text.

    OpenWeather takes the key as the `appid` query parameter, so requests'
    exception messages embed it in the URL — and those messages land in
    CloudWatch. Errors are re-raised with the key redacted.
    """
    response = requests.get(
        f"{OPENWEATHER_BASE_URL}/{path}",
        params={"lat": QUITO_LAT, "lon": QUITO_LON, "appid": api_key, "units": "metric"},
        timeout=10,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        # `from None` matters: chaining would re-expose the unredacted original.
        raise requests.HTTPError(_redact(str(exc))) from None
    return response.json()


def _redact(message: str) -> str:
    return re.sub(r"(appid=)[^&\s]+", r"\1<redacted>", message)


def handler(event, context):
    """Lambda entry point."""
    api_key = get_api_key()
    # Resolved before any request goes out, so a bad STORAGE_BACKEND fails
    # without spending an API call.
    storage = get_storage()
    now = datetime.now(timezone.utc)

    current_raw = fetch_current(api_key)
    forecast_raw = fetch_forecast(api_key)

    df = pd.concat(
        [normalize_current(current_raw, now), normalize_forecast(forecast_raw, now)],
        ignore_index=True,
    )

    key = f"raw/{S3_WEATHER_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/weather.parquet"
    location = write_raw(df, key, storage)

    return {"statusCode": 200, "records_written": len(df), "s3_key": key, "location": location}


def fetch_current(api_key: str) -> dict:
    return _get("weather", api_key)


def fetch_forecast(api_key: str) -> dict:
    return _get("forecast", api_key)


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


def write_raw(df: pd.DataFrame, key: str, storage=None) -> str:
    """Write one Parquet file under `key` to the selected backend (S3 or the
    local folder — see ingestion.storage) and return where it landed."""
    return (storage or get_storage()).put_parquet(df, key)


if __name__ == "__main__":
    result = handler({}, None)
    print(result)
