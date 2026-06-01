"""
Weather Loader
Calls the OpenWeather API (current + 5-day forecast) for Quito's bounding
box, normalises the response, and writes Parquet to
s3://quito-transport-raw/weather/year=.../month=.../day=...
"""
