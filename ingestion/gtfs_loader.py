"""
GTFS Loader
Fetches the Quito GTFS static feed (routes.txt, stops.txt, trips.txt,
stop_times.txt) from the public endpoint, converts to Parquet, and writes
to s3://quito-transport-raw/gtfs/year=.../month=.../day=...
"""

import io
import zipfile
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

from ingestion.constants import GTFS_FILES, GTFS_URL, S3_BUCKET_NAME, S3_GTFS_PREFIX

load_dotenv()


def handler(event, context):
    """Lambda entry point."""
    now = datetime.now(timezone.utc)

    zip_bytes = fetch_gtfs()
    dfs = parse_gtfs(zip_bytes)

    date_prefix = f"raw/{S3_GTFS_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    written = write_to_s3(dfs, S3_BUCKET_NAME, date_prefix)

    return {
        "statusCode": 200,
        "files_written": len(written),
        "total_rows": sum(v["rows"] for v in written.values()),
        "prefix": date_prefix,
    }


def fetch_gtfs() -> bytes:
    response = requests.get(GTFS_URL, timeout=30)
    response.raise_for_status()
    return response.content


def parse_gtfs(zip_bytes: bytes) -> dict[str, pd.DataFrame]:
    dfs = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        available = zf.namelist()
        for filename in GTFS_FILES:
            if filename not in available:
                continue
            with zf.open(filename) as f:
                # dtype=str preserves IDs like "001" that would silently become 1
                dfs[filename.replace(".txt", "")] = pd.read_csv(f, dtype=str)
    return dfs


def write_to_s3(dfs: dict[str, pd.DataFrame], bucket: str, date_prefix: str) -> dict:
    s3 = boto3.client("s3")
    written = {}
    for name, df in dfs.items():
        key = f"{date_prefix}/{name}.parquet"
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        written[name] = {"key": key, "rows": len(df)}
    return written


if __name__ == "__main__":
    result = handler({}, None)
    print(result)
