"""Tests for ingestion/storage.py.

No network and no AWS: LocalStorage runs against pytest's tmp_path, and
S3Storage runs against botocore's Stubber, which drives the real client code
with canned responses and fails on any call it was not told to expect.
"""

import io

import boto3
import pandas as pd
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

import ingestion.storage as storage
from ingestion.constants import (
    LOCAL_STORAGE_ROOT,
    S3_BUCKET_NAME,
    STORAGE_BACKEND_LOCAL,
    STORAGE_BACKEND_S3,
    STORAGE_BACKEND_VAR,
)
from ingestion.storage import LocalStorage, S3Storage, get_storage

# The three key shapes the loaders produce. Traffic is the only one with an
# hour layer.
WEATHER_KEY = "raw/weather/year=2026/month=03/day=07/weather.parquet"
NETWORK_PREFIX = "raw/network/year=2026/month=03/day=07"
TRAFFIC_KEY = "raw/traffic/year=2026/month=03/day=07/hour=09/corridor_travel_time.parquet"


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({"route_id": ["1", "2"], "duration_seconds": [1800.5, None]})


@pytest.fixture
def s3_client():
    """A real S3 client with fixed credentials, so Stubber can wrap it."""
    return boto3.client(
        "s3", region_name="sa-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    return buffer.getvalue()


# ── LocalStorage ─────────────────────────────────────────────────────────────


def test_local_round_trip(tmp_path, df):
    local = LocalStorage(tmp_path)
    local.put_parquet(df, WEATHER_KEY)
    pd.testing.assert_frame_equal(local.get_parquet(WEATHER_KEY), df)


def test_local_file_lands_at_root_slash_key(tmp_path, df):
    local = LocalStorage(tmp_path)
    location = local.put_parquet(df, TRAFFIC_KEY)

    expected = tmp_path / "raw/traffic/year=2026/month=03/day=07/hour=09/corridor_travel_time.parquet"
    assert location == str(expected)
    assert local.uri(TRAFFIC_KEY) == str(expected)
    assert expected.is_file()


def test_local_creates_partition_directories_and_nothing_else(tmp_path, df):
    local = LocalStorage(tmp_path)
    local.put_parquet(df, WEATHER_KEY)
    for name in ("routes", "stops", "route_stops"):
        local.put_parquet(df, f"{NETWORK_PREFIX}/{name}.parquet")
    local.put_parquet(df, TRAFFIC_KEY)

    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file())
    assert files == [
        f"{NETWORK_PREFIX}/route_stops.parquet",
        f"{NETWORK_PREFIX}/routes.parquet",
        f"{NETWORK_PREFIX}/stops.parquet",
        TRAFFIC_KEY,
        WEATHER_KEY,
    ]


def test_local_overwrites_like_s3(tmp_path, df):
    """Two runs in the same partition replace the file, as put_object would."""
    local = LocalStorage(tmp_path)
    local.put_parquet(df, WEATHER_KEY)
    local.put_parquet(df.iloc[:1], WEATHER_KEY)
    assert len(local.get_parquet(WEATHER_KEY)) == 1


def test_local_list_keys_are_relative_and_slash_separated(tmp_path, df):
    local = LocalStorage(tmp_path)
    local.put_parquet(df, WEATHER_KEY)
    local.put_parquet(df, TRAFFIC_KEY)

    assert local.list_keys("raw/weather/") == [WEATHER_KEY]
    assert local.list_keys("raw/") == [TRAFFIC_KEY, WEATHER_KEY]
    assert local.list_keys("raw/network/") == []


def test_local_list_keys_matches_prefix_as_a_string_like_s3(tmp_path, df):
    local = LocalStorage(tmp_path)
    local.put_parquet(df, WEATHER_KEY)
    assert local.list_keys("raw/wea") == [WEATHER_KEY]


def test_local_list_keys_on_missing_root_is_empty(tmp_path):
    assert LocalStorage(tmp_path / "never-written").list_keys("raw/") == []


def test_local_listing_supports_latest_snapshot_selection(tmp_path, df):
    """traffic_loader picks max(dirname) over the listing; the local listing
    must order dated prefixes the same way S3 keys do."""
    local = LocalStorage(tmp_path)
    for day in ("2026/month=03/day=07", "2026/month=03/day=08", "2025/month=12/day=31"):
        local.put_parquet(df, f"raw/network/year={day}/routes.parquet")

    latest = max(key.rsplit("/", 1)[0] for key in local.list_keys("raw/network/"))
    assert latest == "raw/network/year=2026/month=03/day=08"


def test_local_default_root_is_data_local_in_the_repo():
    assert LocalStorage().root == LOCAL_STORAGE_ROOT
    assert LOCAL_STORAGE_ROOT.parts[-2:] == ("data", "local")
    assert (LOCAL_STORAGE_ROOT.parent.parent / "ingestion" / "storage.py").is_file()


def test_local_default_root_is_read_when_constructed_not_when_imported(tmp_path, monkeypatch):
    """Tests redirect the root by patching the constant; that only works if
    the constructor looks it up at call time."""
    monkeypatch.setattr(storage, "LOCAL_STORAGE_ROOT", tmp_path)
    assert LocalStorage().root == tmp_path


# ── S3Storage ────────────────────────────────────────────────────────────────


def test_s3_put_sends_parquet_bytes_to_the_bucket(s3_client, df):
    s3 = S3Storage(client=s3_client)
    sent = {}

    def capture(Bucket, Key, Body):
        sent.update(Bucket=Bucket, Key=Key, Body=Body)
        return {}

    s3_client.put_object = capture
    location = s3.put_parquet(df, WEATHER_KEY)

    assert location == f"s3://{S3_BUCKET_NAME}/{WEATHER_KEY}"
    assert sent["Bucket"] == S3_BUCKET_NAME
    assert sent["Key"] == WEATHER_KEY
    pd.testing.assert_frame_equal(pd.read_parquet(io.BytesIO(sent["Body"])), df)


def test_s3_get_reads_the_object_body(s3_client, df):
    data = _parquet_bytes(df)
    with Stubber(s3_client) as stub:
        stub.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(data), len(data))},
            {"Bucket": S3_BUCKET_NAME, "Key": WEATHER_KEY},
        )
        pd.testing.assert_frame_equal(S3Storage(client=s3_client).get_parquet(WEATHER_KEY), df)


def test_s3_list_keys_follows_pagination(s3_client):
    """One list_objects_v2 call returns at most 1,000 keys; the newest network
    snapshot must still be found once the prefix holds more than that."""
    page_one = [
        {"Key": f"raw/network/year=2025/month=01/day=01/file{i:04d}.parquet"} for i in range(1000)
    ]
    newest = "raw/network/year=2026/month=03/day=07/routes.parquet"
    with Stubber(s3_client) as stub:
        stub.add_response(
            "list_objects_v2",
            {"IsTruncated": True, "NextContinuationToken": "next", "Contents": page_one},
            {"Bucket": S3_BUCKET_NAME, "Prefix": "raw/network/"},
        )
        stub.add_response(
            "list_objects_v2",
            {"IsTruncated": False, "Contents": [{"Key": newest}]},
            {"Bucket": S3_BUCKET_NAME, "Prefix": "raw/network/", "ContinuationToken": "next"},
        )
        keys = S3Storage(client=s3_client).list_keys("raw/network/")
        stub.assert_no_pending_responses()

    assert len(keys) == 1001
    assert max(key.rsplit("/", 1)[0] for key in keys) == "raw/network/year=2026/month=03/day=07"


def test_s3_list_keys_on_empty_prefix_is_empty(s3_client):
    # S3 omits "Contents" entirely when nothing matches.
    with Stubber(s3_client) as stub:
        stub.add_response(
            "list_objects_v2", {"IsTruncated": False}, {"Bucket": S3_BUCKET_NAME, "Prefix": "raw/x/"}
        )
        assert S3Storage(client=s3_client).list_keys("raw/x/") == []


def test_s3_client_is_not_created_until_used(monkeypatch):
    """Importing the loaders must not need credentials or a region."""
    monkeypatch.setattr(storage.boto3, "client", lambda *a, **k: pytest.fail("client created"))
    s3 = S3Storage()
    assert s3.bucket == S3_BUCKET_NAME
    assert s3.uri(WEATHER_KEY) == f"s3://{S3_BUCKET_NAME}/{WEATHER_KEY}"


# ── get_storage: the switch ──────────────────────────────────────────────────


@pytest.mark.parametrize("value", [None, "", STORAGE_BACKEND_S3, "S3"])
def test_unset_or_s3_selects_the_bucket(monkeypatch, value):
    """Unset is the Lambda case: lambda.tf sets no STORAGE_BACKEND."""
    if value is None:
        monkeypatch.delenv(STORAGE_BACKEND_VAR, raising=False)
    else:
        monkeypatch.setenv(STORAGE_BACKEND_VAR, value)
    selected = get_storage()
    assert isinstance(selected, S3Storage)
    assert selected.bucket == S3_BUCKET_NAME


@pytest.mark.parametrize("value", [STORAGE_BACKEND_LOCAL, "LOCAL", "  local\t"])
def test_local_selects_the_folder(monkeypatch, tmp_path, value):
    monkeypatch.setattr(storage, "LOCAL_STORAGE_ROOT", tmp_path)
    monkeypatch.setenv(STORAGE_BACKEND_VAR, value)
    selected = get_storage()
    assert isinstance(selected, LocalStorage)
    assert selected.root == tmp_path


@pytest.mark.parametrize("value", ["locall", "disk", "s3,local", "true"])
def test_anything_else_is_an_error_not_a_fallback(monkeypatch, value):
    monkeypatch.setenv(STORAGE_BACKEND_VAR, value)
    with pytest.raises(ValueError, match=f"{STORAGE_BACKEND_VAR}="):
        get_storage()
