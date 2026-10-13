"""End-to-end runs of the three loader handlers without internet or AWS.

Every external call is replaced: OpenWeather, Overpass and Mapbox through a
fake `requests`, the clock through a frozen `datetime`, and storage through
STORAGE_BACKEND=local rooted at tmp_path. What remains is the real handler
code, from fetch to the file on disk — the same path a developer runs with
`python -m ingestion.<loader>` and STORAGE_BACKEND=local in .env.

The Overpass fake serves the committed trunk fixture, so the traffic run
samples the real corridors, read back from the network snapshot the network
run wrote a moment earlier.

The last tests run the same handlers with STORAGE_BACKEND unset — the Lambda
configuration — against botocore's Stubber, to pin down that the deployed
path still issues exactly the boto3 calls it always has.
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import pytest
import requests
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

import ingestion.network_loader as network_loader
import ingestion.storage as storage
import ingestion.traffic_loader as traffic_loader
import ingestion.weather_loader as weather_loader
from ingestion.constants import (
    MAPBOX_SECRET_KEY,
    OPENWEATHER_SECRET_KEY,
    S3_BUCKET_NAME,
    STORAGE_BACKEND_VAR,
    TRAFFIC_SAMPLING_ENABLED_VAR,
)
from scripts.check_corridors import payload_from_turbo_geojson

FIXTURE = Path(__file__).parent.parent / "data" / "samples" / "network" / "overpass_trunk_geom.json"

# A fixed instant with single-digit month, day and hour, so the zero-padding
# in the partition names is actually exercised.
FROZEN_NOW = datetime(2026, 3, 7, 9, 45, 12, tzinfo=timezone.utc)
DATE_PARTITION = "year=2026/month=03/day=07"
HOUR_PARTITION = f"{DATE_PARTITION}/hour=09"

WEATHER_KEY = f"raw/weather/{DATE_PARTITION}/weather.parquet"
NETWORK_PREFIX = f"raw/network/{DATE_PARTITION}"
TRAFFIC_KEY = f"raw/traffic/{HOUR_PARTITION}/corridor_travel_time.parquet"

FORECAST_ITEMS = 40
MAPBOX_DURATION_S = 1800.0
MAPBOX_DISTANCE_M = 12000.0


# ── external calls, faked ────────────────────────────────────────────────────


class FrozenDatetime(datetime):
    """`datetime` whose now() is fixed; everything else (fromtimestamp, ...)
    keeps working, which a plain stub would break."""

    @classmethod
    def now(cls, tz=None):
        return FROZEN_NOW if tz is None else FROZEN_NOW.astimezone(tz)


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


OPENWEATHER_ITEM = {
    "dt": 1772960000,
    "main": {"temp": 14.2, "feels_like": 13.5, "humidity": 77, "pressure": 1013},
    "weather": [{"main": "Clouds", "description": "broken clouds"}],
    "wind": {"speed": 2.1, "deg": 90},
    "clouds": {"all": 75},
    "visibility": 10000,
}


@pytest.fixture(scope="module")
def overpass_payload() -> dict:
    return payload_from_turbo_geojson(json.loads(FIXTURE.read_text(encoding="utf-8")), FIXTURE.parent)


@pytest.fixture
def api_calls(monkeypatch, overpass_payload) -> list[str]:
    """Fake OpenWeather, Overpass and Mapbox; returns the log of URLs called.

    All three loaders share the one `requests` module, so a single GET fake
    dispatches on the host.
    """
    calls: list[str] = []

    def fake_get(url, params=None, timeout=None, **_):
        calls.append(url)
        if url.startswith("https://api.openweathermap.org/"):
            assert params["appid"] == "test-openweather-key"
            if url.endswith("/weather"):
                return FakeResponse(OPENWEATHER_ITEM)
            return FakeResponse({"list": [OPENWEATHER_ITEM] * FORECAST_ITEMS})
        if url.startswith("https://api.mapbox.com/"):
            assert params["access_token"] == "test-mapbox-token"
            waypoints = url.rsplit("/", 1)[1].count(";") + 1
            return FakeResponse(
                {
                    "routes": [
                        {
                            "duration": MAPBOX_DURATION_S,
                            "distance": MAPBOX_DISTANCE_M,
                            "legs": [{"annotation": {"congestion": ["low", "heavy", "unknown"]}}]
                            * (waypoints - 1),
                        }
                    ]
                }
            )
        pytest.fail(f"unexpected GET {url}")

    def fake_post(url, data=None, headers=None, timeout=None, **_):
        calls.append(url)
        assert "[out:json]" in data["data"]
        return FakeResponse(overpass_payload)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "post", fake_post)
    return calls


@pytest.fixture
def frozen_clock(monkeypatch):
    for module in (weather_loader, network_loader, traffic_loader):
        monkeypatch.setattr(module, "datetime", FrozenDatetime)


@pytest.fixture
def credentials(monkeypatch):
    """Keys from the environment, as .env provides them; never Secrets Manager."""
    monkeypatch.delenv("OPENWEATHER_SECRET_NAME", raising=False)
    monkeypatch.delenv("MAPBOX_SECRET_NAME", raising=False)
    monkeypatch.setenv(OPENWEATHER_SECRET_KEY, "test-openweather-key")
    monkeypatch.setenv(MAPBOX_SECRET_KEY, "test-mapbox-token")
    monkeypatch.setenv(TRAFFIC_SAMPLING_ENABLED_VAR, "true")


@pytest.fixture
def local_root(monkeypatch, tmp_path, api_calls, frozen_clock, credentials) -> Path:
    """STORAGE_BACKEND=local with the raw zone rooted at tmp_path.

    Set explicitly rather than inherited from the developer's .env, so the
    suite behaves the same whatever that file says, and in CI where there is
    none.
    """
    monkeypatch.setenv(STORAGE_BACKEND_VAR, "local")
    monkeypatch.setattr(storage, "LOCAL_STORAGE_ROOT", tmp_path)
    return tmp_path


def _files_under(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


# ── local backend, handler by handler ────────────────────────────────────────


def test_weather_handler_writes_the_daily_file(local_root, api_calls):
    result = weather_loader.handler({}, None)

    assert _files_under(local_root) == [WEATHER_KEY]
    assert result == {
        "statusCode": 200,
        "records_written": 1 + FORECAST_ITEMS,
        "s3_key": WEATHER_KEY,
        "location": str(local_root / WEATHER_KEY),
    }
    df = pd.read_parquet(local_root / WEATHER_KEY)
    assert df["record_type"].value_counts().to_dict() == {"forecast": FORECAST_ITEMS, "current": 1}
    assert (df["fetched_at"] == FROZEN_NOW.isoformat()).all()
    assert len(api_calls) == 2


def test_network_handler_writes_the_three_datasets(local_root, api_calls):
    result = network_loader.handler({}, None)

    assert _files_under(local_root) == [
        f"{NETWORK_PREFIX}/route_stops.parquet",
        f"{NETWORK_PREFIX}/routes.parquet",
        f"{NETWORK_PREFIX}/stops.parquet",
    ]
    assert result["statusCode"] == 200
    assert result["files_written"] == 3
    assert result["prefix"] == NETWORK_PREFIX
    assert result["location"] == str(local_root / NETWORK_PREFIX)

    routes = pd.read_parquet(local_root / NETWORK_PREFIX / "routes.parquet")
    stops = pd.read_parquet(local_root / NETWORK_PREFIX / "stops.parquet")
    route_stops = pd.read_parquet(local_root / NETWORK_PREFIX / "route_stops.parquet")
    assert len(routes) > 0 and len(stops) > 0 and len(route_stops) > 0
    assert result["total_rows"] == len(routes) + len(stops) + len(route_stops)
    assert len(api_calls) == 1


def test_traffic_handler_reads_the_local_snapshot_and_writes_the_hourly_file(local_root, api_calls):
    network_loader.handler({}, None)
    api_calls.clear()

    result = traffic_loader.handler({}, None)

    assert TRAFFIC_KEY in _files_under(local_root)
    assert result["statusCode"] == 200
    assert result["s3_key"] == TRAFFIC_KEY
    assert result["location"] == str(local_root / TRAFFIC_KEY)

    df = pd.read_parquet(local_root / TRAFFIC_KEY)
    assert len(df) == result["corridors_sampled"] > 0
    assert len(api_calls) == len(df)  # one Mapbox request per corridor, nothing else
    assert (df["duration_seconds"] == MAPBOX_DURATION_S).all()
    assert (df["sampled_at"] == FROZEN_NOW.isoformat()).all()
    assert (df["local_hour"] == 4).all()  # 09:45 UTC is 04:45 in Quito


def test_traffic_handler_reads_the_newest_snapshot(local_root, api_calls):
    """Two dated snapshots on disk: the sample must be built from the later one."""
    old = pd.DataFrame({"route_id": [], "route_ref": [], "route_name": [], "fetched_at": []})
    older_prefix = "raw/network/year=2025/month=12/day=31"
    local = storage.LocalStorage(local_root)
    local.put_parquet(old, f"{older_prefix}/routes.parquet")
    local.put_parquet(old, f"{older_prefix}/stops.parquet")
    local.put_parquet(old, f"{older_prefix}/route_stops.parquet")
    network_loader.handler({}, None)

    routes, _, _ = traffic_loader.read_latest_network(local)
    assert len(routes) > 0  # the empty, older snapshot was not the one read


def test_traffic_handler_fails_clearly_without_a_snapshot(local_root, api_calls):
    with pytest.raises(RuntimeError, match="no network snapshot"):
        traffic_loader.handler({}, None)
    assert api_calls == []  # no Mapbox quota spent
    assert _files_under(local_root) == []


def test_traffic_handler_respects_the_sampling_switch(local_root, api_calls, monkeypatch):
    network_loader.handler({}, None)
    api_calls.clear()
    monkeypatch.setenv(TRAFFIC_SAMPLING_ENABLED_VAR, "false")

    result = traffic_loader.handler({}, None)

    assert result["skipped"] is True
    assert api_calls == []
    assert TRAFFIC_KEY not in _files_under(local_root)


@pytest.mark.parametrize(
    "handler", [weather_loader.handler, network_loader.handler, traffic_loader.handler]
)
def test_bad_backend_fails_before_any_request_is_made(local_root, api_calls, monkeypatch, handler):
    monkeypatch.setenv(STORAGE_BACKEND_VAR, "locall")
    with pytest.raises(ValueError, match="STORAGE_BACKEND="):
        handler({}, None)
    assert api_calls == []
    assert _files_under(local_root) == []


# ── S3 backend: the Lambda configuration, unchanged ──────────────────────────


@pytest.fixture
def stubbed_s3(monkeypatch, api_calls, frozen_clock, credentials, tmp_path):
    """STORAGE_BACKEND unset, boto3's S3 client replaced by a Stubber.

    Stubber fails on any call not declared, and on any declared call made
    with different parameters, so a passing test means the handler issued
    exactly these calls with exactly this bucket and these keys.
    """
    monkeypatch.delenv(STORAGE_BACKEND_VAR, raising=False)
    monkeypatch.setattr(storage, "LOCAL_STORAGE_ROOT", tmp_path)  # must stay untouched
    client = boto3.client(
        "s3", region_name="sa-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    monkeypatch.setattr(storage.boto3, "client", lambda *a, **k: client)
    with Stubber(client) as stub:
        yield stub
        stub.assert_no_pending_responses()
    assert _files_under(tmp_path) == []


def test_weather_handler_on_s3_puts_the_same_key_as_before(stubbed_s3):
    stubbed_s3.add_response(
        "put_object", {}, {"Bucket": S3_BUCKET_NAME, "Key": WEATHER_KEY, "Body": ANY}
    )
    result = weather_loader.handler({}, None)
    assert result["s3_key"] == WEATHER_KEY
    assert result["location"] == f"s3://{S3_BUCKET_NAME}/{WEATHER_KEY}"


def test_network_handler_on_s3_puts_the_three_keys_as_before(stubbed_s3):
    for name in ("routes", "stops", "route_stops"):
        stubbed_s3.add_response(
            "put_object",
            {},
            {"Bucket": S3_BUCKET_NAME, "Key": f"{NETWORK_PREFIX}/{name}.parquet", "Body": ANY},
        )
    result = network_loader.handler({}, None)
    assert result["prefix"] == NETWORK_PREFIX
    assert result["location"] == f"s3://{S3_BUCKET_NAME}/{NETWORK_PREFIX}"


def test_traffic_handler_on_s3_lists_reads_and_puts_as_before(stubbed_s3, overpass_payload):
    snapshot = network_loader.parse_network(overpass_payload, FROZEN_NOW)
    names = ("routes", "stops", "route_stops")
    stubbed_s3.add_response(
        "list_objects_v2",
        {"IsTruncated": False, "Contents": [{"Key": f"{NETWORK_PREFIX}/{n}.parquet"} for n in names]},
        {"Bucket": S3_BUCKET_NAME, "Prefix": "raw/network/"},
    )
    for name in names:
        buffer = io.BytesIO()
        snapshot[name].to_parquet(buffer)
        data = buffer.getvalue()
        stubbed_s3.add_response(
            "get_object",
            {"Body": StreamingBody(io.BytesIO(data), len(data))},
            {"Bucket": S3_BUCKET_NAME, "Key": f"{NETWORK_PREFIX}/{name}.parquet"},
        )
    stubbed_s3.add_response(
        "put_object", {}, {"Bucket": S3_BUCKET_NAME, "Key": TRAFFIC_KEY, "Body": ANY}
    )

    result = traffic_loader.handler({}, None)
    assert result["corridors_sampled"] > 0
    assert result["s3_key"] == TRAFFIC_KEY
    assert result["location"] == f"s3://{S3_BUCKET_NAME}/{TRAFFIC_KEY}"
