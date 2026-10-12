"""Tests for the Great Expectations layer under quality/.

Two things are checked, neither of which `great_expectations checkpoint run`
can check on its own:

1. The suites are JSON, so every threshold in them is a literal. Where a
   literal mirrors a constant in ingestion/constants.py (the Quito bounding
   box, the OSM route types, the Mapbox waypoint limit, ...) these tests fail
   if the two drift apart.

2. A suite that passes proves little unless it also fails on the data it
   exists to reject. Each suite is run against the committed sample, which
   must pass, and against an in-memory copy with one specific corruption —
   a stop outside the bounding box, a null timestamp, a non-positive duration,
   a truncated Overpass response — which must fail on exactly the expectation
   built for it. Nothing under data/samples/ is modified.

Validation happens in an ephemeral, in-memory Data Context so the tests leave
nothing behind under gx/uncommitted/ and do not depend on the working
directory.
"""

import json
import re
from pathlib import Path

import pandas as pd
import pytest
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

from ingestion.constants import (
    CORRIDOR_MAX_MEAN_HOP_KM,
    MAPBOX_MAX_WAYPOINTS,
    OSM_ROUTE_TYPES,
    QUITO_BBOX,
    TRUNK_ROUTE_PATTERN,
)
from ingestion.network_loader import STOP_ROLE_PREFIXES

ROOT = Path(__file__).resolve().parents[1]
SUITES_DIR = ROOT / "quality" / "expectations"
SAMPLES_DIR = ROOT / "data" / "samples"

# Which committed sample each suite validates — the same pairing as
# quality/checkpoints/raw_samples_checkpoint.yml.
SUITE_SAMPLES = {
    "network_routes_suite": SAMPLES_DIR / "network" / "routes.parquet",
    "network_stops_suite": SAMPLES_DIR / "network" / "stops.parquet",
    "network_route_stops_suite": SAMPLES_DIR / "network" / "route_stops.parquet",
    "weather_suite": SAMPLES_DIR / "weather" / "weather.parquet",
    "traffic_suite": SAMPLES_DIR / "traffic" / "corridor_travel_time_series.parquet",
}

# network_route_stops_suite reads the stops row count from the stops validation
# in the same checkpoint run. Outside a checkpoint the value has to be supplied.
STOPS_ROW_COUNT_URN = (
    "urn:great_expectations:validations:network_stops_suite:"
    "expect_table_row_count_to_be_between.result.observed_value"
)


# ── helpers ──────────────────────────────────────────────────────────────────


def load_suite(name: str) -> ExpectationSuite:
    with open(SUITES_DIR / f"{name}.json") as f:
        return ExpectationSuite(**json.load(f))


def load_sample(name: str) -> pd.DataFrame:
    return pd.read_parquet(SUITE_SAMPLES[name])


def expectation_kwargs(suite: ExpectationSuite, expectation_type: str, column: str | None = None) -> dict:
    """The kwargs of the single expectation matching type (and column)."""
    matches = [
        e.kwargs
        for e in suite.expectations
        if e.expectation_type == expectation_type and (column is None or e.kwargs.get("column") == column)
    ]
    assert len(matches) == 1, f"{suite.expectation_suite_name}: expected one {expectation_type}({column}), found {len(matches)}"
    return matches[0]


def validate(suite_name: str, df: pd.DataFrame, evaluation_parameters: dict | None = None):
    """Run one suite against a DataFrame in a throwaway in-memory context."""
    context = EphemeralDataContext(
        project_config=DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
    )
    asset = context.sources.add_pandas("in_memory").add_dataframe_asset(suite_name)
    validator = context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        expectation_suite=load_suite(suite_name),
    )
    if suite_name == "network_route_stops_suite" and evaluation_parameters is None:
        # The sample is self-consistent: every referenced stop is in stops.parquet.
        evaluation_parameters = {STOPS_ROW_COUNT_URN: len(load_sample("network_stops_suite"))}
    return validator.validate(evaluation_parameters=evaluation_parameters)


def failures(result) -> set[tuple[str, str | None]]:
    """(expectation_type, column) of every failed expectation."""
    return {
        (r.expectation_config.expectation_type, r.expectation_config.kwargs.get("column"))
        for r in result.results
        if not r.success
    }


# ── 1. thresholds track ingestion/constants.py ──────────────────────────────


class TestThresholdsTrackConstants:
    def test_stops_bbox_matches_quito_bbox(self):
        south, west, north, east = QUITO_BBOX
        suite = load_suite("network_stops_suite")
        lat = expectation_kwargs(suite, "expect_column_values_to_be_between", "latitude")
        lon = expectation_kwargs(suite, "expect_column_values_to_be_between", "longitude")
        assert (lat["min_value"], lat["max_value"]) == (south, north)
        assert (lon["min_value"], lon["max_value"]) == (west, east)

    def test_route_types_match_osm_route_types(self):
        suite = load_suite("network_routes_suite")
        kwargs = expectation_kwargs(suite, "expect_column_values_to_be_in_set", "route_type")
        assert set(kwargs["value_set"]) == set(OSM_ROUTE_TYPES)

    def test_member_role_regex_matches_stop_role_prefixes(self):
        suite = load_suite("network_route_stops_suite")
        kwargs = expectation_kwargs(suite, "expect_column_values_to_match_regex", "member_role")
        assert kwargs["regex"] == "^(" + "|".join(STOP_ROLE_PREFIXES) + ")"

    def test_trunk_route_regex_matches_trunk_route_pattern(self):
        suite = load_suite("traffic_suite")
        kwargs = expectation_kwargs(suite, "expect_column_values_to_match_regex", "route_name")
        assert kwargs["regex"] == TRUNK_ROUTE_PATTERN

    def test_waypoint_limit_matches_mapbox_max_waypoints(self):
        suite = load_suite("traffic_suite")
        kwargs = expectation_kwargs(suite, "expect_column_values_to_be_between", "waypoint_count")
        assert kwargs["max_value"] == MAPBOX_MAX_WAYPOINTS

    def test_hop_limit_matches_corridor_max_mean_hop_km(self):
        suite = load_suite("traffic_suite")
        kwargs = expectation_kwargs(suite, "expect_column_values_to_be_between", "mean_hop_km")
        assert kwargs["max_value"] == CORRIDOR_MAX_MEAN_HOP_KM


# ── 2. every suite passes on its sample and fails on what it guards ─────────


@pytest.mark.parametrize("suite_name", sorted(SUITE_SAMPLES))
def test_suite_passes_on_committed_sample(suite_name):
    result = validate(suite_name, load_sample(suite_name))
    assert result.success, failures(result)


@pytest.mark.parametrize("suite_name", sorted(SUITE_SAMPLES))
def test_suite_is_not_empty(suite_name):
    assert len(load_suite(suite_name).expectations) > 0


def test_stops_suite_fails_on_latitude_outside_bbox():
    df = load_sample("network_stops_suite")
    df.loc[0, "latitude"] = QUITO_BBOX[2] + 0.7  # north of the box
    result = validate("network_stops_suite", df)
    assert not result.success
    assert failures(result) == {("expect_column_values_to_be_between", "latitude")}


def test_stops_suite_fails_on_swapped_lat_lon():
    df = load_sample("network_stops_suite")
    df.loc[0, ["latitude", "longitude"]] = df.loc[0, ["longitude", "latitude"]].to_numpy()
    result = validate("network_stops_suite", df)
    assert failures(result) == {
        ("expect_column_values_to_be_between", "latitude"),
        ("expect_column_values_to_be_between", "longitude"),
    }


def test_weather_suite_fails_on_null_timestamp():
    df = load_sample("weather_suite")
    df.loc[5, "timestamp"] = None
    result = validate("weather_suite", df)
    assert not result.success
    assert failures(result) == {("expect_column_values_to_not_be_null", "timestamp")}


@pytest.mark.parametrize("duration", [0, -30])
def test_traffic_suite_fails_on_non_positive_duration(duration):
    df = load_sample("traffic_suite")
    df.loc[100, "duration_seconds"] = duration
    result = validate("traffic_suite", df)
    assert not result.success
    assert failures(result) == {("expect_column_values_to_be_between", "duration_seconds")}


def test_traffic_suite_fails_when_a_corridor_is_missing():
    """traffic_loader.sample_corridors skips a corridor whose Mapbox call failed
    and logs a warning; the short sample is written without any error."""
    df = load_sample("traffic_suite")
    df = df[df["route_id"] != df["route_id"].iloc[0]]
    result = validate("traffic_suite", df)
    assert failures(result) == {("expect_column_unique_value_count_to_be_between", "route_id")}


def test_routes_suite_fails_on_truncated_overpass_response():
    df = load_sample("network_routes_suite")
    result = validate("network_routes_suite", df.iloc[:350])
    assert failures(result) == {("expect_table_row_count_to_be_between", None)}


def test_route_stops_suite_fails_when_stop_nodes_were_truncated():
    """Overpass emits relations before nodes, so a response cut off mid-stream
    leaves route_stops referencing stops that never arrived. parse_network
    drops those silently, so the stops table simply comes up short."""
    df = load_sample("network_route_stops_suite")
    stops_that_arrived = len(load_sample("network_stops_suite")) - 5
    result = validate(
        "network_route_stops_suite", df, evaluation_parameters={STOPS_ROW_COUNT_URN: stops_that_arrived}
    )
    assert failures(result) == {("expect_column_unique_value_count_to_be_between", "stop_id")}


def test_checkpoint_covers_every_suite_once():
    """The checkpoint is the only thing CI runs, so a suite it omits is a suite
    that never runs."""
    text = (ROOT / "quality" / "checkpoints" / "raw_samples_checkpoint.yml").read_text()
    referenced = re.findall(r"expectation_suite_name:\s*(\S+)", text)
    assert sorted(referenced) == sorted(SUITE_SAMPLES)
    # route_stops consumes the stops row count, so stops must be validated first.
    assert referenced.index("network_stops_suite") < referenced.index("network_route_stops_suite")


def test_ci_runs_the_documented_checkpoint():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "great_expectations checkpoint run raw_samples_checkpoint" in ci
    assert "trips_checkpoint" not in ci
