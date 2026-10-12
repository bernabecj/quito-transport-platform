"""End-to-end checks on the committed trunk-corridor fixture.

tests/test_geometry.py and tests/test_network_loader.py prove the mechanics on
hand-built payloads. These tests prove the outcome on Quito's real relations:
the four Ecovía corridors OSM lists out of order come back, nothing that was
healthy changes, and the misplaced stops are identified by id.

The fixture, data/samples/network/overpass_trunk_geom.json, can be in either
of two shapes (see scripts/check_corridors.py, "Two fixture shapes"). A raw
Overpass response is used as is. The overpass-turbo GeoJSON export committed
on 2026-10-12 is converted on the fly; on that shape way orientation is
arbitrary, so the tests that depend on direction are skipped until the raw
response is committed.

Numbers are pinned to the snapshot described in data/samples/README.md. If the
fixture is regenerated and OSM has changed underneath — a relation fixed
upstream, a stop moved — re-baseline deliberately, with the README.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from ingestion.constants import CORRIDOR_MAX_MEAN_HOP_KM, CORRIDOR_MAX_SNAP_M, TRUNK_ROUTE_PATTERN
from ingestion.geometry import mean_hop_km, polyline_length_km
from ingestion.network_loader import build_route_geometries, parse_network
from ingestion.traffic_loader import build_corridors
from scripts.check_corridors import payload_from_turbo_geojson

FIXTURE = Path(__file__).resolve().parents[1] / "data/samples/network/overpass_trunk_geom.json"

# The four relations the README documents as excluded, and why they were.
SCRAMBLED = {
    "12110323": "Ecovía E1 Las Universidades - T. Sur",
    "12110381": "Ecovía E1R Terminal El Recreo - T. Sur",
    "2025942": "Ecovía E3 Río Coca - Playón de la Marín",
    "12110437": "Ecovia E3 Playón de la Marín - Río Coca",
}
# One more the geometry improves although the gate let it through.
IMPROVED = {"12113852": "Trolebus C6 El Recreo => Quitumbe"}
# Stops that are members of an E3 relation but not on its road.
MISPLACED = {
    "2025942": {"2742764461", "2742764395"},  # Teatro México, Colegio Montúfar: south of Playón
    "12110437": {"2725901941"},  # San Cristóbal: an E4/E6 stop in the far south
}
# Relations whose mapped road has holes; their OSM order must be kept whole.
HOLED = {"85969": "MetroBus Ofelia - Marín", "2021069": "MetroBus Marín - Ofelia"}


@pytest.fixture(scope="module")
def payload() -> dict:
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if "elements" in raw:
        return raw
    return payload_from_turbo_geojson(raw, FIXTURE.parent)


@pytest.fixture(scope="module")
def is_raw_response(payload) -> bool:
    # Converted payloads use negative synthetic way ids.
    return all(e["id"] > 0 for e in payload["elements"] if e["type"] == "way")


@pytest.fixture(scope="module")
def network(payload) -> dict[str, pd.DataFrame]:
    return parse_network(payload, datetime(2026, 10, 12, tzinfo=timezone.utc))


@pytest.fixture(scope="module")
def corridors(network) -> dict[str, dict]:
    built = build_corridors(network["routes"], network["stops"], network["route_stops"])
    return {c["route_id"]: c for c in built}


def trunk_ids(network) -> list[str]:
    routes = network["routes"]
    trunk = routes[routes["route_name"].str.contains(TRUNK_ROUTE_PATTERN, case=False, regex=True)]
    counts = network["route_stops"]["route_id"].value_counts()
    return [r for r in trunk["route_id"] if counts.get(r, 0) >= 2]


def stops_of(network, route_id: str, order: str = "stop_sequence") -> pd.DataFrame:
    rs = network["route_stops"]
    return rs[rs["route_id"] == route_id].sort_values(order)


def points(network, frame: pd.DataFrame) -> list[tuple[float, float]]:
    coords = network["stops"].set_index("stop_id")
    return [(coords.loc[s, "longitude"], coords.loc[s, "latitude"]) for s in frame["stop_id"]]


# ── the outcome ──────────────────────────────────────────────────────────────


def test_fixture_holds_the_eighteen_trunk_corridors(network):
    assert len(trunk_ids(network)) == 18


def test_every_trunk_corridor_is_sampled(network, corridors):
    assert set(corridors) == set(trunk_ids(network))
    assert all(c["mean_hop_km"] <= CORRIDOR_MAX_MEAN_HOP_KM for c in corridors.values())


def test_recovered_corridors_are_ordered_from_geometry(network, corridors):
    for route_id, name in SCRAMBLED.items():
        rs = stops_of(network, route_id)
        assert rs["sequence_source"].unique().tolist() == ["geometry"], name
        # OSM's order really was scrambled, and the road order is BRT-like.
        assert mean_hop_km(points(network, stops_of(network, route_id, "member_index"))) > 1.0, name
        assert corridors[route_id]["mean_hop_km"] <= 0.7, name


def test_recovered_corridors_sit_in_the_healthy_spacing_band(corridors):
    hops = {route_id: corridors[route_id]["mean_hop_km"] for route_id in SCRAMBLED}
    assert all(0.5 <= hop <= 0.7 for hop in hops.values()), hops


def test_healthy_corridors_keep_their_osm_order(network, corridors):
    unchanged = set(trunk_ids(network)) - set(SCRAMBLED) - set(IMPROVED)
    assert len(unchanged) == 13
    for route_id in unchanged:
        rs = stops_of(network, route_id)
        assert rs["sequence_source"].unique().tolist() == ["osm_member"], route_id
        assert rs["member_index"].is_monotonic_increasing, route_id
        assert corridors[route_id]["stops_excluded"] == 0, route_id


def test_c6_el_recreo_to_quitumbe_is_improved_as_a_side_effect(network, corridors):
    (route_id,) = IMPROVED
    rs = stops_of(network, route_id)
    assert rs["sequence_source"].unique().tolist() == ["geometry"]
    before = mean_hop_km(points(network, stops_of(network, route_id, "member_index")))
    assert before == pytest.approx(0.78, abs=0.02)
    assert corridors[route_id]["mean_hop_km"] == pytest.approx(0.53, abs=0.02)
    assert corridors[route_id]["stops_excluded"] == 0


# ── the geometry is trustworthy where it is complete ────────────────────────


def test_projection_reproduces_osm_order_on_complete_healthy_routes(network, payload):
    """The strongest check on the method: where OSM's order is right and the
    road is fully mapped, ordering by distance along the road gives the same
    sequence (or exactly its reverse — direction is a separate question)."""
    geometries = build_route_geometries(payload)
    checked = 0
    for route_id in set(trunk_ids(network)) - set(SCRAMBLED) - set(IMPROVED) - set(HOLED):
        assert not geometries[route_id].unattached, route_id
        rs = stops_of(network, route_id, "member_index")
        by_road = rs.sort_values(["distance_along_m", "member_index"])["stop_id"].tolist()
        osm = rs["stop_id"].tolist()
        assert by_road in (osm, osm[::-1]), route_id
        checked += 1
    assert checked == 11


def test_on_road_stops_snap_within_tolerance(network):
    rs = network["route_stops"]
    misplaced = {s for stops in MISPLACED.values() for s in stops}
    on_geometry_routes = rs[(rs["sequence_source"] == "geometry") & ~rs["stop_id"].isin(misplaced)]
    assert on_geometry_routes["snap_distance_m"].max() < CORRIDOR_MAX_SNAP_M / 2


# ── misplaced stops ──────────────────────────────────────────────────────────


def test_misplaced_e3_stops_are_identified_by_id(network):
    rs = network["route_stops"]
    flagged = rs[(rs["sequence_source"] == "geometry") & (rs["snap_distance_m"] > CORRIDOR_MAX_SNAP_M)]
    assert {r: set(g["stop_id"]) for r, g in flagged.groupby("route_id")} == MISPLACED
    # They are kilometres off, not borderline.
    assert flagged["snap_distance_m"].min() > 1000


def test_misplaced_stops_stay_in_route_stops_but_not_in_waypoints(network, corridors):
    for route_id, stop_ids in MISPLACED.items():
        rs = stops_of(network, route_id)
        assert stop_ids <= set(rs["stop_id"])
        assert corridors[route_id]["stops_excluded"] == len(stop_ids)
        assert len(corridors[route_id]["waypoints"]) == len(rs) - len(stop_ids)


def test_e3_directions_become_mirror_corridors(corridors):
    # With the misplaced stops out, both E3 directions measure the same road.
    lengths = {
        route_id: polyline_length_km(corridors[route_id]["waypoints"])
        for route_id in ("2025942", "12110437")
    }
    assert lengths["2025942"] == pytest.approx(lengths["12110437"], abs=0.3)


# ── holes in the mapped road ─────────────────────────────────────────────────


def test_metrobus_keeps_osm_order_and_every_stop_despite_road_holes(network, corridors, payload):
    geometries = build_route_geometries(payload)
    for route_id, name in HOLED.items():
        assert geometries[route_id].unattached_length_km > 1.0, name
        rs = stops_of(network, route_id)
        assert rs["sequence_source"].unique().tolist() == ["osm_member"], name
        # Stops in the holes are far from the spine — and still waypoints.
        assert (rs["snap_distance_m"] > CORRIDOR_MAX_SNAP_M).sum() >= 6, name
        assert corridors[route_id]["stops_excluded"] == 0, name
        assert len(corridors[route_id]["waypoints"]) == len(rs), name


# ── direction: needs the raw response ───────────────────────────────────────


def test_geometry_ordered_routes_run_from_their_from_tag_to_their_to_tag(
    network, payload, is_raw_response
):
    if not is_raw_response:
        pytest.skip("overpass-turbo GeoJSON has arbitrary line orientation; needs the raw response")
    relations = {str(e["id"]): e.get("tags", {}) for e in payload["elements"] if e["type"] == "relation"}
    names = network["stops"].set_index("stop_id")["stop_name"]

    def alike(a: str, b: str) -> bool:
        a, b = a.casefold(), b.casefold()
        return a in b or b in a

    for route_id in list(SCRAMBLED) + list(IMPROVED):
        rs = stops_of(network, route_id)
        kept = rs[rs["snap_distance_m"] <= CORRIDOR_MAX_SNAP_M]
        first, last = names[kept["stop_id"].iloc[0]], names[kept["stop_id"].iloc[-1]]
        tags = relations[route_id]
        assert alike(first, tags["from"]) or not alike(first, tags["to"]), (route_id, first, tags)
        assert alike(last, tags["to"]) or not alike(last, tags["from"]), (route_id, last, tags)


def test_fixture_query_is_the_one_that_regenerates_it():
    # Whatever shape the fixture is in, the committed query is the contract.
    query = (FIXTURE.parent / "overpass_trunk_geom.overpassql").read_text(encoding="utf-8")
    assert re.search(r"^way\(r\.trunk\);$", query, re.MULTILINE)
    assert re.search(r"^out geom;$", query, re.MULTILINE)
