"""Tests for traffic_loader.build_corridors — turning a network snapshot into
Mapbox waypoint lists. No network access; the snapshot frames are built here
in the shape network_loader writes them.
"""

import pandas as pd
import pytest

from ingestion.constants import CORRIDOR_MAX_MEAN_HOP_KM, CORRIDOR_MAX_SNAP_M
from ingestion.traffic_loader import build_corridors

LON = -78.50
STEP = 0.005  # ~556 m of latitude


def _routes(*names: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "route_id": [str(i + 1) for i in range(len(names))],
            "route_ref": ["X"] * len(names),
            "route_name": list(names),
        }
    )


def _stops(coords: dict[str, tuple[float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stop_id": list(coords),
            "longitude": [lon for lon, _ in coords.values()],
            "latitude": [lat for _, lat in coords.values()],
        }
    )


def _route_stops(route_id: str, rows: list[tuple], with_provenance: bool = True) -> pd.DataFrame:
    """rows: (stop_id, stop_sequence, sequence_source, snap_distance_m)."""
    df = pd.DataFrame(rows, columns=["stop_id", "stop_sequence", "sequence_source", "snap_distance_m"])
    df.insert(0, "route_id", route_id)
    if not with_provenance:
        df = df[["route_id", "stop_id", "stop_sequence"]]
    return df


# Five stops on the road, plus one misplaced stop 7 km away.
ON_ROAD = {f"s{i}": (LON, -0.20 - STEP * i) for i in range(5)}
FAR = {"bogus": (LON + 0.06, -0.20)}
STOPS = _stops({**ON_ROAD, **FAR})


def test_geometry_ordered_route_drops_off_road_stops_from_waypoints(caplog):
    rows = [("bogus", 0, "geometry", 6831.0)] + [
        (f"s{i}", i + 1, "geometry", 0.0) for i in range(5)
    ]
    with caplog.at_level("WARNING", logger="ingestion.traffic_loader"):
        corridors = build_corridors(_routes("Ecovia E3"), STOPS, _route_stops("1", rows))

    assert len(corridors) == 1
    corridor = corridors[0]
    assert corridor["waypoints"] == [ON_ROAD[f"s{i}"] for i in range(5)]
    assert corridor["stops_excluded"] == 1
    assert corridor["sequence_source"] == "geometry"
    assert corridor["mean_hop_km"] <= CORRIDOR_MAX_MEAN_HOP_KM
    assert "Ecovia E3: stop bogus is 6831 m from the road" in " | ".join(caplog.messages)


def test_same_route_would_fail_the_gate_if_the_stop_were_routed_through():
    # Without the exclusion the detour to the misplaced stop pushes the mean
    # hop past the gate — this is the zigzag the gate exists to catch.
    rows = [("s0", 0, "geometry", 0.0), ("bogus", 1, "geometry", 6831.0)] + [
        (f"s{i}", i + 1, "geometry", 0.0) for i in range(1, 5)
    ]
    rows_as_on_road = [(s, seq, src, 0.0) for s, seq, src, _ in rows]
    assert build_corridors(_routes("Ecovia E3"), STOPS, _route_stops("1", rows_as_on_road)) == []
    assert len(build_corridors(_routes("Ecovia E3"), STOPS, _route_stops("1", rows))) == 1


def test_member_ordered_route_keeps_every_stop_even_when_far_from_the_road():
    # A large snap distance on an osm_member route is a hole in the mapped
    # road, not a misplaced stop: the waypoint list must stay whole.
    far_but_right = {f"s{i}": (LON, -0.20 - STEP * i) for i in range(5)}
    rows = [(f"s{i}", i, "osm_member", 3000.0 if i in (2, 3) else 0.0) for i in range(5)]
    corridors = build_corridors(_routes("MetroBus X"), _stops(far_but_right), _route_stops("1", rows))

    assert corridors[0]["waypoints"] == list(far_but_right.values())
    assert corridors[0]["stops_excluded"] == 0
    assert corridors[0]["sequence_source"] == "osm_member"


def test_stop_exactly_at_the_threshold_is_kept():
    rows = [(f"s{i}", i, "geometry", float(CORRIDOR_MAX_SNAP_M) if i == 2 else 0.0) for i in range(5)]
    corridors = build_corridors(_routes("Ecovia E3"), STOPS, _route_stops("1", rows))
    assert corridors[0]["stops_excluded"] == 0
    assert len(corridors[0]["waypoints"]) == 5


def test_snapshots_without_provenance_columns_still_build():
    # Snapshots written before 2026-10 have neither sequence_source nor
    # snap_distance_m; they are treated as member order, nothing excluded.
    rows = [(f"s{i}", i, None, None) for i in range(5)]
    corridors = build_corridors(
        _routes("Trolebus C4"), STOPS, _route_stops("1", rows, with_provenance=False)
    )
    assert len(corridors) == 1
    assert corridors[0]["sequence_source"] == "osm_member"
    assert corridors[0]["stops_excluded"] == 0


def test_gate_still_rejects_a_scrambled_member_order():
    scrambled = ["s0", "s3", "s1", "s4", "s2"]
    rows = [(s, i, "osm_member", 0.0) for i, s in enumerate(scrambled)]
    stops = _stops({f"s{i}": (LON, -0.20 - 0.03 * i) for i in range(5)})  # ~3.3 km apart
    assert build_corridors(_routes("Ecovia E1"), stops, _route_stops("1", rows)) == []


def test_non_trunk_routes_and_single_stop_routes_are_skipped():
    routes = _routes("Cooperativa feeder", "Integración Ecovía")
    route_stops = pd.concat(
        [
            _route_stops("1", [(f"s{i}", i, "osm_member", 0.0) for i in range(5)]),
            _route_stops("2", [("s0", 0, "osm_member", 0.0)]),
        ]
    )
    assert build_corridors(routes, STOPS, route_stops) == []


def test_hop_limit_is_one_kilometre():
    # Pinned: the healthy corridors sit at 0.51-0.81 km, the scrambled ones
    # started at 1.18 km. Changing this silently changes what gets sampled.
    assert CORRIDOR_MAX_MEAN_HOP_KM == pytest.approx(1.0)
