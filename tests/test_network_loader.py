"""Tests for ingestion/network_loader.py.

No network access: build_query() is checked as text, and parse_network() is
fed small in-memory payloads shaped like an Overpass response.
"""

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from ingestion.constants import (
    OSM_ROUTE_TYPES,
    OVERPASS_TIMEOUT,
    QUITO_BBOX,
    TRUNK_ROUTE_PATTERN,
)
from ingestion.network_loader import build_query, parse_network

FETCHED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


# ── build_query ──────────────────────────────────────────────────────────────


def _statements(query: str) -> list[str]:
    """Overpass statements in order, without the trailing semicolons."""
    return [s.strip().rstrip(";") for s in query.strip().split(";\n") if s.strip()]


def test_query_keeps_the_original_route_and_node_blocks():
    query = build_query()
    south, west, north, east = QUITO_BBOX

    assert query.startswith(f"[out:json][timeout:{OVERPASS_TIMEOUT}]")
    assert f"({south},{west},{north},{east})->.routes" in query
    assert f'["route"~"^({"|".join(OSM_ROUTE_TYPES)})$"]' in query
    assert ".routes out body" in query
    assert "node(r.routes)" in query


def test_query_requests_way_geometry_for_trunk_relations_only():
    statements = _statements(build_query())

    # Exactly one `out geom`, and it is the output of the trunk-way selection,
    # not of the relation or node blocks: geometry is asked for roads only.
    assert statements.count("out geom") == 1
    geom_at = statements.index("out geom")
    assert statements[geom_at - 1] == "way(r.trunk)"
    assert statements[geom_at - 2] == f'rel.routes["name"~"{TRUNK_ROUTE_PATTERN}",i]->.trunk'


def test_trunk_selection_uses_the_same_pattern_as_the_traffic_loader():
    # The traffic loader picks corridors with pandas str.contains(case=False).
    # The Overpass filter must select the same relations, so it carries the
    # same pattern and the case-insensitive flag.
    query = build_query()
    match = re.search(r'rel\.routes\["name"~"([^"]+)",i\]', query)
    assert match is not None
    assert match.group(1) == TRUNK_ROUTE_PATTERN

    for name in ("Trolebus C4", "Ecovía E1", "Ecovia E3", "MetroBus Ofelia", "metrobus x"):
        assert re.search(TRUNK_ROUTE_PATTERN, name, re.IGNORECASE)
    assert not re.search(TRUNK_ROUTE_PATTERN, "Cooperativa Quitumbe", re.IGNORECASE)


def test_production_query_does_not_narrow_relations_by_name():
    # Without a pattern, the relation block keeps every route type in the
    # bbox; the name filter only appears in the trunk-geometry block.
    relation_stmt = _statements(build_query())[1]
    assert relation_stmt.startswith('relation["type"="route"]')
    assert '["name"~' not in relation_stmt


def test_name_pattern_narrows_relation_selection_for_fixtures():
    # The fixture under data/samples/network is generated with the trunk
    # pattern, so it holds only trunk relations in the production shape.
    statements = _statements(build_query(name_pattern=TRUNK_ROUTE_PATTERN))
    relation_stmt = statements[1]
    assert f'["name"~"{TRUNK_ROUTE_PATTERN}",i]' in relation_stmt
    assert relation_stmt.endswith("->.routes")
    # Everything after the relation block is identical to the production query.
    assert statements[2:] == _statements(build_query())[2:]


def test_committed_fixture_query_matches_build_query():
    # The query file next to the fixture is what regenerates it. Comment lines
    # are documentation; the statements must be exactly build_query()'s.
    path = Path(__file__).resolve().parents[1] / "data/samples/network/overpass_trunk_geom.overpassql"
    statements = [
        line for line in path.read_text(encoding="utf-8").splitlines() if not line.startswith("//")
    ]
    assert statements == build_query(name_pattern=TRUNK_ROUTE_PATTERN).splitlines()


# ── parse_network ────────────────────────────────────────────────────────────

LON = -78.50
STEP = 0.005  # ~556 m of latitude between consecutive road nodes


def node_xy(node_id: int) -> tuple[float, float]:
    return (LON, -0.20 - STEP * node_id)


def _way(way_id: int, node_ids: list[int]) -> dict:
    return {
        "type": "way",
        "id": way_id,
        "nodes": node_ids,
        "geometry": [{"lon": node_xy(n)[0], "lat": node_xy(n)[1]} for n in node_ids],
    }


def _stop(node_id: int, lon: float, lat: float, name: str | None = None) -> dict:
    node = {"type": "node", "id": node_id, "lat": lat, "lon": lon}
    if name:
        node["tags"] = {"name": name}
    return node


def _relation(rel_id: int, members: list[tuple], name: str) -> dict:
    return {
        "type": "relation",
        "id": rel_id,
        "tags": {"name": name, "route": "bus"},
        "members": [{"type": t, "ref": ref, "role": role} for t, ref, role in members],
    }


# Road: nodes 0..8 south-bound along one meridian; ways 10, 11, 12.
ROAD_WAYS = [_way(10, [0, 1, 2, 3]), _way(11, [3, 4, 5, 6]), _way(12, [6, 7, 8])]
# Stops 100..104 sit on road nodes 0, 2, 4, 6, 8 — in that geographic order.
STOP_NODES = [_stop(100 + i, *node_xy(2 * i), name=f"S{i}") for i in range(5)]
IN_ORDER = [("node", 100 + i, "stop") for i in range(5)]
SCRAMBLED = [("node", n, "stop") for n in (100, 103, 101, 104, 102)]


def _payload(*relations: dict, with_ways: bool = True) -> dict:
    elements = list(relations) + STOP_NODES
    if with_ways:
        elements += ROAD_WAYS
    return {"elements": elements}


def _route_stops(payload: dict, route_id: str) -> pd.DataFrame:
    df = parse_network(payload, FETCHED_AT)["route_stops"]
    return df[df["route_id"] == route_id].sort_values("stop_sequence")


def test_route_stops_carry_provenance_columns():
    df = parse_network(_payload(_relation(1, IN_ORDER + [("way", 10, "")], "Ecovia E9")), FETCHED_AT)
    assert list(df["route_stops"].columns) == [
        "route_id",
        "stop_id",
        "stop_sequence",
        "member_role",
        "member_index",
        "sequence_source",
        "distance_along_m",
        "snap_distance_m",
        "fetched_at",
    ]
    assert df["route_stops"]["stop_sequence"].dtype.kind == "i"


def test_scrambled_member_list_is_reordered_along_the_road(caplog):
    rel = _relation(1, SCRAMBLED + [("way", w, "") for w in (10, 11, 12)], "Ecovia E9")
    with caplog.at_level("INFO", logger="ingestion.network_loader"):
        rs = _route_stops(_payload(rel), "1")

    assert rs["stop_id"].tolist() == ["100", "101", "102", "103", "104"]
    assert rs["stop_sequence"].tolist() == [0, 1, 2, 3, 4]
    assert rs["sequence_source"].unique().tolist() == ["geometry"]
    # OSM's own order is preserved alongside, so nothing is lost.
    assert rs["member_index"].tolist() == [0, 2, 4, 1, 3]
    # 0, 2, 4, 6, 8 nodes along at ~556 m per node.
    assert rs["distance_along_m"].tolist() == pytest.approx([0, 1112, 2224, 3336, 4448], abs=2)
    assert rs["snap_distance_m"].tolist() == pytest.approx([0, 0, 0, 0, 0], abs=0.5)
    assert "Ecovia E9: stop order taken from road geometry" in " | ".join(caplog.messages)


def test_member_order_is_kept_when_it_is_already_geographic():
    rel = _relation(1, IN_ORDER + [("way", w, "") for w in (10, 11, 12)], "Ecovia E9")
    rs = _route_stops(_payload(rel), "1")

    assert rs["stop_id"].tolist() == ["100", "101", "102", "103", "104"]
    assert rs["sequence_source"].unique().tolist() == ["osm_member"]
    # Diagnostics are still filled in, so a consumer can judge the fit.
    assert rs["distance_along_m"].notna().all()
    assert rs["snap_distance_m"].notna().all()


def test_member_order_is_kept_when_geometry_would_make_it_worse():
    # Only way 10 (nodes 0-3) has geometry: the road has a hole from node 3
    # on, so stops 102-104 all snap to node 3 and their order collapses. The
    # member list is already right and must win.
    rel = _relation(1, IN_ORDER + [("way", w, "") for w in (10, 11, 12)], "MetroBus X")
    payload = {"elements": [rel] + STOP_NODES + [ROAD_WAYS[0]]}
    rs = _route_stops(payload, "1")

    assert rs["sequence_source"].unique().tolist() == ["osm_member"]
    assert rs["stop_id"].tolist() == ["100", "101", "102", "103", "104"]
    assert rs["snap_distance_m"].tolist()[2:] == pytest.approx([556, 1668, 2780], abs=2)


def test_reversed_member_list_keeps_osm_order():
    # A list that is merely reversed is still geographic (same hops): OSM
    # wins the tie, and direction is left to whoever reads `member_index`.
    rel = _relation(1, IN_ORDER[::-1] + [("way", w, "") for w in (10, 11, 12)], "Ecovia E9")
    rs = _route_stops(_payload(rel), "1")
    assert rs["stop_id"].tolist() == ["104", "103", "102", "101", "100"]
    assert rs["sequence_source"].unique().tolist() == ["osm_member"]


def test_route_without_geometry_keeps_member_order_with_null_diagnostics():
    rel = _relation(2, SCRAMBLED + [("way", 10, "")], "Cooperativa feeder")
    rs = _route_stops(_payload(rel, with_ways=False), "2")

    assert rs["stop_id"].tolist() == ["100", "103", "101", "104", "102"]
    assert rs["sequence_source"].unique().tolist() == ["osm_member"]
    assert rs["distance_along_m"].isna().all()
    assert rs["snap_distance_m"].isna().all()


def test_other_datasets_are_unaffected_by_way_elements():
    rel = _relation(1, SCRAMBLED + [("way", w, "") for w in (10, 11, 12)], "Ecovia E9")
    without = parse_network(_payload(rel, with_ways=False), FETCHED_AT)
    with_ways = parse_network(_payload(rel), FETCHED_AT)

    pd.testing.assert_frame_equal(without["routes"], with_ways["routes"])
    pd.testing.assert_frame_equal(without["stops"], with_ways["stops"])
    # Same rows in route_stops, different order and provenance.
    assert set(without["route_stops"]["stop_id"]) == set(with_ways["route_stops"]["stop_id"])


def test_non_stop_node_members_are_still_ignored():
    members = IN_ORDER + [("node", 105, ""), ("node", 106, "platform_entry_only")]
    rel = _relation(1, members + [("way", w, "") for w in (10, 11, 12)], "Ecovia E9")
    payload = _payload(rel)
    payload["elements"] += [_stop(105, *node_xy(1)), _stop(106, *node_xy(8))]
    rs = _route_stops(payload, "1")

    assert "105" not in rs["stop_id"].tolist()
    assert rs["member_role"].tolist() == ["stop"] * 5 + ["platform_entry_only"]
