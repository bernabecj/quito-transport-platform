#!/usr/bin/env python3
"""
Does Mapbox actually have live traffic data for Quito?

This is a go/no-go check, not part of the pipeline. The congestion-delay idea
(peak hours, worst corridors) only works if Mapbox observes real traffic on
Quito's roads. Two things make that worth testing before writing any ingestion:

  1. Mapbox's `driving-traffic` profile SILENTLY falls back to free-flow
     `driving` results where it has no traffic coverage. It does not error.
     Months of collection could yield a flat, meaningless series.
  2. TomTom publishes no Ecuador coverage at all, so Mapbox is the only
     realistic candidate. If it fails here, the whole approach is dead and the
     project falls back to structural metrics from OpenStreetMap alone.

The decisive signal is the per-segment `congestion` annotation, which Mapbox
returns only for `driving-traffic`. Where it has no data it reports "unknown".
That verdict does not depend on the time of day, which matters: comparing trip
durations alone is inconclusive at 03:00, when real coverage and no coverage
both look like free flow.

Usage:
    # put MAPBOX_TOKEN=pk.xxxx in .env (gitignored), or export it
    python scripts/verify_mapbox_traffic.py

Makes 6 requests total — 3 corridors x 2 profiles — against a free tier of
100,000/month. Exit code 0 means coverage was confirmed, 1 means it was not.
"""

import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

# Read .env so the token never has to be pasted into a shell or a transcript.
load_dotenv()

MAPBOX_DIRECTIONS_URL = "https://api.mapbox.com/directions/v5/mapbox"
REQUEST_TIMEOUT = 30

QUITO_TZ = timezone(timedelta(hours=-5))
PEAK_HOURS = range(6, 10), range(16, 20)

# Endpoints of real trunk BRT corridors, taken from the OpenStreetMap relations
# this project already ingests (ingestion/network_loader.py). Mapbox wants
# lon,lat order.
CORRIDORS = [
    ("Trolebús C4  Quitumbe → La Colón", (-78.55579, -0.29551), (-78.49584, -0.19777)),
    ("Trolebús C4  La Colón → Quitumbe", (-78.49626, -0.19879), (-78.55579, -0.29551)),
    ("Ecovía E1    T. Sur → Universidades", (-78.54911, -0.33878), (-78.49040, -0.20824)),
]


def request_route(profile: str, start, end, token: str, annotations: str = "") -> dict:
    coords = f"{start[0]},{start[1]};{end[0]},{end[1]}"
    params = {"access_token": token, "overview": "full", "geometries": "geojson"}
    if annotations:
        params["annotations"] = annotations
    response = requests.get(
        f"{MAPBOX_DIRECTIONS_URL}/{profile}/{coords}",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        # Mapbox puts the token in the query string, so keep the URL out of errors.
        raise RuntimeError(
            f"{profile}: HTTP {response.status_code} — {response.json().get('message', '')}"
        )
    payload = response.json()
    if not payload.get("routes"):
        raise RuntimeError(f"{profile}: no route found between the given points")
    return payload["routes"][0]


def check_corridor(name, start, end, token: str) -> bool:
    print(f"\n{name}")
    print("-" * 68)

    try:
        traffic = request_route(
            "driving-traffic", start, end, token, annotations="congestion"
        )
        free_flow = request_route("driving", start, end, token)
    except RuntimeError as exc:
        print(f"  request failed: {exc}")
        return False

    congestion = Counter(traffic.get("legs", [{}])[0].get("annotation", {}).get("congestion", []))
    total = sum(congestion.values())
    known = total - congestion.get("unknown", 0)

    t_min, f_min = traffic["duration"] / 60, free_flow["duration"] / 60
    print(f"  driving-traffic : {t_min:6.1f} min")
    print(f"  driving         : {f_min:6.1f} min")
    if f_min:
        print(f"  difference      : {t_min - f_min:+6.1f} min ({(t_min / f_min - 1) * 100:+.1f}%)")

    if not total:
        print("  congestion      : no annotation returned")
        return False

    detail = ", ".join(f"{lvl}={n}" for lvl, n in congestion.most_common())
    print(f"  segments        : {total} ({detail})")
    print(f"  with real data  : {known}/{total} ({known / total * 100:.0f}%)")
    return known > 0


def main() -> int:
    token = os.environ.get("MAPBOX_TOKEN")
    if not token:
        print("MAPBOX_TOKEN is not set.\n")
        print("Create a free token at https://account.mapbox.com/access-tokens/")
        print("then add this line to .env (already gitignored):")
        print("    MAPBOX_TOKEN=pk.xxxx")
        return 1

    now = datetime.now(QUITO_TZ)
    in_peak = any(now.hour in window for window in PEAK_HOURS)
    print(f"Quito local time : {now:%Y-%m-%d %H:%M} ({'peak' if in_peak else 'off-peak'})")
    print("Testing whether Mapbox reports real traffic on Quito's BRT corridors.")

    results = [check_corridor(*corridor, token) for corridor in CORRIDORS]
    covered = sum(results)

    print("\n" + "=" * 68)
    if covered:
        print(f"VERDICT: traffic coverage CONFIRMED on {covered}/{len(results)} corridors.")
        print("Mapbox reports real congestion data for Quito, so measuring travel")
        print("time over the day will produce a genuine signal. The congestion-delay")
        print("approach — peak hours and worst corridors — is viable.")
        return 0

    print("VERDICT: NO traffic coverage detected.")
    print("Every segment came back 'unknown', meaning driving-traffic is falling")
    print("back to free-flow estimates. Collecting this over time would produce a")
    print("flat series that looks like data but measures nothing.")
    if not in_peak:
        print("\nOne caveat: this ran off-peak. The 'unknown' annotations are decisive")
        print("on their own, but re-running between 07:00-09:00 or 17:00-19:00 Quito")
        print("time removes all doubt before abandoning the approach.")
    print("\nFallback: derive structural metrics from OpenStreetMap alone — route")
    print("length, stop spacing, intersections crossed, corridor overlap. Weaker")
    print("than measured congestion, but certain and free of third parties.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
