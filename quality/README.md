# Data quality gate

The Great Expectations layer described in [docs/PROJECT_PLAN.md](../docs/PROJECT_PLAN.md):
the check that catches bad data before it reaches the warehouse. It runs against
the committed samples under [`data/samples/`](../data/samples/), so it needs no
credentials and no network.

## Run it

From the repository root, inside the project's virtual environment:

```bash
python -m venv .venv                 # once
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt      # once
great_expectations checkpoint run raw_samples_checkpoint
```

The checkpoint name is **`raw_samples_checkpoint`**. It is defined in
[`checkpoints/raw_samples_checkpoint.yml`](checkpoints/raw_samples_checkpoint.yml)
and referenced by name in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml);
rename the two together. The command exits non-zero if any expectation in any
suite fails, which is what fails the CI job.

Run from the repository root: the datasources locate `data/samples/` relative to
the working directory (that is how GX 0.18 fluent datasources resolve paths, and
the only place in this setup where the working directory matters).

After a run, the HTML report is at `gx/uncommitted/data_docs/local_site/index.html`
(git-ignored; CI uploads it as an artifact when the gate fails).

## What runs

| Validation | Sample | Suite | Guards |
| ---------- | ------ | ----- | ------ |
| `network_routes` | `network/routes.parquet` | [`network_routes_suite`](expectations/network_routes_suite.json) | columns, null constraints, `route_type` vocabulary, **minimum route count** (a truncated Overpass response) |
| `network_stops` | `network/stops.parquet` | [`network_stops_suite`](expectations/network_stops_suite.json) | columns, null constraints, **coordinates inside `QUITO_BBOX`**, PTv2 tag vocabulary, minimum stop count |
| `network_route_stops` | `network/route_stops.parquet` | [`network_route_stops_suite`](expectations/network_route_stops_suite.json) | columns, null constraints, `(route_id, stop_sequence)` grain, member-role filter, **every row resolves to a known stop** |
| `weather` | `weather/weather.parquet` | [`weather_suite`](expectations/weather_suite.json) | columns, **non-null ISO-8601 timestamps**, minimum rows per ingestion window (1 current + 40 forecast), vocabularies, physical ranges |
| `traffic` | `traffic/corridor_travel_time_series.parquet` | [`traffic_suite`](expectations/traffic_suite.json) | columns, `(route_id, sampled_at)` grain, all 14 trunk corridors present, loader limits, **strictly positive duration**, plausibility ranges |

Every threshold that mirrors a constant in
[`ingestion/constants.py`](../ingestion/constants.py) is pinned to it by
[`tests/test_quality_suites.py`](../tests/test_quality_suites.py), which also
proves each suite fails on the corruption it exists to catch. Non-obvious
thresholds carry a `meta.notes` entry in the suite itself explaining where the
number comes from; read those before changing one.

### The network snapshot is three suites, not one

A suite binds to one batch, and `routes`, `stops` and `route_stops` are three
tables with different columns. One suite could not assert the presence of
`latitude` and of `route_type` at the same time without failing on whichever
table lacks it, so the network gate is three suites with a shared
`network_` prefix. All three run in the checkpoint.

### One cross-table check

`network_route_stops_suite` asserts that `route_stops` references exactly as many
distinct stops as the stops table holds. `parse_network` builds the stops table
from the ids referenced in `route_stops` and silently skips any node Overpass
failed to return, so this equality is precisely "every route_stops row resolves
to a known stop", and it is the one check that notices a response truncated
after the relations but before the nodes (Overpass emits them in that order).

The stops row count reaches that expectation through an evaluation-parameter URN
(`urn:great_expectations:validations:network_stops_suite:...`) stored by the
stops validation earlier in the same run. Two consequences:

- `network_stops` must precede `network_route_stops` in the checkpoint, and the
  checkpoint keeps `StoreEvaluationParametersAction` in its action list.
- Validating `network_route_stops_suite` on its own raises
  `EvaluationParameterError` rather than passing. That is deliberate: a missing
  dependency should be loud.

### Weather has no coordinates

The original description of `weather_suite` promised coordinate-range checks.
The dataset cannot support them: `weather_loader` queries one fixed point
(`QUITO_LAT`, `QUITO_LON`) and does not write it to the output, so the raw table
has no spatial columns at all (see "Weather has no spatial dimension" in
[`data/samples/README.md`](../data/samples/README.md)). The point is pinned in
`ingestion/constants.py` instead. If the loader ever records coordinates, add
the range checks here and they become meaningful.

### What this gate does not do

It validates one batch at a time, so it cannot compare a sample against a
corridor's own history. In particular a Mapbox **reroute** — the same fixed
waypoints returning a different `distance_meters` between calls, up to ~20 %
per corridor in the committed series — is a documented property of the source,
not an invalid row, and telling a 5 % reroute from a 50 % one needs the history
that lives in the warehouse. The traffic suite bounds distance, speed and
duration to catch the gross case (a response that routed somewhere else
entirely) and leaves the per-corridor comparison to the marts, as
[`docs/ANALYSIS_PLAN.md`](../docs/ANALYSIS_PLAN.md) specifies.

## Layout

```
gx/
└── great_expectations.yml      # the Data Context: datasources + where the stores live
quality/
├── expectations/               # one JSON suite per raw table (the expectations store)
├── checkpoints/
│   └── raw_samples_checkpoint.yml
└── README.md
```

The context has to live in `gx/` because that is the only directory name the
`great_expectations` CLI discovers by itself; its stores point at `quality/` so
the versioned artefacts stay where the project plan puts them. Validation
results and Data Docs go to `gx/uncommitted/`, which is git-ignored.

Each sample directory is its own datasource (`network_samples`,
`weather_samples`, `traffic_samples`), mirroring the raw-zone prefixes, so every
`batching_regex` is a bare file name and matches on any OS.

## Prove a suite can fail

The tests do this without touching the committed files. To see it through the
CLI, two scripts corrupt the samples and put them back:

```bash
python scripts/break_samples.py          # network, weather, traffic drills
great_expectations checkpoint run raw_samples_checkpoint   # exits 1
python scripts/restore_samples.py        # samples pristine again
```

The default drills are one edit per suite — a stop north of `QUITO_BBOX`, a
null weather `timestamp`, a zero `duration_seconds` — and each fails exactly
one expectation. `--list` shows the others (a truncated Overpass response,
truncated stop nodes, a corridor missing from the Mapbox sample); `--all`
applies every one.

`break_samples.py` copies each pristine file to `data/samples/.backup/`
(git-ignored) before editing it and never overwrites an existing backup, so
running it twice cannot lose the good copy. `restore_samples.py` copies the
backups back, verifies the bytes match, and removes the backup directory; with
no backup present it falls back to `git checkout -- data/samples/`.
`restore_samples.py --check` reports whether the samples are pristine without
changing anything. The samples are real captures and the traffic series cannot
be re-fetched; never commit a broken one.

## Adding a check

- **A new expectation on an existing table:** append it to the suite JSON. If
  its threshold mirrors a constant, add a line to
  `TestThresholdsTrackConstants`; if it guards a specific failure, add a test
  that corrupts a copy of the sample and asserts the failure.
- **A new table:** add an asset to the right datasource in
  `gx/great_expectations.yml`, a suite under `expectations/`, a validation in
  the checkpoint, and the pairing in `SUITE_SAMPLES` in the tests
  (`test_checkpoint_covers_every_suite_once` will insist).
- **Running the same suites in production:** point a second checkpoint at a
  `pandas_s3` datasource over the raw zone. The suites do not care where the
  batch came from.
