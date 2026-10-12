// Regenerates data/samples/network/overpass_trunk_geom.json.
// Produced by ingestion.network_loader.build_query(name_pattern=TRUNK_ROUTE_PATTERN):
// the production query, with the relation block narrowed to the trunk corridors
// so the fixture holds only them, in the same shape the Lambda receives.
//
//   curl -sS -H "User-Agent: quito-transport-platform/1.0" \
//        --data-urlencode "data@data/samples/network/overpass_trunk_geom.overpassql" \
//        https://overpass-api.de/api/interpreter \
//        > data/samples/network/overpass_trunk_geom.json
//
// tests/test_network_loader.py fails if this file and build_query() drift apart.
[out:json][timeout:90];
relation["type"="route"]["route"~"^(bus|trolleybus|subway|light_rail|share_taxi|tram)$"]["name"~"Trole|Ecov|MetroBus|Metrobus",i](-0.45,-78.65,0.05,-78.3)->.routes;
.routes out body;
node(r.routes);
out body;
rel.routes["name"~"Trole|Ecov|MetroBus|Metrobus",i]->.trunk;
way(r.trunk);
out geom;
