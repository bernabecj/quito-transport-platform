-- Custom dbt test: fail if any stop falls outside Quito's bounding box.
-- Coordinates outside the box mean an Overpass extract picked up geometry from
-- outside the city, or lat/lon were swapped somewhere in the pipeline.
select stop_id
from {{ ref('dim_stops') }}
where latitude not between -0.45 and 0.05
   or longitude not between -78.65 and -78.30
