-- Custom dbt test: fail if any trip has a negative delay.
-- A negative delay means the trip arrived before its scheduled time,
-- which indicates a data pipeline error rather than a real early arrival.
select trip_id
from {{ ref('fct_trips') }}
where delay_minutes < 0
