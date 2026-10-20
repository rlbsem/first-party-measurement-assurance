-- Never select ambiguous versions or make a silent arbitrary winner.
select event_key, source_version, count(*) as rows_per_version
from {{ ref('stg_event_versions') }}
where arrival_batch <= {{ var('active_batch', 2) }}
group by event_key, source_version
having count(*) <> 1
