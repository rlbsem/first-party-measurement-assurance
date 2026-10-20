-- Event facts must match the latest version in all admitted landing batches.
with ranked as (
  select event_key, source_version, arrival_batch, revenue_cents, event_date,
         row_number() over (
           partition by event_key order by source_version desc, arrival_batch desc
         ) as rn
  from {{ ref('stg_event_versions') }}
  where arrival_batch <= {{ var('active_batch', 2) }}
),
latest as (select * from ranked where rn=1)
select l.event_key
from latest l
left join {{ ref('fct_ga4_events') }} f on l.event_key=f.event_key
where f.event_key is null
   or f.source_version is distinct from l.source_version
   or f.arrival_batch is distinct from l.arrival_batch
   or f.revenue_cents is distinct from l.revenue_cents
   or f.event_date is distinct from l.event_date
