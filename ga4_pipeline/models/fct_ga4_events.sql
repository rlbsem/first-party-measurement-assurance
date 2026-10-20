{% if target.type == 'bigquery' %}
  {{ config(materialized='incremental', unique_key='event_key',
            incremental_strategy='merge', partition_by={'field':'event_date', 'data_type':'date'}) }}
{% else %}
  {{ config(materialized='incremental', unique_key='event_key', incremental_strategy='delete+insert') }}
{% endif %}

-- Landing batches are strictly increasing; do not use event date as an
-- incremental cursor. Older event dates may arrive in a later batch.
with versions as (
  select *,
         row_number() over (
           partition by event_key order by source_version desc, arrival_batch desc
         ) as version_rank
  from {{ ref('stg_event_versions') }}
  where arrival_batch <= {{ var('active_batch', 2) }}
),
latest as (
  select event_key, event_date, arrival_batch, source_version, event_name,
         user_pseudo_id, session_id, transaction_id, revenue_cents
  from versions
  where version_rank = 1
)
select * from latest
{% if is_incremental() %}
where event_key in (
  select distinct event_key from {{ ref('stg_event_versions') }}
  where arrival_batch > (select coalesce(max(arrival_batch), 0) from {{ this }})
    and arrival_batch <= {{ var('active_batch', 2) }}
)
{% endif %}
