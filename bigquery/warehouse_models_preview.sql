-- Read-only public GA4 sample; ONE SHARD and purchase-user cohort, not the full export.
-- USD values use the provider's *_in_usd fields. Unknowns remain NULL.
with raw_day as (
  select event_date, event_timestamp, event_name, user_pseudo_id,
         event_bundle_sequence_id, event_server_timestamp_offset,
         (select p.value.int_value from unnest(event_params) p where p.key='ga_session_id') as source_session_id,
         ecommerce.transaction_id as source_transaction_id,
         ecommerce.purchase_revenue_in_usd as revenue_usd, items
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX = '20210101'
),
cohort as (
  select * from raw_day
  where event_name='purchase'
     or user_pseudo_id in (select user_pseudo_id from raw_day where event_name='purchase')
)
, snapshot_payloads as (
  select c.*, to_hex(sha256(to_json_string(c))) as content_hash from cohort c
), snapshot_rows as (
  -- Preserve identical/repeated rows. These are snapshot-local keys, NOT canonical GA4 event IDs.
  select *, concat(content_hash, ':', cast(row_number() over (partition by content_hash order by event_timestamp) as string)) as event_key
  from snapshot_payloads
), landing_events as (
  select event_key, parse_date('%Y%m%d',event_date) as event_date, 1 as arrival_batch,
         1 as source_version, event_name, user_pseudo_id,
         cast(source_session_id as string) as session_id,
         nullif(source_transaction_id,'') as transaction_id,
         case when event_name='purchase' then cast(round(revenue_usd*100) as int64) else 0 end as revenue_cents
  from snapshot_rows
), landing_items as (
  select e.event_key, 1 as source_version, item_offset as item_index, i.item_id,
         cast(round(i.price_in_usd*100) as int64) as price_cents, i.quantity
  from snapshot_rows e cross join unnest(e.items) i with offset as item_offset
),
stg_event_versions as (
select
  cast(event_key as string) as event_key,
  cast(event_date as date) as event_date,
  cast(arrival_batch as INT64) as arrival_batch,
  cast(source_version as INT64) as source_version,
  cast(event_name as string) as event_name,
  nullif(trim(cast(user_pseudo_id as string)), '') as user_pseudo_id,
  nullif(trim(cast(session_id as string)), '') as session_id,
  nullif(trim(cast(transaction_id as string)), '') as transaction_id,
  cast(revenue_cents as INT64) as revenue_cents
from landing_events
),
stg_item_versions as (
select
  cast(event_key as string) as event_key,
  cast(source_version as INT64) as source_version,
  cast(item_index as INT64) as item_index,
  cast(item_id as string) as item_id,
  cast(price_cents as INT64) as price_cents,
  cast(quantity as INT64) as quantity
from landing_items
),
fct_ga4_events as (
-- Landing batches are strictly increasing; do not use event date as an
-- incremental cursor. Older event dates may arrive in a later batch.
with versions as (
  select *,
         row_number() over (
           partition by event_key order by source_version desc, arrival_batch desc
         ) as version_rank
  from stg_event_versions
  where arrival_batch <= 1
),
latest as (
  select event_key, event_date, arrival_batch, source_version, event_name,
         user_pseudo_id, session_id, transaction_id, revenue_cents
  from versions
  where version_rank = 1
)
select * from latest
),
fct_ga4_items as (
-- Rebuild the item projection from the selected event revision. This removes
-- old item rows when a corrected event replaces its previous item set.
select i.event_key, i.item_index, i.item_id, i.price_cents, i.quantity,
       i.price_cents * i.quantity as extended_cents, e.event_date,
       e.source_version
from stg_item_versions i
join fct_ga4_events e
  on i.event_key = e.event_key and i.source_version = e.source_version
),
mart_ga4_purchase_quality as (
with item_totals as (
  select event_key, count(*) as item_rows, sum(extended_cents) as item_value_cents
  from fct_ga4_items
  group by event_key
),
purchase_events as (
  select *,
         count(*) over (partition by transaction_id) as transaction_occurrences
  from fct_ga4_events
  where event_name = 'purchase'
)
select p.event_key, p.event_date, p.arrival_batch, p.transaction_id,
       p.user_pseudo_id, p.session_id, p.revenue_cents,
       coalesce(i.item_rows, 0) as item_rows,
       coalesce(i.item_value_cents, 0) as item_value_cents,
       p.revenue_cents - coalesce(i.item_value_cents, 0) as revenue_variance_cents,
       case when p.transaction_id is null or p.transaction_occurrences > 1
         then 1 else 0 end as transaction_id_finding,
       case when p.user_pseudo_id is null or p.session_id is null
         then 1 else 0 end as identity_finding,
       case when p.revenue_cents is null or i.item_value_cents is null
             or abs(p.revenue_cents - i.item_value_cents) > 1
         then 1 else 0 end as revenue_finding
from purchase_events p
left join item_totals i on p.event_key = i.event_key
),
mart_ga4_sessions as (
-- Session grain is pseudo-user plus session identifier, not session_id alone.
select user_pseudo_id, session_id,
       min(event_date) as first_event_date, max(event_date) as last_event_date,
       count(*) as event_count,
       sum(case when event_name = 'purchase' then 1 else 0 end) as purchase_events
from fct_ga4_events
where user_pseudo_id is not null and session_id is not null
group by user_pseudo_id, session_id
),
mart_ga4_daily as (
select e.event_date, count(*) as event_count,
       sum(case when e.event_name = 'purchase' then 1 else 0 end) as purchase_events,
       sum(coalesce(p.item_rows, 0)) as purchase_item_rows,
       sum(coalesce(p.revenue_cents, 0)) as reported_revenue_cents,
       sum(coalesce(p.item_value_cents, 0)) as item_value_cents,
       sum(coalesce(p.transaction_id_finding, 0)) as transaction_id_findings,
       sum(coalesce(p.revenue_finding, 0)) as revenue_findings,
       sum(case when e.user_pseudo_id is null or e.session_id is null then 1 else 0 end)
         as identity_findings
from fct_ga4_events e
left join mart_ga4_purchase_quality p on e.event_key = p.event_key
group by e.event_date
),
preview_items as (
 select e.event_date,e.event_name,e.user_pseudo_id,e.session_id,e.transaction_id,e.revenue_cents,
        i.item_index,i.item_id,i.price_cents,i.quantity,i.extended_cents,i.source_version
 from fct_ga4_items i join fct_ga4_events e on i.event_key=e.event_key
)
select 'events' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.arrival_batch, r.source_version, r.event_name, r.user_pseudo_id, r.session_id, r.transaction_id, r.revenue_cents)))) as row_hash from fct_ga4_events r)
union all
select 'items' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.event_name, r.user_pseudo_id, r.session_id, r.transaction_id, r.revenue_cents, r.item_index, r.item_id, r.price_cents, r.quantity, r.extended_cents, r.source_version)))) as row_hash from preview_items r)
union all
select 'purchases' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.arrival_batch, r.transaction_id, r.user_pseudo_id, r.session_id, r.revenue_cents, r.item_rows, r.item_value_cents, r.revenue_variance_cents, r.transaction_id_finding, r.identity_finding, r.revenue_finding)))) as row_hash from mart_ga4_purchase_quality r)
union all
select 'sessions' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.user_pseudo_id, r.session_id, r.first_event_date, r.last_event_date, r.event_count, r.purchase_events)))) as row_hash from mart_ga4_sessions r)
union all
select 'daily' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.event_count, r.purchase_events, r.purchase_item_rows, r.reported_revenue_cents, r.item_value_cents, r.transaction_id_findings, r.revenue_findings, r.identity_findings)))) as row_hash from mart_ga4_daily r)
order by model
