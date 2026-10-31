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
, source_events as (
  select parse_date('%Y%m%d',event_date) as event_date, 1 as arrival_batch, 1 as source_version,
         event_name, nullif(trim(user_pseudo_id),'') as user_pseudo_id,
         nullif(trim(cast(source_session_id as string)),'') as session_id,
         nullif(trim(source_transaction_id),'') as transaction_id,
         case when event_name='purchase' then cast(round(revenue_usd*100) as int64) else 0 end as revenue_cents,
         items
  from cohort
), source_items as (
  select e.event_date,e.event_name,e.user_pseudo_id,e.session_id,e.transaction_id,e.revenue_cents,
         item_offset as item_index, i.item_id, cast(round(i.price_in_usd*100) as int64) as price_cents,
         i.quantity, cast(round(i.price_in_usd*100) as int64)*i.quantity as extended_cents, 1 as source_version
  from source_events e cross join unnest(e.items) i with offset as item_offset
), raw_purchases as (
  select *, (select count(*) from unnest(items)) as item_rows,
         (select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i) as source_item_value,
         count(*) over (partition by transaction_id) as tx_rows
  from source_events where event_name='purchase'
), source_purchases as (
  select event_date,arrival_batch,transaction_id,user_pseudo_id,session_id,revenue_cents,item_rows,
         coalesce(source_item_value,0) as item_value_cents,
         revenue_cents-coalesce(source_item_value,0) as revenue_variance_cents,
         case when transaction_id is null or tx_rows>1 then 1 else 0 end as transaction_id_finding,
         case when user_pseudo_id is null or session_id is null then 1 else 0 end as identity_finding,
         case when revenue_cents is null or source_item_value is null
                   or abs(revenue_cents-source_item_value)>1 then 1 else 0 end as revenue_finding
  from raw_purchases
), source_sessions as (
  select user_pseudo_id,session_id,min(event_date) as first_event_date,max(event_date) as last_event_date,
         count(*) as event_count,countif(event_name='purchase') as purchase_events
  from source_events where user_pseudo_id is not null and session_id is not null
  group by user_pseudo_id,session_id
), source_daily as (
  select event_date,count(*) as event_count,countif(event_name='purchase') as purchase_events,
         sum(if(event_name='purchase',array_length(items),0)) as purchase_item_rows,
         sum(if(event_name='purchase',coalesce(revenue_cents,0),0)) as reported_revenue_cents,
         sum(if(event_name='purchase',coalesce((select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i),0),0)) as item_value_cents,
         sum(case when event_name='purchase' and (transaction_id is null or transaction_id in
              (select transaction_id from source_events where event_name='purchase' group by transaction_id having count(*)>1)) then 1 else 0 end) as transaction_id_findings,
         sum(case when event_name='purchase' and (revenue_cents is null or array_length(items)=0
              or (select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i) is null
              or abs(revenue_cents-(select sum(cast(round(i.price_in_usd*100) as int64)*i.quantity) from unnest(items) i))>1) then 1 else 0 end) as revenue_findings,
         countif(user_pseudo_id is null or session_id is null) as identity_findings
  from source_events group by event_date
)
select 'events' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.arrival_batch, r.source_version, r.event_name, r.user_pseudo_id, r.session_id, r.transaction_id, r.revenue_cents)))) as row_hash from source_events r)
union all
select 'items' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.event_name, r.user_pseudo_id, r.session_id, r.transaction_id, r.revenue_cents, r.item_index, r.item_id, r.price_cents, r.quantity, r.extended_cents, r.source_version)))) as row_hash from source_items r)
union all
select 'purchases' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.arrival_batch, r.transaction_id, r.user_pseudo_id, r.session_id, r.revenue_cents, r.item_rows, r.item_value_cents, r.revenue_variance_cents, r.transaction_id_finding, r.identity_finding, r.revenue_finding)))) as row_hash from source_purchases r)
union all
select 'sessions' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.user_pseudo_id, r.session_id, r.first_event_date, r.last_event_date, r.event_count, r.purchase_events)))) as row_hash from source_sessions r)
union all
select 'daily' as model, count(*) as row_count,
  to_hex(sha256(coalesce(string_agg(row_hash, '\n' order by row_hash), ''))) as multiset_sha256
from (select to_hex(sha256(to_json_string(struct(r.event_date, r.event_count, r.purchase_events, r.purchase_item_rows, r.reported_revenue_cents, r.item_value_cents, r.transaction_id_findings, r.revenue_findings, r.identity_findings)))) as row_hash from source_daily r)
order by model
