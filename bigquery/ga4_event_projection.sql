-- OPTIONAL NATIVE SOURCE PROJECTION. Not executed by the credential-free tests.
-- @shard: one pre-approved YYYYMMDD shard, e.g. 20210101.
-- @arrival_batch: monotonically increasing source-admission batch.
-- Output contract: ga4_landing.event_versions. Destination is never created here.
with bounded as (
  select e.*
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where _TABLE_SUFFIX = @shard
)
select
  to_hex(sha256(to_json_string(struct(
    event_date, user_pseudo_id, event_timestamp, event_name,
    event_bundle_sequence_id, event_server_timestamp_offset,
    ecommerce.transaction_id
  )))) as event_key,
  parse_date('%Y%m%d', event_date) as event_date,
  @arrival_batch as arrival_batch,
  1 as source_version,
  event_name,
  user_pseudo_id,
  cast((select ep.value.int_value from unnest(event_params) ep
        where ep.key='ga_session_id' limit 1) as string) as session_id,
  nullif(ecommerce.transaction_id, '') as transaction_id,
  cast(round(coalesce(ecommerce.purchase_revenue,0)*100) as int64) as revenue_cents
from bounded
