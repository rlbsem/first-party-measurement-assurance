-- OPTIONAL NATIVE SOURCE PROJECTION. Uses the same immutable event-key recipe.
-- @shard: one approved YYYYMMDD shard, @arrival_batch is recorded separately
-- in the matching event_versions materialization. No writes occur in this SQL.
with bounded as (
  select e.*
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where _TABLE_SUFFIX = @shard
)
select
  to_hex(sha256(to_json_string(struct(
    e.event_date, e.user_pseudo_id, e.event_timestamp, e.event_name,
    e.event_bundle_sequence_id, e.event_server_timestamp_offset,
    e.ecommerce.transaction_id
  )))) as event_key,
  1 as source_version,
  offset as item_index,
  i.item_id as item_id,
  cast(round(coalesce(i.price,0)*100) as int64) as price_cents,
  cast(coalesce(i.quantity,0) as int64) as quantity
from bounded e
cross join unnest(e.items) i with offset as offset
