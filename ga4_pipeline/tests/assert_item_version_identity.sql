-- Check the whole landed ledger, including superseded versions, before a join
-- can hide orphan rows. Identity is event + source version + item position.
with duplicate_positions as (
  select event_key, source_version, item_index
  from {{ ref('stg_item_versions') }}
  group by event_key, source_version, item_index
  having count(*) <> 1
), orphan_revisions as (
  select i.event_key, i.source_version, i.item_index
  from {{ ref('stg_item_versions') }} i
  left join {{ ref('stg_event_versions') }} e
    on i.event_key=e.event_key and i.source_version=e.source_version
  where e.event_key is null
)
select * from duplicate_positions
union all
select * from orphan_revisions
