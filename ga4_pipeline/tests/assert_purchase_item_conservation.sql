-- Item rows belong to a selected event revision and may not inflate purchases.
with purchase_rows as (
  select event_key from {{ ref('fct_ga4_events') }} where event_name='purchase'
), item_count as (
  select event_key, count(*) as items from {{ ref('fct_ga4_items') }} group by event_key
)
select q.event_key
from {{ ref('mart_ga4_purchase_quality') }} q
left join item_count i on q.event_key=i.event_key
left join purchase_rows p on q.event_key=p.event_key
where p.event_key is null or q.item_rows <> coalesce(i.items,0)
