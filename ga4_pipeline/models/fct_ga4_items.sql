-- Rebuild the item projection from the selected event revision. This removes
-- old item rows when a corrected event replaces its previous item set.
select i.event_key, i.item_index, i.item_id, i.price_cents, i.quantity,
       i.price_cents * i.quantity as extended_cents, e.event_date,
       e.source_version
from {{ ref('stg_item_versions') }} i
join {{ ref('fct_ga4_events') }} e
  on i.event_key = e.event_key and i.source_version = e.source_version
