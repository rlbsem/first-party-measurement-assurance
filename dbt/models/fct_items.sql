select i.event_id, cast(i.item_index as integer) as item_index, i.item_id,
       cast(i.price_cents as bigint) as price_cents, cast(i.quantity as integer) as quantity,
       cast(i.price_cents as bigint) * cast(i.quantity as integer) as extended_cents
from {{ ref('event_items') }} i
join {{ ref('fct_events') }} e using (event_id)
