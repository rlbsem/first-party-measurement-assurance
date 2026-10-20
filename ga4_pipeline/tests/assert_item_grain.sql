select event_key, item_index
from {{ ref('fct_ga4_items') }}
group by event_key, item_index
having count(*) <> 1 or min(price_cents) < 0 or min(quantity) <= 0
