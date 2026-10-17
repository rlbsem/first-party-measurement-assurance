select event_id, item_index from {{ ref('fct_items') }}
group by event_id, item_index having count(*) <> 1
